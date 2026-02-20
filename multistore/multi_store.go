// Package multistore provides specialized store implementations optimized
// for specific use cases.
//
// # MultiStore
//
// MultiStore is a store implementation that uses multiple allocators to optimize
// storage for different object sizes. It's particularly optimized for persistent
// treap nodes which have predictable size patterns.
//
// Features:
//   - Root allocator for large/variable-size objects
//   - Block allocators optimized for fixed-size treap nodes
//   - Automatic size-based allocation routing
//   - Reduced fragmentation for treap-heavy workloads
//
// Usage:
//
//	ms, err := multistore.NewMultiStore("data.db")
//	if err != nil {
//	    return err
//	}
//	defer ms.Close()
//
//	// Use ms as a regular store.Storer
//	objId, err := ms.NewObj(1024)
package multistore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator"
	"github.com/cbehopkins/bobbob/allocator/types"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/stringstore"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

// buildComprehensiveBlockSizes creates a complete set of block allocator sizes.
// It combines:
// 1. Treap node sizes (predictable fixed sizes for treap internal nodes)
// 2. Binary growth pattern from 32 to 4096 bytes (covers common allocation ranges)
//
// The binary growth pattern ensures efficient block allocation coverage across
// a wide range of object sizes without gaps that would cause allocation fallthrough
// to the parent allocator.
func buildComprehensiveBlockSizes() []int {
	// Binary growth pattern: 32, 64, 128, 256, 512, 1024, 2048, 4096
	binaryGrowth := []int{
		16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
	}

	// Add treap-specific sizes
	treapSizes := treap.PersistentTreapObjectSizes()

	// Combine and deduplicate
	sizeMap := make(map[int]bool)
	for _, size := range binaryGrowth {
		sizeMap[size] = true
	}
	for _, size := range treapSizes {
		sizeMap[size] = true
	}

	// Convert map to sorted slice
	sizes := make([]int, 0, len(sizeMap))
	for size := range sizeMap {
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)

	return sizes
}

const (
	// DefaultBlockCount is the default number of blocks to allocate per block size
	// in the OmniBlockAllocator. This affects memory overhead vs. allocation efficiency.
	DefaultBlockCount = 1024

	// DefaultDeleteQueueBufferSize is the buffer size for the asynchronous delete queue.
	// Larger values reduce contention but use more memory.
	DefaultDeleteQueueBufferSize = 1024
	stringStoreMetaVersion       = 1
	multiStoreStateVersion       = 1
)

var multiStoreStateMagic = [8]byte{'B', 'B', 'M', 'S', 'T', 'A', 'T', 'E'}

const (
	multiStoreStatePathMax     = 1024
	multiStoreStatePayloadSize = 1104
)

type persistedStringStoreMeta struct {
	Version              int             `json:"version"`
	WrapperStateObjId    bobbob.ObjectId `json:"wrapper_state_obj_id"`
	FilePath             string          `json:"file_path"`
	MaxNumberOfStrings   int             `json:"max_number_of_strings"`
	StartingObjectId     bobbob.ObjectId `json:"starting_object_id"`
	ObjectIdInterval     bobbob.ObjectId `json:"object_id_interval"`
	WriteFlushNanos      int64           `json:"write_flush_nanos"`
	WriteMaxBatchBytes   int             `json:"write_max_batch_bytes"`
	UnloadOnFull         bool            `json:"unload_on_full,omitempty"`
	UnloadAfterIdleNanos int64           `json:"unload_after_idle_nanos,omitempty"`
	UnloadScanNanos      int64           `json:"unload_scan_nanos,omitempty"`
	LazyLoadShards       bool            `json:"lazy_load_shards,omitempty"`
}

type persistedMultiStoreState struct {
	Version     int                       `json:"version"`
	StringStore *persistedStringStoreMeta `json:"string_store,omitempty"`
}

var ErrStoreAlreadyExists = errors.New("store file already exists and is non-empty; use LoadMultiStore")

type deleteQueue struct {
	mu       sync.Mutex // Protects isClosed
	isClosed bool       // Flag to prevent sends after close
	q        chan bobbob.ObjectId
	wg       sync.WaitGroup
	closed   sync.Once
	flushCh  chan chan struct{} // For flush synchronization
}

func newDeleteQueue(bufferSize int, delCallback func(store.ObjectId)) *deleteQueue {
	dq := &deleteQueue{
		q:       make(chan bobbob.ObjectId, bufferSize),
		flushCh: make(chan chan struct{}),
	}
	dq.wg.Add(1)
	go dq.worker(delCallback)
	return dq
}

func (dq *deleteQueue) worker(delCallback func(store.ObjectId)) {
	defer dq.wg.Done()
	for {
		select {
		case objId, ok := <-dq.q:
			if !ok {
				// Channel closed, exit
				return
			}
			delCallback(objId)
		case done := <-dq.flushCh:
			// Drain all pending items before signaling flush complete
			for {
				select {
				case objId, ok := <-dq.q:
					if !ok {
						// Channel closed during flush
						close(done)
						return
					}
					delCallback(objId)
				default:
					// Queue is empty, flush complete
					close(done)
					goto continueMainLoop
				}
			}
		continueMainLoop:
		}
	}
}

func (dq *deleteQueue) Enqueue(objId bobbob.ObjectId) {
	dq.mu.Lock()
	if dq.isClosed {
		dq.mu.Unlock()
		// Queue is closed; silently drop the deletion request
		// This is safe because we're in shutdown and the file is being closed anyway
		return
	}
	dq.mu.Unlock()

	// Send to queue; if it panics (channel closed), recover gracefully
	defer func() {
		if r := recover(); r != nil {
			// Channel was closed; safely ignore
			// (This shouldn't happen with the flag check, but safe guard anyway)
		}
	}()

	dq.q <- objId
}

func (dq *deleteQueue) Close() {
	dq.closed.Do(func() {
		dq.mu.Lock()
		dq.isClosed = true
		dq.mu.Unlock()

		close(dq.q)
		dq.wg.Wait()
	})
}

func (dq *deleteQueue) Flush() {
	dq.mu.Lock()
	if dq.isClosed {
		dq.mu.Unlock()
		return // Already closed, can't flush
	}
	dq.mu.Unlock()

	done := make(chan struct{})
	dq.flushCh <- done
	<-done
}

// multiStore implements a store with multiple allocators for different object sizes.
// It uses a root allocator and a block allocator optimized for persistent treap nodes.
type multiStore struct {
	filePath     string
	file         *os.File
	top          *allocator.Top          // FIXME: Can we not use parent and the Allocator interface instead of exposing the Top directly?
	tokenManager *store.DiskTokenManager // nil for unlimited, otherwise limits concurrent disk ops
	deleteQueue  *deleteQueue
	lock         sync.RWMutex
	// StringStore support (lazy-initialized)
	stringStore           *stringstore.StringStore
	stringStoreLock       sync.RWMutex
	stringStoreWrapperObj bobbob.ObjectId
	stateObjId            bobbob.ObjectId
}

func marshalMultiStoreState(state persistedMultiStoreState) ([]byte, error) {
	if state.Version == 0 {
		state.Version = multiStoreStateVersion
	}
	payload, err := encodeMultiStoreStateFixed(state)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(multiStoreStateMagic)+4+len(payload)))
	if _, err := buf.Write(multiStoreStateMagic[:]); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(payload))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalMultiStoreState(data []byte) (*persistedMultiStoreState, bool, error) {
	minLen := len(multiStoreStateMagic) + 4
	if len(data) < minLen {
		return nil, false, nil
	}
	if !bytes.Equal(data[:len(multiStoreStateMagic)], multiStoreStateMagic[:]) {
		return nil, false, nil
	}
	payloadLen := int(binary.LittleEndian.Uint32(data[len(multiStoreStateMagic):minLen]))
	if payloadLen < 0 || len(data) < minLen+payloadLen {
		return nil, true, fmt.Errorf("invalid multistore state payload length")
	}
	payload := data[minLen : minLen+payloadLen]
	if payloadLen == multiStoreStatePayloadSize {
		state, err := decodeMultiStoreStateFixed(payload)
		if err != nil {
			return nil, true, err
		}
		if state.Version != multiStoreStateVersion {
			return nil, true, fmt.Errorf("unsupported multistore state version: %d", state.Version)
		}
		return state, true, nil
	}

	// Legacy JSON payloads (variable length)
	var state persistedMultiStoreState
	if err := json.Unmarshal(payload, &state); err != nil {
		return nil, true, err
	}
	if state.Version != multiStoreStateVersion {
		return nil, true, fmt.Errorf("unsupported multistore state version: %d", state.Version)
	}
	return &state, true, nil
}

func encodeMultiStoreStateFixed(state persistedMultiStoreState) ([]byte, error) {
	payload := make([]byte, multiStoreStatePayloadSize)
	off := 0

	var encodeErr error
	putU32 := func(v uint32) {
		if encodeErr != nil {
			return
		}
		if off+4 > len(payload) {
			encodeErr = fmt.Errorf("multistore payload overflow")
			return
		}
		binary.LittleEndian.PutUint32(payload[off:], v)
		off += 4
	}
	putU64 := func(v uint64) {
		if encodeErr != nil {
			return
		}
		if off+8 > len(payload) {
			encodeErr = fmt.Errorf("multistore payload overflow")
			return
		}
		binary.LittleEndian.PutUint64(payload[off:], v)
		off += 8
	}
	putBool := func(v bool) {
		if encodeErr != nil {
			return
		}
		if off+1 > len(payload) {
			encodeErr = fmt.Errorf("multistore payload overflow")
			return
		}
		if v {
			payload[off] = 1
		} else {
			payload[off] = 0
		}
		off++
	}
	putPad := func(n int) {
		if encodeErr != nil {
			return
		}
		if off+n > len(payload) {
			encodeErr = fmt.Errorf("multistore payload overflow")
			return
		}
		for i := 0; i < n; i++ {
			payload[off+i] = 0
		}
		off += n
	}
	putFixedString := func(s string, max int) {
		if encodeErr != nil {
			return
		}
		if len(s) > max {
			encodeErr = fmt.Errorf("stringstore file path exceeds max length %d", max)
			return
		}
		if off+max > len(payload) {
			encodeErr = fmt.Errorf("multistore payload overflow")
			return
		}
		copy(payload[off:off+max], s)
		off += max
	}

	putU32(uint32(state.Version))
	putBool(state.StringStore != nil)
	putPad(3)

	var meta persistedStringStoreMeta
	if state.StringStore != nil {
		meta = *state.StringStore
	}

	putU32(uint32(meta.Version))
	putU64(uint64(meta.WrapperStateObjId))
	putFixedString(meta.FilePath, multiStoreStatePathMax)
	putU32(uint32(meta.MaxNumberOfStrings))
	putU64(uint64(meta.StartingObjectId))
	putU64(uint64(meta.ObjectIdInterval))
	putU64(uint64(meta.WriteFlushNanos))
	putU32(uint32(meta.WriteMaxBatchBytes))
	putBool(meta.UnloadOnFull)
	putPad(3)
	putU64(uint64(meta.UnloadAfterIdleNanos))
	putU64(uint64(meta.UnloadScanNanos))
	putBool(meta.LazyLoadShards)
	putPad(7)

	if off != len(payload) {
		return nil, fmt.Errorf("multistore payload size mismatch: %d", off)
	}
	return payload, nil
}

func decodeMultiStoreStateFixed(payload []byte) (*persistedMultiStoreState, error) {
	if len(payload) != multiStoreStatePayloadSize {
		return nil, fmt.Errorf("invalid multistore payload size")
	}
	off := 0

	readU32 := func() uint32 {
		v := binary.LittleEndian.Uint32(payload[off:])
		off += 4
		return v
	}
	readU64 := func() uint64 {
		v := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		return v
	}
	readBool := func() bool {
		v := payload[off] != 0
		off++
		return v
	}
	readPad := func(n int) {
		off += n
	}
	readFixedString := func(max int) string {
		buf := payload[off : off+max]
		off += max
		return string(bytes.TrimRight(buf, "\x00"))
	}

	state := persistedMultiStoreState{}
	state.Version = int(readU32())
	hasStringStore := readBool()
	readPad(3)

	meta := persistedStringStoreMeta{}
	meta.Version = int(readU32())
	meta.WrapperStateObjId = bobbob.ObjectId(readU64())
	meta.FilePath = readFixedString(multiStoreStatePathMax)
	meta.MaxNumberOfStrings = int(readU32())
	meta.StartingObjectId = bobbob.ObjectId(readU64())
	meta.ObjectIdInterval = bobbob.ObjectId(readU64())
	meta.WriteFlushNanos = int64(readU64())
	meta.WriteMaxBatchBytes = int(readU32())
	meta.UnloadOnFull = readBool()
	readPad(3)
	meta.UnloadAfterIdleNanos = int64(readU64())
	meta.UnloadScanNanos = int64(readU64())
	meta.LazyLoadShards = readBool()
	readPad(7)

	if hasStringStore {
		state.StringStore = &meta
	}
	return &state, nil
}

func (s *multiStore) loadPersistedState() (*persistedMultiStoreState, error) {
	meta := s.top.GetStoreMeta()
	if !store.IsValidObjectId(meta.ObjId) || meta.Sz <= 0 {
		return nil, nil
	}

	data, err := store.ReadBytesFromObj(s, meta.ObjId)
	if err != nil {
		// Legacy files may have non-state metadata in this slot; treat as absent.
		return nil, nil
	}

	state, matched, err := unmarshalMultiStoreState(data)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}

	s.stateObjId = meta.ObjId
	return state, nil
}

func (s *multiStore) persistState(state persistedMultiStoreState) error {
	data, err := marshalMultiStoreState(state)
	if err != nil {
		return err
	}

	if store.IsValidObjectId(s.stateObjId) {
		info, infoErr := s.getObjectInfo(s.stateObjId)
		if infoErr == nil && info.Size == len(data) {
			writer, finisher, writeErr := s.WriteToObj(s.stateObjId)
			if writeErr == nil {
				n, writeErr := writer.Write(data)
				if finisher != nil {
					finishErr := finisher()
					if writeErr == nil {
						writeErr = finishErr
					}
				}
				if writeErr == nil && n == len(data) {
					s.top.SetStoreMeta(allocator.FileInfo{
						ObjId: s.stateObjId,
						Fo:    info.Offset,
						Sz:    types.FileSize(info.Size),
					})
					return nil
				}
			}
		}
	}

	newObjId, err := store.WriteNewObjFromBytes(s, data)
	if err != nil {
		return err
	}
	info, err := s.getObjectInfo(newObjId)
	if err != nil {
		return err
	}

	oldObjId := s.stateObjId
	s.stateObjId = newObjId

	s.top.SetStoreMeta(allocator.FileInfo{
		ObjId: newObjId,
		Fo:    info.Offset,
		Sz:    types.FileSize(info.Size),
	})

	if store.IsValidObjectId(oldObjId) && oldObjId != newObjId {
		s.lock.Lock()
		_ = s.top.DeleteObj(oldObjId)
		s.lock.Unlock()
	}

	return nil
}

// NewMultiStore creates a new multiStore at the given file path.
// It initializes a root allocator and an omni block allocator optimized
// for the sizes used by persistent treap nodes.
// If maxDiskTokens > 0, limits concurrent disk I/O operations to that count.
// Pass 0 for unlimited concurrent disk operations.
func NewMultiStore(filePath string, maxDiskTokens int) (*multiStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() > 0 {
		_ = file.Close()
		return nil, ErrStoreAlreadyExists
	}

	topAlloc, err := allocator.NewTop(file, buildComprehensiveBlockSizes(), DefaultBlockCount)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	ms := &multiStore{
		filePath:              filePath,
		file:                  file,
		top:                   topAlloc,
		tokenManager:          store.NewDiskTokenManager(maxDiskTokens),
		stringStoreWrapperObj: bobbob.ObjNotAllocated,
		stateObjId:            bobbob.ObjNotAllocated,
	}
	ms.deleteQueue = newDeleteQueue(DefaultDeleteQueueBufferSize, ms.deleteObj)
	return ms, nil
}

// LoadMultiStore loads an existing multiStore from the given file path.
// It reads the metadata from the file and reconstructs the allocators.
// If maxDiskTokens > 0, limits concurrent disk I/O operations to that count.
func LoadMultiStore(filePath string, maxDiskTokens int) (*multiStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	topAlloc, err := allocator.NewTopFromFile(file, buildComprehensiveBlockSizes(), DefaultBlockCount)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	ms := &multiStore{
		filePath:              filePath,
		file:                  file,
		top:                   topAlloc,
		tokenManager:          store.NewDiskTokenManager(maxDiskTokens),
		stringStoreWrapperObj: bobbob.ObjNotAllocated,
		stateObjId:            bobbob.ObjNotAllocated,
	}
	ms.deleteQueue = newDeleteQueue(DefaultDeleteQueueBufferSize, ms.deleteObj)

	state, err := ms.loadPersistedState()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("load multistore metadata: %w", err)
	}
	if state != nil && state.StringStore != nil && store.IsValidObjectId(state.StringStore.WrapperStateObjId) {
		cfg := stringstore.Config{
			FilePath:           state.StringStore.FilePath,
			MaxNumberOfStrings: state.StringStore.MaxNumberOfStrings,
			StartingObjectId:   state.StringStore.StartingObjectId,
			ObjectIdInterval:   state.StringStore.ObjectIdInterval,
			WriteFlushInterval: time.Duration(state.StringStore.WriteFlushNanos),
			WriteMaxBatchBytes: state.StringStore.WriteMaxBatchBytes,
			UnloadOnFull:       state.StringStore.UnloadOnFull,
			UnloadAfterIdle:    time.Duration(state.StringStore.UnloadAfterIdleNanos),
			UnloadScanInterval: time.Duration(state.StringStore.UnloadScanNanos),
			LazyLoadShards:     state.StringStore.LazyLoadShards,
		}
		ss, loadErr := stringstore.LoadStringStoreFromStore(cfg, ms, state.StringStore.WrapperStateObjId)
		if loadErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("restore StringStore: %w", loadErr)
		}
		ss.AttachParentStore(ms)
		ms.stringStore = ss
		ms.stringStoreWrapperObj = state.StringStore.WrapperStateObjId
	}

	return ms, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// NewConcurrentMultiStore creates a multiStore wrapped with concurrent access support.
// This provides thread-safe access to a multi-allocator store with optional disk I/O limiting.
// If maxDiskTokens > 0, limits concurrent disk I/O operations to that count.
// Pass 0 for unlimited concurrent disk operations.
//
// This is a convenience constructor equivalent to:
//
//	ms, _ := NewMultiStore(filePath, 0)
//	cs := store.NewConcurrentStoreWrapping(ms, maxDiskTokens)
func NewConcurrentMultiStore(filePath string, maxDiskTokens int) (store.Storer, error) {
	ms, err := NewMultiStore(filePath, 0)
	if err != nil {
		return nil, err
	}
	return store.NewConcurrentStoreWrapping(ms, maxDiskTokens), nil
}

// LoadConcurrentMultiStore loads an existing multiStore and wraps it with concurrent access support.
// This provides thread-safe access to a persisted multi-allocator store with optional disk I/O limiting.
// If maxDiskTokens > 0, limits concurrent disk I/O operations to that count.
// Pass 0 for unlimited concurrent disk operations.
//
// This is a convenience constructor equivalent to:
//
//	ms, _ := LoadMultiStore(filePath, 0)
//	cs := store.NewConcurrentStoreWrapping(ms, maxDiskTokens)
func LoadConcurrentMultiStore(filePath string, maxDiskTokens int) (store.Storer, error) {
	ms, err := LoadMultiStore(filePath, 0)
	if err != nil {
		return nil, err
	}
	return store.NewConcurrentStoreWrapping(ms, maxDiskTokens), nil
}

// Allocator returns the primary allocator (OmniBlockAllocator) backing the multiStore.
// External callers can use this to attach allocation callbacks and, via Parent(),
// reach the root BasicAllocator for monitoring.
func (s *multiStore) Allocator() types.Allocator {
	if s.top == nil {
		return nil
	}
	return s.top
}

// AllocateRun allocates multiple contiguous ObjectIds efficiently.
func (s *multiStore) AllocateRun(size int, count int) ([]bobbob.ObjectId, []store.FileOffset, error) {
	if s.top == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	return s.top.AllocateRun(size, count)
}

// StringStorer interface implementation

// ensureStringStoreOpen lazily initializes the StringStore on first string allocation.
func (s *multiStore) ensureStringStoreOpen() error {
	s.stringStoreLock.Lock()
	defer s.stringStoreLock.Unlock()

	if s.stringStore != nil {
		return nil
	}

	// Derive temp file path from existing file path
	stringStorePath := s.filePath + ".strings"

	// Allocate ObjectId range from BasicAllocator for StringStore's first shard
	// The state object serves dual purpose: stores shard metadata + defines ObjectId namespace
	const stateSize = 1024 * 1024 // 1MB for state metadata (handles growth)
	s.lock.Lock()
	stateObjId, _, err := s.top.Allocate(stateSize)
	s.lock.Unlock()
	if err != nil {
		return fmt.Errorf("failed to allocate StringStore state object: %w", err)
	}

	// StringStore ObjectIds start after the state object, using the allocated space as namespace
	// This ensures no collision with other allocator-managed objects
	cfg := stringstore.Config{
		FilePath:           stringStorePath,
		MaxNumberOfStrings: 10000,                  // Tunable per deployment
		StartingObjectId:   stateObjId + 4,         // Offset by 4 to avoid state object itself
		ObjectIdInterval:   4,                      // Standard spacing
		WriteFlushInterval: 100 * time.Millisecond, // Background flush interval
		WriteMaxBatchBytes: 1024 * 1024,            // 1MB batch size
		UnloadOnFull:       true,
		LazyLoadShards:     true,
	}

	ss, err := stringstore.NewStringStore(cfg)
	if err != nil {
		return fmt.Errorf("failed to create StringStore: %w", err)
	}
	ss.AttachParentStore(s)

	s.stringStore = ss
	s.stringStoreWrapperObj = bobbob.ObjNotAllocated
	return nil
}

// NewStringObj stores a string and returns its ObjectId (StringStorer interface).
func (s *multiStore) NewStringObj(data string) (bobbob.ObjectId, error) {
	if err := s.ensureStringStoreOpen(); err != nil {
		return bobbob.ObjNotAllocated, err
	}

	s.stringStoreLock.RLock()
	defer s.stringStoreLock.RUnlock()

	// Allocate space and write string data
	objId, err := s.stringStore.NewObj(len(data))
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}

	writer, finisher, err := s.stringStore.WriteToObj(objId)
	if err != nil {
		_ = s.stringStore.DeleteObj(objId)
		return bobbob.ObjNotAllocated, err
	}

	_, err = writer.Write([]byte(data))
	if err != nil {
		if finisher != nil {
			_ = finisher()
		}
		_ = s.stringStore.DeleteObj(objId)
		return bobbob.ObjNotAllocated, err
	}

	if finisher != nil {
		if err := finisher(); err != nil {
			_ = s.stringStore.DeleteObj(objId)
			return bobbob.ObjNotAllocated, err
		}
	}

	return objId, nil
}

// StringFromObjId retrieves a string by ObjectId (StringStorer interface).
func (s *multiStore) StringFromObjId(objId bobbob.ObjectId) (string, error) {
	s.stringStoreLock.RLock()
	defer s.stringStoreLock.RUnlock()

	if s.stringStore == nil {
		return "", fmt.Errorf("string object %d: StringStore not initialized", objId)
	}

	reader, finisher, err := s.stringStore.LateReadObj(objId)
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(reader)
	if finisher != nil {
		finishErr := finisher()
		if err != nil {
			if finishErr != nil {
				return "", fmt.Errorf("read failed: %w; finisher failed: %v", err, finishErr)
			}
			return "", err
		}
		if finishErr != nil {
			return "", fmt.Errorf("read finisher failed: %w", finishErr)
		}
	}
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// HasStringObj checks if a given ObjectId is a string object (StringStorer interface).
func (s *multiStore) HasStringObj(objId bobbob.ObjectId) bool {
	s.stringStoreLock.RLock()
	defer s.stringStoreLock.RUnlock()

	if s.stringStore == nil {
		return false
	}

	// Check if object currently exists (not just if ID is in range)
	// Try to read it - if successful, it exists
	// FIXME there must be a more efficient way to do this....
	reader, finisher, err := s.stringStore.LateReadObj(objId)
	if err != nil {
		return false
	}
	_ = reader
	if finisher != nil {
		finisher()
	}
	return true
}

// All legacy metadata marshal/unmarshal helpers removed; allocator.Top handles persistence

// Close closes the multiStore and releases the file handle.
// Persistence is delegated to allocator.Top.Save, which writes allocator
// state and prime table metadata.
func (s *multiStore) Close() error {
	if s.deleteQueue != nil {
		s.deleteQueue.Close()
	}

	// Close StringStore if it exists
	var persistedState persistedMultiStoreState
	persistedState.Version = multiStoreStateVersion

	s.stringStoreLock.Lock()
	if s.stringStore != nil {
		cfg := s.stringStore.Config()
		wrapperObjId, persistErr := s.stringStore.PersistToStore(s, s.stringStoreWrapperObj)
		if persistErr != nil {
			s.stringStoreLock.Unlock()
			return fmt.Errorf("failed to persist StringStore: %w", persistErr)
		}
		s.stringStoreWrapperObj = wrapperObjId
		persistedState.StringStore = &persistedStringStoreMeta{
			Version:              stringStoreMetaVersion,
			WrapperStateObjId:    wrapperObjId,
			FilePath:             cfg.FilePath,
			MaxNumberOfStrings:   cfg.MaxNumberOfStrings,
			StartingObjectId:     cfg.StartingObjectId,
			ObjectIdInterval:     cfg.ObjectIdInterval,
			WriteFlushNanos:      int64(cfg.WriteFlushInterval),
			WriteMaxBatchBytes:   cfg.WriteMaxBatchBytes,
			UnloadOnFull:         cfg.UnloadOnFull,
			UnloadAfterIdleNanos: int64(cfg.UnloadAfterIdle),
			UnloadScanNanos:      int64(cfg.UnloadScanInterval),
			LazyLoadShards:       cfg.LazyLoadShards,
		}

		if err := s.stringStore.Close(); err != nil {
			s.stringStoreLock.Unlock()
			return fmt.Errorf("failed to close StringStore: %w", err)
		}
		s.stringStore = nil
	} else if store.IsValidObjectId(s.stringStoreWrapperObj) {
		persistedState.StringStore = &persistedStringStoreMeta{
			Version:           stringStoreMetaVersion,
			WrapperStateObjId: s.stringStoreWrapperObj,
		}
	}
	s.stringStoreLock.Unlock()

	if s.file == nil {
		return nil
	}

	if persistedState.StringStore != nil {
		if err := s.persistState(persistedState); err != nil {
			return fmt.Errorf("failed to persist multistore metadata: %w", err)
		}
	} else {
		// Preserve historical behavior for stores that never used StringStore.
		if s.top.GetStoreMeta().Sz == 0 {
			primeObjectId := bobbob.ObjectId(store.PrimeObjectStart())
			if info, err := s.getObjectInfo(primeObjectId); err == nil {
				s.top.SetStoreMeta(allocator.FileInfo{
					ObjId: primeObjectId,
					Fo:    info.Offset,
					Sz:    types.FileSize(info.Size),
				})
			} else {
				s.top.SetStoreMeta(allocator.FileInfo{
					ObjId: 0,
					Fo:    0,
					Sz:    1,
				})
			}
		}
	}

	// Always try to close resources, even if Save fails, to avoid leaking
	// open descriptors (Windows TempDir cleanup is strict about open handles).
	var firstErr error
	file := s.file

	s.lock.Lock()
	if err := s.top.Save(); err != nil {
		firstErr = err
	}
	s.lock.Unlock()

	// Close the allocator (closes file-based trackers) BEFORE closing the file
	if err := s.top.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	s.file = nil
	return firstErr
}

// All legacy marshal/write helpers removed; allocator.Top.Save handles persistence

// getObjectInfo retrieves object metadata using allocator.Top routing.
func (s *multiStore) getObjectInfo(objId bobbob.ObjectId) (store.ObjectInfo, error) {
	s.lock.RLock()
	offset, size, err := s.top.GetObjectInfo(objId)
	s.lock.RUnlock()
	if err != nil {
		return store.ObjectInfo{}, err
	}
	if size > types.FileSize(^uint(0)>>1) {
		return store.ObjectInfo{}, fmt.Errorf("object size %d exceeds int range", size)
	}

	return store.ObjectInfo{Offset: offset, Size: int(size)}, nil
}

// DeleteObj removes an object from the store and frees its space.
// It retrieves object metadata from the allocator and frees its space.
// For string objects, routes deletion to StringStore.
func (s *multiStore) DeleteObj(objId bobbob.ObjectId) error {
	if !store.IsValidObjectId(store.ObjectId(objId)) {
		return nil
	}

	// Check if this ObjectId belongs to StringStore's range and route accordingly
	// Use ownership check (not existence) since object may already be deleted
	s.stringStoreLock.RLock()
	if s.stringStore != nil && s.stringStore.OwnsObjectId(objId) {
		ss := s.stringStore
		s.stringStoreLock.RUnlock()
		if err := ss.DeleteObj(objId); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// No shard exists for this ObjectId; fall back to allocator delete.
			} else {
				return err
			}
		} else {
			return nil
		}
	} else {
		s.stringStoreLock.RUnlock()
	}

	// Otherwise route to allocator (existing behavior)
	s.deleteQueue.Enqueue(objId)
	return nil
}

// flushDeletes waits for all pending deletions to complete.
// This is primarily useful for testing.
func (s *multiStore) flushDeletes() {
	if s.deleteQueue != nil {
		s.deleteQueue.Flush()
	}
}
func (s *multiStore) deleteObj(objId bobbob.ObjectId) {
	s.lock.Lock()
	_ = s.top.DeleteObj(objId)
	s.lock.Unlock()
}

// PrimeObject returns a dedicated ObjectId for application metadata.
// For multiStore, this is the first object after the PrimeTable region at ObjectId 0.
// If it doesn't exist yet, it allocates it with the specified size.
// This provides a stable, known location for storing top-level metadata. This must
// be the first allocation in the file; MultiStore disables omni preallocation
// specifically so this ID is predictable and stable across reloads.
func (s *multiStore) PrimeObject(size int) (bobbob.ObjectId, error) {
	// Sanity check: prevent unreasonably large prime objects
	if size < 0 || size > store.MaxPrimeObjectSize {
		return bobbob.ObjectId(bobbob.ObjNotAllocated), errors.New("invalid prime object size")
	}

	// For multiStore, the prime object starts immediately after the PrimeTable
	primeObjectId := bobbob.ObjectId(store.PrimeObjectStart())

	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the prime object already exists using the allocator
	if offset, existingSize, err := s.top.GetObjectInfo(primeObjectId); err == nil {
		if int(existingSize) < size {
			return bobbob.ObjNotAllocated, fmt.Errorf("prime object size %d smaller than requested %d", existingSize, size)
		}
		// Object exists and is large enough
		s.top.SetStoreMeta(allocator.FileInfo{
			ObjId: primeObjectId,
			Fo:    offset,
			Sz:    existingSize,
		})
		return primeObjectId, nil
	}

	// Allocate the prime object - this should be the very first allocation
	objId, fileOffset, err := s.top.AllocateAtParent(size)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}

	// Verify we got the expected ObjectId (should be PrimeTable size for first allocation)
	if objId != primeObjectId {
		return bobbob.ObjNotAllocated, fmt.Errorf("expected prime object to be first allocation at offset %d, got %d", primeObjectId, objId)
	}

	// Initialize the object with zeros
	n, err := store.WriteZeros(s.file, fileOffset, size)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}
	if n != size {
		return bobbob.ObjNotAllocated, errors.New("failed to write all bytes for prime object")
	}

	s.top.SetStoreMeta(allocator.FileInfo{
		ObjId: primeObjectId,
		Fo:    fileOffset,
		Sz:    types.FileSize(size),
	})

	return primeObjectId, nil
}

// NewObj allocates a new object of the given size.
// It uses the block allocator to find space. The allocator records the allocation internally.
func (s *multiStore) NewObj(size int) (store.ObjectId, error) {
	s.lock.Lock()
	objId, _, err := s.top.Allocate(size)
	s.lock.Unlock()
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}

	return objId, nil
}

// LateReadObj returns a reader for the object with the given ID.
// Returns an error if the object is not found.
func (s *multiStore) LateReadObj(id store.ObjectId) (io.Reader, bobbob.Finisher, error) {
	return store.LateReadWithTokens(s.file, id, s, s.tokenManager)
}

// LateWriteNewObj allocates a new object and returns a writer for it.
// The object is allocated from the omni block allocator. The allocator records the allocation internally.
func (s *multiStore) LateWriteNewObj(size int) (store.ObjectId, io.Writer, bobbob.Finisher, error) {
	allocateFn := func(size int) (store.ObjectId, store.FileOffset, error) {
		s.lock.Lock()
		objId, fo, err := s.top.Allocate(size)
		s.lock.Unlock()
		return objId, fo, err
	}
	return store.LateWriteNewWithTokens(s.file, size, allocateFn, s.tokenManager)
}

// WriteToObj returns a writer for an existing object.
// Returns an error if the object is not found.
func (s *multiStore) WriteToObj(objectId store.ObjectId) (io.Writer, bobbob.Finisher, error) {
	return store.WriteToObjWithTokens(s.file, objectId, s, s.tokenManager)
}

// WriteBatchedObjs writes data to multiple consecutive objects in a single operation.
// This is a performance optimization for writing multiple small objects that are
// adjacent in the file, reducing system call overhead.
func (s *multiStore) WriteBatchedObjs(objIds []store.ObjectId, data []byte, sizes []int) error {
	if len(objIds) != len(sizes) {
		return io.ErrUnexpectedEOF
	}

	if len(objIds) == 0 {
		return nil
	}

	// Verify all objects exist and are consecutive
	var firstOffset store.FileOffset
	expectedOffset := store.FileOffset(0)

	for i, objId := range objIds {
		obj, err := s.getObjectInfo(objId)
		if err != nil {
			return io.ErrUnexpectedEOF
		}

		if i == 0 {
			firstOffset = obj.Offset
			expectedOffset = obj.Offset + store.FileOffset(obj.Size)
		} else {
			if obj.Offset != expectedOffset {
				return io.ErrUnexpectedEOF // Objects not consecutive
			}
			expectedOffset = obj.Offset + store.FileOffset(obj.Size)
		}
	}

	// Write all data in one operation
	n, err := store.WriteBatchedSections(s.file, firstOffset, [][]byte{data})
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	return nil
}

// GetObjectInfo returns the ObjectInfo for a given ObjectId.
// This is used internally for optimization decisions like batched writes.
func (s *multiStore) GetObjectInfo(objId store.ObjectId) (store.ObjectInfo, bool) {
	info, err := s.getObjectInfo(objId)
	if err != nil {
		return store.ObjectInfo{}, false
	}
	return info, true
}
