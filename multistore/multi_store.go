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
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator"
	"github.com/cbehopkins/bobbob/allocator/types"
	"github.com/cbehopkins/bobbob/store"
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
		32, 64, 128, 256, 512, 1024, 2048, 4096,
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
)

type deleteQueue struct {
	q       chan bobbob.ObjectId
	wg      sync.WaitGroup
	closed  sync.Once
	flushCh chan chan struct{} // For flush synchronization
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
	dq.q <- objId
}

func (dq *deleteQueue) Close() {
	dq.closed.Do(func() {
		close(dq.q)
		dq.wg.Wait()
	})
}

func (dq *deleteQueue) Flush() {
	done := make(chan struct{})
	dq.flushCh <- done
	<-done
}

// multiStore implements a store with multiple allocators for different object sizes.
// It uses a root allocator and a block allocator optimized for persistent treap nodes.
type multiStore struct {
	filePath     string
	file         *os.File
	top          *allocator.Top
	tokenManager *store.DiskTokenManager // nil for unlimited, otherwise limits concurrent disk ops
	deleteQueue  *deleteQueue
	lock         sync.RWMutex
}

// NewMultiStore creates a new multiStore at the given file path.
// It initializes a root allocator and an omni block allocator optimized
// for the sizes used by persistent treap nodes.
// If maxDiskTokens > 0, limits concurrent disk I/O operations to that count.
// Pass 0 for unlimited concurrent disk operations.
func NewMultiStore(filePath string, maxDiskTokens int) (*multiStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return nil, err
	}

	topAlloc, err := allocator.NewTop(file, buildComprehensiveBlockSizes(), DefaultBlockCount)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	ms := &multiStore{
		filePath:     filePath,
		file:         file,
		top:          topAlloc,
		tokenManager: store.NewDiskTokenManager(maxDiskTokens),
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
		filePath:     filePath,
		file:         file,
		top:          topAlloc,
		tokenManager: store.NewDiskTokenManager(maxDiskTokens),
	}
	ms.deleteQueue = newDeleteQueue(DefaultDeleteQueueBufferSize, ms.deleteObj)

	return ms, nil
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

// All legacy metadata marshal/unmarshal helpers removed; allocator.Top handles persistence

// Close closes the multiStore and releases the file handle.
// Persistence is delegated to allocator.Top.Save, which writes allocator
// state and prime table metadata.
func (s *multiStore) Close() error {
	if s.deleteQueue != nil {
		s.deleteQueue.Close()
	}
	if s.file == nil {
		return nil
	}

	// Ensure store metadata is set (required by Top.Save)
	// Use minimal metadata if no prime object exists
	if s.top.GetStoreMeta().Sz == 0 {
		primeObjectId := bobbob.ObjectId(store.PrimeObjectStart())
		if info, err := s.getObjectInfo(primeObjectId); err == nil {
			// Prime object exists, use it
			s.top.SetStoreMeta(allocator.FileInfo{
				ObjId: primeObjectId,
				Fo:    info.Offset,
				Sz:    types.FileSize(info.Size),
			})
		} else {
			// No prime object; use minimal metadata (1 byte is sufficient for Top.Save check)
			s.top.SetStoreMeta(allocator.FileInfo{
				ObjId: 0,
				Fo:    0,
				Sz:    1, // Non-zero to pass the Save() check
			})
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
func (s *multiStore) DeleteObj(objId bobbob.ObjectId) error {
	if !store.IsValidObjectId(store.ObjectId(objId)) {
		return nil
	}
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
func (s *multiStore) LateReadObj(id store.ObjectId) (io.Reader, store.Finisher, error) {
	return store.LateReadWithTokens(s.file, id, s, s.tokenManager)
}

// LateWriteNewObj allocates a new object and returns a writer for it.
// The object is allocated from the omni block allocator. The allocator records the allocation internally.
func (s *multiStore) LateWriteNewObj(size int) (store.ObjectId, io.Writer, store.Finisher, error) {
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
func (s *multiStore) WriteToObj(objectId store.ObjectId) (io.Writer, store.Finisher, error) {
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
