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
	"sync"

	"github.com/cbehopkins/bobbob/internal"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/store/allocator"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

const (
	// DefaultBlockCount is the default number of blocks to allocate per block size
	// in the OmniBlockAllocator. This affects memory overhead vs. allocation efficiency.
	DefaultBlockCount = 1024

	// DefaultDeleteQueueBufferSize is the buffer size for the asynchronous delete queue.
	// Larger values reduce contention but use more memory.
	DefaultDeleteQueueBufferSize = 1024
)

type deleteQueue struct {
	q       chan store.ObjectId
	wg      sync.WaitGroup
	closed  sync.Once
	flushCh chan chan struct{} // For flush synchronization
}

func newDeleteQueue(bufferSize int, delCallback func(store.ObjectId)) *deleteQueue {
	dq := &deleteQueue{
		q:       make(chan store.ObjectId, bufferSize),
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

func (dq *deleteQueue) Enqueue(objId store.ObjectId) {
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
	allocators   []allocator.Allocator
	tokenManager *store.DiskTokenManager // nil for unlimited, otherwise limits concurrent disk ops
	deleteQueue  *deleteQueue
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

	// Check if file is new (empty)
	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	rootAllocator, err := allocator.NewBasicAllocator(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// If new file, reserve first 8 bytes for metadata offset header
	if fileInfo.Size() == 0 {
		_, _, err = rootAllocator.Allocate(8)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	blockCount := DefaultBlockCount
	omniAllocator, err := allocator.NewOmniBlockAllocator(
		treap.PersistentTreapObjectSizes(),
		blockCount,
		rootAllocator,
		file,
		allocator.WithoutPreallocation(),
	)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	ms := &multiStore{
		filePath:     filePath,
		file:         file,
		allocators:   []allocator.Allocator{rootAllocator, omniAllocator},
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

	// Read metadata offset from header
	metadataOffset, err := readHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// Read and parse metadata
	omniData, rootData, err := readMetadata(file, metadataOffset)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// Restore allocators (allocations tracked internally by allocators)
	rootAllocator, omniAllocator, err := unmarshalComponents(rootData, omniData, file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	ms := &multiStore{
		filePath:     filePath,
		file:         file,
		allocators:   []allocator.Allocator{rootAllocator, omniAllocator},
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
func (s *multiStore) Allocator() allocator.Allocator {
	if len(s.allocators) > 1 {
		return s.allocators[1]
	}
	if len(s.allocators) == 1 {
		return s.allocators[0]
	}
	return nil
}

// readHeader reads the metadata offset from the first 8 bytes of the file.
func readHeader(file *os.File) (int64, error) {
	header := make([]byte, 8)
	_, err := file.ReadAt(header, 0)
	if err != nil {
		return 0, err
	}

	metadataOffset := int64(header[0])<<56 | int64(header[1])<<48 |
		int64(header[2])<<40 | int64(header[3])<<32 |
		int64(header[4])<<24 | int64(header[5])<<16 |
		int64(header[6])<<8 | int64(header[7])

	return metadataOffset, nil
}

// readMetadata reads and parses the metadata block from the file.
func readMetadata(file *os.File, metadataOffset int64) (omniData, rootData []byte, err error) {
	// Read length headers (8 bytes: 2 x 4-byte lengths)
	lengthHeader := make([]byte, 8)
	_, err = file.ReadAt(lengthHeader, metadataOffset)
	if err != nil {
		return nil, nil, err
	}

	offset := 0
	omniLen, offset := readInt32(lengthHeader, offset)
	rootLen, _ := readInt32(lengthHeader, offset)

	// Read all metadata
	totalLen := omniLen + rootLen
	metadata := make([]byte, totalLen)
	_, err = file.ReadAt(metadata, metadataOffset+8)
	if err != nil {
		return nil, nil, err
	}

	// Extract individual components
	omniData = metadata[0:omniLen]
	rootData = metadata[omniLen : omniLen+rootLen]

	return omniData, rootData, nil
}

// unmarshalComponents deserializes the allocators from their byte representations.
func unmarshalComponents(rootData, omniData []byte, file *os.File) (*allocator.BasicAllocator, allocator.Allocator, error) {
	type unmarshaler interface {
		Unmarshal([]byte) error
	}

	// Create and unmarshal rootAllocator
	rootAllocator := allocator.NewEmptyBasicAllocator()
	rootAllocator.SetFile(file)

	rootUnmarshaler, ok := any(rootAllocator).(unmarshaler)
	if !ok {
		return nil, nil, errors.New("rootAllocator does not support unmarshaling")
	}

	if err := rootUnmarshaler.Unmarshal(rootData); err != nil {
		return nil, nil, err
	}

	// Create and unmarshal omniAllocator
	blockCount := DefaultBlockCount
	omniAllocator, err := allocator.NewOmniBlockAllocator(
		treap.PersistentTreapObjectSizes(),
		blockCount,
		rootAllocator,
		file,
		allocator.WithoutPreallocation(),
	)
	if err != nil {
		return nil, nil, err
	}

	omniUnmarshaler, ok := any(omniAllocator).(unmarshaler)
	if !ok {
		return nil, nil, errors.New("omniAllocator does not support unmarshaling")
	}

	if err := omniUnmarshaler.Unmarshal(omniData); err != nil {
		return nil, nil, err
	}

	return rootAllocator, omniAllocator, nil
}

// readInt32 reads a 32-bit integer from the buffer at the given offset.
// Returns the value and the new offset after reading.
func readInt32(buf []byte, offset int) (int, int) {
	value := int(buf[offset])<<24 | int(buf[offset+1])<<16 |
		int(buf[offset+2])<<8 | int(buf[offset+3])
	return value, offset + 4
}

// Close closes the multiStore and releases the file handle.
// It marshals the allocators to the file for persistence.
// The layout is:
// - First 8 bytes: offset to metadata object (allocated by root allocator)
// - Metadata object contains:
//   - omniAllocator marshaled state
//   - rootAllocator marshaled state
func (s *multiStore) Close() error {
	if s.deleteQueue != nil {
		s.deleteQueue.Close()
	}
	if s.file == nil {
		return nil
	}

	// Marshal allocators only
	omniData, rootData, err := s.marshalComponents()
	if err != nil {
		return err
	}

	// Create combined metadata block
	metadata := s.buildMetadata(omniData, rootData)

	// Write metadata to file and update header
	if err := s.writeMetadataToFile(metadata); err != nil {
		return err
	}

	// Close file
	err = s.file.Close()
	if err != nil {
		return err
	}
	s.file = nil

	return nil
}

// marshalComponents marshals all store components (allocators only).
func (s *multiStore) marshalComponents() (omniData, rootData []byte, err error) {

	// Marshal omniAllocator

	if omniComplexMarshaler, ok := s.Allocator().(store.MarshalComplex); ok {
		// MarshalComplex is currently single-object: use MarshalMultiple() output directly.
		sizes, err := omniComplexMarshaler.PreMarshal()
		if err != nil {
			return nil, nil, err
		}
		// Expect a single payload size; fall back if empty.
		var objIds []allocator.ObjectId
		if len(sizes) > 0 {
			objIds = []allocator.ObjectId{allocator.ObjectId(1)}
		} else {
			objIds = []allocator.ObjectId{allocator.ObjectId(1)}
		}
		identityFn, byteFuncs, err := omniComplexMarshaler.MarshalMultiple(objIds)
		if err != nil {
			return nil, nil, err
		}
		_ = identityFn // identity unused in metadata storage
		if len(byteFuncs) == 0 {
			return nil, nil, errors.New("MarshalMultiple returned no payloads")
		}
		if len(byteFuncs) != 1 {
			return nil, nil, errors.New("MarshalMultiple expected single payload")
		}
		omniData, err = byteFuncs[0].ByteFunc()
		if err != nil {
			return nil, nil, err
		}
	} else if omniSimpleMarshaler, ok := s.Allocator().(store.MarshalSimple); ok {
		omniData, err = omniSimpleMarshaler.Marshal()
		if err != nil {
			return nil, nil, err
		}
	} else {
		return nil, nil, errors.New("omniAllocator does not support marshaling")
	}

	// Marshal rootAllocator
	rootSimpleMarshaler, ok := s.allocators[0].(store.MarshalSimple)
	if !ok {
		return nil, nil, errors.New("rootAllocator does not support marshaling")
	}
	rootData, err = rootSimpleMarshaler.Marshal()
	if err != nil {
		return nil, nil, err
	}

	return omniData, rootData, nil
}

// buildMetadata creates a combined metadata block with length prefixes.
// Format: [omniLen:4][rootLen:4][omniData][rootData]
func (s *multiStore) buildMetadata(omniData, rootData []byte) []byte {
	metadataSize := 8 + len(omniData) + len(rootData)
	metadata := make([]byte, metadataSize)

	offset := 0
	// Write omni length
	offset = writeInt32(metadata, offset, len(omniData))
	// Write root length
	offset = writeInt32(metadata, offset, len(rootData))

	// Copy data
	copy(metadata[offset:], omniData)
	offset += len(omniData)
	copy(metadata[offset:], rootData)

	return metadata
}

// writeMetadataToFile writes the metadata block to the file and updates the header.
func (s *multiStore) writeMetadataToFile(metadata []byte) error {
	// Allocate space for metadata using root allocator
	_, metadataOffset, err := s.allocators[0].Allocate(len(metadata))
	if err != nil {
		return err
	}

	// Write metadata to file
	_, err = s.file.WriteAt(metadata, int64(metadataOffset))
	if err != nil {
		return err
	}

	// Write metadata offset to first 8 bytes of file
	return s.writeHeader(metadataOffset)
}

// writeHeader writes the metadata offset to the first 8 bytes of the file.
func (s *multiStore) writeHeader(metadataOffset store.FileOffset) error {
	header := make([]byte, 8)
	header[0] = byte(metadataOffset >> 56)
	header[1] = byte(metadataOffset >> 48)
	header[2] = byte(metadataOffset >> 40)
	header[3] = byte(metadataOffset >> 32)
	header[4] = byte(metadataOffset >> 24)
	header[5] = byte(metadataOffset >> 16)
	header[6] = byte(metadataOffset >> 8)
	header[7] = byte(metadataOffset)

	_, err := s.file.WriteAt(header, 0)
	return err
}

// writeInt32 writes a 32-bit integer to the buffer at the given offset.
// Returns the new offset after writing.
func writeInt32(buf []byte, offset, value int) int {
	buf[offset] = byte(value >> 24)
	buf[offset+1] = byte(value >> 16)
	buf[offset+2] = byte(value >> 8)
	buf[offset+3] = byte(value)
	return offset + 4
}

// getObjectInfo retrieves object metadata using the allocator hierarchy.
// It queries the OmniBlockAllocator (which will check BlockAllocators and fall back to BasicAllocator).
func (s *multiStore) getObjectInfo(objId store.ObjectId) (store.ObjectInfo, error) {
	type objectInfoGetter interface {
		GetObjectInfo(store.ObjectId) (store.FileOffset, int, error)
	}

	allocator, ok := s.allocators[1].(objectInfoGetter)
	if !ok {
		return store.ObjectInfo{}, errors.New("allocator does not support GetObjectInfo")
	}

	offset, size, err := allocator.GetObjectInfo(objId)
	if err != nil {
		return store.ObjectInfo{}, err
	}

	return store.ObjectInfo{Offset: offset, Size: size}, nil
}

// DeleteObj removes an object from the store and frees its space.
// It retrieves object metadata from the allocator and frees its space.
func (s *multiStore) DeleteObj(objId store.ObjectId) error {
	if !store.IsValidObjectId(objId) {
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
func (s *multiStore) deleteObj(objId store.ObjectId) {
	// Retrieve object info from allocator
	// err means not found == nothing to delete
	objectInfo, _ := s.getObjectInfo(objId)
	_ = s.allocators[1].Free(objectInfo.Offset, objectInfo.Size)
}

// PrimeObject returns a dedicated ObjectId for application metadata.
// For multiStore, this is the first object after the 8-byte header at ObjectId 0.
// If it doesn't exist yet, it allocates it with the specified size.
// This provides a stable, known location for storing top-level metadata. This must
// be the first allocation in the file; MultiStore disables omni preallocation
// specifically so this ID is predictable and stable across reloads.
func (s *multiStore) PrimeObject(size int) (store.ObjectId, error) {
	// Sanity check: prevent unreasonably large prime objects
	if size < 0 || size > store.MaxPrimeObjectSize {
		return internal.ObjNotAllocated, errors.New("invalid prime object size")
	}

	// For multiStore, the prime object is the first object after the header
	const primeObjectId = store.ObjectId(store.PrimeObjectId)

	// Check if the prime object already exists using the allocator
	_, err := s.getObjectInfo(primeObjectId)
	if err == nil {
		// Object exists
		return primeObjectId, nil
	}

	// Allocate the prime object - this should be the very first allocation
	// Use the omni allocator to avoid ObjectId collisions
	objId, fileOffset, err := s.allocators[1].Allocate(size)
	if err != nil {
		return internal.ObjNotAllocated, err
	}

	// Verify we got the expected ObjectId (should be headerSize for first allocation)
	if objId != primeObjectId {
		return internal.ObjNotAllocated, fmt.Errorf("expected prime object to be first allocation at offset %d, got %d", primeObjectId, objId)
	}

	// Initialize the object with zeros
	n, err := store.WriteZeros(s.file, fileOffset, size)
	if err != nil {
		return internal.ObjNotAllocated, err
	}
	if n != size {
		return internal.ObjNotAllocated, errors.New("failed to write all bytes for prime object")
	}

	return primeObjectId, nil
}

// NewObj allocates a new object of the given size.
// It uses the block allocator to find space. The allocator records the allocation internally.
func (s *multiStore) NewObj(size int) (store.ObjectId, error) {
	objId, _, err := s.allocators[1].Allocate(size)
	if err != nil {
		return internal.ObjNotAllocated, err
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
		return s.allocators[1].Allocate(size)
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
