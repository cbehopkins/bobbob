package store

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator"
	"github.com/cbehopkins/bobbob/allocator/types"
)

var errStoreNotInitialized = errors.New("store is not initialized")

type baseStore struct {
	filePath  string
	file      *os.File
	allocator *allocator.Top // Thread-safe TopAllocator
	closed    bool           // Track if store is closed
}

// NewBasicStore creates a new baseStore at the given file path.
// If the file already exists, it loads the existing store.
// Otherwise, it creates a new file and initializes an empty store.
func NewBasicStore(filePath string) (*baseStore, error) {
	if _, err := os.Stat(filePath); err == nil {
		return LoadBaseStore(filePath)
	}
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Create the new TopAllocator
	blockSizes := []int{64, 256, 1024}
	maxBlockCount := 1024
	topAlloc, err := allocator.NewTop(file, blockSizes, maxBlockCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create TopAllocator: %w", err)
	}

	// Initialize the Store
	store := &baseStore{
		filePath:  filePath,
		file:      file,
		allocator: topAlloc,
	}

	return store, nil
}

// LoadBaseStore loads an existing baseStore from the given file path.
// It reads the object map from the file and initializes the allocator.
func LoadBaseStore(filePath string) (*baseStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to open store file %q: %w", filePath, err)
	}

	// Read the initial offset

	// Load the new TopAllocator from the file
	blockSizes := []int{64, 256, 1024}
	maxBlockCount := 1024
	topAlloc, err := allocator.NewTopFromFile(file, blockSizes, maxBlockCount)
	if err != nil {
		return nil, fmt.Errorf("failed to load TopAllocator from %q: %w", filePath, err)
	}

	// Initialize the Store
	store := &baseStore{
		filePath:  filePath,
		file:      file,
		allocator: topAlloc,
	}
	return store, nil
}

// Helper function to check if the file is initialized
func (s *baseStore) checkFileInitialized() error {
	if s.file == nil {
		return errStoreNotInitialized
	}
	if s.closed {
		return errors.New("store is closed")
	}
	return nil
}

// PrimeObject returns a dedicated ObjectId for application metadata.
// For baseStore, this is the first allocated object (after the 8-byte header at ObjectId 0).
// If it doesn't exist yet, it allocates it with the specified size.
// This provides a stable, known location for storing top-level metadata.
func (s *baseStore) PrimeObject(size int) (ObjectId, error) {
	// Sanity check: prevent unreasonably large prime objects
	if size < 0 || size > MaxPrimeObjectSize {
		return bobbob.ObjNotAllocated, fmt.Errorf("invalid prime object size %d (must be between 0 and %d)", size, MaxPrimeObjectSize)
	}

	if err := s.checkFileInitialized(); err != nil {
		return bobbob.ObjNotAllocated, err
	}

	// For baseStore, the prime object is the first object after the PrimeTable
	primeObjectId := ObjectId(PrimeObjectStart())

	// Check if the prime object already exists
	if s.allocator.ContainsObjectId(types.ObjectId(primeObjectId)) {
		return primeObjectId, nil
	}

	// Allocate the prime object - this should be the very first allocation
	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return bobbob.ObjNotAllocated, fmt.Errorf("failed to allocate prime object: %w", err)
	}

	// Verify we got the expected ObjectId (should be PrimeTable size for first allocation)
	if objId != primeObjectId {
		return bobbob.ObjNotAllocated, fmt.Errorf("expected prime object to be ObjectId %d, got %d", primeObjectId, objId)
	}

	// Initialize the object with zeros
	n, err := WriteZeros(s.file, fileOffset, size)
	if err != nil {
		return bobbob.ObjNotAllocated, fmt.Errorf("failed to initialize prime object: %w", err)
	}
	if n != size {
		return bobbob.ObjNotAllocated, errors.New("failed to write all bytes for prime object")
	}

	return primeObjectId, nil
}

// NewObj is a convenience wrapper around LateWriteNewObj that allocates an object
// without returning a writer. Use this when you want to allocate an object but
// will write to it later via WriteToObj.
func (s *baseStore) NewObj(size int) (ObjectId, error) {
	objId, _, finisher, err := s.LateWriteNewObj(size)
	if err != nil {
		return 0, err
	}
	if finisher != nil {
		if err := finisher(); err != nil {
			return 0, err
		}
	}
	return objId, nil
}

// LateWriteNewObj is the fundamental allocation method that allocates an object
// and returns a writer for immediate use. This is the primitive operation;
// NewObj is a convenience wrapper around this method.
func (s *baseStore) LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error) {
	if err := s.checkFileInitialized(); err != nil {
		return bobbob.ObjNotAllocated, nil, nil, err
	}

	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return bobbob.ObjNotAllocated, nil, nil, err
	}
	// Create a section writer that writes to the correct offset in the file
	writer := CreateSectionWriter(s.file, fileOffset, size)

	return objId, writer, nil, nil
}

// AllocateRun attempts to reserve a contiguous run of objects of the given size.
// Returns ErrAllocateRunUnsupported when the underlying allocator cannot guarantee contiguity.
func (s *baseStore) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, nil, err
	}

	objIds, offsets, err := s.allocator.AllocateRun(size, count)
	if err != nil {
		return nil, nil, err
	}

	return objIds, offsets, nil
}

// WriteToObj is a Late method that returns a writer for an existing object.
// This should be done with extreme caution and avoided where possible.
// Prefer creating a new object, writing to it, then deleting the old one.
func (s *baseStore) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, nil, err
	}
	offset, size, err := s.allocator.GetObjectInfo(types.ObjectId(objectId))
	if err != nil {
		return nil, nil, fmt.Errorf("object not found: %w", err)
	}
	writer := CreateSectionWriter(s.file, FileOffset(offset), int(size))
	// BaseStore has no per-object resources that need cleanup, return a no-op finisher
	return writer, func() error { return nil }, nil
}

// WriteBatchedObjs implements batched writing for consecutive objects.
// This is a performance optimization that writes multiple consecutive objects
// in a single WriteAt call, reducing system call overhead.
func (s *baseStore) WriteBatchedObjs(objIds []ObjectId, data []byte, sizes []int) error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	if len(objIds) != len(sizes) {
		return fmt.Errorf("objIds length %d does not match sizes length %d", len(objIds), len(sizes))
	}

	if len(objIds) == 0 {
		return nil
	}

	// Verify all objects exist and are consecutive
	var firstOffset FileOffset
	expectedOffset := FileOffset(0)

	for i, objId := range objIds {
		offset, size, err := s.allocator.GetObjectInfo(types.ObjectId(objId))
		if err != nil {
			return fmt.Errorf("object %d not found: %w", objId, err)
		}

		if i == 0 {
			firstOffset = FileOffset(offset)
			// Use the ALLOCATED size, not the written size
			expectedOffset = FileOffset(offset) + FileOffset(size)
		} else {
			if FileOffset(offset) != expectedOffset {
				return fmt.Errorf("objects are not consecutive: gap at object %d (expected offset %d, got %d)",
					objId, expectedOffset, offset)
			}
			// Use the ALLOCATED size, not the written size
			expectedOffset = FileOffset(offset) + FileOffset(size)
		}
	}

	// Write all data in one operation
	// Note: We don't validate that sizes[i] matches obj.Size because the caller
	// might write less than the allocated size (similar to WriteBytesToObj)
	n, err := WriteBatchedSections(s.file, firstOffset, [][]byte{data})
	if err != nil {
		return fmt.Errorf("failed to write batched objects: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("incomplete batched write: wrote %d of %d bytes", n, len(data))
	}

	return nil
}

// GetObjectInfo returns the ObjectInfo for a given ObjectId.
// This is used internally for optimization decisions like batched writes.
func (s *baseStore) GetObjectInfo(objId ObjectId) (ObjectInfo, bool) {
	offset, size, err := s.allocator.GetObjectInfo(types.ObjectId(objId))
	if err != nil {
		return ObjectInfo{}, false
	}
	return ObjectInfo{Offset: FileOffset(offset), Size: int(size)}, true
}

// LateReadObj is the fundamental read method that returns a reader for streaming access.
// This is the primitive operation; ReadBytesFromObj is a convenience wrapper.
func (s *baseStore) LateReadObj(objId ObjectId) (io.Reader, Finisher, error) {
	if !IsValidObjectId(objId) {
		return nil, nil, errors.New("invalid objectId")
	}
	return s.lateReadObj(objId)
}

func (s *baseStore) lateReadObj(objId ObjectId) (io.Reader, Finisher, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, nil, err
	}
	offset, size, err := s.allocator.GetObjectInfo(types.ObjectId(objId))
	if err != nil {
		return nil, nil, fmt.Errorf("object not found: %w", err)
	}
	reader := CreateSectionReader(s.file, FileOffset(offset), int(size))
	// BaseStore has no per-object resources that need cleanup, return a no-op finisher
	return reader, func() error { return nil }, nil
}

func (s *baseStore) DeleteObj(objId ObjectId) error {
	// Note: DeleteObj only frees the single object identified by objId.
	// Complex structures that allocate additional objects must ensure those
	// dependent ObjectIds are deleted by higher-level code before or after
	// this call (e.g., a payload-specific deleter that walks its children).
	if !IsValidObjectId(objId) {
		return nil
	}
	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	// Use the allocator's DeleteObj method
	if err := s.allocator.DeleteObj(types.ObjectId(objId)); err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

// Sync flushes the file to disk
func (s *baseStore) Sync() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	return s.file.Sync()
}

// Close closes the store and persists allocator state to disk
func (s *baseStore) Close() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	// Mark as closed to prevent further operations
	s.closed = true

	// Set allocator state metadata (required for proper serialization)
	s.allocator.SetStoreMeta(allocator.FileInfo{
		ObjId: 0,
		Fo:    0,
		Sz:    1, // Non-zero to pass the Save() check
	})

	// Save the allocator state
	if err := s.allocator.Save(); err != nil {
		return fmt.Errorf("failed to save allocator state: %w", err)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file to disk: %w", err)
	}
	return s.file.Close()
}

// GetObjectCount returns the number of objects tracked in the allocator.
func (s *baseStore) GetObjectCount() int {
	// The allocator tracks objects internally; we cannot easily query the count
	// without adding an introspection method. For now, return 0 as a placeholder.
	// TODO: Add GetAllocatedCount() to allocator.Top interface
	return 0
}

// Allocator returns the allocator backing this store, enabling external callers
// to configure allocation callbacks (e.g., SetOnAllocate).
func (s *baseStore) Allocator() *allocator.Top {
	return s.allocator
}
