package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cbehopkins/bobbob/store/allocator"
)

var errStoreNotInitialized = errors.New("store is not initialized")

type baseStore struct {
	filePath  string
	file      *os.File
	objectMap *ObjectMap
	allocator allocator.Allocator
	closed    bool // Track if store is closed
}

// NewBasicStore creates a new baseStore at the given file path.
// If the file already exists, it loads the existing store.
// Otherwise, it creates a new file and initializes an empty store.
func NewBasicStore(filePath string) (*baseStore, error) {
	if _, err := os.Stat(filePath); err == nil {
		return LoadBaseStore(filePath)
	}
	pointerSize := 8
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	alloc, err := allocator.NewBasicAllocator(file)
	if err != nil {
		return nil, err
	}
	alloc.End = int64(pointerSize)
	// Initialize the Store
	store := &baseStore{
		filePath:  filePath,
		file:      file,
		objectMap: NewObjectMap(),
		allocator: alloc,
	}
	store.objectMap.Set(0, ObjectInfo{Offset: 0, Size: pointerSize})

	// Write the initial offset (zero) to the store
	_, err = file.Write(make([]byte, pointerSize))
	if err != nil {
		return nil, err
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
	var initialOffset int64
	err = binary.Read(file, binary.LittleEndian, &initialOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read initial offset from %q: %w", filePath, err)
	}

	// Seek to the initial offset
	_, err = file.Seek(initialOffset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d in %q: %w", initialOffset, filePath, err)
	}

	// Deserialize the ObjectMap directly from the file
	objectMap := NewObjectMap()
	err = objectMap.Deserialize(file)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize object map from %q: %w", filePath, err)
	}
	alloc := allocator.NewEmptyBasicAllocator()
	alloc.End = initialOffset
	// Initialize the Store
	store := &baseStore{
		filePath:  filePath,
		file:      file,
		objectMap: objectMap,
		allocator: alloc,
	}

	err = alloc.RefreshFreeListFromGaps(objectMap.FindGaps())
	if err != nil {
		return nil, fmt.Errorf("failed to refresh free list for %q: %w", filePath, err)
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

// Helper function to update the initial offset in the file
func (s *baseStore) updateInitialOffset(fileOffset FileOffset) error {
	objOffset := int64(fileOffset)
	offsetBytes := make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		offsetBytes[i] = byte(objOffset >> (i * 8))
	}

	if _, err := s.file.WriteAt(offsetBytes, 0); err != nil {
		return err
	}

	return nil
}

// PrimeObject returns a dedicated ObjectId for application metadata.
// For baseStore, this is the first allocated object (after the 8-byte header at ObjectId 0).
// If it doesn't exist yet, it allocates it with the specified size.
// This provides a stable, known location for storing top-level metadata.
func (s *baseStore) PrimeObject(size int) (ObjectId, error) {
	// Sanity check: prevent unreasonably large prime objects
	const maxPrimeObjectSize = 1024 * 1024 // 1MB should be plenty for metadata
	if size < 0 || size > maxPrimeObjectSize {
		return ObjNotAllocated, fmt.Errorf("invalid prime object size %d (must be between 0 and %d)", size, maxPrimeObjectSize)
	}

	if err := s.checkFileInitialized(); err != nil {
		return ObjNotAllocated, err
	}

	// For baseStore, the prime object is the first object after the header (ObjectId 8)
	const headerSize = 8
	const primeObjectId = ObjectId(headerSize)

	// Check if the prime object already exists
	_, found := s.objectMap.Get(primeObjectId)
	if found {
		return primeObjectId, nil
	}

	// Allocate the prime object - this should be the very first allocation
	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return ObjNotAllocated, fmt.Errorf("failed to allocate prime object: %w", err)
	}

	// Verify we got the expected ObjectId (should be headerSize for first allocation)
	if objId != primeObjectId {
		return ObjNotAllocated, fmt.Errorf("expected prime object to be ObjectId %d, got %d", primeObjectId, objId)
	}

	// Add to objectMap so it persists across sessions
	s.objectMap.Set(objId, ObjectInfo{
		Offset: fileOffset,
		Size:   size,
	})

	// Initialize the object with zeros
	zeros := make([]byte, size)
	n, err := s.file.WriteAt(zeros, int64(fileOffset))
	if err != nil {
		return ObjNotAllocated, fmt.Errorf("failed to initialize prime object: %w", err)
	}
	if n != size {
		return ObjNotAllocated, errors.New("failed to write all bytes for prime object")
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
		return ObjNotAllocated, nil, nil, err
	}

	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return ObjNotAllocated, nil, nil, err
	}
	// Create a section writer that writes to the correct offset in the file
	writer := NewSectionWriter(s.file, int64(fileOffset), int64(size))

	s.objectMap.Set(objId, ObjectInfo{Offset: fileOffset, Size: size})

	return objId, writer, nil, nil
}

// WriteToObj is a Late method that returns a writer for an existing object.
// This should be done with extreme caution and avoided where possible.
// Prefer creating a new object, writing to it, then deleting the old one.
func (s *baseStore) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, nil, err
	}
	obj, found := s.objectMap.Get(objectId)
	if !found {
		return nil, nil, errors.New("object not found")
	}
	writer := NewSectionWriter(s.file, int64(obj.Offset), int64(obj.Size))
	return writer, s.createCloser(objectId), nil
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
		obj, found := s.objectMap.Get(objId)
		if !found {
			return fmt.Errorf("object %d not found", objId)
		}

		if i == 0 {
			firstOffset = obj.Offset
			// Use the ALLOCATED size, not the written size
			expectedOffset = obj.Offset + FileOffset(obj.Size)
		} else {
			if obj.Offset != expectedOffset {
				return fmt.Errorf("objects are not consecutive: gap at object %d (expected offset %d, got %d)",
					objId, expectedOffset, obj.Offset)
			}
			// Use the ALLOCATED size, not the written size
			expectedOffset = obj.Offset + FileOffset(obj.Size)
		}
	}

	// Write all data in one operation
	// Note: We don't validate that sizes[i] matches obj.Size because the caller
	// might write less than the allocated size (similar to WriteBytesToObj)
	n, err := s.file.WriteAt(data, int64(firstOffset))
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
	return s.objectMap.Get(objId)
}

func (s *baseStore) createCloser(objectId ObjectId) func() error {
	return func() error {
		_, found := s.objectMap.Get(objectId)
		if !found { // This should never happen
			return errors.New("object not found in the closer. This is very bad!")
		}
		return nil
	}
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
	obj, found := s.objectMap.Get(objId)
	if !found {
		return nil, nil, errors.New("object not found")
	}
	// For now they are always the same but we will implement a mapping in the future
	fileOffset := FileOffset(obj.Offset)
	reader := io.NewSectionReader(s.file, int64(fileOffset), int64(obj.Size))
	return reader, s.createCloser(objId), nil
}

func (s *baseStore) DeleteObj(objId ObjectId) error {
	// FIXME: Complex types have one object that points to others.
	// We need a method that lists all the objects that are part of a complex object
	// and then recursively delete them. However, we can only do that on the
	// unmarshalled type - so we do not have that information here.
	// Therefore, complex objects need to implement a delete method.
	if !IsValidObjectId(objId) {
		return nil
	}
	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	var freeErr error
	_, found := s.objectMap.GetAndDelete(objId, func(obj ObjectInfo) {
		freeErr = s.allocator.Free(obj.Offset, obj.Size)
	})
	if !found {
		return errors.New("object not found")
	}
	if freeErr != nil {
		return freeErr
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

// Close closes the store and writes the ObjectMap to disk
func (s *baseStore) Close() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	// Mark as closed to prevent further operations
	s.closed = true

	data, err := s.objectMap.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal object map: %w", err)
	}
	// We need a special method here to allocate LAST item in the file
	// This is because we need to write the object map to the end of the file
	_, fileOffset, err := s.allocator.Allocate(len(data))
	if err != nil {
		return fmt.Errorf("failed to allocate space for object map: %w", err)
	}

	n, err := s.file.WriteAt(data, int64(fileOffset))
	if err != nil {
		return fmt.Errorf("failed to write object map at offset %d: %w", fileOffset, err)
	}
	if n != len(data) {
		return errors.New("did not write all the data")
	}

	// Update the first object in the store with the offset of the serialized ObjectMap
	if err := s.updateInitialOffset(FileOffset(fileOffset)); err != nil {
		return fmt.Errorf("failed to update initial offset to %d: %w", fileOffset, err)
	}

	err = s.file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync file to disk: %w", err)
	}
	return s.file.Close()
}

// GetObjectCount returns the number of objects tracked in the store's ObjectMap.
func (s *baseStore) GetObjectCount() int {
	return s.objectMap.Len()
}
