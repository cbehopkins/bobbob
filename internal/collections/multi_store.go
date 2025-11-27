// Package collections provides specialized store implementations optimized
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
//	ms, err := collections.NewMultiStore("data.db")
//	if err != nil {
//	    return err
//	}
//	defer ms.Close()
//
//	// Use ms as a regular store.Storer
//	objId, err := ms.NewObj(1024)
package collections

import (
	"errors"
	"io"
	"os"

	"bobbob/internal/store"
	"bobbob/internal/yggdrasil"
)

// deduplicateBlockSizes removes duplicate values from a slice of block sizes.
// This ensures that the omniBlockAllocator doesn't create redundant allocators
// for the same block size.
func deduplicateBlockSizes(sizes []int) []int {
	if len(sizes) == 0 {
		return sizes
	}

	seen := make(map[int]bool)
	result := make([]int, 0, len(sizes))

	for _, size := range sizes {
		if !seen[size] {
			seen[size] = true
			result = append(result, size)
		}
	}

	return result
}

// multiStore implements a store with multiple allocators for different object sizes.
// It uses a root allocator and a block allocator optimized for persistent treap nodes.
type multiStore struct {
	filePath   string
	file       *os.File
	objectMap  *store.ObjectMap
	allocators []store.Allocator
}

// NewMultiStore creates a new multiStore at the given file path.
// It initializes a root allocator and an omni block allocator optimized
// for the sizes used by persistent treap nodes.
func NewMultiStore(filePath string) (*multiStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	// Check if file is new (empty)
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	rootAllocator, err := store.NewBasicAllocator(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	// If new file, reserve first 8 bytes for metadata offset header
	if fileInfo.Size() == 0 {
		_, _, err = rootAllocator.Allocate(8)
		if err != nil {
			file.Close()
			return nil, err
		}
	}

	blockCount := 1024
	blockSizes := deduplicateBlockSizes(yggdrasil.PersistentTreapObjectSizes())
	omniAllocator := store.NewOmniBlockAllocator(blockSizes, blockCount, rootAllocator)

	ms := &multiStore{
		filePath:   filePath,
		file:       file,
		objectMap:  store.NewObjectMap(),
		allocators: []store.Allocator{rootAllocator, omniAllocator},
	}
	return ms, nil
}

// LoadMultiStore loads an existing multiStore from the given file path.
// It reads the metadata from the file and reconstructs the allocators and objectMap.
func LoadMultiStore(filePath string) (*multiStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	// Read metadata offset from header
	metadataOffset, err := readHeader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Read and parse metadata
	omniData, rootData, mapData, err := readMetadata(file, metadataOffset)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Restore allocators and objectMap
	rootAllocator, omniAllocator, objectMap, err := unmarshalComponents(rootData, omniData, mapData)
	if err != nil {
		file.Close()
		return nil, err
	}

	ms := &multiStore{
		filePath:   filePath,
		file:       file,
		objectMap:  objectMap,
		allocators: []store.Allocator{rootAllocator, omniAllocator},
	}

	return ms, nil
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
func readMetadata(file *os.File, metadataOffset int64) (omniData, rootData, mapData []byte, err error) {
	// Read length headers (12 bytes: 3 x 4-byte lengths)
	lengthHeader := make([]byte, 12)
	_, err = file.ReadAt(lengthHeader, metadataOffset)
	if err != nil {
		return nil, nil, nil, err
	}

	offset := 0
	omniLen, offset := readInt32(lengthHeader, offset)
	rootLen, offset := readInt32(lengthHeader, offset)
	mapLen, _ := readInt32(lengthHeader, offset)

	// Read all metadata
	totalLen := omniLen + rootLen + mapLen
	metadata := make([]byte, totalLen)
	_, err = file.ReadAt(metadata, metadataOffset+12)
	if err != nil {
		return nil, nil, nil, err
	}

	// Extract individual components
	omniData = metadata[0:omniLen]
	rootData = metadata[omniLen : omniLen+rootLen]
	mapData = metadata[omniLen+rootLen : omniLen+rootLen+mapLen]

	return omniData, rootData, mapData, nil
}

// unmarshalComponents deserializes the allocators and objectMap from their byte representations.
func unmarshalComponents(rootData, omniData, mapData []byte) (*store.BasicAllocator, store.Allocator, *store.ObjectMap, error) {
	type unmarshaler interface {
		Unmarshal([]byte) error
	}

	// Create and unmarshal rootAllocator (don't seek to end, we'll restore state)
	rootAllocator := store.NewEmptyBasicAllocator()

	rootUnmarshaler, ok := interface{}(rootAllocator).(unmarshaler)
	if !ok {
		return nil, nil, nil, errors.New("rootAllocator does not support unmarshaling")
	}

	if err := rootUnmarshaler.Unmarshal(rootData); err != nil {
		return nil, nil, nil, err
	}

	// Create and unmarshal omniAllocator
	blockCount := 1024
	blockSizes := deduplicateBlockSizes(yggdrasil.PersistentTreapObjectSizes())
	omniAllocator := store.NewOmniBlockAllocator(blockSizes, blockCount, rootAllocator)

	omniUnmarshaler, ok := interface{}(omniAllocator).(unmarshaler)
	if !ok {
		return nil, nil, nil, errors.New("omniAllocator does not support unmarshaling")
	}

	if err := omniUnmarshaler.Unmarshal(omniData); err != nil {
		return nil, nil, nil, err
	}

	// Unmarshal objectMap
	objectMap := store.NewObjectMap()
	if err := objectMap.Unmarshal(mapData); err != nil {
		return nil, nil, nil, err
	}

	return rootAllocator, omniAllocator, objectMap, nil
}

// readInt32 reads a 32-bit integer from the buffer at the given offset.
// Returns the value and the new offset after reading.
func readInt32(buf []byte, offset int) (int, int) {
	value := int(buf[offset])<<24 | int(buf[offset+1])<<16 |
		int(buf[offset+2])<<8 | int(buf[offset+3])
	return value, offset + 4
}

// Close closes the multiStore and releases the file handle.
// It marshals the allocators and objectMap to the file for persistence.
// The layout is:
// - First 8 bytes: offset to metadata object (allocated by root allocator)
// - Metadata object contains:
//   - omniAllocator marshaled state
//   - rootAllocator marshaled state
//   - objectMap marshaled state
func (s *multiStore) Close() error {
	if s.file == nil {
		return nil
	}

	// Marshal all components
	omniData, rootData, mapData, err := s.marshalComponents()
	if err != nil {
		return err
	}

	// Create combined metadata block
	metadata := s.buildMetadata(omniData, rootData, mapData)

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

// marshalComponents marshals all store components (allocators and objectMap).
func (s *multiStore) marshalComponents() (omniData, rootData, mapData []byte, err error) {
	type marshaler interface {
		Marshal() ([]byte, error)
	}

	// Marshal omniAllocator
	omniMarshaler, ok := s.allocators[1].(marshaler)
	if !ok {
		return nil, nil, nil, errors.New("omniAllocator does not support marshaling")
	}
	omniData, err = omniMarshaler.Marshal()
	if err != nil {
		return nil, nil, nil, err
	}

	// Marshal rootAllocator
	rootMarshaler, ok := s.allocators[0].(marshaler)
	if !ok {
		return nil, nil, nil, errors.New("rootAllocator does not support marshaling")
	}
	rootData, err = rootMarshaler.Marshal()
	if err != nil {
		return nil, nil, nil, err
	}

	// Marshal objectMap
	mapData, err = s.objectMap.Marshal()
	if err != nil {
		return nil, nil, nil, err
	}

	return omniData, rootData, mapData, nil
}

// buildMetadata creates a combined metadata block with length prefixes.
// Format: [omniLen:4][rootLen:4][mapLen:4][omniData][rootData][mapData]
func (s *multiStore) buildMetadata(omniData, rootData, mapData []byte) []byte {
	metadataSize := 12 + len(omniData) + len(rootData) + len(mapData)
	metadata := make([]byte, metadataSize)

	offset := 0
	// Write omni length
	offset = writeInt32(metadata, offset, len(omniData))
	// Write root length
	offset = writeInt32(metadata, offset, len(rootData))
	// Write map length
	offset = writeInt32(metadata, offset, len(mapData))

	// Copy data
	copy(metadata[offset:], omniData)
	offset += len(omniData)
	copy(metadata[offset:], rootData)
	offset += len(rootData)
	copy(metadata[offset:], mapData)

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

// DeleteObj removes an object from the store and frees its space.
// It atomically retrieves the object info and marks the space as free.
func (s *multiStore) DeleteObj(objId store.ObjectId) error {
	objectInfo, found := s.objectMap.GetAndDelete(objId, func(info store.ObjectInfo) {
		// Free the space in the allocator using the stored info
		s.allocators[1].Free(info.Offset, info.Size)
	})

	if !found {
		return nil // Object not found, nothing to delete
	}

	// The callback already freed the space
	_ = objectInfo
	return nil
}

// PrimeObject returns a dedicated ObjectId for application metadata.
// For multiStore, this is the first object after the 8-byte header at ObjectId 0.
// If it doesn't exist yet, it allocates it with the specified size.
// This provides a stable, known location for storing top-level metadata.
func (s *multiStore) PrimeObject(size int) (store.ObjectId, error) {
	// Sanity check: prevent unreasonably large prime objects
	const maxPrimeObjectSize = 1024 * 1024 // 1MB should be plenty for metadata
	if size < 0 || size > maxPrimeObjectSize {
		return store.ObjNotAllocated, errors.New("invalid prime object size")
	}

	// For multiStore, the prime object is the first object after the header
	const headerSize = 8
	const primeObjectId = store.ObjectId(headerSize)

	// Check if the prime object already exists
	_, found := s.objectMap.Get(primeObjectId)
	if found {
		return primeObjectId, nil
	}

	// Allocate the prime object - this should be the very first allocation
	objId, fileOffset, err := s.allocators[1].Allocate(size)
	if err != nil {
		return store.ObjNotAllocated, err
	}

	// Verify we got the expected ObjectId (should be headerSize for first allocation)
	if objId != primeObjectId {
		return store.ObjNotAllocated, errors.New("expected prime object to be first allocation")
	}

	// Store the object info in the object map
	objectInfo := store.ObjectInfo{
		Offset: fileOffset,
		Size:   size,
	}
	s.objectMap.Set(objId, objectInfo)

	// Initialize the object with zeros
	zeros := make([]byte, size)
	n, err := s.file.WriteAt(zeros, int64(fileOffset))
	if err != nil {
		return store.ObjNotAllocated, err
	}
	if n != size {
		return store.ObjNotAllocated, errors.New("failed to write all bytes for prime object")
	}

	return primeObjectId, nil
}

// NewObj allocates a new object of the given size.
// It uses the block allocator to find space and records the object in the object map.
func (s *multiStore) NewObj(size int) (store.ObjectId, error) {
	objId, fileOffset, err := s.allocators[1].Allocate(size)
	if err != nil {
		return store.ObjNotAllocated, err
	}

	// Store the object info in the object map
	objectInfo := store.ObjectInfo{
		Offset: fileOffset,
		Size:   size,
	}
	s.objectMap.Set(objId, objectInfo)

	return objId, nil
}

// LateReadObj returns a reader for the object with the given ID.
// Returns an error if the object is not found.
func (s *multiStore) LateReadObj(id store.ObjectId) (io.Reader, store.Finisher, error) {
	obj, found := s.objectMap.Get(id)
	if !found {
		return nil, nil, io.ErrUnexpectedEOF
	}

	// Create a section reader for this object
	reader := io.NewSectionReader(s.file, int64(obj.Offset), int64(obj.Size))

	// Return a no-op finisher since we don't need cleanup for reads
	finisher := func() error { return nil }

	return reader, finisher, nil
}

// LateWriteNewObj allocates a new object and returns a writer for it.
// The object is allocated from the block allocator and recorded in the object map.
func (s *multiStore) LateWriteNewObj(size int) (store.ObjectId, io.Writer, store.Finisher, error) {
	objId, fileOffset, err := s.allocators[1].Allocate(size)
	if err != nil {
		return store.ObjNotAllocated, nil, nil, err
	}

	// Store the object info in the object map
	objectInfo := store.ObjectInfo{
		Offset: fileOffset,
		Size:   size,
	}
	s.objectMap.Set(objId, objectInfo)

	// Create a section writer that writes to the correct offset in the file
	writer := store.NewSectionWriter(s.file, int64(fileOffset), int64(size))

	// Return a no-op finisher
	finisher := func() error { return nil }

	return objId, writer, finisher, nil
}

// WriteToObj returns a writer for an existing object.
// Returns an error if the object is not found.
func (s *multiStore) WriteToObj(objectId store.ObjectId) (io.Writer, store.Finisher, error) {
	obj, found := s.objectMap.Get(objectId)
	if !found {
		return nil, nil, io.ErrUnexpectedEOF
	}

	// Create a section writer for the existing object
	writer := store.NewSectionWriter(s.file, int64(obj.Offset), int64(obj.Size))

	// Return a no-op finisher
	finisher := func() error { return nil }

	return writer, finisher, nil
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
		obj, found := s.objectMap.Get(objId)
		if !found {
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
	n, err := s.file.WriteAt(data, int64(firstOffset))
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
	return s.objectMap.Get(objId)
}
