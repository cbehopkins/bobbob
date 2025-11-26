package collections

import (
	"io"
	"os"

	"bobbob/internal/store"
	"bobbob/internal/yggdrasil"
)

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
	rootAllocator, err := store.NewBasicAllocator(file)
	if err != nil {
		return nil, err
	}
	blockCount := 1024
	// FIXME blocksizes should be a set
	blockSizes := yggdrasil.PersistentTreapObjectSizes()
	omniAllocator := store.NewOmniBlockAllocator(blockSizes, blockCount, rootAllocator)

	ms := &multiStore{
		filePath:   filePath,
		file:       file,
		objectMap:  store.NewObjectMap(),
		allocators: []store.Allocator{rootAllocator, omniAllocator},
	}
	return ms, nil
}

// Close closes the multiStore and releases the file handle.
// FIXME: Should marshal the allocators and objectMap to the file.
func (s *multiStore) Close() error {
	if s.file != nil {
		err := s.file.Close()
		if err != nil {
			return err
		}
		s.file = nil
	}
	return nil
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
