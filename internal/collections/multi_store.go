package collections

import (
	"io"
	"os"

	"bobbob/internal/store"
	"bobbob/internal/yggdrasil"
)

type multiStore struct {
	filePath   string
	file       *os.File
	objectMap  *store.ObjectMap
	allocators []store.Allocator
}

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

func (s *multiStore) Close() error {
	// FIXME Marshal the allocators and objectMap to the file

	if s.file != nil {
		err := s.file.Close()
		if err != nil {
			return err
		}
		s.file = nil
	}
	return nil
}

func (s *multiStore) DeleteObj(objId store.ObjectId) error {
	// Retrieve and delete the object info from the object map
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

func (s *multiStore) NewObj(size int) (store.ObjectId, error) {
	// Allocate the object from the allocator
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

func (s *multiStore) LateWriteNewObj(size int) (store.ObjectId, io.Writer, store.Finisher, error) {
	// Allocate the object from the allocator
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
