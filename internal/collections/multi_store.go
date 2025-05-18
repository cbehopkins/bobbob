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
	objectMap  *yggdrasil.PersistentTreap[yggdrasil.PersistentObjectId]
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
	// The object map is a map from object ID to object info
	cmpFunc := func(a, b yggdrasil.PersistentObjectId) bool {
		return a == b
	}
	template := yggdrasil.PersistentObjectId(store.ObjNotAllocated)
	store := &multiStore{
		filePath:   filePath,
		file:       file,
		allocators: []store.Allocator{rootAllocator, omniAllocator},
	}
	bob := yggdrasil.NewPersistentTreap[yggdrasil.PersistentObjectId](cmpFunc, &template, store)
	store.objectMap = bob
	return store, nil
}

func (s *multiStore) Close() error {
	// FIXME Marshal the allocators to the file

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
	// FIXME delete the object from the object map
	// and free the space in the allocator
	return nil
}

func (s *multiStore) NewObj(size int) (store.ObjectId, error) {
	// FIXME allocate the object from the allocator
	// and add it to the object map
	objId, _, err := s.allocators[1].Allocate(size)
	if err != nil {
		return store.ObjNotAllocated, err
	}
	var bob yggdrasil.PersistentObjectId
	bob = yggdrasil.PersistentObjectId(objId)
	s.objectMap.Insert(&bob, 0) // FIXME
	return store.ObjNotAllocated, nil
}

func (s *multiStore) LateReadObj(id store.ObjectId) (io.Reader, store.Finisher, error) {
	// FIXME read the object from the allocator
	return nil, nil, nil
}

func (s *multiStore) LateWriteNewObj(size int) (store.ObjectId, io.Writer, store.Finisher, error) {
	// FIXME allocate the object from the allocator
	// and add it to the object map
	return store.ObjNotAllocated, nil, nil, nil
}

func (s *multiStore) WriteToObj(objectId store.ObjectId) (io.Writer, store.Finisher, error) {
	// FIXME write to the object in the allocator
	return nil, nil, nil
}
