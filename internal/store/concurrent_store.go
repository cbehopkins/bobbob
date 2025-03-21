package store

import (
	"io"
	"sync"
)

type concurrentStore struct {
	baseStore
	lock sync.RWMutex
	lockMap map[ObjectId] *sync.RWMutex
}

func NewConcurrentStore(filePath string) (*concurrentStore, error) {
	baseStore, err := NewBasicStore(filePath)
	if err != nil {
		return nil, err
	}
	return &concurrentStore{
		*baseStore,
		sync.RWMutex{},
		make(map[ObjectId]*sync.RWMutex),
		}, nil
}

func (s *concurrentStore) lookupObjectMutex(objectId ObjectId) *sync.RWMutex {
	s.lock.Lock()
	mutex, ok := s.lockMap[objectId]
	if !ok {
		mutex = &sync.RWMutex{}
		s.lockMap[objectId] = mutex
	}
	s.lock.Unlock()
	return mutex
}
func (s *concurrentStore) LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error) {
	objectId, writer, finisher, err := s.baseStore.LateWriteNewObj(size)
	if err != nil {
		return objectId, nil, nil, err
	}
	objLock := s.lookupObjectMutex(objectId)
	objLock.Lock()
	newFinisher := func() error {
		defer objLock.Unlock()
		return finisher()
	}
	return objectId, writer, newFinisher, nil
}
func (s *concurrentStore) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
	objLock := s.lookupObjectMutex(objectId)
	objLock.Lock()
	defer objLock.Unlock()
	return s.baseStore.WriteToObj(objectId)
}
func (s *concurrentStore) LateReadObj(offset ObjectId) (io.Reader, error) {
	objLock := s.lookupObjectMutex(offset)
	objLock.RLock()
	defer objLock.RUnlock()
	return s.baseStore.LateReadObj(offset)
}