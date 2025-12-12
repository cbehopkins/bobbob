package store

import (
	"io"
	"sync"
)

// concurrentStore wraps baseStore with per-object locking to allow
// safe concurrent access to different objects.
// TBD: Add the cunning plan to reuse the io.writer by a queue that the closer writes to with the current io.writer
type concurrentStore struct {
	baseStore *baseStore
	lock      sync.RWMutex
	lockMap   map[ObjectId]*sync.RWMutex
}

// NewConcurrentStore creates a new concurrentStore at the given file path.
// It provides thread-safe access for concurrent operations on different objects.
func NewConcurrentStore(filePath string) (*concurrentStore, error) {
	baseStore, err := NewBasicStore(filePath)
	if err != nil {
		return nil, err
	}
	return &concurrentStore{
		baseStore: baseStore,
		lock:      sync.RWMutex{},
		lockMap:   make(map[ObjectId]*sync.RWMutex),
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
		if finisher != nil {
			return finisher()
		}
		return nil
	}
	return objectId, writer, newFinisher, nil
}

func (s *concurrentStore) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
	objLock := s.lookupObjectMutex(objectId)
	objLock.Lock()
	writer, finisher, err := s.baseStore.WriteToObj(objectId)
	if err != nil {
		objLock.Unlock()
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.Unlock()
		if finisher != nil {
			return finisher()
		}
		return nil
	}
	return writer, newFinisher, nil
}

func (s *concurrentStore) LateReadObj(offset ObjectId) (io.Reader, Finisher, error) {
	objLock := s.lookupObjectMutex(offset)
	objLock.RLock()
	reader, finisher, err := s.baseStore.LateReadObj(offset)
	if err != nil {
		objLock.RUnlock()
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.RUnlock()
		if finisher != nil {
			return finisher()
		}
		return nil
	}
	return reader, newFinisher, nil
}

// WriteBatchedObjs writes data to multiple consecutive objects in a single operation.
// Locks all objects in the range for the duration of the write.
// Objects are locked in sorted order to prevent deadlocks.
func (s *concurrentStore) WriteBatchedObjs(objIds []ObjectId, data []byte, sizes []int) error {
	// Create a sorted copy of objIds to prevent deadlock
	// We lock in ObjectId order, not the order they were passed in
	sortedIndices := make([]int, len(objIds))
	for i := range sortedIndices {
		sortedIndices[i] = i
	}
	// Sort indices by their corresponding ObjectId values
	for i := 0; i < len(sortedIndices); i++ {
		for j := i + 1; j < len(sortedIndices); j++ {
			if objIds[sortedIndices[i]] > objIds[sortedIndices[j]] {
				sortedIndices[i], sortedIndices[j] = sortedIndices[j], sortedIndices[i]
			}
		}
	}

	// Lock all objects in sorted order
	locks := make([]*sync.RWMutex, len(objIds))
	for _, idx := range sortedIndices {
		objId := objIds[idx]
		locks[idx] = s.lookupObjectMutex(objId)
		locks[idx].Lock()
	}

	// Ensure all locks are released
	defer func() {
		for _, lock := range locks {
			lock.Unlock()
		}
	}()

	return s.baseStore.WriteBatchedObjs(objIds, data, sizes)
}

// GetObjectInfo returns the ObjectInfo for a given ObjectId.
func (s *concurrentStore) GetObjectInfo(objId ObjectId) (ObjectInfo, bool) {
	return s.baseStore.GetObjectInfo(objId)
}

// NewObj allocates a new object of the given size and returns its ID.
func (s *concurrentStore) NewObj(size int) (ObjectId, error) {
	return s.baseStore.NewObj(size)
}

// PrimeObject returns ObjectId 1, which is reserved for application metadata.
// Delegates to the underlying baseStore.
func (s *concurrentStore) PrimeObject(size int) (ObjectId, error) {
	return s.baseStore.PrimeObject(size)
}

// DeleteObj removes the object with the given ID.
// Also removes the object's mutex from the lock map to prevent memory leaks.
func (s *concurrentStore) DeleteObj(objId ObjectId) error {
	err := s.baseStore.DeleteObj(objId)
	if err == nil {
		// Clean up the lock map to prevent memory leak
		s.lock.Lock()
		delete(s.lockMap, objId)
		s.lock.Unlock()
	}
	return err
}

// Close closes the store and releases all resources.
func (s *concurrentStore) Close() error {
	return s.baseStore.Close()
}
