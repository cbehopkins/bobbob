package store

import (
	"io"
	"sync"

	"github.com/cbehopkins/bobbob/store/allocator"
)

// concurrentStore wraps any Storer with per-object locking to allow
// safe concurrent access to different objects.
// Optionally limits concurrent disk I/O operations via a token pool.
type concurrentStore struct {
	innerStore Storer
	lock       sync.RWMutex
	lockMap    map[ObjectId]*sync.RWMutex
	diskTokens chan struct{} // nil for unlimited, otherwise limits concurrent disk ops
}

// NewConcurrentStore creates a new concurrentStore at the given file path.
// It provides thread-safe access for concurrent operations on different objects.
// If maxDiskTokens > 0, limits concurrent disk I/O operations to that count.
// Pass 0 for unlimited concurrent disk operations.
// This wraps a BasicStore for backward compatibility.
func NewConcurrentStore(filePath string, maxDiskTokens int) (*concurrentStore, error) {
	baseStore, err := NewBasicStore(filePath)
	if err != nil {
		return nil, err
	}
	return NewConcurrentStoreWrapping(baseStore, maxDiskTokens), nil
}

// NewConcurrentStoreWrapping wraps any Storer with per-object locking and optional disk token pool.
// This enables adding concurrency to any store implementation (baseStore, multiStore, etc.).
// If maxDiskTokens > 0, limits concurrent disk I/O operations to that count.
// Pass 0 for unlimited concurrent disk operations.
func NewConcurrentStoreWrapping(innerStore Storer, maxDiskTokens int) *concurrentStore {
	var diskTokens chan struct{}
	if maxDiskTokens > 0 {
		diskTokens = make(chan struct{}, maxDiskTokens)
		for range maxDiskTokens {
			diskTokens <- struct{}{}
		}
	}
	return &concurrentStore{
		innerStore: innerStore,
		lock:       sync.RWMutex{},
		lockMap:    make(map[ObjectId]*sync.RWMutex),
		diskTokens: diskTokens,
	}
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
	if s.diskTokens != nil {
		<-s.diskTokens // Acquire disk token (blocks if none available)
	}

	// Allocation modifies allocator state, so use global lock
	s.lock.Lock()
	objectId, writer, finisher, err := s.innerStore.LateWriteNewObj(size)
	s.lock.Unlock()

	if err != nil {
		if s.diskTokens != nil {
			s.diskTokens <- struct{}{} // Release on error
		}
		return objectId, nil, nil, err
	}
	objLock := s.lookupObjectMutex(objectId)
	objLock.Lock()
	newFinisher := func() error {
		defer objLock.Unlock()
		if s.diskTokens != nil {
			defer func() { s.diskTokens <- struct{}{} }() // Release disk token
		}
		if finisher != nil {
			return finisher()
		}
		return nil
	}
	return objectId, writer, newFinisher, nil
}

func (s *concurrentStore) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
	if s.diskTokens != nil {
		<-s.diskTokens // Acquire disk token
	}
	objLock := s.lookupObjectMutex(objectId)
	objLock.Lock()
	writer, finisher, err := s.innerStore.WriteToObj(objectId)
	if err != nil {
		objLock.Unlock()
		if s.diskTokens != nil {
			s.diskTokens <- struct{}{} // Release on error
		}
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.Unlock()
		if s.diskTokens != nil {
			defer func() { s.diskTokens <- struct{}{} }() // Release disk token
		}
		if finisher != nil {
			return finisher()
		}
		return nil
	}
	return writer, newFinisher, nil
}

func (s *concurrentStore) LateReadObj(offset ObjectId) (io.Reader, Finisher, error) {
	if s.diskTokens != nil {
		<-s.diskTokens // Acquire disk token
	}
	objLock := s.lookupObjectMutex(offset)
	objLock.RLock()
	reader, finisher, err := s.innerStore.LateReadObj(offset)
	if err != nil {
		objLock.RUnlock()
		if s.diskTokens != nil {
			s.diskTokens <- struct{}{} // Release on error
		}
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.RUnlock()
		if s.diskTokens != nil {
			defer func() { s.diskTokens <- struct{}{} }() // Release disk token
		}
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

	return s.innerStore.WriteBatchedObjs(objIds, data, sizes)
}

// GetObjectInfo returns the ObjectInfo for a given ObjectId.
// Returns false if the inner store doesn't support GetObjectInfo.
func (s *concurrentStore) GetObjectInfo(objId ObjectId) (ObjectInfo, bool) {
	// Try type assertion to get GetObjectInfo support
	if getter, ok := s.innerStore.(interface {
		GetObjectInfo(ObjectId) (ObjectInfo, bool)
	}); ok {
		return getter.GetObjectInfo(objId)
	}
	return ObjectInfo{}, false
}

// AllocateRun proxies to the underlying store when it supports contiguous allocation.
// Returns ErrAllocateRunUnsupported if the inner store doesn't support it.
func (s *concurrentStore) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	// Try type assertion to get RunAllocator support
	if allocator, ok := s.innerStore.(RunAllocator); ok {
		return allocator.AllocateRun(size, count)
	}
	return nil, nil, ErrAllocateRunUnsupported
}

// NewObj allocates a new object of the given size and returns its ID.
// Allocation operations may modify internal allocator state, so we use a global lock.
func (s *concurrentStore) NewObj(size int) (ObjectId, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.innerStore.NewObj(size)
}

// PrimeObject returns ObjectId 1, which is reserved for application metadata.
// Delegates to the underlying inner store with global lock protection.
func (s *concurrentStore) PrimeObject(size int) (ObjectId, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.innerStore.PrimeObject(size)
}

// DeleteObj removes the object with the given ID.
// Also removes the object's mutex from the lock map to prevent memory leaks.
// Uses global lock to protect allocator state modifications.
func (s *concurrentStore) DeleteObj(objId ObjectId) error {
	s.lock.Lock()
	err := s.innerStore.DeleteObj(objId)
	delete(s.lockMap, objId) // Clean up lock map while we have the lock
	s.lock.Unlock()
	return err
}

// Close closes the store and releases all resources.
func (s *concurrentStore) Close() error {
	return s.innerStore.Close()
}

// Allocator returns the underlying allocator when the wrapped store exposes it.
// This allows external callers to configure allocation callbacks through the
// concurrency wrapper without breaking encapsulation.
func (s *concurrentStore) Allocator() allocator.Allocator {
	if provider, ok := s.innerStore.(interface{ Allocator() allocator.Allocator }); ok {
		return provider.Allocator()
	}
	return nil
}
