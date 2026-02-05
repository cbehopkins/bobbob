package store

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// objectLock wraps a mutex with a reference count for garbage collection.
// When refCount drops to 0, the lock can be safely removed from the map.
type objectLock struct {
	mu       sync.RWMutex
	refCount atomic.Int32
}

// objectLockPool reduces allocation overhead by reusing objectLock structs.
var objectLockPool = sync.Pool{
	New: func() interface{} {
		return &objectLock{}
	},
}

// concurrentStore wraps any Storer with per-object locking to allow
// safe concurrent access to different objects.
// Optionally limits concurrent disk I/O operations via a token pool.
type concurrentStore struct {
	innerStore Storer
	lock       sync.RWMutex
	lockMap    map[ObjectId]*objectLock
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
		lockMap:    make(map[ObjectId]*objectLock),
		diskTokens: diskTokens,
	}
}

// acquireObjectLock looks up or creates an object lock and increments its reference count.
// Returns the lock for the caller to lock/unlock.
// Uses objectLockPool to reduce allocation overhead.
func (s *concurrentStore) acquireObjectLock(objectId ObjectId) *objectLock {
	s.lock.Lock()
	objLock, ok := s.lockMap[objectId]
	if !ok {
		objLock = objectLockPool.Get().(*objectLock)
		// refCount is already 0 from reset or new allocation
		s.lockMap[objectId] = objLock
	}
	objLock.refCount.Add(1)
	s.lock.Unlock()
	return objLock
}

// releaseObjectLock decrements the reference count and removes the lock from the map if unused.
// Returns the lock to objectLockPool for reuse to reduce allocation overhead.
func (s *concurrentStore) releaseObjectLock(objectId ObjectId, objLock *objectLock) {
	if objLock.refCount.Add(-1) == 0 {
		// Reference count is now 0; try to remove from map
		s.lock.Lock()
		// Double-check refCount is still 0 (race protection)
		if objLock.refCount.Load() == 0 {
			delete(s.lockMap, objectId)
			objLock.refCount.Store(0) // Reset for pool reuse (safety)
			objectLockPool.Put(objLock)
		}
		s.lock.Unlock()
	}
}

func (s *concurrentStore) LateWriteNewObj(size int) (bobbob.ObjectId, io.Writer, bobbob.Finisher, error) {
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
	objLock := s.acquireObjectLock(objectId)
	objLock.mu.Lock()
	newFinisher := func() error {
		defer objLock.mu.Unlock()
		defer s.releaseObjectLock(objectId, objLock)
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

func (s *concurrentStore) WriteToObj(objectId bobbob.ObjectId) (io.Writer, bobbob.Finisher, error) {
	if s.diskTokens != nil {
		<-s.diskTokens // Acquire disk token
	}
	objLock := s.acquireObjectLock(objectId)
	objLock.mu.Lock()
	writer, finisher, err := s.innerStore.WriteToObj(objectId)
	if err != nil {
		objLock.mu.Unlock()
		s.releaseObjectLock(objectId, objLock)
		if s.diskTokens != nil {
			s.diskTokens <- struct{}{} // Release on error
		}
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.mu.Unlock()
		defer s.releaseObjectLock(objectId, objLock)
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

func (s *concurrentStore) LateReadObj(offset ObjectId) (io.Reader, bobbob.Finisher, error) {
	if s.diskTokens != nil {
		<-s.diskTokens // Acquire disk token
	}
	objLock := s.acquireObjectLock(offset)
	objLock.mu.RLock()
	reader, finisher, err := s.innerStore.LateReadObj(offset)
	if err != nil {
		objLock.mu.RUnlock()
		s.releaseObjectLock(offset, objLock)
		if s.diskTokens != nil {
			s.diskTokens <- struct{}{} // Release on error
		}
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.mu.RUnlock()
		defer s.releaseObjectLock(offset, objLock)
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
	for i := range sortedIndices {
		for j := i + 1; j < len(sortedIndices); j++ {
			if objIds[sortedIndices[i]] > objIds[sortedIndices[j]] {
				sortedIndices[i], sortedIndices[j] = sortedIndices[j], sortedIndices[i]
			}
		}
	}

	// Acquire all object locks in sorted order
	locks := make([]*objectLock, len(objIds))
	for _, idx := range sortedIndices {
		objId := objIds[idx]
		locks[idx] = s.acquireObjectLock(objId)
		locks[idx].mu.Lock()
	}

	// Ensure all locks are released and reference counts decremented
	defer func() {
		for i, lock := range locks {
			lock.mu.Unlock()
			s.releaseObjectLock(objIds[i], lock)
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
func (s *concurrentStore) Allocator() types.Allocator {
	if provider, ok := s.innerStore.(interface{ Allocator() types.Allocator }); ok {
		return provider.Allocator()
	}
	return nil
}
