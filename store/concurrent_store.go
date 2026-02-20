package store

import (
	"io"
	"sync"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// concurrentStore wraps any Storer with per-object locking to allow
// safe concurrent access to different objects.
// Optionally limits concurrent disk I/O operations via a token pool.
type concurrentStore struct {
	innerStore Storer
	lock       sync.RWMutex
	objectLocks *ObjectLockMap
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
		objectLocks: NewObjectLockMap(),
		diskTokens: diskTokens,
	}
}

// acquireObjectLock is a convenience wrapper for ObjectLockMap.AcquireObjectLock.
func (s *concurrentStore) acquireObjectLock(objectId ObjectId) *ObjectLock {
	return s.objectLocks.AcquireObjectLock(objectId)
}

// releaseObjectLock is a convenience wrapper for ObjectLockMap.ReleaseObjectLock.
func (s *concurrentStore) releaseObjectLock(objectId ObjectId, objLock *ObjectLock) {
	s.objectLocks.ReleaseObjectLock(objectId, objLock)
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
	objLock.Mu.Lock()
	newFinisher := func() error {
		defer objLock.Mu.Unlock()
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
	objLock.Mu.Lock()
	writer, finisher, err := s.innerStore.WriteToObj(objectId)
	if err != nil {
		objLock.Mu.Unlock()
		s.releaseObjectLock(objectId, objLock)
		if s.diskTokens != nil {
			s.diskTokens <- struct{}{} // Release on error
		}
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.Mu.Unlock()
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
	objLock.Mu.RLock()
	reader, finisher, err := s.innerStore.LateReadObj(offset)
	if err != nil {
		objLock.Mu.RUnlock()
		s.releaseObjectLock(offset, objLock)
		if s.diskTokens != nil {
			s.diskTokens <- struct{}{} // Release on error
		}
		return nil, nil, err
	}
	newFinisher := func() error {
		defer objLock.Mu.RUnlock()
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
	locks := make([]*ObjectLock, len(objIds))
	for _, idx := range sortedIndices {
		objId := objIds[idx]
		locks[idx] = s.acquireObjectLock(objId)
		locks[idx].Mu.Lock()
	}

	// Ensure all locks are released and reference counts decremented
	defer func() {
		for i, lock := range locks {
			lock.Mu.Unlock()
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
	s.objectLocks.DeleteLock(objId) // Clean up lock map while we have the lock
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
	if provider, ok := s.innerStore.(AllocatorProvider); ok {
		return provider.Allocator()
	}
	return nil
}

// NewStringObj stores a string and returns its ObjectId.
// StringStore manages its own ObjectId namespace and internal locking, so no global lock needed.
// Unlike NewObj(), this doesn't modify the main allocator state.
func (s *concurrentStore) NewStringObj(data string) (bobbob.ObjectId, error) {
	if stringer, ok := s.innerStore.(bobbob.StringStorer); ok {
		return stringer.NewStringObj(data)
	}
	return bobbob.ObjNotAllocated, bobbob.ErrStringStorerNotSupported
}

// StringFromObjId retrieves a string by ObjectId.
// Delegates to the wrapped store's StringStorer interface if available.
func (s *concurrentStore) StringFromObjId(objId bobbob.ObjectId) (string, error) {
	// StringStorer methods are read-only on the StringStore (which has its own locks)
	// We don't need to acquire our per-object lock here since StringStore is thread-safe
	if stringer, ok := s.innerStore.(bobbob.StringStorer); ok {
		return stringer.StringFromObjId(objId)
	}
	return "", bobbob.ErrStringStorerNotSupported
}

// HasStringObj checks if a given ObjectId is a string object in this store.
// Delegates to the wrapped store's StringStorer interface if available.
func (s *concurrentStore) HasStringObj(objId bobbob.ObjectId) bool {
	if stringer, ok := s.innerStore.(bobbob.StringStorer); ok {
		return stringer.HasStringObj(objId)
	}
	return false
}
