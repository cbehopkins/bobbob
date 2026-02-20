package store

import (
	"sync"
	"sync/atomic"
)

// ObjectLock wraps a mutex with a reference count for garbage collection.
// When refCount drops to 0, the lock can be safely removed from the map.
type ObjectLock struct {
	Mu       sync.RWMutex
	refCount atomic.Int32
}

// objectLockPool reduces allocation overhead by reusing ObjectLock structs.
var objectLockPool = sync.Pool{
	New: func() any {
		return &ObjectLock{}
	},
}

// ObjectLockMap provides per-object locking with automatic cleanup.
// Multiple components can embed this to get per-object locking behavior.
type ObjectLockMap struct {
	mu      sync.RWMutex
	lockMap map[ObjectId]*ObjectLock
}

// NewObjectLockMap creates a new ObjectLockMap.
func NewObjectLockMap() *ObjectLockMap {
	return &ObjectLockMap{
		lockMap: make(map[ObjectId]*ObjectLock),
	}
}

// AcquireObjectLock looks up or creates an object lock and increments its reference count.
// Returns the lock for the caller to lock/unlock.
// Uses objectLockPool to reduce allocation overhead.
func (m *ObjectLockMap) AcquireObjectLock(objectId ObjectId) *ObjectLock {
	m.mu.Lock()
	objLock, ok := m.lockMap[objectId]
	if !ok {
		objLock = objectLockPool.Get().(*ObjectLock)
		// refCount is already 0 from reset or new allocation
		m.lockMap[objectId] = objLock
	}
	objLock.refCount.Add(1)
	m.mu.Unlock()
	return objLock
}

// ReleaseObjectLock decrements the reference count and removes the lock from the map if unused.
// Returns the lock to objectLockPool for reuse to reduce allocation overhead.
func (m *ObjectLockMap) ReleaseObjectLock(objectId ObjectId, objLock *ObjectLock) {
	if objLock.refCount.Add(-1) == 0 {
		// Reference count is now 0; try to remove from map
		m.mu.Lock()
		// Double-check refCount is still 0 (race protection)
		if objLock.refCount.Load() == 0 {
			delete(m.lockMap, objectId)
			objLock.refCount.Store(0) // Reset for pool reuse (safety)
			objectLockPool.Put(objLock)
		}
		m.mu.Unlock()
	}
}

// DeleteLock forcibly removes a lock from the map.
// Used when an object is deleted to prevent memory leaks.
func (m *ObjectLockMap) DeleteLock(objectId ObjectId) {
	m.mu.Lock()
	delete(m.lockMap, objectId)
	m.mu.Unlock()
}

// LockCount returns the current number of locks in the map.
// Useful for testing and debugging.
func (m *ObjectLockMap) LockCount() int {
	m.mu.RLock()
	count := len(m.lockMap)
	m.mu.RUnlock()
	return count
}

// HasLock returns true if a lock exists for the given object ID.
// Useful for testing and debugging.
func (m *ObjectLockMap) HasLock(objectId ObjectId) bool {
	m.mu.RLock()
	_, exists := m.lockMap[objectId]
	m.mu.RUnlock()
	return exists
}
