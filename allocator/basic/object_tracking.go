package basic

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"

	"github.com/cbehopkins/bobbob/allocator/types"
)

// objectTracker defines the interface for object location tracking
type objectTracker interface {
	Get(objId types.ObjectId) (types.FileSize, bool)
	Set(objId types.ObjectId, size types.FileSize)
	Delete(objId types.ObjectId)
	Len() int
	Contains(objId types.ObjectId) bool
	ForEach(fn func(types.ObjectId, types.FileSize))
	GetAllObjectIds() []types.ObjectId
	Clear()
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// memoryObjectTracker tracks object locations and sizes in memory.
// This is the original in-memory implementation.
type memoryObjectTracker struct {
	mu    sync.RWMutex
	store map[types.ObjectId]types.FileSize
}

// newMemoryObjectTracker creates a new empty object tracking map.
func newMemoryObjectTracker() *memoryObjectTracker {
	return &memoryObjectTracker{
		store: make(map[types.ObjectId]types.FileSize),
	}
}

// Get retrieves the size for an ObjectId.
// Returns (size, found).
func (om *memoryObjectTracker) Get(objId types.ObjectId) (types.FileSize, bool) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	size, exists := om.store[objId]
	return size, exists
}

// Set stores the size for an ObjectId.
func (om *memoryObjectTracker) Set(objId types.ObjectId, size types.FileSize) {
	om.mu.Lock()
	defer om.mu.Unlock()

	om.store[objId] = size
}

// Delete removes an ObjectId from tracking.
func (om *memoryObjectTracker) Delete(objId types.ObjectId) {
	om.mu.Lock()
	defer om.mu.Unlock()

	delete(om.store, objId)
}

// Len returns the number of tracked objects.
func (om *memoryObjectTracker) Len() int {
	om.mu.RLock()
	defer om.mu.RUnlock()

	return len(om.store)
}

// Contains returns whether an ObjectId is tracked.
func (om *memoryObjectTracker) Contains(objId types.ObjectId) bool {
	om.mu.RLock()
	defer om.mu.RUnlock()

	_, exists := om.store[objId]
	return exists
}

// ForEach iterates over all tracked objects, calling fn for each.
// The function signature is fn(objId, size).
func (om *memoryObjectTracker) ForEach(fn func(types.ObjectId, types.FileSize)) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	for objId, size := range om.store {
		fn(objId, size)
	}
}

// GetAllObjectIds returns a slice of all tracked ObjectIds.
// Useful for iteration without holding locks during processing.
func (om *memoryObjectTracker) GetAllObjectIds() []types.ObjectId {
	om.mu.RLock()
	defer om.mu.RUnlock()

	objIds := make([]types.ObjectId, 0, len(om.store))
	for objId := range om.store {
		objIds = append(objIds, objId)
	}
	return objIds
}

// Clear removes all tracked objects.
func (om *memoryObjectTracker) Clear() {
	om.mu.Lock()
	defer om.mu.Unlock()

	om.store = make(map[types.ObjectId]types.FileSize)
}

// Marshal serializes the tracked objects to bytes.
// Format: numEntries (4) | [objId (8) | size (8)]...
// Entries are sorted by ObjectId for deterministic output.
func (om *memoryObjectTracker) Marshal() ([]byte, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	numEntries := uint32(len(om.store))
	totalSize := 4 + (numEntries * 16) // count + entries
	data := make([]byte, totalSize)

	// Write entry count
	binary.LittleEndian.PutUint32(data[0:4], numEntries)

	// Write entries (sorted by ObjectId for deterministic output)
	objIds := make([]types.ObjectId, 0, len(om.store))
	for objId := range om.store {
		objIds = append(objIds, objId)
	}
	sort.Slice(objIds, func(i, j int) bool {
		return objIds[i] < objIds[j]
	})

	offset := 4
	for _, objId := range objIds {
		size := om.store[objId]
		binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(objId))
		binary.LittleEndian.PutUint64(data[offset+8:offset+16], uint64(size))
		offset += 16
	}

	return data, nil
}

// Unmarshal restores tracked objects from bytes.
// Clears existing state before restoring.
func (om *memoryObjectTracker) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("insufficient data for objectTracker unmarshal")
	}

	// Read entry count
	numEntries := binary.LittleEndian.Uint32(data[0:4])
	expectedSize := 4 + (numEntries * 16)

	if len(data) < int(expectedSize) {
		return errors.New("insufficient data for objectTracker entries")
	}

	om.mu.Lock()
	defer om.mu.Unlock()

	// Clear existing state
	om.store = make(map[types.ObjectId]types.FileSize)

	// Restore objects
	offset := 4
	for i := uint32(0); i < numEntries; i++ {
		objId := types.ObjectId(binary.LittleEndian.Uint64(data[offset : offset+8]))
		size := types.FileSize(binary.LittleEndian.Uint64(data[offset+8 : offset+16]))
		om.store[objId] = size
		offset += 16
	}

	return nil
}
