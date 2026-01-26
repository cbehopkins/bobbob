package store

import (
	"fmt"
	"sync"

	"github.com/cbehopkins/bobbob/allocator"
	"github.com/cbehopkins/bobbob/allocator/types"
	"github.com/cbehopkins/bobbob/internal"
)

// TopAllocatorAdapter adapts the new bobbob/allocator.TopAllocator to work with
// the store. The key difference is:
//   - Old allocator: Free(fileOffset, size)
//   - New allocator: DeleteObj(objectId)
//
// The adapter provides both interfaces and adds mutex protection for concurrent access,
// since TopAllocator is not internally synchronized.
type TopAllocatorAdapter struct {
	mu  sync.Mutex
	top *allocator.Top
}

// NewTopAllocatorAdapter creates a new adapter wrapping a TopAllocator.
func NewTopAllocatorAdapter(top *allocator.Top) *TopAllocatorAdapter {
	return &TopAllocatorAdapter{
		top: top,
	}
}

// Allocate delegates to the TopAllocator's Allocate method.
func (a *TopAllocatorAdapter) Allocate(size int) (internal.ObjectId, internal.FileOffset, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	objId, fileOffset, err := a.top.Allocate(size)
	return internal.ObjectId(objId), internal.FileOffset(fileOffset), err
}

// Free is the old interface (offset/size based). It's not recommended for new code
// but is kept for backwards compatibility. The problem is we can't reliably map
// offset/size back to ObjectId, so this will error.
func (a *TopAllocatorAdapter) Free(fileOffset internal.FileOffset, size int) error {
	return fmt.Errorf("TopAllocatorAdapter.Free(offset=%d, size=%d) is not supported - "+
		"use DeleteObjID() instead to delete by ObjectId", fileOffset, size)
}

// DeleteObjID deletes an object by its ObjectId. This is the preferred method.
func (a *TopAllocatorAdapter) DeleteObjID(objId internal.ObjectId) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.top.DeleteObj(types.ObjectId(objId))
}

// AllocateRun allocates multiple contiguous ObjectIds efficiently.
func (a *TopAllocatorAdapter) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	objIds, fileOffsets, err := a.top.AllocateRun(size, count)
	if err != nil {
		return nil, nil, err
	}

	// Convert from allocator/types to store types
	storeObjIds := make([]ObjectId, len(objIds))
	storeOffsets := make([]FileOffset, len(fileOffsets))

	for i := 0; i < len(objIds); i++ {
		storeObjIds[i] = ObjectId(objIds[i])
		storeOffsets[i] = FileOffset(fileOffsets[i])
	}

	return storeObjIds, storeOffsets, nil
}

// SetStoreMeta passes store metadata to the underlying Top allocator for PrimeTable persistence.
func (a *TopAllocatorAdapter) SetStoreMeta(fi allocator.FileInfo) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.top.SetStoreMeta(fi)
}

// GetStoreMeta retrieves store metadata loaded from the PrimeTable.
func (a *TopAllocatorAdapter) GetStoreMeta() allocator.FileInfo {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.top.GetStoreMeta()
}

// Save persists the allocator's state to disk.
func (a *TopAllocatorAdapter) Save() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.top.Save()
}
