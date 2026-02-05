package testutil

import (
	"os"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// MockAllocator is a functional implementation of all allocator interfaces for testing.
// It tracks non-overlapping allocations in memory.
type MockAllocator struct {
	nextObjectId bobbob.ObjectId
	nextOffset   bobbob.FileOffset
	allocations  map[bobbob.ObjectId]allocationInfo
	callback     func(bobbob.ObjectId, bobbob.FileOffset, int)
}

type allocationInfo struct {
	offset bobbob.FileOffset
	size   bobbob.FileSize
}

// NewMockAllocator creates a new mock allocator for testing.
func NewMockAllocator() *MockAllocator {
	return &MockAllocator{
		nextObjectId: 1,
		nextOffset:   1,
		allocations:  make(map[bobbob.ObjectId]allocationInfo),
	}
}

func (m *MockAllocator) Allocate(size int) (bobbob.ObjectId, bobbob.FileOffset, error) {
	if size <= 0 {
		return bobbob.ObjNotAllocated, 0, types.ErrAllocationFailed
	}

	objId := bobbob.ObjectId(m.nextOffset)
	offset := m.nextOffset

	m.allocations[objId] = allocationInfo{
		offset: offset,
		size:   bobbob.FileSize(size),
	}

	m.nextOffset += bobbob.FileOffset(size)
	m.nextObjectId = objId + 1

	if m.callback != nil {
		m.callback(objId, offset, size)
	}

	return objId, offset, nil
}

func (m *MockAllocator) DeleteObj(objId bobbob.ObjectId) error {
	if _, exists := m.allocations[objId]; !exists {
		return types.ErrNotFound
	}
	delete(m.allocations, objId)
	return nil
}

func (m *MockAllocator) GetObjectInfo(objId bobbob.ObjectId) (bobbob.FileOffset, bobbob.FileSize, error) {
	info, exists := m.allocations[objId]
	if !exists {
		return 0, 0, types.ErrNotFound
	}
	return info.offset, info.size, nil
}

func (m *MockAllocator) GetFile() *os.File {
	return nil
}

func (m *MockAllocator) ContainsObjectId(objId types.ObjectId) bool {
	_, exists := m.allocations[objId]
	return exists
}

func (m *MockAllocator) AllocateRun(size int, count int) ([]bobbob.ObjectId, []bobbob.FileOffset, error) {
	if size <= 0 || count <= 0 {
		return nil, nil, types.ErrAllocationFailed
	}

	objIds := make([]types.ObjectId, count)
	offsets := make([]types.FileOffset, count)

	for i := 0; i < count; i++ {
		objId, offset, err := m.Allocate(size)
		if err != nil {
			// Roll back any allocations we've made
			for j := 0; j < i; j++ {
				delete(m.allocations, objIds[j])
			}
			return nil, nil, err
		}
		objIds[i] = objId
		offsets[i] = offset
	}

	return objIds, offsets, nil
}

func (m *MockAllocator) SetOnAllocate(callback func(bobbob.ObjectId, bobbob.FileOffset, int)) {
	m.callback = callback
}

func (m *MockAllocator) Marshal() ([]byte, error) {
	return nil, nil
}

func (m *MockAllocator) Unmarshal(data []byte) error {
	return nil
}

func (m *MockAllocator) Parent() types.Allocator {
	return nil
}

func (m *MockAllocator) GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []bobbob.ObjectId {
	return nil
}
