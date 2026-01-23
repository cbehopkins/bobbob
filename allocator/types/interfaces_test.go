package types

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/internal"
)

// TestInterfaceCompatibility verifies that the interface hierarchy is properly defined
// and can be used for polymorphism. This test doesn't require actual implementations;
// it just validates the interface contract.
func TestInterfaceHierarchy(t *testing.T) {
	// These are compile-time checks that verify the interface hierarchy
	// If these compile, the interfaces are properly defined
	var _ Allocator = (*mockAllocator)(nil)
	var _ BasicAllocator = (*mockAllocator)(nil)
	var _ ManyAllocatable = (*mockAllocator)(nil)
	var _ AllocateCallbackable = (*mockAllocator)(nil)
	var _ Marsheller = (*mockAllocator)(nil)
	var _ HierarchicalAllocator = (*mockAllocator)(nil)
	var _ Introspectable = (*mockAllocator)(nil)
}

// TestMockAllocatorBasicAllocation verifies basic allocation functionality.
func TestMockAllocatorBasicAllocation(t *testing.T) {
	mock := NewMockAllocator()

	// Allocate first object
	objId1, offset1, err := mock.Allocate(100)
	if err != nil {
		t.Fatalf("First allocation failed: %v", err)
	}
	if objId1 != 1 {
		t.Errorf("Expected first ObjectId to be 1, got %d", objId1)
	}
	if offset1 != 0 {
		t.Errorf("Expected first offset to be 0, got %d", offset1)
	}

	// Allocate second object (should not overlap)
	objId2, offset2, err := mock.Allocate(200)
	if err != nil {
		t.Fatalf("Second allocation failed: %v", err)
	}
	if objId2 != 2 {
		t.Errorf("Expected second ObjectId to be 2, got %d", objId2)
	}
	if offset2 != 100 {
		t.Errorf("Expected second offset to be 100, got %d", offset2)
	}

	// Verify GetObjectInfo
	offset, size, err := mock.GetObjectInfo(objId1)
	if err != nil {
		t.Fatalf("GetObjectInfo failed: %v", err)
	}
	if offset != offset1 || size != 100 {
		t.Errorf("GetObjectInfo mismatch: got offset=%d size=%d, want offset=%d size=100", offset, size, offset1)
	}

	// Verify ContainsObjectId
	if !mock.ContainsObjectId(objId1) {
		t.Error("ContainsObjectId returned false for allocated object")
	}
	if mock.ContainsObjectId(999) {
		t.Error("ContainsObjectId returned true for non-existent object")
	}
}

// TestMockAllocatorDeletion verifies deletion functionality.
func TestMockAllocatorDeletion(t *testing.T) {
	mock := NewMockAllocator()

	objId, _, err := mock.Allocate(100)
	if err != nil {
		t.Fatalf("Allocation failed: %v", err)
	}

	// Delete the object
	err = mock.DeleteObj(objId)
	if err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}

	// Verify it's gone
	if mock.ContainsObjectId(objId) {
		t.Error("Object still exists after deletion")
	}

	_, _, err = mock.GetObjectInfo(objId)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after deletion, got %v", err)
	}

	// Deleting again should fail
	err = mock.DeleteObj(objId)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound when deleting non-existent object, got %v", err)
	}
}

// TestMockAllocatorRun verifies AllocateRun functionality.
func TestMockAllocatorRun(t *testing.T) {
	mock := NewMockAllocator()

	objIds, offsets, err := mock.AllocateRun(50, 3)
	if err != nil {
		t.Fatalf("AllocateRun failed: %v", err)
	}

	if len(objIds) != 3 || len(offsets) != 3 {
		t.Fatalf("Expected 3 allocations, got %d objIds and %d offsets", len(objIds), len(offsets))
	}

	// Verify non-overlapping allocations
	expectedOffsets := []FileOffset{0, 50, 100}
	for i := 0; i < 3; i++ {
		if offsets[i] != expectedOffsets[i] {
			t.Errorf("Allocation %d: expected offset %d, got %d", i, expectedOffsets[i], offsets[i])
		}

		// Verify each object is tracked
		if !mock.ContainsObjectId(objIds[i]) {
			t.Errorf("Object %d not tracked", objIds[i])
		}

		offset, size, err := mock.GetObjectInfo(objIds[i])
		if err != nil {
			t.Errorf("GetObjectInfo failed for object %d: %v", objIds[i], err)
		}
		if offset != offsets[i] || size != 50 {
			t.Errorf("Object %d: got offset=%d size=%d, want offset=%d size=50", objIds[i], offset, size, offsets[i])
		}
	}
}

// TestMockAllocatorCallback verifies SetOnAllocate callback functionality.
func TestMockAllocatorCallback(t *testing.T) {
	mock := NewMockAllocator()

	var callbackCalled bool
	var callbackObjId ObjectId
	var callbackOffset FileOffset
	var callbackSize int

	mock.SetOnAllocate(func(objId ObjectId, offset FileOffset, size int) {
		callbackCalled = true
		callbackObjId = objId
		callbackOffset = offset
		callbackSize = size
	})

	objId, offset, err := mock.Allocate(100)
	if err != nil {
		t.Fatalf("Allocation failed: %v", err)
	}

	if !callbackCalled {
		t.Error("Callback was not called")
	}
	if callbackObjId != objId {
		t.Errorf("Callback received wrong ObjectId: got %d, want %d", callbackObjId, objId)
	}
	if callbackOffset != offset {
		t.Errorf("Callback received wrong offset: got %d, want %d", callbackOffset, offset)
	}
	if callbackSize != 100 {
		t.Errorf("Callback received wrong size: got %d, want 100", callbackSize)
	}
}

// TestMockAllocatorInvalidInputs verifies error handling.
func TestMockAllocatorInvalidInputs(t *testing.T) {
	mock := NewMockAllocator()

	// Zero size should fail
	_, _, err := mock.Allocate(0)
	if err != ErrAllocationFailed {
		t.Errorf("Expected ErrAllocationFailed for zero size, got %v", err)
	}

	// Negative size should fail
	_, _, err = mock.Allocate(-10)
	if err != ErrAllocationFailed {
		t.Errorf("Expected ErrAllocationFailed for negative size, got %v", err)
	}

	// AllocateRun with zero count should fail
	_, _, err = mock.AllocateRun(100, 0)
	if err != ErrAllocationFailed {
		t.Errorf("Expected ErrAllocationFailed for zero count, got %v", err)
	}

	// AllocateRun with zero size should fail
	_, _, err = mock.AllocateRun(0, 5)
	if err != ErrAllocationFailed {
		t.Errorf("Expected ErrAllocationFailed for zero size, got %v", err)
	}
}

// mockAllocator is a functional implementation of all allocator interfaces for testing.
// It tracks non-overlapping allocations in memory.
type mockAllocator struct {
	nextObjectId ObjectId
	nextOffset   FileOffset
	allocations  map[ObjectId]allocationInfo
	callback     func(ObjectId, FileOffset, int)
}

type allocationInfo struct {
	offset FileOffset
	size   FileSize
}

// NewMockAllocator creates a new mock allocator for testing.
func NewMockAllocator() *mockAllocator {
	return &mockAllocator{
		nextObjectId: 1,
		nextOffset:   0,
		allocations:  make(map[ObjectId]allocationInfo),
	}
}

func (m *mockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	if size <= 0 {
		return internal.ObjNotAllocated, 0, ErrAllocationFailed
	}

	objId := m.nextObjectId
	offset := m.nextOffset

	m.allocations[objId] = allocationInfo{
		offset: offset,
		size:   FileSize(size),
	}

	m.nextObjectId++
	m.nextOffset += FileOffset(size)

	if m.callback != nil {
		m.callback(objId, offset, size)
	}

	return objId, offset, nil
}

func (m *mockAllocator) DeleteObj(objId ObjectId) error {
	if _, exists := m.allocations[objId]; !exists {
		return ErrNotFound
	}
	delete(m.allocations, objId)
	return nil
}

func (m *mockAllocator) GetObjectInfo(objId ObjectId) (FileOffset, FileSize, error) {
	info, exists := m.allocations[objId]
	if !exists {
		return 0, 0, ErrNotFound
	}
	return info.offset, info.size, nil
}

func (m *mockAllocator) GetFile() *os.File {
	return nil
}

func (m *mockAllocator) ContainsObjectId(objId ObjectId) bool {
	_, exists := m.allocations[objId]
	return exists
}

func (m *mockAllocator) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	if size <= 0 || count <= 0 {
		return nil, nil, ErrAllocationFailed
	}

	objIds := make([]ObjectId, count)
	offsets := make([]FileOffset, count)

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

func (m *mockAllocator) SetOnAllocate(callback func(ObjectId, FileOffset, int)) {
	m.callback = callback
}

func (m *mockAllocator) Marshal() ([]byte, error) {
	return nil, nil
}

func (m *mockAllocator) Unmarshal(data []byte) error {
	return nil
}

func (m *mockAllocator) Parent() Allocator {
	return nil
}

func (m *mockAllocator) GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []ObjectId {
	return nil
}
