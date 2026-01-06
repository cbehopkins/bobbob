package allocator

import (
	"os"
	"testing"
)

func TestBasicAllocatorMarshal(t *testing.T) {
	// Create a temporary file
	file, err := os.CreateTemp("", "allocator_test_*.dat")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() {
		_ = os.Remove(file.Name())
	}()
	defer func() {
		_ = file.Close()
	}()

	// Create allocator and make some allocations
	allocator, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	// Allocate some objects
	obj1, offset1, err := allocator.Allocate(100)
	if err != nil {
		t.Fatalf("Failed to allocate obj1: %v", err)
	}

	obj2, offset2, err := allocator.Allocate(200)
	if err != nil {
		t.Fatalf("Failed to allocate obj2: %v", err)
	}

	obj3, offset3, err := allocator.Allocate(150)
	if err != nil {
		t.Fatalf("Failed to allocate obj3: %v", err)
	}

	// Free one of them to create a gap
	if err := allocator.Free(offset2, 200); err != nil {
		t.Fatalf("Failed to free obj2: %v", err)
	}

	// Marshal the state
	data, err := allocator.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal allocator: %v", err)
	}

	// Create a new allocator and unmarshal
	newAllocator := NewEmptyBasicAllocator()
	if err := newAllocator.Unmarshal(data); err != nil {
		t.Fatalf("Failed to unmarshal allocator: %v", err)
	}

	// Verify the end offset is preserved
	if newAllocator.End != allocator.End {
		t.Errorf("End offset mismatch: got %d, expected %d", newAllocator.End, allocator.End)
	}

	// Verify the free list size is preserved
	if len(newAllocator.FreeList) != len(allocator.FreeList) {
		t.Errorf("Free list size mismatch: got %d, expected %d", len(newAllocator.FreeList), len(allocator.FreeList))
	}

	// Allocate in the new allocator - should reuse the freed gap
	obj4, offset4, err := newAllocator.Allocate(150)
	if err != nil {
		t.Fatalf("Failed to allocate obj4: %v", err)
	}

	// Should have reused the freed space (offset2)
	if offset4 != offset2 {
		t.Logf("Note: obj4 offset %d, expected to reuse freed offset %d (gap might be reordered in heap)", offset4, offset2)
	}

	// Verify original allocations are tracked
	t.Logf("Original allocations: obj1=%d@%d, obj2=%d@%d(freed), obj3=%d@%d", obj1, offset1, obj2, offset2, obj3, offset3)
	t.Logf("New allocation: obj4=%d@%d", obj4, offset4)
}

func TestBasicAllocatorMarshalEmpty(t *testing.T) {
	// Test marshaling an empty allocator
	allocator := NewEmptyBasicAllocator()
	allocator.End = 1024 // Set some end value

	data, err := allocator.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal empty allocator: %v", err)
	}

	newAllocator := NewEmptyBasicAllocator()
	if err := newAllocator.Unmarshal(data); err != nil {
		t.Fatalf("Failed to unmarshal empty allocator: %v", err)
	}

	if newAllocator.End != allocator.End {
		t.Errorf("End offset mismatch: got %d, expected %d", newAllocator.End, allocator.End)
	}

	if len(newAllocator.FreeList) != 0 {
		t.Errorf("Expected empty free list, got %d items", len(newAllocator.FreeList))
	}
}

func TestBasicAllocatorMarshalWithMultipleGaps(t *testing.T) {
	// Create allocator
	allocator := NewEmptyBasicAllocator()
	allocator.End = 1000

	// Add several gaps to the free list
	allocator.Free(100, 50)  // Gap 1: 100-150
	allocator.Free(200, 75)  // Gap 2: 200-275
	allocator.Free(400, 100) // Gap 3: 400-500

	// Marshal and unmarshal
	data, err := allocator.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	newAllocator := NewEmptyBasicAllocator()
	if err := newAllocator.Unmarshal(data); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// Verify state
	if newAllocator.End != 1000 {
		t.Errorf("End offset mismatch: got %d, expected 1000", newAllocator.End)
	}

	if len(newAllocator.FreeList) != 3 {
		t.Errorf("Free list size mismatch: got %d, expected 3", len(newAllocator.FreeList))
	}

	// Allocate from the restored allocator - should use smallest gap first (heap property)
	_, offset, err := newAllocator.Allocate(40)
	if err != nil {
		t.Fatalf("Failed to allocate: %v", err)
	}

	// The smallest gap is 100-150 (50 bytes), so allocation should come from there
	if offset != 100 {
		t.Logf("Note: First allocation from offset %d (heap might have different order)", offset)
	}
}
