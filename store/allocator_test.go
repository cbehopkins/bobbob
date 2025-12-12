package store

import (
	"container/heap"
	"os"
	"testing"
)

func TestNewAllocator(t *testing.T) {
	file, err := os.CreateTemp("", "allocator_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	allocator, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	if allocator.end != 0 {
		t.Errorf("Expected end to be 0, got %d", allocator.end)
	}

	if len(allocator.freeList) != 0 {
		t.Errorf("Expected freeList to be empty, got %d", len(allocator.freeList))
	}
}

func TestAllocate(t *testing.T) {
	file, err := os.CreateTemp("", "allocator_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	allocator, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	objId, fileOffset, err := allocator.Allocate(100)
	if err != nil {
		t.Fatalf("Failed to allocate space: %v", err)
	}

	if objId != 0 {
		t.Errorf("Expected objId to be 0, got %d", objId)
	}

	if fileOffset != 0 {
		t.Errorf("Expected fileOffset to be 0, got %d", fileOffset)
	}

	if allocator.end != 100 {
		t.Errorf("Expected end to be 100, got %d", allocator.end)
	}
}

func TestFree(t *testing.T) {
	file, err := os.CreateTemp("", "allocator_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	allocator, err := NewBasicAllocator(file)
	if err != nil {
		t.Fatalf("Failed to create allocator: %v", err)
	}

	_, _, err = allocator.Allocate(100)
	if err != nil {
		t.Fatalf("Failed to allocate space: %v", err)
	}

	err = allocator.Free(0, 100)
	if err != nil {
		t.Fatalf("Failed to free space: %v", err)
	}

	if len(allocator.freeList) != 1 {
		t.Errorf("Expected freeList to have 1 element, got %d", len(allocator.freeList))
	}

	gap := heap.Pop(&allocator.freeList).(Gap)
	if gap.Start != 0 || gap.End != 100 {
		t.Errorf("Expected gap to be {0, 100}, got {%d, %d}", gap.Start, gap.End)
	}
}
