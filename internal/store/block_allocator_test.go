package store

import (
	"testing"
)

type MockAllocator struct{}

func (m *MockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	return 1, 1, nil
}

func (m *MockAllocator) Free(fileOffset FileOffset, size int) error {
	return nil
}

func TestMultiBlockAllocator(t *testing.T) {
	parentAllocator := &MockAllocator{}
	blockSize := 1024
	multiAllocator := NewMultiBlockAllocator(blockSize, 2, parentAllocator)

	// Allocate some blocks
	for i := 0; i < 4; i++ {
		_, _, err := multiAllocator.Allocate(blockSize)
		if err != nil {
			t.Fatalf("unexpected error during allocation: %v", err)
		}
	}

	// Ensure that we have created two block allocators
	if len(multiAllocator.allocators) != 2 {
		t.Fatalf("expected 2 block allocators, got %d", len(multiAllocator.allocators))
	}

	// Free a block and ensure it can be reallocated
	err := multiAllocator.Free(1, 1024) // Pass the block size explicitly
	if err != nil {
		t.Fatalf("unexpected error during free: %v", err)
	}

	_, _, err = multiAllocator.Allocate(blockSize)
	if err != nil {
		t.Fatalf("unexpected error during reallocation: %v", err)
	}
}

func TestMultiBlockAllocatorMarshalUnmarshal(t *testing.T) {
	parentAllocator := &MockAllocator{}
	blockSize := 1024
	multiAllocator := NewMultiBlockAllocator(blockSize, 2, parentAllocator)

	// Allocate some blocks
	for i := 0; i < 4; i++ {
		_, _, err := multiAllocator.Allocate(blockSize)
		if err != nil {
			t.Fatalf("unexpected error during allocation: %v", err)
		}
	}

	// Marshal the multiBlockAllocator
	data, err := multiAllocator.Marshal()
	if err != nil {
		t.Fatalf("unexpected error during marshal: %v", err)
	}

	// Create a new multiBlockAllocator and unmarshal the data
	newMultiAllocator := NewMultiBlockAllocator(1024, 2, parentAllocator)
	err = newMultiAllocator.Unmarshal(data)
	if err != nil {
		t.Fatalf("unexpected error during unmarshal: %v", err)
	}

	// Verify that the state is preserved
	if len(newMultiAllocator.allocators) != len(multiAllocator.allocators) {
		t.Fatalf("expected %d allocators, got %d", len(multiAllocator.allocators), len(newMultiAllocator.allocators))
	}

	for i, allocator := range newMultiAllocator.allocators {
		if len(allocator.allocatedList) != len(multiAllocator.allocators[i].allocatedList) {
			t.Fatalf("allocator %d: expected %d blocks, got %d", i, len(multiAllocator.allocators[i].allocatedList), len(allocator.allocatedList))
		}
		for j, allocated := range allocator.allocatedList {
			if allocated != multiAllocator.allocators[i].allocatedList[j] {
				t.Fatalf("allocator %d, block %d: expected %v, got %v", i, j, multiAllocator.allocators[i].allocatedList[j], allocated)
			}
		}
	}

	if len(newMultiAllocator.startOffsets) != len(multiAllocator.startOffsets) {
		t.Fatalf("expected %d startOffsets, got %d", len(multiAllocator.startOffsets), len(newMultiAllocator.startOffsets))
	}

	for i, offset := range newMultiAllocator.startOffsets {
		if offset != multiAllocator.startOffsets[i] {
			t.Fatalf("startOffset %d: expected %d, got %d", i, multiAllocator.startOffsets[i], offset)
		}
	}
}
