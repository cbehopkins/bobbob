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
	multiAllocator := NewMultiBlockAllocator(1024, 2, parentAllocator)

	// Allocate some blocks 
	for i := 0; i < 4; i++ {
		_, _, err := multiAllocator.Allocate()
		if err != nil {
			t.Fatalf("unexpected error during allocation: %v", err)
		}
	}

	// Ensure that we have created two block allocators
	if len(multiAllocator.allocators) != 2 {
		t.Fatalf("expected 2 block allocators, got %d", len(multiAllocator.allocators))
	}

	// Free a block and ensure it can be reallocated
	err := multiAllocator.Free(1)
	if err != nil {
		t.Fatalf("unexpected error during free: %v", err)
	}

	_, _, err = multiAllocator.Allocate()
	if err != nil {
		t.Fatalf("unexpected error during reallocation: %v", err)
	}
}
