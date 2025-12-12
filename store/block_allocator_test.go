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

func TestMultiBlockAllocatorRequestsNewBlockFromParent(t *testing.T) {
	parentAllocator := &MockAllocator{}
	blockSize := 1024
	blockCount := 2
	multiAllocator := NewMultiBlockAllocator(blockSize, blockCount, parentAllocator)

	// Track if preParentAllocate is called
	preParentAllocateCallCount := 0
	multiAllocator.preParentAllocate = func(size int) error {
		preParentAllocateCallCount += 1
		return nil
	}

	// Allocate all slots in the first block
	for i := 0; i < blockCount; i++ {
		_, _, err := multiAllocator.Allocate(blockSize)
		if err != nil {
			t.Fatalf("unexpected error during allocation: %v", err)
		}
	}
	if preParentAllocateCallCount != 1 {
		t.Fatalf("expected preParentAllocate have been called once, but it was called %d times", preParentAllocateCallCount)
	}

	// Allocate one more block, which should trigger a request to the parent
	_, _, err := multiAllocator.Allocate(blockSize)
	if err != nil {
		t.Fatalf("unexpected error during allocation: %v", err)
	}

	// Ensure that preParentAllocate was called
	if preParentAllocateCallCount != 2 {
		t.Fatalf("expected preParentAllocate have been called again, but it was called %d times", preParentAllocateCallCount)
	}

	// Ensure that a new block allocator was created
	if len(multiAllocator.allocators) != 2 {
		t.Fatalf("expected 2 block allocators, got %d", len(multiAllocator.allocators))
	}
}

func TestMultiBlockAllocatorReusesDeallocatedSegment(t *testing.T) {
	parentAllocator := &MockAllocator{}
	blockSize := 1024
	blockCount := 1
	multiAllocator := NewMultiBlockAllocator(blockSize, blockCount, parentAllocator)

	// Track if preParentAllocate is called
	preParentAllocateCallCount := 0
	multiAllocator.preParentAllocate = func(size int) error {
		preParentAllocateCallCount += 1
		return nil
	}

	// Map to track allocation number to ObjectId and FileOffset
	allocationMap := make(map[int]struct {
		objectId   ObjectId
		fileOffset FileOffset
	})

	// Allocate all slots in the first block
	for i := 0; i < blockCount; i++ {
		objectId, fileOffset, err := multiAllocator.Allocate(blockSize)
		if err != nil {
			t.Fatalf("unexpected error during allocation: %v", err)
		}
		allocationMap[i] = struct {
			objectId   ObjectId
			fileOffset FileOffset
		}{objectId, fileOffset}
	}
	if preParentAllocateCallCount != 1 {
		t.Fatalf("expected preParentAllocate to have been called once, but it was called %d times", preParentAllocateCallCount)
	}

	// Test cases for first, middle, and last allocations
	testCases := []int{0, blockCount / 2, blockCount - 1}
	for _, testCase := range testCases {
		// Deallocate one segment using a tracked allocation
		toFree := allocationMap[testCase]
		err := multiAllocator.Free(toFree.fileOffset, blockSize)
		if err != nil {
			t.Fatalf("unexpected error during deallocation for test case %d: %v", testCase, err)
		}

		// Allocate again, which should reuse the deallocated segment
		_, _, err = multiAllocator.Allocate(blockSize)
		if err != nil {
			t.Fatalf("unexpected error during reallocation for test case %d: %v", testCase, err)
		}

		// Ensure that preParentAllocate was not called again
		if preParentAllocateCallCount != 1 {
			t.Fatalf("expected preParentAllocate to not be called again for test case %d, but it was called %d times", testCase, preParentAllocateCallCount)
		}
	}
}

func TestMultiBlockAllocatorReusesDeallocatedSegmentsAcrossBlocks(t *testing.T) {
	parentAllocator := &MockAllocator{}
	blockSize := 1024
	blockCount := 3 // Number of allocation units in a block
	numBlocks := 2  // Number of allocation blocks to allocate
	multiAllocator := NewMultiBlockAllocator(blockSize, blockCount, parentAllocator)

	// Track if preParentAllocate is called
	preParentAllocateCallCount := 0
	multiAllocator.preParentAllocate = func(size int) error {
		preParentAllocateCallCount += 1
		return nil
	}

	// Map to track allocation number to ObjectId and FileOffset
	allocationMap := make(map[int]struct {
		objectId   ObjectId
		fileOffset FileOffset
	})

	// Calculate total allocations
	totalAllocations := blockCount * numBlocks

	// Allocate all slots in all blocks
	for i := 0; i < totalAllocations; i++ {
		objectId, fileOffset, err := multiAllocator.Allocate(blockSize)
		if err != nil {
			t.Fatalf("unexpected error during allocation: %v", err)
		}
		allocationMap[i] = struct {
			objectId   ObjectId
			fileOffset FileOffset
		}{objectId, fileOffset}
	}
	if preParentAllocateCallCount != numBlocks {
		t.Fatalf("expected preParentAllocate to have been called %d times, but it was called %d times", numBlocks, preParentAllocateCallCount)
	}

	// Deallocate segments in the first and second blocks
	for i := 0; i < 2; i++ {
		bob, _ := allocationMap[i]
		err := multiAllocator.Free(bob.fileOffset, blockSize)
		if err != nil {
			t.Fatalf("unexpected error during deallocation: %v", err)
		}
	}

	// Allocate again, which should reuse the deallocated segments
	for i := 0; i < 2; i++ {
		_, _, err := multiAllocator.Allocate(blockSize)
		if err != nil {
			t.Fatalf("unexpected error during reallocation: %v", err)
		}
	}

	// Ensure that preParentAllocate was not called again
	if preParentAllocateCallCount != numBlocks {
		t.Fatalf("expected preParentAllocate to not be called again, but it was called %d times", preParentAllocateCallCount)
	}

	// Allocate one more block, which should trigger a new parent allocation
	_, _, err := multiAllocator.Allocate(blockSize)
	if err != nil {
		t.Fatalf("unexpected error during allocation: %v", err)
	}

	// Ensure that preParentAllocate was called again
	if preParentAllocateCallCount != numBlocks+1 {
		t.Fatalf("expected preParentAllocate to have been called %d times, but it was called %d times", numBlocks+1, preParentAllocateCallCount)
	}
}
