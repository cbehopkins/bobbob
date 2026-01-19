package allocator

import (
	"testing"
)

// TestOmniBlockAllocatorExpandsWhenFull verifies that when a block allocator for a given size
// fills up, the OmniBlockAllocator provisions a new block allocator for that size rather than
// falling back to the parent for subsequent small allocations.
func TestOmniBlockAllocatorExpandsWhenFull(t *testing.T) {
	parent := NewEmptyBasicAllocator()

	// Track parent allocations: first allocations provision block allocators,
	// subsequent small allocations should NOT go to parent (bug shows they do).
	parentAllocations := make([]int, 0)
	parent.SetOnAllocate(func(_ ObjectId, _ FileOffset, size int) {
		parentAllocations = append(parentAllocations, size)
	})

	blockCount := 10 // Small count for faster test
	omni, err := NewOmniBlockAllocator(nil, blockCount, parent, nil)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator: %v", err)
	}

	// Target a specific block size (e.g., 64 bytes from default sizes)
	targetSize := 64
	expectedBlockAllocatorSize := blockCount * targetSize // 640 bytes per block allocator

	// Reset parent allocations after initial provisioning
	parentAllocations = parentAllocations[:0]

	// Fill the first block allocator (10 slots)
	for i := 0; i < blockCount; i++ {
		_, _, err := omni.Allocate(targetSize)
		if err != nil {
			t.Fatalf("allocation %d failed: %v", i, err)
		}
	}

	// At this point, parent should NOT have been called (all allocations came from preallocated block allocator)
	if len(parentAllocations) != 0 {
		t.Fatalf("expected 0 parent allocations during fill, got %d: %v", len(parentAllocations), parentAllocations)
	}

	// Now allocate one more block of the same size
	// Expected behavior: provision a new block allocator from parent (640 bytes), then allocate from it
	// Bug behavior: falls through to parent and allocates 64 bytes directly
	_, _, err = omni.Allocate(targetSize)
	if err != nil {
		t.Fatalf("overflow allocation failed: %v", err)
	}

	// Verify parent was called exactly once to provision a new block allocator
	if len(parentAllocations) != 1 {
		t.Fatalf("expected exactly 1 parent allocation for new block allocator, got %d: %v", len(parentAllocations), parentAllocations)
	}

	// Verify the parent allocation was for a full block allocator (640 bytes), not a single small block (64 bytes)
	if parentAllocations[0] != expectedBlockAllocatorSize {
		t.Errorf("expected parent allocation of %d bytes (new block allocator), got %d bytes (BUG: small allocation fell through to parent)",
			expectedBlockAllocatorSize, parentAllocations[0])
	}

	// Continue allocating to verify the new block allocator is being used
	parentAllocations = parentAllocations[:0]
	for i := 0; i < blockCount-1; i++ { // -1 because we already allocated one from the new block allocator
		_, _, err := omni.Allocate(targetSize)
		if err != nil {
			t.Fatalf("allocation %d from new block allocator failed: %v", i, err)
		}
	}

	// These should all come from the second block allocator, not the parent
	if len(parentAllocations) != 0 {
		t.Errorf("expected 0 parent allocations when using second block allocator, got %d: %v", len(parentAllocations), parentAllocations)
	}
}

// TestOmniBlockAllocatorMultipleExpansions verifies that the allocator can expand multiple times
// for the same block size, creating a pool/list of block allocators.
func TestOmniBlockAllocatorMultipleExpansions(t *testing.T) {
	parent := NewEmptyBasicAllocator()

	parentLargeAllocations := 0 // Count of block allocator provisions
	parent.SetOnAllocate(func(_ ObjectId, _ FileOffset, size int) {
		if size > 100 { // Large allocations are block allocator provisions
			parentLargeAllocations++
		}
	})

	blockCount := 5
	omni, err := NewOmniBlockAllocator(nil, blockCount, parent, nil)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator: %v", err)
	}

	targetSize := 128
	totalAllocations := blockCount * 3 // Fill 3 block allocators

	// Reset count after initial provisioning
	parentLargeAllocations = 0

	// Allocate enough to require 3 block allocators
	for i := 0; i < totalAllocations; i++ {
		_, _, err := omni.Allocate(targetSize)
		if err != nil {
			t.Fatalf("allocation %d failed: %v", i, err)
		}
	}

	// Should have provisioned 2 new block allocators (first one was preallocated)
	expectedNewAllocators := 2
	if parentLargeAllocations != expectedNewAllocators {
		t.Errorf("expected %d new block allocator provisions, got %d (BUG: allocations falling through to parent instead of expanding pool)",
			expectedNewAllocators, parentLargeAllocations)
	}
}
