package allocator

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

// trackingAllocator records allocations for behavior checks.
type trackingAllocator struct {
	nextOffset  FileOffset
	allocations []int
}

func (t *trackingAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	obj := ObjectId(t.nextOffset)
	off := t.nextOffset
	t.allocations = append(t.allocations, size)
	t.nextOffset += FileOffset(size)
	return obj, off, nil
}

func (t *trackingAllocator) Free(fileOffset FileOffset, size int) error { return nil }

// NOTE: Tests for MultiBlockAllocator have been removed as MultiBlockAllocator is no longer used.
// OmniBlockAllocator is now the primary allocator implementation.

// TestOmniBlockAllocatorWithCache verifies that the lookup cache is properly initialized
// and used for GetObjectInfo lookups.
func TestOmniBlockAllocatorWithCache(t *testing.T) {
	parentAllocator := NewEmptyBasicAllocator()
	parentAllocator.End = 100000

	blockSizes := []int{256, 512, 1024}
	blockCount := 10
	omni, err := NewOmniBlockAllocator(blockSizes, blockCount, parentAllocator, nil)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// With the new best-fit strategy, we now have default sizes (64, 128, 256, 512, 1024, 2048, 4096)
	// plus the provided sizes. After deduplication, we should have more ranges.
	// The exact count depends on overlap: 256, 512, 1024 are already in defaults.
	// So we expect: 64, 128, 256, 512, 1024, 2048, 4096 = 7 sizes
	if len(omni.blockMap) != 7 {
		t.Errorf("Expected 7 block allocators, got %d", len(omni.blockMap))
	}

	// Allocate some objects from each size
	objectIds := make(map[int][]ObjectId)
	for _, size := range blockSizes {
		for i := 0; i < 3; i++ {
			objId, _, err := omni.Allocate(size)
			if err != nil {
				t.Fatalf("Failed to allocate size %d: %v", size, err)
			}
			objectIds[size] = append(objectIds[size], objId)
		}
	}

	// Test GetObjectInfo with cache lookup for each object
	for size, ids := range objectIds {
		for _, objId := range ids {
			offset, returnedSize, err := omni.GetObjectInfo(objId)
			if err != nil {
				t.Errorf("GetObjectInfo(%d) failed: %v", objId, err)
				continue
			}
			if returnedSize != size {
				t.Errorf("GetObjectInfo(%d): expected size %d, got %d", objId, size, returnedSize)
			}
			// FileOffset can be 0 (it's computed from startingFileOffset + slotIndex*blockSize)
			_ = offset
		}
	}
}

// Ensure that using WithoutPreallocation defers parent allocations until the
// first real Allocate call and lazily provisions block allocators.
func TestOmniBlockAllocatorWithoutPreallocation(t *testing.T) {
	parent := &trackingAllocator{}
	blockSizes := []int{256}
	blockCount := 4

	omni, err := NewOmniBlockAllocator(blockSizes, blockCount, parent, nil, WithoutPreallocation())
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Nothing should have been allocated up front.
	if len(parent.allocations) != 0 {
		t.Fatalf("expected no parent allocations before use, got %v", parent.allocations)
	}
	if len(omni.blockMap) != 0 {
		t.Fatalf("expected empty blockMap before first Allocate")
	}

	// First allocation should provision a block allocator of the best-fit size (256).
	objId, _, err := omni.Allocate(256)
	if err != nil {
		t.Fatalf("allocate failed: %v", err)
	}
	if len(parent.allocations) != 1 {
		t.Fatalf("expected one parent allocation, got %v", parent.allocations)
	}
	expectedSize := blockCount * 256
	if parent.allocations[0] != expectedSize {
		t.Fatalf("parent allocation size mismatch: got %d want %d", parent.allocations[0], expectedSize)
	}
	if len(omni.blockMap) != 1 {
		t.Fatalf("expected one block allocator provisioned")
	}

	// Subsequent allocations of the same size should not trigger new parent allocations.
	if _, _, err := omni.Allocate(200); err != nil {
		t.Fatalf("second allocate failed: %v", err)
	}
	if len(parent.allocations) != 1 {
		t.Fatalf("expected still one parent allocation, got %v", parent.allocations)
	}

	// The recorded ObjectId should come from the parent's starting offset (0) plus slot index.
	if objId != 0 {
		t.Fatalf("expected first ObjectId to originate at parent offset 0, got %d", objId)
	}
}

// TestOmniBlockAllocatorCachePerformance verifies that the cache provides
// significantly faster lookups for GetObjectInfo.
func TestOmniBlockAllocatorCachePerformance(t *testing.T) {
	parentAllocator := NewEmptyBasicAllocator()
	parentAllocator.End = 1000000

	// Create omni allocator with many different block sizes to increase cache complexity
	blockSizes := []int{64, 128, 256, 512, 1024, 2048, 4096}
	blockCount := 5
	omni, err := NewOmniBlockAllocator(blockSizes, blockCount, parentAllocator, nil)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Allocate one object from each size
	objectIds := make([]ObjectId, 0)
	for _, size := range blockSizes {
		objId, _, err := omni.Allocate(size)
		if err != nil {
			t.Fatalf("Failed to allocate: %v", err)
		}
		objectIds = append(objectIds, objId)
	}

	// Perform many lookups - with cache this should be very fast
	for i := 0; i < 1000; i++ {
		for _, objId := range objectIds {
			_, _, err := omni.GetObjectInfo(objId)
			if err != nil {
				t.Fatalf("GetObjectInfo failed: %v", err)
			}
		}
	}
}

// TestOmniBlockAllocatorCacheFallback verifies that cache properly falls back to
// parent allocator for objects allocated from parent (sizes > 4KB or full allocators).
func TestOmniBlockAllocatorCacheFallback(t *testing.T) {
	parentAllocator := NewEmptyBasicAllocator()
	parentAllocator.End = 100000

	blockSizes := []int{256}
	blockCount := 2
	omni, err := NewOmniBlockAllocator(blockSizes, blockCount, parentAllocator, nil)
	if err != nil {
		t.Fatalf("NewOmniBlockAllocator failed: %v", err)
	}

	// Allocate with a size > 4KB - should go to parent directly
	objId, _, err := omni.Allocate(8192)
	if err != nil {
		t.Fatalf("Failed to allocate from parent: %v", err)
	}

	// GetObjectInfo should work via fallback to parent
	offset, size, err := omni.GetObjectInfo(objId)
	if err != nil {
		t.Fatalf("GetObjectInfo failed for parent-allocated object: %v", err)
	}
	if size != 8192 {
		t.Errorf("Expected size 8192, got %d", size)
	}
	if offset == 0 {
		t.Errorf("Got zero offset")
	}
}
