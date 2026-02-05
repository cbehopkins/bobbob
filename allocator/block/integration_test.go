package block_test

import (
	"testing"

	"github.com/cbehopkins/bobbob/allocator/block"
	"github.com/cbehopkins/bobbob/allocator/testutil"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestBlockAllocatorWithParentTracking demonstrates using parent allocator callback
// to track child BlockAllocator allocations.
func TestBlockAllocatorWithParentTracking(t *testing.T) {
	// Create mock parent allocator
	mockParent := testutil.NewMockAllocator()

	// Track all allocations through callback
	var allocationLog []types.ObjectId
	mockParent.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, size int) {
		allocationLog = append(allocationLog, objId)
	})

	// Parent allocates space for BlockAllocator (simulates PoolAllocator behavior)
	const totalSize = 10 * 1024
	parentObjId, parentOffset, err := mockParent.Allocate(totalSize)
	if err != nil {
		t.Fatalf("Parent allocation failed: %v", err)
	}

	// Verify parent callback was invoked
	if len(allocationLog) != 1 || allocationLog[0] != parentObjId {
		t.Errorf("Expected parent callback for ObjectId %d, got log: %v", parentObjId, allocationLog)
	}

	// Create BlockAllocator as child of parent's allocation
	const blockSize = 1024
	const blockCount = 10
	blockAlloc := block.New(blockSize, blockCount, parentOffset, parentObjId, nil)

	// Verify child's range is within parent's allocation
	if !mockParent.ContainsObjectId(parentObjId) {
		t.Error("Parent should contain its own ObjectId")
	}

	// Allocate from BlockAllocator
	childObjId1, childOffset1, err := blockAlloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("First child allocation failed: %v", err)
	}

	// Child ObjectId should be within parent's range
	// (In real hierarchy, parent would subdivide its ObjectId range to child)
	if childObjId1 != parentObjId {
		t.Errorf("Expected child to use parent's ObjectId range starting at %d, got %d", parentObjId, childObjId1)
	}

	// Child offset should be exactly at parent's offset
	if childOffset1 != parentOffset {
		t.Errorf("Expected child offset to start at parent offset %d, got %d", parentOffset, childOffset1)
	}

	// Allocate second block from child
	childObjId2, childOffset2, err := blockAlloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("Second child allocation failed: %v", err)
	}

	// Verify non-overlapping allocations
	if childObjId2 != parentObjId+1 {
		t.Errorf("Expected sequential ObjectId %d, got %d", parentObjId+1, childObjId2)
	}
	if childOffset2 != parentOffset+types.FileOffset(blockSize) {
		t.Errorf("Expected offset %d, got %d", parentOffset+types.FileOffset(blockSize), childOffset2)
	}

	// Verify child allocation is within parent's boundaries
	endOffset := childOffset2 + types.FileOffset(blockSize)
	maxOffset := parentOffset + types.FileOffset(totalSize)
	if endOffset > maxOffset {
		t.Errorf("Child allocation at %d exceeds parent boundary at %d", endOffset, maxOffset)
	}
}

// TestBlockAllocatorObjectIdUniqueness verifies ObjectId uniqueness in hierarchy.
func TestBlockAllocatorObjectIdUniqueness(t *testing.T) {
	// Create two mock parent allocations (simulating two pools)
	mockParent := testutil.NewMockAllocator()

	// Pool 1: use starting ObjectId based on offset to avoid overlap
	pool1ObjId, pool1Offset, _ := mockParent.Allocate(5 * 1024)
	// Use offset 10000 to create non-overlapping ObjectId range
	pool1StartObj := types.ObjectId(10000)
	blockAlloc1 := block.New(1024, 5, pool1Offset, pool1StartObj, nil)

	// Pool 2: different offset, different ObjectId range
	pool2ObjId, pool2Offset, _ := mockParent.Allocate(5 * 1024)
	// Use offset 20000 to create non-overlapping ObjectId range
	pool2StartObj := types.ObjectId(20000)
	blockAlloc2 := block.New(1024, 5, pool2Offset, pool2StartObj, nil)

	// Verify pools have different starting points
	if pool1ObjId == pool2ObjId {
		t.Fatal("Pools should have different ObjectIds")
	}
	if pool1Offset == pool2Offset {
		t.Fatal("Pools should have different offsets")
	}
	if pool1StartObj == pool2StartObj {
		t.Fatal("Pools should have different ObjectId ranges")
	}

	// Allocate from both pools
	obj1, _, _ := blockAlloc1.Allocate(1024)
	obj2, _, _ := blockAlloc2.Allocate(1024)

	// ObjectIds should be unique across pools
	if obj1 == obj2 {
		t.Errorf("ObjectIds should be unique across pools: both are %d", obj1)
	}

	// Verify ContainsObjectId respects boundaries
	if blockAlloc1.ContainsObjectId(obj2) {
		t.Errorf("Pool 1 [%d..%d) should not contain Pool 2's ObjectId %d", pool1StartObj, pool1StartObj+5, obj2)
	}
	if blockAlloc2.ContainsObjectId(obj1) {
		t.Errorf("Pool 2 [%d..%d) should not contain Pool 1's ObjectId %d", pool2StartObj, pool2StartObj+5, obj1)
	}

	// Each pool should contain its own allocations
	if !blockAlloc1.ContainsObjectId(obj1) {
		t.Error("Pool 1 should contain its own ObjectId")
	}
	if !blockAlloc2.ContainsObjectId(obj2) {
		t.Error("Pool 2 should contain its own ObjectId")
	}
}

// TestBlockAllocatorStatsIntegration verifies stats reporting in hierarchy.
func TestBlockAllocatorStatsIntegration(t *testing.T) {
	mockParent := testutil.NewMockAllocator()

	// Create pool with 10 blocks
	poolObjId, poolOffset, _ := mockParent.Allocate(10 * 1024)
	blockAlloc := block.New(1024, 10, poolOffset, poolObjId, nil)

	// Initially: 0 allocated, 10 free, 10 total
	allocated, free, total := blockAlloc.Stats()
	if allocated != 0 || free != 10 || total != 10 {
		t.Errorf("Initial stats wrong: allocated=%d free=%d total=%d", allocated, free, total)
	}

	// Allocate 3 blocks
	for i := 0; i < 3; i++ {
		_, _, err := blockAlloc.Allocate(1024)
		if err != nil {
			t.Fatalf("Allocation %d failed: %v", i, err)
		}
	}

	// Now: 3 allocated, 7 free, 10 total
	allocated, free, total = blockAlloc.Stats()
	if allocated != 3 || free != 7 || total != 10 {
		t.Errorf("After allocations: allocated=%d free=%d total=%d", allocated, free, total)
	}

	// Delete 1 block
	blockAlloc.DeleteObj(poolObjId + 1)

	// Now: 2 allocated, 8 free, 10 total
	allocated, free, total = blockAlloc.Stats()
	if allocated != 2 || free != 8 || total != 10 {
		t.Errorf("After deletion: allocated=%d free=%d total=%d", allocated, free, total)
	}
}

// TestBlockAllocatorIntrospectionIntegration demonstrates GetObjectIdsInAllocator usage.
func TestBlockAllocatorIntrospectionIntegration(t *testing.T) {
	mockParent := testutil.NewMockAllocator()

	// Create pool starting at ObjectId 1000
	poolObjId := types.ObjectId(1000)
	_, poolOffset, _ := mockParent.Allocate(5 * 1024)
	blockAlloc := block.New(1024, 5, types.FileOffset(poolOffset), poolObjId, nil)

	// Allocate some blocks: [1000, 1001, _, 1003, _]
	blockAlloc.Allocate(1024) // 1000
	blockAlloc.Allocate(1024) // 1001
	blockAlloc.Allocate(1024) // 1002 (to be deleted)
	blockAlloc.Allocate(1024) // 1003
	blockAlloc.DeleteObj(1002)

	// Get all allocated ObjectIds (for compaction/migration)
	objIds := blockAlloc.GetObjectIdsInAllocator()

	// Should return [1000, 1001, 1003]
	if len(objIds) != 3 {
		t.Fatalf("Expected 3 ObjectIds, got %d: %v", len(objIds), objIds)
	}

	expectedIds := []types.ObjectId{1000, 1001, 1003}
	for i, objId := range objIds {
		if objId != expectedIds[i] {
			t.Errorf("ObjectId %d: expected %d, got %d", i, expectedIds[i], objId)
		}
	}

	// Verify we can get info for each introspected ObjectId
	for _, objId := range objIds {
		_, _, err := blockAlloc.GetObjectInfo(objId)
		if err != nil {
			t.Errorf("Failed to get info for introspected ObjectId %d: %v", objId, err)
		}
	}
}
