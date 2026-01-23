package block

import (
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestBlockAllocatorBasicAllocation verifies basic single-block allocation.
func TestBlockAllocatorBasicAllocation(t *testing.T) {
	const blockSize = 1024
	const blockCount = 10
	const startingOffset = types.FileOffset(0)
	const startingObjId = types.ObjectId(0)

	alloc := New(blockSize, blockCount, startingOffset, startingObjId, nil)

	// Allocate first block
	objId1, offset1, err := alloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("First allocation failed: %v", err)
	}
	if objId1 != 0 {
		t.Errorf("Expected ObjectId 0, got %d", objId1)
	}
	if offset1 != 0 {
		t.Errorf("Expected offset 0, got %d", offset1)
	}

	// Allocate second block (should not overlap)
	objId2, offset2, err := alloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("Second allocation failed: %v", err)
	}
	if objId2 != 1 {
		t.Errorf("Expected ObjectId 1, got %d", objId2)
	}
	if offset2 != blockSize {
		t.Errorf("Expected offset %d, got %d", blockSize, offset2)
	}

	// Verify GetObjectInfo
	offset, size, err := alloc.GetObjectInfo(objId1)
	if err != nil {
		t.Fatalf("GetObjectInfo failed: %v", err)
	}
	if offset != offset1 {
		t.Errorf("GetObjectInfo offset mismatch: got %d, want %d", offset, offset1)
	}
	if size != blockSize {
		t.Errorf("GetObjectInfo size mismatch: got %d, want %d", size, blockSize)
	}

	// Verify ContainsObjectId
	if !alloc.ContainsObjectId(objId1) {
		t.Error("ContainsObjectId returned false for allocated object")
	}
	if alloc.ContainsObjectId(types.ObjectId(999)) {
		t.Error("ContainsObjectId returned true for out-of-range ObjectId")
	}
}

// TestBlockAllocatorSizeMismatch verifies that wrong-sized allocations fail.
func TestBlockAllocatorSizeMismatch(t *testing.T) {
	const blockSize = 1024
	alloc := New(blockSize, 10, 0, 0, nil)

	// Try to allocate with wrong size
	_, _, err := alloc.Allocate(512)
	if err != ErrSizeMismatch {
		t.Errorf("Expected ErrSizeMismatch, got %v", err)
	}

	_, _, err = alloc.Allocate(2048)
	if err != ErrSizeMismatch {
		t.Errorf("Expected ErrSizeMismatch, got %v", err)
	}
}

// TestBlockAllocatorExhaustion verifies behavior when all blocks are allocated.
func TestBlockAllocatorExhaustion(t *testing.T) {
	const blockSize = 128
	const blockCount = 5
	alloc := New(blockSize, blockCount, 0, 0, nil)

	// Allocate all blocks
	for i := 0; i < blockCount; i++ {
		_, _, err := alloc.Allocate(blockSize)
		if err != nil {
			t.Fatalf("Allocation %d failed: %v", i, err)
		}
	}

	// Next allocation should fail
	_, _, err := alloc.Allocate(blockSize)
	if err != ErrAllAllocated {
		t.Errorf("Expected ErrAllAllocated, got %v", err)
	}

	// Subsequent allocations should also fail quickly (allAllocated flag set)
	_, _, err = alloc.Allocate(blockSize)
	if err != ErrAllAllocated {
		t.Errorf("Expected ErrAllAllocated on second attempt, got %v", err)
	}
}

// TestBlockAllocatorDeletion verifies deletion and reuse.
func TestBlockAllocatorDeletion(t *testing.T) {
	const blockSize = 256
	alloc := New(blockSize, 5, 0, 0, nil)

	// Allocate two blocks
	objId1, _, err := alloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("First allocation failed: %v", err)
	}

	objId2, _, err := alloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("Second allocation failed: %v", err)
	}

	// Delete first block
	err = alloc.DeleteObj(objId1)
	if err != nil {
		t.Fatalf("DeleteObj failed: %v", err)
	}

	// Verify it's no longer allocated
	_, _, err = alloc.GetObjectInfo(objId1)
	if err != ErrSlotNotFound {
		t.Errorf("Expected ErrSlotNotFound after deletion, got %v", err)
	}

	// Second block should still be valid
	_, _, err = alloc.GetObjectInfo(objId2)
	if err != nil {
		t.Errorf("Second block should still be valid: %v", err)
	}

	// Should be able to allocate again (reusing first slot)
	objId3, offset3, err := alloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("Reallocation failed: %v", err)
	}
	if objId3 != objId1 {
		t.Errorf("Expected reuse of ObjectId %d, got %d", objId1, objId3)
	}
	if offset3 != 0 {
		t.Errorf("Expected offset 0 for reused slot, got %d", offset3)
	}
}

// TestBlockAllocatorAllocateRun verifies contiguous run allocation.
func TestBlockAllocatorAllocateRun(t *testing.T) {
	const blockSize = 512
	const blockCount = 10
	alloc := New(blockSize, blockCount, 0, 0, nil)

	// Allocate run of 3 blocks
	objIds, offsets, err := alloc.AllocateRun(blockSize, 3)
	if err != nil {
		t.Fatalf("AllocateRun failed: %v", err)
	}

	if len(objIds) != 3 || len(offsets) != 3 {
		t.Fatalf("Expected 3 allocations, got %d objIds and %d offsets", len(objIds), len(offsets))
	}

	// Verify contiguity
	expectedOffsets := []types.FileOffset{0, 512, 1024}
	for i := 0; i < 3; i++ {
		if objIds[i] != types.ObjectId(i) {
			t.Errorf("ObjectId %d: expected %d, got %d", i, i, objIds[i])
		}
		if offsets[i] != expectedOffsets[i] {
			t.Errorf("Offset %d: expected %d, got %d", i, expectedOffsets[i], offsets[i])
		}

		// Verify each block is tracked
		offset, size, err := alloc.GetObjectInfo(objIds[i])
		if err != nil {
			t.Errorf("GetObjectInfo failed for ObjectId %d: %v", objIds[i], err)
		}
		if offset != offsets[i] {
			t.Errorf("Offset mismatch for ObjectId %d: got %d, want %d", objIds[i], offset, offsets[i])
		}
		if size != blockSize {
			t.Errorf("Size mismatch for ObjectId %d: got %d, want %d", objIds[i], size, blockSize)
		}
	}
}

// TestBlockAllocatorAllocateRunPartial verifies partial run allocation.
func TestBlockAllocatorAllocateRunPartial(t *testing.T) {
	const blockSize = 256
	const blockCount = 5
	alloc := New(blockSize, blockCount, 0, 0, nil)

	// Allocate first 2 blocks individually
	_, _, _ = alloc.Allocate(blockSize)
	_, _, _ = alloc.Allocate(blockSize)

	// Request 10 blocks (more than available)
	objIds, offsets, err := alloc.AllocateRun(blockSize, 10)
	if err != nil {
		t.Fatalf("AllocateRun failed: %v", err)
	}
	_ = offsets // not checking offsets in detail here

	// Should return 3 blocks (the remaining contiguous run)
	if len(objIds) != 3 {
		t.Errorf("Expected 3 blocks (partial fulfillment), got %d", len(objIds))
	}

	// Verify they're contiguous starting at slot 2
	for i, objId := range objIds {
		expectedObjId := types.ObjectId(2 + i)
		if objId != expectedObjId {
			t.Errorf("Block %d: expected ObjectId %d, got %d", i, expectedObjId, objId)
		}
	}
}

// TestBlockAllocatorAllocateRunFragmented verifies run finds longest contiguous span.
func TestBlockAllocatorAllocateRunFragmented(t *testing.T) {
	const blockSize = 128
	const blockCount = 10
	alloc := New(blockSize, blockCount, 0, 0, nil)

	// Create fragmentation: allocate [0, 1, 2, _, _, 5, _, _, 8, 9]
	alloc.Allocate(blockSize) // 0
	alloc.Allocate(blockSize) // 1
	alloc.Allocate(blockSize) // 2
	alloc.Allocate(blockSize) // 3 (to be deleted)
	alloc.Allocate(blockSize) // 4 (to be deleted)
	alloc.Allocate(blockSize) // 5
	alloc.Allocate(blockSize) // 6 (to be deleted)
	alloc.Allocate(blockSize) // 7 (to be deleted)
	alloc.Allocate(blockSize) // 8
	alloc.Allocate(blockSize) // 9

	// Delete to create gaps: [0, 1, 2, _, _, 5, _, _, 8, 9]
	alloc.DeleteObj(types.ObjectId(3))
	alloc.DeleteObj(types.ObjectId(4))
	alloc.DeleteObj(types.ObjectId(6))
	alloc.DeleteObj(types.ObjectId(7))

	// Request 5 contiguous blocks - should get longest run (slots 3-4, length 2)
	objIds, _, err := alloc.AllocateRun(blockSize, 5)
	if err != nil {
		t.Fatalf("AllocateRun failed: %v", err)
	}

	if len(objIds) != 2 {
		t.Errorf("Expected longest run of 2 blocks, got %d", len(objIds))
	}

	// Should be the first gap encountered (slots 3-4)
	if objIds[0] != types.ObjectId(3) {
		t.Errorf("Expected first ObjectId to be 3, got %d", objIds[0])
	}
}

// TestBlockAllocatorAllocateRunNoneAvailable verifies empty return when fully allocated.
func TestBlockAllocatorAllocateRunNoneAvailable(t *testing.T) {
	const blockSize = 64
	const blockCount = 3
	alloc := New(blockSize, blockCount, 0, 0, nil)

	// Allocate all blocks
	for i := 0; i < blockCount; i++ {
		alloc.Allocate(blockSize)
	}

	// Try to allocate run - should return empty slices
	objIds, offsets, err := alloc.AllocateRun(blockSize, 2)
	if err != nil {
		t.Fatalf("AllocateRun should not error when returning empty: %v", err)
	}
	_ = offsets // use variable
	if len(objIds) != 0 || len(offsets) != 0 {
		t.Errorf("Expected empty slices, got %d objIds and %d offsets", len(objIds), len(offsets))
	}
}

// TestBlockAllocatorCallback verifies SetOnAllocate callback.
func TestBlockAllocatorCallback(t *testing.T) {
	const blockSize = 1024
	alloc := New(blockSize, 5, 0, 0, nil)

	callbackInvoked := 0
	var lastObjId types.ObjectId
	var lastOffset types.FileOffset
	var lastSize int

	alloc.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, size int) {
		callbackInvoked++
		lastObjId = objId
		lastOffset = offset
		lastSize = size
	})

	// Allocate single block
	objId, offset, err := alloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("Allocation failed: %v", err)
	}

	if callbackInvoked != 1 {
		t.Errorf("Expected callback invoked once, got %d", callbackInvoked)
	}
	if lastObjId != objId {
		t.Errorf("Callback ObjectId mismatch: got %d, want %d", lastObjId, objId)
	}
	if lastOffset != offset {
		t.Errorf("Callback offset mismatch: got %d, want %d", lastOffset, offset)
	}
	if lastSize != blockSize {
		t.Errorf("Callback size mismatch: got %d, want %d", lastSize, blockSize)
	}

	// Allocate run - callback should be invoked for each block
	_, _, err = alloc.AllocateRun(blockSize, 2)
	if err != nil {
		t.Fatalf("AllocateRun failed: %v", err)
	}

	if callbackInvoked != 3 {
		t.Errorf("Expected callback invoked 3 times total, got %d", callbackInvoked)
	}
}

// TestBlockAllocatorMarshalUnmarshal verifies persistence.
func TestBlockAllocatorMarshalUnmarshal(t *testing.T) {
	const blockSize = 512
	const blockCount = 8
	const startingOffset = types.FileOffset(4096)
	const startingObjId = types.ObjectId(100)

	alloc1 := New(blockSize, blockCount, startingOffset, startingObjId, nil)

	// Allocate some blocks
	alloc1.Allocate(blockSize) // ObjectId 100
	alloc1.Allocate(blockSize) // ObjectId 101
	alloc1.Allocate(blockSize) // ObjectId 102

	// Marshal
	data, err := alloc1.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Create new allocator and unmarshal
	alloc2 := New(blockSize, blockCount, 0, 0, nil)
	err = alloc2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify state restored
	if alloc2.blockSize != blockSize {
		t.Errorf("blockSize mismatch: got %d, want %d", alloc2.blockSize, blockSize)
	}
	if alloc2.startingFileOffset != startingOffset {
		t.Errorf("startingFileOffset mismatch: got %d, want %d", alloc2.startingFileOffset, startingOffset)
	}
	if alloc2.startingObjectId != types.ObjectId(startingOffset) {
		t.Errorf("startingObjectId mismatch: got %d, want %d (derived from offset)", alloc2.startingObjectId, startingOffset)
	}

	// Verify allocated blocks
	for i := 0; i < 3; i++ {
		objId := types.ObjectId(startingOffset) + types.ObjectId(i)
		offset, size, err := alloc2.GetObjectInfo(objId)
		if err != nil {
			t.Errorf("ObjectId %d should be allocated: %v", objId, err)
		}
		expectedOffset := startingOffset + types.FileOffset(i*blockSize)
		if offset != expectedOffset {
			t.Errorf("ObjectId %d offset mismatch: got %d, want %d", objId, offset, expectedOffset)
		}
		if size != blockSize {
			t.Errorf("ObjectId %d size mismatch: got %d, want %d", objId, size, blockSize)
		}
	}

	// Verify free blocks
	for i := 3; i < blockCount; i++ {
		objId := types.ObjectId(startingOffset) + types.ObjectId(i)
		_, _, err := alloc2.GetObjectInfo(objId)
		if err != ErrSlotNotFound {
			t.Errorf("ObjectId %d should be free", objId)
		}
	}

	// Verify can allocate from restored state
	objId, _, err := alloc2.Allocate(blockSize)
	if err != nil {
		t.Fatalf("Allocation from restored state failed: %v", err)
	}
	expectedObjId := types.ObjectId(startingOffset) + types.ObjectId(3)
	if objId != expectedObjId {
		t.Errorf("Expected next allocation to be ObjectId %d, got %d", expectedObjId, objId)
	}
}

// TestBlockAllocatorGetObjectIdsInAllocator verifies introspection.
func TestBlockAllocatorGetObjectIdsInAllocator(t *testing.T) {
	const blockSize = 256
	alloc := New(blockSize, 10, 0, types.ObjectId(1000), nil)

	// Initially empty
	objIds := alloc.GetObjectIdsInAllocator()
	if len(objIds) != 0 {
		t.Errorf("Expected empty list initially, got %d ObjectIds", len(objIds))
	}

	// Allocate some blocks
	alloc.Allocate(blockSize) // 1000
	alloc.Allocate(blockSize) // 1001
	alloc.Allocate(blockSize) // 1002

	objIds = alloc.GetObjectIdsInAllocator()
	if len(objIds) != 3 {
		t.Fatalf("Expected 3 ObjectIds, got %d", len(objIds))
	}

	expectedObjIds := []types.ObjectId{1000, 1001, 1002}
	for i, objId := range objIds {
		if objId != expectedObjIds[i] {
			t.Errorf("ObjectId %d: expected %d, got %d", i, expectedObjIds[i], objId)
		}
	}

	// Delete one
	alloc.DeleteObj(types.ObjectId(1001))

	objIds = alloc.GetObjectIdsInAllocator()
	if len(objIds) != 2 {
		t.Fatalf("Expected 2 ObjectIds after deletion, got %d", len(objIds))
	}

	expectedObjIds = []types.ObjectId{1000, 1002}
	for i, objId := range objIds {
		if objId != expectedObjIds[i] {
			t.Errorf("ObjectId %d: expected %d, got %d", i, expectedObjIds[i], objId)
		}
	}
}

// TestBlockAllocatorStats verifies allocation statistics.
func TestBlockAllocatorStats(t *testing.T) {
	const blockSize = 128
	const blockCount = 10
	alloc := New(blockSize, blockCount, 0, 0, nil)

	// Initially all free
	allocated, free, total := alloc.Stats()
	if allocated != 0 || free != 10 || total != 10 {
		t.Errorf("Initial stats: got (%d, %d, %d), want (0, 10, 10)", allocated, free, total)
	}

	// Allocate 3 blocks
	for i := 0; i < 3; i++ {
		alloc.Allocate(blockSize)
	}

	allocated, free, total = alloc.Stats()
	if allocated != 3 || free != 7 || total != 10 {
		t.Errorf("After 3 allocations: got (%d, %d, %d), want (3, 7, 10)", allocated, free, total)
	}

	// Delete 1 block
	alloc.DeleteObj(types.ObjectId(1))

	allocated, free, total = alloc.Stats()
	if allocated != 2 || free != 8 || total != 10 {
		t.Errorf("After 1 deletion: got (%d, %d, %d), want (2, 8, 10)", allocated, free, total)
	}
}

// TestBlockAllocatorInvalidOperations verifies error handling.
func TestBlockAllocatorInvalidOperations(t *testing.T) {
	const blockSize = 1024
	alloc := New(blockSize, 5, 0, types.ObjectId(0), nil)

	// Delete non-existent ObjectId
	err := alloc.DeleteObj(types.ObjectId(999))
	if err != ErrOutOfRange {
		t.Errorf("Expected ErrOutOfRange for out-of-range ObjectId, got %v", err)
	}

	// Delete unallocated slot
	err = alloc.DeleteObj(types.ObjectId(0))
	if err != ErrSlotNotFound {
		t.Errorf("Expected ErrSlotNotFound for unallocated slot, got %v", err)
	}

	// GetObjectInfo for unallocated slot
	_, _, err = alloc.GetObjectInfo(types.ObjectId(0))
	if err != ErrSlotNotFound {
		t.Errorf("Expected ErrSlotNotFound for unallocated slot, got %v", err)
	}

	// AllocateRun with invalid count
	_, _, err = alloc.AllocateRun(blockSize, 0)
	if err != ErrInvalidCount {
		t.Errorf("Expected ErrInvalidCount for zero count, got %v", err)
	}

	_, _, err = alloc.AllocateRun(blockSize, -5)
	if err != ErrInvalidCount {
		t.Errorf("Expected ErrInvalidCount for negative count, got %v", err)
	}
}

// TestBlockAllocatorHierarchy demonstrates ObjectId range management.
func TestBlockAllocatorHierarchy(t *testing.T) {
	// This test demonstrates how BlockAllocator manages ObjectId ranges
	// when created by a parent allocator.
	
	// Simulate parent providing a range starting at offset 10000, ObjectId 1000
	const parentOffset = types.FileOffset(10000)
	const parentObjId = types.ObjectId(1000)
	const totalSize = 10 * 1024

	// Create BlockAllocator using parent's allocation
	const blockSize = 1024
	const blockCount = 10
	blockAlloc := New(blockSize, blockCount, parentOffset, parentObjId, nil)

	// Verify BlockAllocator respects parent's allocation
	if blockAlloc.startingFileOffset != parentOffset {
		t.Errorf("BlockAllocator offset mismatch: got %d, want %d", blockAlloc.startingFileOffset, parentOffset)
	}
	if blockAlloc.startingObjectId != parentObjId {
		t.Errorf("BlockAllocator ObjectId mismatch: got %d, want %d", blockAlloc.startingObjectId, parentObjId)
	}

	// Allocate from BlockAllocator
	childObjId, childOffset, err := blockAlloc.Allocate(blockSize)
	if err != nil {
		t.Fatalf("Child allocation failed: %v", err)
	}

	// Verify child allocation is within parent's range
	if childOffset < parentOffset {
		t.Errorf("Child offset %d is before parent offset %d", childOffset, parentOffset)
	}
	if childOffset >= parentOffset+types.FileOffset(totalSize) {
		t.Errorf("Child offset %d is beyond parent range [%d, %d)", childOffset, parentOffset, parentOffset+types.FileOffset(totalSize))
	}

	// Verify ObjectId hierarchy
	if childObjId < parentObjId {
		t.Errorf("Child ObjectId %d is before parent ObjectId %d", childObjId, parentObjId)
	}
	if childObjId >= parentObjId+types.ObjectId(blockCount) {
		t.Errorf("Child ObjectId %d is beyond parent range", childObjId)
	}

	// Verify ContainsObjectId respects range
	if !blockAlloc.ContainsObjectId(parentObjId) {
		t.Error("Should contain starting ObjectId")
	}
	if !blockAlloc.ContainsObjectId(parentObjId + types.ObjectId(blockCount-1)) {
		t.Error("Should contain last ObjectId in range")
	}
	if blockAlloc.ContainsObjectId(parentObjId - 1) {
		t.Error("Should not contain ObjectId before range")
	}
	if blockAlloc.ContainsObjectId(parentObjId + types.ObjectId(blockCount)) {
		t.Error("Should not contain ObjectId after range")
	}
}

func TestBlockAllocatorSizeInBytes(t *testing.T) {
	alloc := New(1024, 100, 0, 1000, nil)
	
	// Expected: 8 bytes (offset) + ceil(100/8) = 8 + 13 = 21 bytes
	expected := 8 + (100+7)/8
	if alloc.SizeInBytes() != expected {
		t.Errorf("SizeInBytes() = %d, want %d", alloc.SizeInBytes(), expected)
	}
	
	// Verify it matches actual Marshal output
	data, err := alloc.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) != alloc.SizeInBytes() {
		t.Errorf("Marshal returned %d bytes, SizeInBytes() = %d", len(data), alloc.SizeInBytes())
	}
}

func TestBlockAllocatorRangeSizeInBytes(t *testing.T) {
	blockSize := 1024
	blockCount := 100
	alloc := New(blockSize, blockCount, 0, 1000, nil)
	
	// Expected: blockSize * blockCount
	expected := blockSize * blockCount
	if alloc.RangeSizeInBytes() != expected {
		t.Errorf("RangeSizeInBytes() = %d, want %d", alloc.RangeSizeInBytes(), expected)
	}
	
	// This is the size you'd request from a parent allocator
	if alloc.RangeSizeInBytes() != 102400 {
		t.Errorf("RangeSizeInBytes() = %d, want 102400 (100 * 1024)", alloc.RangeSizeInBytes())
	}
}
