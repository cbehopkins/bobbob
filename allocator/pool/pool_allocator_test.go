package pool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/testutil"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// TestPoolAllocatorBasicAllocation verifies basic single allocation.
func TestPoolAllocatorBasicAllocation(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, err := New(1024, parent, nil)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	
	// First allocation should create a BlockAllocator
	objId1, _, err := pool.Allocate(1024)
	if err != nil {
		t.Fatalf("Allocate() failed: %v", err)
	}
	
	if objId1 == 0 {
		t.Error("Expected non-zero ObjectId")
	}
	
	// Verify pool has 1 available allocator
	if len(pool.available) != 1 {
		t.Errorf("available count = %d, want 1", len(pool.available))
	}
	if len(pool.full) != 0 {
		t.Errorf("full count = %d, want 0", len(pool.full))
	}
}

// TestPoolAllocatorSizeMismatch verifies size validation.
func TestPoolAllocatorSizeMismatch(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)
	
	// Wrong size should fail
	_, _, err := pool.Allocate(512)
	if err != ErrSizeMismatch {
		t.Errorf("Allocate(512) = %v, want %v", err, ErrSizeMismatch)
	}
	
	_, _, err = pool.Allocate(2048)
	if err != ErrSizeMismatch {
		t.Errorf("Allocate(2048) = %v, want %v", err, ErrSizeMismatch)
	}
}

// TestPoolAllocatorMultipleAllocations verifies multiple allocations reuse BlockAllocator.
func TestPoolAllocatorMultipleAllocations(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)
	
	// Allocate 10 blocks - should all come from same BlockAllocator
	objIds := make([]types.ObjectId, 10)
	for i := 0; i < 10; i++ {
		objId, _, err := pool.Allocate(1024)
		if err != nil {
			t.Fatalf("Allocate() #%d failed: %v", i, err)
		}
		objIds[i] = objId
	}
	
	// Verify all ObjectIds are unique
	seen := make(map[types.ObjectId]bool)
	for _, objId := range objIds {
		if seen[objId] {
			t.Errorf("Duplicate ObjectId: %d", objId)
		}
		seen[objId] = true
	}
	
	// Should still have 1 available allocator
	if len(pool.available) != 1 {
		t.Errorf("available count = %d, want 1", len(pool.available))
	}
}

// TestPoolAllocatorBlockAllocatorExhaustion verifies transition to full list.
func TestPoolAllocatorBlockAllocatorExhaustion(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(64, parent, nil) // Small block size
	
	// Allocate all slots in first BlockAllocator (1024 slots)
	for i := 0; i < 1024; i++ {
		_, _, err := pool.Allocate(64)
		if err != nil {
			t.Fatalf("Allocate() #%d failed: %v", i, err)
		}
	}
	
	// First allocator should now be full
	if len(pool.available) != 0 {
		t.Errorf("available count = %d, want 0", len(pool.available))
	}
	if len(pool.full) != 1 {
		t.Errorf("full count = %d, want 1", len(pool.full))
	}
	
	// Next allocation should create new BlockAllocator
	_, _, err := pool.Allocate(64)
	if err != nil {
		t.Fatalf("Allocate() after exhaustion failed: %v", err)
	}
	
	if len(pool.available) != 1 {
		t.Errorf("available count = %d, want 1", len(pool.available))
	}
	if len(pool.full) != 1 {
		t.Errorf("full count = %d, want 1", len(pool.full))
	}
}

// TestPoolAllocatorBlockCountDoubling verifies doubling sequence.
func TestPoolAllocatorBlockCountDoubling(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(64, parent, nil)
	
	// Initial nextBlockCount should be 1024
	if pool.nextBlockCount != 1024 {
		t.Errorf("initial nextBlockCount = %d, want 1024", pool.nextBlockCount)
	}
	
	// Force creation of first BlockAllocator
	pool.Allocate(64)
	
	// After first, nextBlockCount should be 2048
	if pool.nextBlockCount != 2048 {
		t.Errorf("nextBlockCount after 1st = %d, want 2048", pool.nextBlockCount)
	}
	
	// Exhaust first allocator and create second
	for i := 1; i < 1024; i++ {
		pool.Allocate(64)
	}
	pool.Allocate(64) // Triggers new allocator
	
	// After second, nextBlockCount should be 4096
	if pool.nextBlockCount != 4096 {
		t.Errorf("nextBlockCount after 2nd = %d, want 4096", pool.nextBlockCount)
	}
}

// TestPoolAllocatorBlockCountCapping verifies max cap.
func TestPoolAllocatorBlockCountCapping(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(64, parent, nil)
	
	// Manually set to near cap
	pool.nextBlockCount = 16384
	
	// Create allocator - should double to 32768
	pool.Allocate(64)
	if pool.nextBlockCount != 32768 {
		t.Errorf("nextBlockCount = %d, want 32768", pool.nextBlockCount)
	}
	
	// Create another - should stay at 32768 (capped)
	for i := 1; i < 16384; i++ {
		pool.Allocate(64)
	}
	pool.Allocate(64) // New allocator
	
	if pool.nextBlockCount != 32768 {
		t.Errorf("nextBlockCount = %d, want 32768 (capped)", pool.nextBlockCount)
	}
}

// TestPoolAllocatorDeleteObj verifies deletion and full→available transition.
func TestPoolAllocatorDeleteObj(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(64, parent, nil)
	
	// Allocate 3 blocks
	objId1, _, _ := pool.Allocate(64)
	objId2, _, _ := pool.Allocate(64)
	objId3, _, _ := pool.Allocate(64)
	
	// Fill allocator completely
	for i := 3; i < 1024; i++ {
		pool.Allocate(64)
	}
	
	// Should be in full list
	if len(pool.full) != 1 {
		t.Errorf("full count = %d, want 1", len(pool.full))
	}
	if len(pool.available) != 0 {
		t.Errorf("available count = %d, want 0", len(pool.available))
	}
	
	// Delete one object
	err := pool.DeleteObj(objId2)
	if err != nil {
		t.Fatalf("DeleteObj() failed: %v", err)
	}
	
	// Should move back to available
	if len(pool.full) != 0 {
		t.Errorf("full count = %d, want 0", len(pool.full))
	}
	if len(pool.available) != 1 {
		t.Errorf("available count = %d, want 1", len(pool.available))
	}
	
	// Other objects should still be valid
	_, _, err = pool.GetObjectInfo(objId1)
	if err != nil {
		t.Errorf("GetObjectInfo(objId1) failed: %v", err)
	}
	_, _, err = pool.GetObjectInfo(objId3)
	if err != nil {
		t.Errorf("GetObjectInfo(objId3) failed: %v", err)
	}
	
	// Deleted object should fail
	_, _, err = pool.GetObjectInfo(objId2)
	if err == nil {
		t.Error("GetObjectInfo(deleted) should fail")
	}
}

// TestPoolAllocatorGetObjectInfo verifies info retrieval.
func TestPoolAllocatorGetObjectInfo(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)
	
	objId, expectedOffset, _ := pool.Allocate(1024)
	
	offset, size, err := pool.GetObjectInfo(objId)
	if err != nil {
		t.Fatalf("GetObjectInfo() failed: %v", err)
	}
	
	if offset != expectedOffset {
		t.Errorf("offset = %d, want %d", offset, expectedOffset)
	}
	if size != 1024 {
		t.Errorf("size = %d, want 1024", size)
	}
}

// TestPoolAllocatorContainsObjectId verifies ownership checks.
func TestPoolAllocatorContainsObjectId(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)
	
	objId, _, _ := pool.Allocate(1024)
	
	if !pool.ContainsObjectId(objId) {
		t.Error("ContainsObjectId(allocated) should return true")
	}
	
	if pool.ContainsObjectId(types.ObjectId(99999)) {
		t.Error("ContainsObjectId(unknown) should return false")
	}
}

// TestPoolAllocatorAllocateRun verifies contiguous run allocation.
func TestPoolAllocatorAllocateRun(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(512, parent, nil)
	
	objIds, offsets, err := pool.AllocateRun(512, 10)
	if err != nil {
		t.Fatalf("AllocateRun() failed: %v", err)
	}
	
	if len(objIds) != 10 {
		t.Errorf("got %d ObjectIds, want 10", len(objIds))
	}
	if len(offsets) != 10 {
		t.Errorf("got %d offsets, want 10", len(offsets))
	}
	
	// Verify contiguous ObjectIds
	for i := 1; i < len(objIds); i++ {
		if objIds[i] != objIds[i-1]+1 {
			t.Errorf("ObjectIds not contiguous: %d -> %d", objIds[i-1], objIds[i])
		}
	}
}

// TestPoolAllocatorCallback verifies SetOnAllocate.
func TestPoolAllocatorCallback(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(256, parent, nil)
	
	callbackCount := 0
	var lastObjId types.ObjectId
	var lastOffset types.FileOffset
	var lastSize int
	
	pool.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, size int) {
		callbackCount++
		lastObjId = objId
		lastOffset = offset
		lastSize = size
	})
	
	objId, offset, _ := pool.Allocate(256)
	
	if callbackCount != 1 {
		t.Errorf("callback count = %d, want 1", callbackCount)
	}
	if lastObjId != objId {
		t.Errorf("callback objId = %d, want %d", lastObjId, objId)
	}
	if lastOffset != offset {
		t.Errorf("callback offset = %d, want %d", lastOffset, offset)
	}
	if lastSize != 256 {
		t.Errorf("callback size = %d, want 256", lastSize)
	}
}

// TestPoolAllocatorStats verifies statistics aggregation.
func TestPoolAllocatorStats(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(128, parent, nil)
	
	// Initial stats
	allocated, free, total := pool.Stats()
	if allocated != 0 || free != 0 || total != 0 {
		t.Errorf("initial stats = (%d, %d, %d), want (0, 0, 0)", allocated, free, total)
	}
	
	// Allocate 5 blocks
	for i := 0; i < 5; i++ {
		pool.Allocate(128)
	}
	
	allocated, free, total = pool.Stats()
	if allocated != 5 {
		t.Errorf("allocated = %d, want 5", allocated)
	}
	if free != 1019 { // 1024 - 5
		t.Errorf("free = %d, want 1019", free)
	}
	if total != 1024 {
		t.Errorf("total = %d, want 1024", total)
	}
}

// TestPoolAllocatorMarshalUnmarshal verifies persistence.
func TestPoolAllocatorMarshalUnmarshal(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "pool_test.bin")
	
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Create file failed: %v", err)
	}
	defer file.Close()
	
	parent := testutil.NewMockAllocator()
	pool, _ := New(512, parent, file)
	
	// Allocate some blocks
	pool.Allocate(512)
	pool.Allocate(512)
	pool.Allocate(512)
	
	// Marshal
	data, err := pool.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}
	
	// Create new pool and unmarshal
	newPool, _ := New(512, parent, file)
	if err := newPool.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal() failed: %v", err)
	}
	
	// Verify basic state restored
	if newPool.blockSize != 512 {
		t.Errorf("blockSize = %d, want 512", newPool.blockSize)
	}
	if newPool.nextBlockCount < 1024 {
		t.Errorf("nextBlockCount = %d, want >= 1024", newPool.nextBlockCount)
	}
}

// TestPoolAllocatorNoParent verifies error when parent is nil.
func TestPoolAllocatorNoParent(t *testing.T) {
	_, err := New(1024, nil, nil)
	if err != ErrNoParent {
		t.Errorf("New(nil parent) = %v, want %v", err, ErrNoParent)
	}
}
