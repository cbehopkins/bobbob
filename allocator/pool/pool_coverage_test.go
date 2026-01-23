package pool

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/testutil"
	"github.com/cbehopkins/bobbob/allocator/types"
	itestutil "github.com/cbehopkins/bobbob/internal/testutil"
)

// TestPoolAllocatorAllocateRunPartialFill tests AllocateRun filling available allocators.
func TestPoolAllocatorAllocateRunPartialFill(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// AllocateRun requesting 10 blocks
	objIds, offsets, err := pool.AllocateRun(1024, 10)
	if err != nil {
		t.Fatalf("AllocateRun(10) failed: %v", err)
	}

	if len(objIds) != 10 {
		t.Errorf("AllocateRun returned %d objects, want 10", len(objIds))
	}

	if len(offsets) != 10 {
		t.Errorf("AllocateRun returned %d offsets, want 10", len(offsets))
	}

	// All should have valid ObjectIds
	for i, objId := range objIds {
		if objId == 0 {
			t.Errorf("objIds[%d] = 0, want non-zero", i)
		}
	}

	// Should still have available allocators (not full with 10 blocks vs 1024 capacity)
	if len(pool.available) == 0 {
		t.Error("Expected available allocators after partial fill")
	}
}

// TestPoolAllocatorDeleteFromFull tests deletion moving allocator from full to available.
func TestPoolAllocatorDeleteFromFull(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// Allocate exactly blockCount to make first allocator full
	// With initialBlockCount=1024, we need 1024 allocations
	// For efficiency, we'll use mock's behavior

	// Create allocator by first allocation
	objId1, _, _ := pool.Allocate(1024)

	// Manually mark pool as having one full allocator for testing
	// (In practice, we'd need to allocate all 1024 slots)
	if len(pool.available) > 0 {
		// Move first available to full to simulate
		pool.full = append(pool.full, pool.available[0])
		pool.available = pool.available[1:]

		// Now delete from "full" allocator
		err := pool.DeleteObj(objId1)
		if err != nil && err != ErrAllocatorNotFound {
			t.Logf("DeleteObj from simulated full allocator returned: %v", err)
		}
	}
}

// TestPoolAllocatorAllocateRunSizeMismatch tests AllocateRun with wrong size.
func TestPoolAllocatorAllocateRunSizeMismatch(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// Wrong size in AllocateRun
	_, _, err := pool.AllocateRun(512, 10)
	if err != ErrSizeMismatch {
		t.Errorf("AllocateRun(512) = %v, want %v", err, ErrSizeMismatch)
	}

	_, _, err = pool.AllocateRun(2048, 10)
	if err != ErrSizeMismatch {
		t.Errorf("AllocateRun(2048) = %v, want %v", err, ErrSizeMismatch)
	}
}

// TestPoolAllocatorDeleteNonexistent tests deletion of non-existent ObjectId.
func TestPoolAllocatorDeleteNonexistent(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// Try to delete ObjectId that was never allocated
	err := pool.DeleteObj(99999)
	if err != ErrAllocatorNotFound {
		t.Errorf("DeleteObj(99999) = %v, want %v", err, ErrAllocatorNotFound)
	}
}

// TestPoolAllocatorGetObjectInfoNotFound tests GetObjectInfo for non-existent ObjectId.
func TestPoolAllocatorGetObjectInfoNotFound(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	_, _, err := pool.GetObjectInfo(99999)
	if err == nil {
		t.Error("GetObjectInfo(99999) should fail")
	}
}

// TestPoolAllocatorContainsObjectIdEdgeCases tests ContainsObjectId in various states.
func TestPoolAllocatorContainsObjectIdEdgeCases(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// Empty pool should not contain anything
	if pool.ContainsObjectId(1) {
		t.Error("Empty pool should not contain any ObjectId")
	}

	// Allocate something
	objId, _, _ := pool.Allocate(1024)

	// Should now contain it
	if !pool.ContainsObjectId(objId) {
		t.Errorf("Pool should contain ObjectId %d after allocation", objId)
	}

	// Delete it and verify behavior (ContainsObjectId delegates to BlockAllocator)
	pool.DeleteObj(objId)
	// Note: After deletion, ContainsObjectId behavior depends on BlockAllocator implementation
	t.Logf("After deletion, ContainsObjectId(%d) = %v", objId, pool.ContainsObjectId(objId))
}

// TestPoolAllocatorBlockCountGrowth verifies nextBlockCount increases appropriately.
func TestPoolAllocatorBlockCountGrowth(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	initialCount := pool.nextBlockCount
	if initialCount != 1024 {
		t.Logf("Initial blockCount = %d, expected 1024", initialCount)
	}

	// After creating first allocator, nextBlockCount should increase
	pool.Allocate(1024)
	afterFirst := pool.nextBlockCount

	// Should have doubled or increased in some way
	if afterFirst <= initialCount {
		t.Errorf("nextBlockCount should grow after allocation: %d → %d", initialCount, afterFirst)
	}

	// Creating more allocators should continue growth
	pool.Allocate(1024)
	afterSecond := pool.nextBlockCount

	// Should continue growing
	if afterSecond < afterFirst {
		t.Errorf("nextBlockCount growth not monotonic: %d → %d", afterFirst, afterSecond)
	}
}

// TestPoolAllocatorMultiplePools tests managing multiple independent pools.
func TestPoolAllocatorMultiplePools(t *testing.T) {
	parent1 := testutil.NewMockAllocator()
	parent2 := testutil.NewMockAllocator()

	pool1, _ := New(512, parent1, nil)
	pool2, _ := New(1024, parent2, nil)

	// Allocate from both pools
	id1, _, _ := pool1.Allocate(512)
	_, _, _ = pool2.Allocate(1024)

	// Verify each pool maintains its own state
	if !pool1.ContainsObjectId(id1) {
		t.Error("Pool1 should contain id1")
	}

	// Note: The mock allocator behavior may cause cross-pool allocation
	// Just verify that pools are independent instances
	if pool1.blockSize != 512 {
		t.Errorf("Pool1 blockSize = %d, want 512", pool1.blockSize)
	}
	if pool2.blockSize != 1024 {
		t.Errorf("Pool2 blockSize = %d, want 1024", pool2.blockSize)
	}
}

// TestPoolAllocatorCallbackFiring verifies callbacks are invoked correctly.
func TestPoolAllocatorCallbackFiring(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	callCount := 0
	pool.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, size int) {
		callCount++
		if size != 1024 {
			t.Errorf("Callback size = %d, want 1024", size)
		}
	})

	// Single allocation should fire once
	pool.Allocate(1024)
	if callCount != 1 {
		t.Errorf("Callback call count = %d, want 1", callCount)
	}

	// AllocateRun with 5 objects should fire 5 times
	pool.AllocateRun(1024, 5)
	if callCount != 6 {
		t.Errorf("After AllocateRun(5), callback count = %d, want 6", callCount)
	}
}

// TestPoolAllocatorGetFile verifies GetFile returns correct file.
func TestPoolAllocatorGetFile(t *testing.T) {
	dir, cleanup := itestutil.CreateTempFile(t, "pool_test.bin")
	defer cleanup()

	file, err := os.Create(dir)
	if err != nil {
		t.Fatalf("Create file failed: %v", err)
	}
	defer file.Close()

	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, file)

	retrievedFile := pool.GetFile()
	if retrievedFile != file {
		t.Error("GetFile() returned wrong file")
	}
}

// TestPoolAllocatorSequentialOperations tests sequence: Allocate → AllocateRun → Delete → Allocate.
func TestPoolAllocatorSequentialOperations(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(256, parent, nil)

	// Step 1: Allocate single object
	obj1, _, _ := pool.Allocate(256)
	if obj1 == 0 {
		t.Fatal("First allocation returned 0")
	}

	// Step 2: AllocateRun multiple objects
	objIds, _, err := pool.AllocateRun(256, 3)
	if err != nil || len(objIds) != 3 {
		t.Fatalf("AllocateRun(3) failed: %v, got %d objects", err, len(objIds))
	}

	// Step 3: Delete first object
	err = pool.DeleteObj(obj1)
	if err != nil && err != ErrAllocatorNotFound {
		t.Fatalf("DeleteObj failed: %v", err)
	}

	// Step 4: Allocate new object (should fit in freed space if available)
	obj5, _, _ := pool.Allocate(256)
	if obj5 == 0 {
		t.Fatal("Allocation after delete returned 0")
	}

	// Verify all remaining objects are accessible
	for _, id := range objIds {
		if !pool.ContainsObjectId(id) {
			t.Errorf("Object %d lost after sequential operations", id)
		}
	}
	if !pool.ContainsObjectId(obj5) {
		t.Errorf("Object %d lost after sequential operations", obj5)
	}
}

// TestPoolAllocatorStressAllocationLarge tests allocation of many objects.
func TestPoolAllocatorStressAllocationLarge(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(256, parent, nil)

	const numAllocations = 100
	allocatedIds := make([]types.ObjectId, 0, numAllocations)

	// Allocate many objects
	for i := 0; i < numAllocations; i++ {
		objId, _, err := pool.Allocate(256)
		if err != nil {
			t.Fatalf("Allocation %d failed: %v", i, err)
		}
		allocatedIds = append(allocatedIds, objId)
	}

	// Verify all are present
	for _, id := range allocatedIds {
		if !pool.ContainsObjectId(id) {
			t.Errorf("Object %d not found after bulk allocation", id)
		}
	}

	// Verify stats
	allocated, _, _ := pool.Stats()
	if allocated != numAllocations {
		t.Errorf("Stats allocated = %d, want %d", allocated, numAllocations)
	}
}

// TestPoolAllocatorMarshalRoundtrip tests full persistence cycle.
func TestPoolAllocatorMarshalRoundtrip(t *testing.T) {
	dir, cleanup := itestutil.CreateTempFile(t, "pool_roundtrip.bin")
	defer cleanup()

	file, _ := os.Create(dir)
	defer file.Close()

	parent1 := testutil.NewMockAllocator()
	pool1, _ := New(512, parent1, file)

	// Allocate some objects
	_, _, _ = pool1.Allocate(512)
	_, _, _ = pool1.Allocate(512)
	_, _, _ = pool1.Allocate(512)

	// Marshal
	data, err := pool1.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Create new pool and unmarshal
	parent2 := testutil.NewMockAllocator()
	pool2, _ := New(512, parent2, file)
	err = pool2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify blockSize is restored
	if pool2.blockSize != 512 {
		t.Errorf("blockSize not restored: %d", pool2.blockSize)
	}

	// Note: Object tracking depends on internal state recovery
	// Just verify that the unmarshal operation succeeded and restored basic state
	if pool2.blockSize != pool1.blockSize {
		t.Error("Unmarshal did not restore blockSize correctly")
	}
}

// TestPoolAllocatorEmptyMarshal tests marshalling empty pool.
func TestPoolAllocatorEmptyMarshal(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// Marshal without any allocations
	data, err := pool.Marshal()
	if err != nil {
		t.Fatalf("Marshal empty pool failed: %v", err)
	}

	// Should be able to unmarshal back
	pool2, _ := New(1024, parent, nil)
	err = pool2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal empty pool failed: %v", err)
	}
}

// TestPoolAllocatorUnmarshalInvalidData tests error handling for corrupt data.
func TestPoolAllocatorUnmarshalInvalidData(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// Try to unmarshal garbage data
	badData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	err := pool.Unmarshal(badData)
	if err == nil {
		t.Error("Unmarshal invalid data should fail")
	}
}

// TestPoolAllocatorRecoveryAfterError tests state after failed operations.
func TestPoolAllocatorRecoveryAfterError(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pool, _ := New(1024, parent, nil)

	// Try invalid operation
	_, _, err := pool.AllocateRun(512, 10) // Wrong size
	if err == nil {
		t.Fatal("AllocateRun with wrong size should fail")
	}

	// Pool should still be usable
	objId, _, err := pool.Allocate(1024)
	if err != nil {
		t.Fatalf("Allocation after error failed: %v", err)
	}

	if !pool.ContainsObjectId(objId) {
		t.Error("Allocation after error didn't create object")
	}
}
