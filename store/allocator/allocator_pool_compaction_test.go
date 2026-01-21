package allocator

import (
	"testing"
)

func TestFindSmallestBlockAllocatorForSize(t *testing.T) {
	parent, file := newTestBasicAllocator(t)
	pool := NewAllocatorPool(64, 3, parent, file)

	// Create some allocations to populate the pool
	for i := 0; i < 5; i++ {
		if _, _, err := pool.Allocate(); err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
	}

	// Manually create a sub-optimal allocator by reducing blockCount
	if len(pool.available) > 0 && pool.available[0] != nil {
		pool.available[0].allocator.blockCount = 2 // Make it smaller than current (3)
	}

	// Try to find the smallest allocator
	_, idx, blockCount, found := pool.FindSmallestBlockAllocatorForSize(64)
	if !found {
		t.Fatalf("expected to find smallest allocator")
	}
	if blockCount != 2 {
		t.Errorf("expected blockCount 2, got %d", blockCount)
	}
	if idx != 0 {
		t.Errorf("expected index 0, got %d", idx)
	}
	t.Logf("Found smallest allocator at index %d with blockCount %d", idx, blockCount)
}

func TestGetObjectIdsInAllocator(t *testing.T) {
	parent, file := newTestBasicAllocator(t)
	pool := NewAllocatorPool(64, 2, parent, file)

	// Allocate objects
	var allocatedIds []ObjectId
	for i := 0; i < 3; i++ {
		objId, _, err := pool.Allocate()
		if err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
		allocatedIds = append(allocatedIds, objId)
	}

	// Get ObjectIds from first allocator (index 0 in available list)
	objectIds := pool.GetObjectIdsInAllocator(64, 0)
	if len(objectIds) == 0 {
		t.Fatalf("expected ObjectIds in allocator 0, got none")
	}

	t.Logf("Found %d ObjectIds in allocator 0: %v", len(objectIds), objectIds)

	// Verify the ObjectIds match what we allocated
	for _, expectedId := range allocatedIds[:len(allocatedIds)-1] { // First two go in first allocator
		found := false
		for _, id := range objectIds {
			if id == expectedId {
				found = true
				break
			}
		}
		if !found {
			t.Logf("Warning: expected ObjectId %d not found in allocator", expectedId)
		}
	}
}

func TestCompactionWorkflow(t *testing.T) {
	// Simulate the compaction workflow:
	// 1. Find smallest allocator
	// 2. Get its ObjectIds
	// 3. Delete those nodes (simulated)
	// 4. Next persist uses new allocator

	parent, file := newTestBasicAllocator(t)
	pool := NewAllocatorPool(64, 3, parent, file)

	// Create allocations
	for i := 0; i < 6; i++ {
		if _, _, err := pool.Allocate(); err != nil {
			t.Fatalf("allocate %d: %v", i, err)
		}
	}

	// Simulate old allocator with smaller blockCount
	if len(pool.available) > 0 && pool.available[0] != nil {
		pool.available[0].allocator.blockCount = 2
	}

	// Step 1: Find smallest allocator
	_, idx, blockCount, found := pool.FindSmallestBlockAllocatorForSize(64)
	if !found {
		t.Fatalf("expected to find smallest allocator")
	}

	t.Logf("Step 1: Found smallest allocator at index %d (blockCount=%d)", idx, blockCount)

	// Step 2: Get its ObjectIds
	objectIds := pool.GetObjectIdsInAllocator(64, idx)
	t.Logf("Step 2: Found %d ObjectIds to migrate: %v", len(objectIds), objectIds)

	// Step 3: In PersistentTreap, would delete those nodes
	t.Logf("Step 3: [Would delete %d nodes from treap]", len(objectIds))

	// Step 4: Next persist would reuse freed space in optimal allocator
	t.Logf("Step 4: [Next persist would reallocate in blockCount=%d allocator]", pool.blockCount)

	if len(objectIds) == 0 {
		t.Errorf("expected ObjectIds to migrate, got none")
	}
}
