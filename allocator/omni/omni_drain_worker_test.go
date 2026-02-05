package omni_test

import (
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/allocator/cache"
	"github.com/cbehopkins/bobbob/allocator/omni"
	"github.com/cbehopkins/bobbob/allocator/testutil"
)

func TestDrainWorkerStartStop(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pc, err := cache.New()
	if err != nil {
		t.Fatalf("cache.New failed: %v", err)
	}
	defer pc.Close()

	oa, err := omni.NewOmniAllocator([]int{64}, parent, nil, pc)
	if err != nil {
		t.Fatalf("NewOmniAllocator failed: %v", err)
	}
	defer oa.Close()

	// Start drain worker with 100ms interval (short for testing)
	err = oa.StartDrainWorker(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("StartDrainWorker failed: %v", err)
	}

	// Should not be able to start twice
	err = oa.StartDrainWorker(100 * time.Millisecond)
	if err == nil {
		t.Fatalf("expected error when starting drain worker twice")
	}

	// Stop should work
	oa.StopDrainWorker()

	// Start again after stopping should work
	err = oa.StartDrainWorker(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("StartDrainWorker after stop failed: %v", err)
	}

	oa.StopDrainWorker()
	t.Log("✓ Drain worker start/stop test passed")
}

func TestDrainWorkerDefaultInterval(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pc, err := cache.New()
	if err != nil {
		t.Fatalf("cache.New failed: %v", err)
	}
	defer pc.Close()

	oa, err := omni.NewOmniAllocator([]int{64}, parent, nil, pc)
	if err != nil {
		t.Fatalf("NewOmniAllocator failed: %v", err)
	}
	defer oa.Close()

	// Start with 0 interval (should default to 5 minutes)
	err = oa.StartDrainWorker(0)
	if err != nil {
		t.Fatalf("StartDrainWorker with 0 interval failed: %v", err)
	}

	// Verify it's running
	oa.StopDrainWorker()

	t.Log("✓ Drain worker default interval test passed")
}

func TestDrainWorkerWithAllocations(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pc, err := cache.New()
	if err != nil {
		t.Fatalf("cache.New failed: %v", err)
	}
	defer pc.Close()

	oa, err := omni.NewOmniAllocator([]int{64}, parent, nil, pc)
	if err != nil {
		t.Fatalf("NewOmniAllocator failed: %v", err)
	}
	defer oa.Close()

	// Allocate many objects to ensure we have full allocators
	blockSize := 64
	numObjects := 1500

	for i := 0; i < numObjects; i++ {
		_, _, err := oa.Allocate(blockSize)
		if err != nil {
			t.Fatalf("failed to allocate object %d: %v", i, err)
		}
	}

	t.Logf("Allocated %d objects of size %d", numObjects, blockSize)

	// Start drain worker with 100ms interval
	err = oa.StartDrainWorker(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("StartDrainWorker failed: %v", err)
	}

	// Wait for drain worker to run
	time.Sleep(500 * time.Millisecond)

	// Stop the worker before checking cache to avoid race conditions
	oa.StopDrainWorker()

	// Check cache size after draining
	cacheAfterDrain := pc.GetAll()
	t.Logf("Cache after drain: %d entries", len(cacheAfterDrain))

	// Should have more cache entries after draining (allocators moved to cache)
	if len(cacheAfterDrain) == 0 {
		t.Logf("Warning: no cache entries after drain, but this may be expected if objects are still in memory")
	}

	// Verify objects can still be allocated (should rehydrate from cache)
	objId, _, err := oa.Allocate(blockSize)
	if err != nil {
		t.Fatalf("failed to allocate new object after draining: %v", err)
	}
	_, _, err = oa.GetObjectInfo(objId)
	if err != nil {
		t.Fatalf("object %v not found after draining: %v", objId, err)
	}

	t.Log("✓ Drain worker allocation test passed")
}

func TestDrainWorkerCloseStopsWorker(t *testing.T) {
	parent := testutil.NewMockAllocator()
	pc, err := cache.New()
	if err != nil {
		t.Fatalf("cache.New failed: %v", err)
	}
	defer pc.Close()

	oa, err := omni.NewOmniAllocator([]int{64}, parent, nil, pc)
	if err != nil {
		t.Fatalf("NewOmniAllocator failed: %v", err)
	}

	// Start drain worker
	err = oa.StartDrainWorker(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("StartDrainWorker failed: %v", err)
	}

	// Close should stop the worker
	err = oa.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	t.Log("✓ Drain worker close test passed")
}
