package omni_test

import (
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/allocator/cache"
	"github.com/cbehopkins/bobbob/allocator/omni"
	"github.com/cbehopkins/bobbob/allocator/testutil"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// Soak test: with a stable number of allocations, the pool cache temp file should not grow unbounded.
func TestDrainWorkerTempFileStableUnderChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

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

	if err := oa.StartDrainWorker(5 * time.Millisecond); err != nil {
		t.Fatalf("StartDrainWorker failed: %v", err)
	}

	const blockSize = 64
	const initialObjects = 5000
	const rounds = 50
	const churnPerRound = 200

	objIds := make([]types.ObjectId, initialObjects)
	for i := 0; i < initialObjects; i++ {
		id, _, err := oa.Allocate(blockSize)
		if err != nil {
			t.Fatalf("initial allocate %d failed: %v", i, err)
		}
		objIds[i] = id
	}

	time.Sleep(20 * time.Millisecond)
	baseSize, err := pc.TempFileSize()
	if err != nil {
		t.Fatalf("TempFileSize failed: %v", err)
	}

	for round := 0; round < rounds; round++ {
		for i := 0; i < churnPerRound; i++ {
			idx := (round*churnPerRound + i) % len(objIds)
			_ = oa.DeleteObj(objIds[idx])
			id, _, err := oa.Allocate(blockSize)
			if err != nil {
				t.Fatalf("allocate during churn failed: %v", err)
			}
			objIds[idx] = id
		}

		_ = oa.DelegateFullAllocatorsToCache()
		time.Sleep(5 * time.Millisecond)
	}

	finalSize, err := pc.TempFileSize()
	if err != nil {
		t.Fatalf("TempFileSize failed: %v", err)
	}

	const maxGrowth = int64(64 * 1024) // 64KB guardrail for soak test
	if finalSize > baseSize+maxGrowth {
		t.Fatalf("pool cache temp file grew unexpectedly: base=%d final=%d", baseSize, finalSize)
	}
}
