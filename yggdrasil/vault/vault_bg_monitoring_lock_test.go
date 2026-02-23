package vault

import (
	"strconv"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestConcurrentMultiStore_BackgroundMonitoring tests StringStorer with background flush
// to isolate the deadlock issue from the large dataset.
func TestConcurrentMultiStore_BackgroundMonitoring(t *testing.T) {
	tempDir := t.TempDir()
	storePath := tempDir + "/bg_monitoring_test.db"

	// Create ConcurrentMultiStore
	stre, err := multistore.NewConcurrentMultiStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent multistore: %v", err)
	}
	defer stre.Close()

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[string]{})

	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
		v, "bg_test_col", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Enable background monitoring with aggressive flushing
	v.SetMemoryBudgetWithPercentile(1000, 25) // Small memory budget to trigger flushes
	v.SetCheckInterval(100)                   // Check every 100ms
	v.StartBackgroundMonitoring()

	// Smaller initial dataset: 500 items
	size := 500
	batchSize := 50

	start := time.Now()
	t.Logf("Inserting %d items...", size)

	// Initial load
	for i := range size {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(i)}
		coll.Insert(&key, payload)
	}

	t.Logf("Initial insert took %v", time.Since(start))

	start = time.Now()
	if err := coll.Persist(); err != nil {
		t.Fatalf("Initial Persist failed: %v", err)
	}
	t.Logf("Initial Persist took %v", time.Since(start))

	nextKey := size
	numIterations := 3 // Only 3 iterations instead of b.N
	batchesInserted := 0

	// Run a few iterations while background monitoring is active
	for iteration := 0; iteration < numIterations; iteration++ {
		t.Logf("Iteration %d: Inserting batch of %d items...", iteration, batchSize)
		start = time.Now()

		for range batchSize {
			key := types.IntKey(nextKey)
			payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(nextKey)}
			coll.Insert(&key, payload)
			nextKey++
		}

		t.Logf("  Insert took %v", time.Since(start))

		start = time.Now()
		t.Logf("Iteration %d: Calling Persist()...", iteration)

		persistDone := make(chan error, 1)
		go func() {
			persistDone <- coll.Persist()
		}()

		// Use a timeout to detect if Persist() is hanging
		select {
		case err := <-persistDone:
			t.Logf("  Persist took %v, err=%v", time.Since(start), err)
			if err != nil {
				t.Fatalf("Persist failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Persist() is hanging after %v (iteration %d after %d batches)", time.Since(start), iteration, batchesInserted)
		}
		batchesInserted++
	}

	v.StopBackgroundMonitoring()
	t.Logf("SUCCESS: Completed %d iterations with background monitoring enabled", numIterations)
}
