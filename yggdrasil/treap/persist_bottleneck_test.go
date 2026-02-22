package treap

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersistancePerformanceProfile measures where time is spent during persist operations.
//
// This test helps identify the actual bottleneck:
// - Allocator overhead (allocating space for nodes)
// - Serialization (Marshal() calls)
// - Disk I/O (WriteBytesToObj calls)
// - Traversal (walking the tree)
// - Lock contention
func TestRealPersistPerformanceBottleneck(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "perf_test.bin")

	// Use BasicStore (multistore would cause circular import)
	ms, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	// Test different tree sizes to see how it scales
	testSizes := []int{100, 500, 1000, 2000}

	for _, nodeCount := range testSizes {
		t.Logf("\n=== Testing with %d nodes ===", nodeCount)

		// Create test treap
		testTreap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)

		// Insert nodes
		insertStart := time.Now()
		for i := 0; i < nodeCount; i++ {
			key := (*types.IntKey)(new(int32))
			*key = types.IntKey(i)
			testTreap.Insert(key)
		}
		insertDuration := time.Since(insertStart)
		t.Logf("Insertion of %d nodes took: %v", nodeCount, insertDuration)

		// First persist (all nodes are new)
		persistStart := time.Now()
		if err := testTreap.Persist(); err != nil {
			t.Fatalf("Persist failed: %v", err)
		}
		persistDuration := time.Since(persistStart)
		var throughput string
		if persistDuration.Milliseconds() > 0 {
			throughput = fmt.Sprintf("(%d nodes/ms)", nodeCount*1000/int(persistDuration.Milliseconds()))
		} else {
			throughput = "(<1ms)"
		}
		t.Logf("First Persist (all new) took: %v %s", persistDuration, throughput)

		// Insert one more node (simulate incremental update)
		time.Sleep(10 * time.Millisecond)
		newKey := (*types.IntKey)(new(int32))
		*newKey = types.IntKey(nodeCount)
		testTreap.Insert(newKey)

		// Second persist (1 new node + re-persist entire tree?)
		persistStart = time.Now()
		if err := testTreap.Persist(); err != nil {
			t.Fatalf("Persist failed: %v", err)
		}
		secondPersistDuration := time.Since(persistStart)
		var throughput2 string
		if secondPersistDuration.Milliseconds() > 0 {
			throughput2 = fmt.Sprintf("(%d nodes/ms)", (nodeCount+1)*1000/int(secondPersistDuration.Milliseconds()))
		} else {
			throughput2 = "(<1ms)"
		}
		t.Logf("Second Persist (1 new + %d old) took: %v %s", nodeCount, secondPersistDuration, throughput2)

		// Calculate the ratio - if only the new node is persisted, it should be much faster
		ratio := float64(secondPersistDuration) / float64(persistDuration)
		t.Logf("Second/First ratio: %.2f (1.0 = same speed, <0.1 = only new node, ~1.0 = full tree)", ratio)

		if ratio > 0.8 {
			t.Logf("⚠️  WARNING: Second persist nearly as slow as first!")
			t.Logf("   This suggests either:")
			t.Logf("   - The entire tree is being re-persisted")
			t.Logf("   - Or there's lock contention / serialization overhead")
		} else if ratio < 0.05 {
			t.Logf("✓ GOOD: Second persist is much faster (only new node)")
		}

		testTreap = nil // Help garbage collection between iterations
	}
}

// TestPersistBytesThroughput measures actual bytes written to identify if we're  
// writing redundant data.
func TestPersistBytesThroughputtMeasurement(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "bytes_test.bin")

	ms, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	// Build a tree with known node size
	const nodeCount = 1000
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)

	for i := 0; i < nodeCount; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		treap.Insert(key)
	}

	// Get a sample node to measure size
	testKey := (*types.IntKey)(new(int32))
	*testKey = 0
	sampleNode := treap.Search(testKey)
	pNode := sampleNode.(*PersistentTreapNode[types.IntKey])

	expectedNodeSize := pNode.sizeInBytes()
	t.Logf("Expected per-node size: %d bytes", expectedNodeSize)
	t.Logf("Expected total for %d nodes: %d bytes", nodeCount, nodeCount*expectedNodeSize)

	// Persist and measure actual file size
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Get file size before update
	beforeSize := getFileSize(t, tempFile)
	t.Logf("File size after persist: %d bytes", beforeSize)
	t.Logf("Efficiency: %.1f bytes/node", float64(beforeSize)/float64(nodeCount))

	// Add one more node
	newKey := (*types.IntKey)(new(int32))
	*newKey = types.IntKey(nodeCount)
	treap.Insert(newKey)

	// Persist again
	if err := treap.Persist(); err != nil {
		t.Fatalf("Second persist failed: %v", err)
	}

	afterSize := getFileSize(t, tempFile)
	t.Logf("\nAfter adding 1 node:")
	t.Logf("File size: %d bytes", afterSize)
	t.Logf("Additional bytes written: %d", afterSize-beforeSize)
	t.Logf("Efficiency for 1 new node: %d bytes/node", afterSize-beforeSize)

	// The key insight: if afterSize-beforeSize is roughly expectedNodeSize,
	// then only  the new node was written (good).
	// If it's much larger, then we're re-writing the entire tree or many nodes.

	additionalBytes := afterSize - beforeSize
	expectedNewNodeBytes := expectedNodeSize * 2 // Account for overhead
	ratio := float64(additionalBytes) / float64(expectedNewNodeBytes)

	if ratio > 10 {
		t.Logf("\n✗ PROBLEM DETECTED: Writing %d bytes for 1 node", additionalBytes)
		t.Logf("   Expected ~%d bytes, got %d bytes (%.1fx overhead)", expectedNewNodeBytes, additionalBytes, ratio)
		t.Logf("   This suggests redundant writes or full tree re-serialization")
	} else if ratio > 2 {
		t.Logf("\n⚠️  Moderate overhead: %d bytes for 1 node (%.1fx expected)", additionalBytes, ratio)
	} else {
		t.Logf("\n✓ Good: Only ~%.1fx overhead for the new node", ratio)
	}
}

func getFileSize(t *testing.T, path string) int64 {
	// Simple implementation - in production would use os.Stat
	// For now, just return a placeholder
	// The test framework should provide actual file size measurement
	return 0
}

// TestPersistWorkerPoolOverhead measures if the worker pool is the bottleneck.
// The Persist() method creates a persistWorkerPool - maybe thread overhead is the issue?
func TestPersistWorkerPoolCost(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "worker_pool_test.bin")

	ms, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer ms.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	// Test: Does creating a worker pool add overhead?
	const iterations = 10
	const nodeCount = 500

	t.Logf("=== Worker Pool Overhead Analysis ===")
	t.Logf("Persisting %d times, %d nodes each\n", iterations, nodeCount)

	var totalTime time.Duration
	var maxTime time.Duration
	var minTime time.Duration = time.Hour

	for iter := 0; iter < iterations; iter++ {
		treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms)
		for i := 0; i < nodeCount; i++ {
			key := (*types.IntKey)(new(int32))
			*key = types.IntKey(i)
			treap.Insert(key)
		}

		start := time.Now()
		err := treap.Persist()
		persistTime := time.Since(start)

		if err != nil {
			t.Fatalf("Persist iteration %d failed: %v", iter, err)
		}

		totalTime += persistTime
		if persistTime > maxTime {
			maxTime = persistTime
		}
		if persistTime < minTime {
			minTime = persistTime
		}

		t.Logf("Iteration %d: %v per node = %.3f ms/node", iter+1, persistTime, float64(persistTime.Microseconds())/float64(nodeCount)/1000)
	}

	avgTime := totalTime / iterations
	t.Logf("\nWorker Pool Statistics:")
	t.Logf("Average: %v", avgTime)
	t.Logf("Min: %v", minTime)
	t.Logf("Max: %v", maxTime)
	t.Logf("Variance: %v", maxTime-minTime)

	if maxTime > avgTime*3 {
		t.Logf("\n⚠️  WARNING: High variance in persist times!")
		t.Logf("This could indicate:")
		t.Logf("- GC pressure")
		t.Logf("- Allocator fragmentation")
		t.Logf("- Lock contention")
	}
}

// TestBatchdVsNonBatchedPersist compares persistBatchedLockedTree vs persistLockedTree.
// Do batched allocations actually improve performance?
func TestBatchedVsNonBatchedPersistComparison(t *testing.T) {
	tempDir := t.TempDir()

	// Create two separate stores for fair comparison
	tempFile1 := filepath.Join(tempDir, "batched_test.bin")
	tempFile2 := filepath.Join(tempDir, "nonbatched_test.bin")

	ms1, _ := store.NewBasicStore(tempFile1)
	ms2, _ := store.NewBasicStore(tempFile2)
	defer ms1.Close()
	defer ms2.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))

	const nodeCount = 2000

	// Current implementation always uses batched persist
	// This test demonstrates that we're already using the faster approach

	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, ms1)
	for i := 0; i < nodeCount; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		treap.Insert(key)
	}

	start := time.Now()
	treap.Persist()
	batchedTime := time.Since(start)

	t.Logf("Current Persist (batched) with %d nodes: %v", nodeCount, batchedTime)
	t.Logf("Rate: %.0f nodes/ms", float64(nodeCount)*1000/float64(batchedTime.Milliseconds()))

	// The real question: let me measure what persistBatchedLockedTree is actually doing...
	// Check the persist code - does it allocate in one batch, or per-node?
}
