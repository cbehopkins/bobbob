package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestAggressiveFlushing verifies that explicit flushing reduces memory usage
// after iteration, without relying on iterator options.
func TestAggressiveFlushing(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	// Create a balanced tree with deterministic priorities
	// Use InsertComplex to control the tree structure
	// Priority decreases from center to edges for a more balanced tree
	k1, k2, k3, k4, k5, k6, k7 := types.IntKey(1), types.IntKey(2), types.IntKey(3), types.IntKey(4), types.IntKey(5), types.IntKey(6), types.IntKey(7)
	treap.InsertComplex(&k4, Priority(1000)) // Root
	treap.InsertComplex(&k2, Priority(900))
	treap.InsertComplex(&k6, Priority(900))
	treap.InsertComplex(&k1, Priority(800))
	treap.InsertComplex(&k3, Priority(800))
	treap.InsertComplex(&k5, Priority(800))
	treap.InsertComplex(&k7, Priority(800))

	// Persist everything
	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Get baseline - all nodes in memory
	initialNodes := treap.GetInMemoryNodes()
	t.Logf("Initial in-memory nodes: %d", len(initialNodes))

	// Iterate
	visitedCount := 0
	err = treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		visitedCount++

		// Check memory after each visit
		inMemory := treap.GetInMemoryNodes()
		t.Logf("After visiting key, in-memory nodes: %d", len(inMemory))

		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	if visitedCount != 7 {
		t.Errorf("Expected to visit 7 nodes, visited %d", visitedCount)
	}

	// Explicitly flush after iteration to reduce memory
	if _, err := treap.FlushOldestPercentile(75); err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}

	finalNodes := treap.GetInMemoryNodes()
	t.Logf("Final in-memory nodes: %d", len(finalNodes))

	if len(finalNodes) >= len(initialNodes) {
		t.Errorf("Expected explicit flush to reduce memory, but initial=%d, final=%d",
			len(initialNodes), len(finalNodes))
	}
}

// TestFlushingComparison compares memory usage with and without explicit flushing.
func TestFlushingComparison(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()

	// Test 1: No explicit flush
	treap1 := NewPersistentTreap(types.IntLess, templateKey, stre)
	for i := types.IntKey(1); i <= 20; i++ {
		key := i
		treap1.Insert(&key)
	}
	treap1.Persist()

	treap1.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error { return nil })

	keepInMemoryCount := len(treap1.GetInMemoryNodes())
	t.Logf("With KeepInMemory=true: %d nodes in memory", keepInMemoryCount)

	// Test 2: Explicit flush after iteration
	treap2 := NewPersistentTreap(types.IntLess, templateKey, stre)
	for i := types.IntKey(1); i <= 20; i++ {
		key := i
		treap2.Insert(&key)
	}
	treap2.Persist()

	treap2.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error { return nil })
	if _, err := treap2.FlushOldestPercentile(75); err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}

	aggressiveFlushCount := len(treap2.GetInMemoryNodes())
	t.Logf("With explicit flush: %d nodes in memory", aggressiveFlushCount)

	// Explicit flushing should use significantly less memory
	if aggressiveFlushCount >= keepInMemoryCount {
		t.Errorf("Explicit flushing should reduce memory: keepInMemory=%d, aggressive=%d",
			keepInMemoryCount, aggressiveFlushCount)
	}

	// Calculate memory reduction
	reduction := float64(keepInMemoryCount-aggressiveFlushCount) / float64(keepInMemoryCount) * 100
	t.Logf("Memory reduction: %.1f%%", reduction)
}
