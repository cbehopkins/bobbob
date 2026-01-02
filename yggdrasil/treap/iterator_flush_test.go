package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

// TestAggressiveFlushing verifies that nodes are flushed after processing their right subtree
// TestAggressiveFlushing verifies that WalkKeys with KeepInMemory=false aggressively
// flushes nodes during iteration, reducing memory usage during traversal.
func TestAggressiveFlushing(t *testing.T) {
	stre, err := store.NewBasicStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	templateKey := IntKey(0).New()
	treap := NewPersistentTreap(IntLess, templateKey, stre)

	// Create a balanced tree with deterministic priorities
	// Use InsertComplex to control the tree structure
	// Priority decreases from center to edges for a more balanced tree
	k1, k2, k3, k4, k5, k6, k7 := IntKey(1), IntKey(2), IntKey(3), IntKey(4), IntKey(5), IntKey(6), IntKey(7)
	treap.InsertComplex(&k4, Priority(1000)) // Root
	treap.InsertComplex(&k2, Priority(900))
	treap.InsertComplex(&k6, Priority(900))
	treap.InsertComplex(&k1, Priority(800))
	treap.InsertComplex(&k3, Priority(800))
	treap.InsertComplex(&k5, Priority(800))
	treap.InsertComplex(&k7, Priority(800))

	// Persist everything
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Get baseline - all nodes in memory
	initialNodes := treap.GetInMemoryNodes()
	t.Logf("Initial in-memory nodes: %d", len(initialNodes))

	// Iterate with aggressive flushing
	opts := IteratorOptions{
		KeepInMemory: false,
		LoadPayloads: false,
	}

	visitedCount := 0
	err = treap.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error {
		visitedCount++

		// Check memory after each visit
		inMemory := treap.GetInMemoryNodes()
		t.Logf("After visiting key, in-memory nodes: %d", len(inMemory))

		return nil
	})
	if err != nil {
		t.Fatalf("WalkInOrderKeys failed: %v", err)
	}

	if visitedCount != 7 {
		t.Errorf("Expected to visit 7 nodes, visited %d", visitedCount)
	}

	// After iteration, very few nodes should remain
	finalNodes := treap.GetInMemoryNodes()
	t.Logf("Final in-memory nodes: %d", len(finalNodes))

	// With aggressive flushing, we should have significantly fewer nodes
	// The exact number depends on tree structure, but it should be much less than the initial count
	if len(finalNodes) >= len(initialNodes) {
		t.Errorf("Expected aggressive flushing to reduce memory, but initial=%d, final=%d",
			len(initialNodes), len(finalNodes))
	}
}

// TestFlushingComparison compares memory usage with and without aggressive flushing
// TestFlushingComparison compares memory usage between KeepInMemory=true and
// KeepInMemory=false, demonstrating the memory reduction from aggressive flushing.
func TestFlushingComparison(t *testing.T) {
	stre, err := store.NewBasicStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	templateKey := IntKey(0).New()

	// Test 1: Keep in memory
	treap1 := NewPersistentTreap(IntLess, templateKey, stre)
	for i := IntKey(1); i <= 20; i++ {
		key := i
		treap1.Insert(&key)
	}
	treap1.Persist()

	opts := IteratorOptions{KeepInMemory: true, LoadPayloads: false}
	treap1.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error { return nil })

	keepInMemoryCount := len(treap1.GetInMemoryNodes())
	t.Logf("With KeepInMemory=true: %d nodes in memory", keepInMemoryCount)

	// Test 2: Aggressive flushing
	treap2 := NewPersistentTreap(IntLess, templateKey, stre)
	for i := IntKey(1); i <= 20; i++ {
		key := i
		treap2.Insert(&key)
	}
	treap2.Persist()

	opts = IteratorOptions{KeepInMemory: false, LoadPayloads: false}
	treap2.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error { return nil })

	aggressiveFlushCount := len(treap2.GetInMemoryNodes())
	t.Logf("With KeepInMemory=false (aggressive flush): %d nodes in memory", aggressiveFlushCount)

	// Aggressive flushing should use significantly less memory
	if aggressiveFlushCount >= keepInMemoryCount {
		t.Errorf("Aggressive flushing should reduce memory: keepInMemory=%d, aggressive=%d",
			keepInMemoryCount, aggressiveFlushCount)
	}

	// Calculate memory reduction
	reduction := float64(keepInMemoryCount-aggressiveFlushCount) / float64(keepInMemoryCount) * 100
	t.Logf("Memory reduction: %.1f%%", reduction)
}
