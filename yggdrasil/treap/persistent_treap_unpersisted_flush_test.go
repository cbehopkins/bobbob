package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersistentTreapFlushWithoutPersist tests the bug where nodes are flushed
// without being persisted first, causing them to be lost during iteration.
//
// This reproduces the external test failure where FlushOldestPercentile (via background
// monitoring) flushes nodes that have never been persisted, resulting in data loss.
func TestPersistentTreapFlushWithoutPersist(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Insert many nodes WITHOUT persisting
	const nodeCount = 100
	for i := 0; i < nodeCount; i++ {
		key := types.IntKey(i)
		treap.Insert(&key)
	}

	t.Logf("Inserted %d nodes WITHOUT persisting", nodeCount)

	// Check in-memory count before flush attempt
	inMemBefore := len(treap.GetInMemoryNodes())
	t.Logf("In-memory nodes before flush: %d", inMemBefore)

	// Try to flush oldest 50% WITHOUT persisting first
	flushed, err := treap.FlushOldestPercentile(50)
	if err != nil {
		t.Logf("FlushOldestPercentile error (expected): %v", err)
	}
	t.Logf("Attempted to flush 50%%, actually flushed: %d nodes", flushed)

	// Check in-memory count after flush
	inMemAfter := len(treap.GetInMemoryNodes())
	t.Logf("In-memory nodes after flush: %d", inMemAfter)

	// The bug: Flush should return 0 if nodes can't be flushed (no valid objectIds)
	// But the real problem is that users expect FlushOldestPercentile to work
	// even without explicit Persist calls

	// Count how many nodes we can iterate
	iterCount := 0
	err = treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		iterCount++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	t.Logf("InOrderVisit yielded %d nodes", iterCount)

	// The bug: iterCount should equal nodeCount, but it will only equal inMemAfter
	// because unflushed nodes without valid objectIds can't be reloaded
	if iterCount != nodeCount {
		t.Errorf("BUG: Expected to iterate %d nodes, but only got %d", nodeCount, iterCount)
		t.Errorf("This is because nodes without valid objectIds can't be flushed/reloaded")
		t.Errorf("In-memory before: %d, after: %d, yielded: %d", inMemBefore, inMemAfter, iterCount)
	}
}

// TestPersistentTreapFlushAfterPersist tests that flushing AFTER persisting works correctly.
func TestPersistentTreapFlushAfterPersist(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Insert many nodes
	const nodeCount = 100
	for i := 0; i < nodeCount; i++ {
		key := types.IntKey(i)
		treap.Insert(&key)
	}

	t.Logf("Inserted %d nodes", nodeCount)

	// PERSIST before flushing
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}
	t.Logf("Persisted all nodes")

	// Check in-memory count before flush
	inMemBefore := len(treap.GetInMemoryNodes())
	t.Logf("In-memory nodes before flush: %d", inMemBefore)

	// Now flush oldest 50% AFTER persisting
	flushed, err := treap.FlushOldestPercentile(50)
	if err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}
	t.Logf("Flushed %d nodes (50%% of %d)", flushed, nodeCount)

	// Check in-memory count after flush
	inMemAfter := len(treap.GetInMemoryNodes())
	t.Logf("In-memory nodes after flush: %d", inMemAfter)

	// Count how many nodes we can iterate
	iterCount := 0
	err = treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		iterCount++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	t.Logf("InOrderVisit yielded %d nodes", iterCount)

	// This SHOULD work: iterCount should equal nodeCount
	if iterCount != nodeCount {
		t.Errorf("FAIL: Expected to iterate %d nodes, but only got %d", nodeCount, iterCount)
		t.Errorf("Even after persisting, iteration is incomplete")
		t.Errorf("In-memory before: %d, after: %d, yielded: %d", inMemBefore, inMemAfter, iterCount)
	} else {
		t.Logf("SUCCESS: All %d nodes were iterated even after flushing", nodeCount)
	}
}
