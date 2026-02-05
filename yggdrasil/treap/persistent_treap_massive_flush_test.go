package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersistentTreapMassiveFlush tests flushing a very high percentage of nodes
// to reproduce the external test failure.
func TestPersistentTreapMassiveFlush(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	keyTemplate := (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Insert many nodes
	const nodeCount = 1000
	for i := 0; i < nodeCount; i++ {
		key := types.IntKey(i)
		treap.Insert(&key)
	}

	t.Logf("Inserted %d nodes", nodeCount)

	// Check in-memory count before flush
	inMemBefore := len(treap.GetInMemoryNodes())
	t.Logf("In-memory nodes before flush: %d", inMemBefore)

	// Flush 90% (similar to external test)
	flushed, err := treap.FlushOldestPercentile(90)
	if err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}
	t.Logf("Flushed %d nodes (90%%)", flushed)

	// Check in-memory count after flush
	inMemAfter := len(treap.GetInMemoryNodes())
	t.Logf("In-memory nodes after flush: %d", inMemAfter)
	t.Logf("Expected remaining: ~%d, Actual: %d", nodeCount-flushed, inMemAfter)

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

	// Check final in-memory count (should be nodeCount since we reloaded everything)
	inMemFinal := len(treap.GetInMemoryNodes())
	t.Logf("In-memory nodes after iteration: %d", inMemFinal)

	// This should work: iterCount should equal nodeCount
	if iterCount != nodeCount {
		t.Errorf("BUG REPRODUCED: Expected to iterate %d nodes, but only got %d", nodeCount, iterCount)
		t.Errorf("This matches the external test failure pattern")
		t.Errorf("In-memory: before=%d, after flush=%d, after iter=%d, yielded=%d",
			inMemBefore, inMemAfter, inMemFinal, iterCount)
	} else {
		t.Logf("SUCCESS: All %d nodes were iterated", nodeCount)
	}
}
