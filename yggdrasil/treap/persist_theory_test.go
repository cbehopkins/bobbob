package treap

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestTheory1PersistInvalidationThrashing tests whether inserting a node and then persisting
// invalidates previously inserted nodes, causing thrashing where old nodes must be re-written.
//
// THEORY: When you insert a node and then persist, the entire tree is written.
// If all nodes get invalidated on each insert, this would cause massive re-writes.
func TestTheory1PersistInvalidationThrashing(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_store.bin")
	stre, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Track object IDs and invalidation events
	type nodeRecord struct {
		key      int32
		objectId bobbob.ObjectId
	}

	nodes := make([]nodeRecord, 0, 20)
	invalidationCount := 0
	initializationCount := 0

	// Insert 20 nodes and persistafter each insert to simulate real usage
	for i := 0; i < 20; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		treap.Insert(key)

		// Record the node after insertion
		node := treap.Search(key)
		if node == nil {
			t.Fatalf("Failed to find key %d after insert", i)
		}
		pNode := node.(*PersistentTreapNode[types.IntKey])
		nodes = append(nodes, nodeRecord{key: int32(i), objectId: pNode.objectId})

		// Persist the tree
		if err := treap.Persist(); err != nil {
			t.Fatalf("Failed to persist tree: %v", err)
		}

		// After persist, the newly inserted node should have a valid objectId
		node = treap.Search(key)
		pNode = node.(*PersistentTreapNode[types.IntKey])
		if pNode.objectId < 0 {
			t.Logf("WARNING: Node %d has invalid objectId %d after persist", i, pNode.objectId)
			initializationCount++
		} else {
			t.Logf("Node %d persisted with objectId %d", i, pNode.objectId)
		}
	}

	// Now check if previously persisted nodes got invalidated
	t.Logf("\n=== Invalidation Check ===")
	for i := 0; i < len(nodes)-1; i++ { // Don't check the last one (it's always new)
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(nodes[i].key)
		node := treap.Search(key)
		if node == nil {
			t.Logf("ERROR: Node %d disappeared from tree", nodes[i].key)
			invalidationCount++
			continue
		}
		pNode := node.(*PersistentTreapNode[types.IntKey])

		if pNode.objectId != nodes[i].objectId && nodes[i].objectId > 0 {
			t.Logf("INVALIDATION DETECTED: Node %d changed from objectId %d to %d",
				nodes[i].key, nodes[i].objectId, pNode.objectId)
			invalidationCount++
		} else if pNode.objectId < 0 && nodes[i].objectId > 0 {
			t.Logf("INVALIDATION DETECTED: Node %d went from valid (%d) to invalid (%d)",
				nodes[i].key, nodes[i].objectId, pNode.objectId)
			invalidationCount++
		}
	}

	t.Logf("\n=== THEORY 1 SUMMARY ===")
	t.Logf("Total nodes: %d", len(nodes))
	t.Logf("Invalidation events: %d (%.1f%%)", invalidationCount, float64(invalidationCount)*100.0/float64(len(nodes)-1))
	t.Logf("Nodes with uninitialized objectIds after persist: %d", initializationCount)

	if invalidationCount > 0 {
		t.Logf("\n✗ THEORY 1 CONFIRMED: Nodes are being invalidated/re-persisted")
		t.Logf("This indicates thrashing: previouslywritten nodes are being rewritten on each insert+persist cycle")
	} else {
		t.Logf("\n✓ THEORY 1 DISPROVEN: Nodes are NOT being invalidated")
		t.Logf("ObjectIds remain stable across persist operations")
	}
}

// TestTheory2TimestampContaminationWalk tests whether tree walks (InOrderVisit, Count, etc)
// touch the lastAccessTime field on all nodes, making the timestamp useless for LRU.
//
// THEORY: When you do a tree walk, nodes are loaded from disk with lastAccessTime=0,
// but some operation in the walk might update all timestamps to "now", making it
// impossible to distinguish between old nodes and freshly accessed nodes.
func TestTheory2TimestampContaminationWalk(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_store.bin")
	stre, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Insert 50 nodes
	const nodeCount = 50
	for i := 0; i < nodeCount; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		treap.Insert(key)
	}

	// Persist everything
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Record creation time
	creationTime := currentUnixTime()

	// Sleep briefly so any access after this would have a later timestamp
	time.Sleep(10 * time.Millisecond)
	accessAfterWalk := currentUnixTime()

	t.Logf("Creation time: %d", creationTime)
	t.Logf("Time after sleep: %d (delta: %dms)", accessAfterWalk, (accessAfterWalk-creationTime)/1000000)

	// ===== TEST A: GetInMemoryNodes (should NOT touch timestamps) =====
	t.Logf("\n=== TEST A: GetInMemoryNodes (read-only) ===")

	inMemoryNodes := treap.GetInMemoryNodes()

	t.Logf("Nodes in memory before walk: %d", len(inMemoryNodes))

	touchedByGetInMemory := 0
	for _, nodeInfo := range inMemoryNodes {
		if nodeInfo.LastAccessTime > creationTime+(1*time.Second.Nanoseconds()) {
			touchedByGetInMemory++
			t.Logf("  Node touched by GetInMemoryNodes: lastAccessTime=%d (delta=%dms)",
				nodeInfo.LastAccessTime, (nodeInfo.LastAccessTime-creationTime)/1000000)
		}
	}

	t.Logf("Nodes with updated timestamps after GetInMemoryNodes: %d / %d", touchedByGetInMemory, len(inMemoryNodes))

	// ===== TEST B: InOrderVisit (may touch timestamps if callback does) =====
	t.Logf("\n=== TEST B: InOrderVisit + callback ===")

	beforeVisit := currentUnixTime()
	visitCount := 0
	err = treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		visitCount++
		return nil
	})
	afterVisit := currentUnixTime()

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	t.Logf("Visited nodes: %d", visitCount)
	t.Logf("InOrderVisit took: %dms", (afterVisit-beforeVisit)/1000000)

	// ===== TEST C: Count() method (uses InOrderVisit internally) =====
	t.Logf("\n=== TEST C: Count() ===")

	beforeCount := currentUnixTime()
	count, err := treap.Count()
	afterCount := currentUnixTime()

	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	t.Logf("Count returned: %d", count)
	t.Logf("Count() took: %dms", (afterCount-beforeCount)/1000000)

	// ===== ANALYSIS: Check if timestamps changed after Count ===
	t.Logf("\n=== Timestamp Analysis After Count() ===")

	afterCountNodes := treap.GetInMemoryNodes()
	t.Logf("Nodes in memory after Count(): %d", len(afterCountNodes))

	touchedByCount := 0
	for _, nodeInfo := range afterCountNodes {
		// If timestamp is after the Count() operation, it was touched
		if nodeInfo.LastAccessTime >= beforeCount {
			touchedByCount++
		}
	}

	t.Logf("Nodes with timestamps >= beforeCount: %d / %d", touchedByCount, len(afterCountNodes))

	// ===== SEARCH TEST: Verify Search DOES touch timestamps =====
	t.Logf("\n=== Sanity Check: Verify Search touches timestamps ===")

	beforeSearch := currentUnixTime()
	key := (*types.IntKey)(new(int32))
	*key = 25
	searchNode := treap.Search(key)
	pSearchNode := searchNode.(*PersistentTreapNode[types.IntKey])
	searchTimestamp := pSearchNode.GetLastAccessTime()
	afterSearch := currentUnixTime()

	t.Logf("Key 25 timestamp after Search: %d (current: %d)", searchTimestamp, afterSearch)
	if searchTimestamp >= beforeSearch {
		t.Logf("✓ SANITY CHECK PASSED: Search() properly touches timestamps")
	} else {
		t.Logf("✗ SANITY CHECK FAILED: Search() did not touch timestamp")
	}

	// ===== THEORY 2 VERDICT =====
	t.Logf("\n=== THEORY 2 SUMMARY ===")

	if touchedByCount == len(afterCountNodes) && len(afterCountNodes) > 0 {
		t.Logf("✗ THEORY 2 CONFIRMED: Count() touched ALL nodes")
		t.Logf("All %d nodes have current timestamps, making LRU tracking impossible", touchedByCount)
		t.Logf("Timestamp values cannot distinguish between old and recent accesses")
	} else if touchedByCount > 0 {
		t.Logf("⚠ THEORY 2 PARTIALLY CONFIRMED: Count() touched %d / %d nodes", touchedByCount, len(afterCountNodes))
		t.Logf("This is still problematic for LRU as it contaminates timestamps")
	} else {
		t.Logf("✓ THEORY 2 DISPROVEN: Count() did NOT touch any timestamps")
		t.Logf("Timestamps remain stable across tree walks, LRU tracking is viable")
	}

	t.Logf("\nNotes:")
	t.Logf("- Nodes loaded from disk get lastAccessTime=0 unless Search() is called")
	t.Logf("- InOrderVisit uses GetTransientChild() which loads from disk without updating timestamps")
	t.Logf("- Real contamination would happen if tree walks call TouchAccessTime() on all nodes")
}

// TestTheory2FlushOldestWithBadTimestamps tests whether directory 2 - if timestamps are
// contaminated, can FlushOldestPercentile() still work correctly?
func TestTheory2FlushOldestWithContaminatedTimestamps(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_store.bin")
	stre, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, stre)

	// Insert and persist 100 nodes
	for i := 0; i < 100; i++ {
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		treap.Insert(key)
	}

	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	// Sleep so there's a clear time gap
	time.Sleep(50 * time.Millisecond)

	// Access specific nodes (simulate recent access)
	recentAccessTime := currentUnixTime()
	for i := 0; i < 10; i++ { // Access first 10 nodes
		key := (*types.IntKey)(new(int32))
		*key = types.IntKey(i)
		node := treap.Search(key)
		if node != nil {
			pNode := node.(*PersistentTreapNode[types.IntKey])
			pNode.TouchAccessTime()
		}
	}

	// Now do a tree walk (Count) which might contaminate timestamps
	count, _ := treap.Count()
	t.Logf("Count returned: %d", count)

	afterWalk := currentUnixTime()

	// Check if the first 10 nodes are still "recent"
	inMemory := treap.GetInMemoryNodes()
	recentCount := 0
	oldCount := 0

	for _, nodeInfo := range inMemory {
		// Recent nodes should have timestamps close to recentAccessTime
		// Old nodes should have timestamps close to creationTime
		if nodeInfo.LastAccessTime >= recentAccessTime {
			recentCount++
		} else {
			oldCount++
		}
	}

	t.Logf("\nAfter Count():")
	t.Logf("Recent nodes (accessed since recentAccessTime): %d", recentCount)
	t.Logf("Old nodes (not accessed recently): %d", oldCount)
	t.Logf("Time since recent access: %dms", (afterWalk-recentAccessTime)/1000000)

	// Try to flush the oldest 80%
	flushed, err := treap.FlushOldestPercentile(80)
	if err != nil {
		t.Logf("FlushOldestPercentile returned error: %v", err)
	}

	t.Logf("\nFlushOldestPercentile(80) flushed: %d nodes", flushed)
	t.Logf("Expected approximately: ~80 nodes")

	remaining := treap.GetInMemoryNodes()
	t.Logf("Nodes remaining in memory: %d (expected ~20)", len(remaining))

	// The test is: if theory 2 is true (timestamps contaminated),
	// then FlushOldestPercentile will flush randomly because all nodes have "now" as timestamp
	if flushed < 10 {
		t.Logf("\n⚠ THEORY 2 SIDE EFFECT: FlushOldestPercentile couldn't work properly")
		t.Logf("   Flushed only %d nodes when expecting ~80", flushed)
		t.Logf("   This suggests timestamp contamination makes LRU impossible")
	} else if len(remaining) > 30 {
		t.Logf("\n✗ THEORY 2 LIKELY CONFIRMED: Too many nodes remain")
		t.Logf("   This indicates timestamps are \"now\" for all nodes")
	} else {
		t.Logf("\n✓ THEORY 2 DISPROVEN: FlushOldestPercentile worked correctly")
		t.Logf("   Flushed %d nodes as expected, %d remain", flushed, len(remaining))
	}
}
