package treap

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestMinimalPayloadFlushLoadBug is a minimal reproducer for the bug where
// InOrderVisit does not load flushed nodes from disk for payload treaps.
//
// Steps:
// 1. Create a small payload treap with 10 nodes
// 2. Persist the entire tree to disk
// 3. Manually flush some nodes (clear pointers, keep objectIds)
// 4. Call InOrderVisit which should reload flushed nodes via GetLeft()/GetRight()
// 5. Verify all 10 nodes are yielded
//
// Expected: All 10 nodes yielded with correct payloads
// Actual (BUG): Only in-memory nodes yielded, flushed nodes not loaded
func TestMinimalPayloadFlushLoadBug(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, templateKey, stre)

	// Insert 10 nodes with payloads
	keys := []types.IntKey{50, 30, 70, 20, 40, 60, 80, 10, 25, 35}
	payloads := make([]MockPayload, len(keys))
	for i, k := range keys {
		key := k
		payloads[i] = MockPayload{Data: fmt.Sprintf("payload-%d", k)}
		treap.Insert(&key, payloads[i])
	}

	t.Logf("Inserted %d nodes with payloads", len(keys))

	// Persist the entire tree
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Count nodes in memory before flush
	inMemoryBefore := treap.CountInMemoryNodes()
	t.Logf("Nodes in memory before flush: %d", inMemoryBefore)

	// Get the root node and manually flush its children
	// This simulates what background monitoring does
	treap.mu.Lock()
	rootNode, ok := treap.root.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
	if !ok {
		treap.mu.Unlock()
		t.Fatalf("Root is not a PersistentPayloadTreapNode")
	}

	// Flush the root's children (this should set their pointers to nil but keep objectIds)
	t.Logf("Before flush: left=%v, right=%v", rootNode.TreapNode.left != nil, rootNode.TreapNode.right != nil)
	if err := rootNode.Flush(); err != nil {
		t.Logf("Flush returned error: %v", err)
	} else {
		t.Logf("Flush returned success (nil)")
	}
	t.Logf("After flush: left=%v, right=%v", rootNode.TreapNode.left != nil, rootNode.TreapNode.right != nil)

	// Verify that child pointers are nil but objectIds are valid (if they exist)
	if rootNode.TreapNode.left != nil {
		t.Errorf("Expected left pointer to be nil after flush, but it's not")
	}
	// Only check leftObjectId if it was valid before (tree might not have left child due to priorities)
	if store.IsValidObjectId(rootNode.leftObjectId) {
		// Child exists on disk, pointer should be nil after flush
		if rootNode.TreapNode.left != nil {
			t.Errorf("Left child exists on disk but pointer not nil after flush")
		}
	}

	if rootNode.TreapNode.right != nil {
		t.Errorf("Expected right pointer to be nil after flush, but it's not")
	}
	// Only check rightObjectId if it was valid before
	if store.IsValidObjectId(rootNode.rightObjectId) {
		// Child exists on disk, pointer should be nil after flush
		if rootNode.TreapNode.right != nil {
			t.Errorf("Right child exists on disk but pointer not nil after flush")
		}
	}
	treap.mu.Unlock()

	// Count nodes in memory after flush
	inMemoryAfter := treap.CountInMemoryNodes()
	t.Logf("Nodes in memory after flush: %d (expected: 1 root node)", inMemoryAfter)

	if inMemoryAfter >= inMemoryBefore {
		t.Errorf("Expected fewer nodes in memory after flush, before=%d, after=%d", inMemoryBefore, inMemoryAfter)
	}

	// Now walk the tree - this should load flushed nodes from disk
	yieldedCount := 0
	var yieldedKeys []types.IntKey
	expectedPayloads := make(map[types.IntKey]string)
	for i, k := range keys {
		expectedPayloads[k] = fmt.Sprintf("payload-%d", k)
		_ = i
	}

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		keyPtr := pNode.GetKey().(*types.IntKey)
		yieldedKeys = append(yieldedKeys, *keyPtr)
		
		// Verify payload
		expectedPayloadData := expectedPayloads[*keyPtr]
		payload := pNode.GetPayload()
		if payload.Data != expectedPayloadData {
			t.Errorf("Payload mismatch for key %d: got %q, expected %q", *keyPtr, payload.Data, expectedPayloadData)
		}
		
		yieldedCount++
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	t.Logf("InOrderVisit yielded %d nodes (expected %d)", yieldedCount, len(keys))
	t.Logf("Yielded keys: %v", yieldedKeys)

	// THE BUG: This assertion fails because iteration only yields in-memory nodes
	if yieldedCount != len(keys) {
		t.Errorf("BUG REPRODUCED: InOrderVisit yielded only %d of %d nodes", yieldedCount, len(keys))
		t.Errorf("This means flushed nodes were NOT loaded from disk via GetLeft()/GetRight()")
		t.Errorf("In-memory after flush: %d, Yielded: %d", inMemoryAfter, yieldedCount)
	} else {
		t.Logf("SUCCESS: All nodes were yielded (bug is fixed)")
	}
}

// TestManualPayloadGetLeftGetRightAfterFlush tests the lazy-loading mechanism directly
// for payload treaps. This verifies that GetLeft()/GetRight() actually load flushed nodes.
func TestManualPayloadGetLeftGetRightAfterFlush(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, templateKey, stre)

	// Create a simple 3-node tree: 50 (root), 30 (left), 70 (right)
	// Use InsertComplex to control priorities and ensure balanced tree
	k50, k30, k70 := types.IntKey(50), types.IntKey(30), types.IntKey(70)
	p50, p30, p70 := MockPayload{Data: "p50"}, MockPayload{Data: "p30"}, MockPayload{Data: "p70"}
	treap.InsertComplex(&k50, 100, p50) // Highest priority - will be root
	treap.InsertComplex(&k30, 50, p30)  // Lower priority - will be left child
	treap.InsertComplex(&k70, 50, p70)  // Same priority as left - will be right child

	// Persist
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	// Get the root
	treap.mu.Lock()
	rootNode, ok := treap.root.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
	if !ok {
		treap.mu.Unlock()
		t.Fatalf("Root is not a PersistentPayloadTreapNode")
	}

	// Verify children are in memory before flush
	if rootNode.TreapNode.left == nil {
		t.Errorf("Left child should be in memory before flush")
	}
	if rootNode.TreapNode.right == nil {
		t.Errorf("Right child should be in memory before flush")
	}

	// Flush children
	if err := rootNode.Flush(); err != nil {
		t.Logf("Flush returned: %v", err)
	}

	// Verify children are flushed (pointers nil, objectIds valid)
	if rootNode.TreapNode.left != nil {
		t.Errorf("Left pointer should be nil after flush")
	}
	if !store.IsValidObjectId(rootNode.leftObjectId) {
		t.Errorf("leftObjectId should be valid after flush")
	}

	if rootNode.TreapNode.right != nil {
		t.Errorf("Right pointer should be nil after flush")
	}
	if !store.IsValidObjectId(rootNode.rightObjectId) {
		t.Errorf("rightObjectId should be valid after flush")
	}

	t.Logf("After flush: left pointer=%v, leftObjectId=%v", rootNode.TreapNode.left, rootNode.leftObjectId)
	t.Logf("After flush: right pointer=%v, rightObjectId=%v", rootNode.TreapNode.right, rootNode.rightObjectId)

	// Now call GetLeft() - it SHOULD reload from disk
	leftNode := rootNode.GetLeft()
	if leftNode == nil {
		t.Errorf("BUG: GetLeft() returned nil, should have loaded from disk (objectId=%v)", rootNode.leftObjectId)
	} else {
		t.Logf("SUCCESS: GetLeft() loaded node from disk")
		leftKey := leftNode.GetKey().(*types.IntKey)
		t.Logf("Loaded left key: %v", *leftKey)
		
		// Verify payload was loaded correctly
		if payloadNode, ok := leftNode.(PersistentPayloadNodeInterface[types.IntKey, MockPayload]); ok {
			payload := payloadNode.GetPayload()
			if payload.Data != "p30" {
				t.Errorf("Left node payload mismatch: got %q, expected %q", payload.Data, "p30")
			} else {
				t.Logf("Left node payload correct: %q", payload.Data)
			}
		}
	}

	// Call GetRight() - it SHOULD reload from disk
	rightNode := rootNode.GetRight()
	if rightNode == nil {
		t.Errorf("BUG: GetRight() returned nil, should have loaded from disk (objectId=%v)", rootNode.rightObjectId)
	} else {
		t.Logf("SUCCESS: GetRight() loaded node from disk")
		rightKey := rightNode.GetKey().(*types.IntKey)
		t.Logf("Loaded right key: %v", *rightKey)
		
		// Verify payload was loaded correctly
		if payloadNode, ok := rightNode.(PersistentPayloadNodeInterface[types.IntKey, MockPayload]); ok {
			payload := payloadNode.GetPayload()
			if payload.Data != "p70" {
				t.Errorf("Right node payload mismatch: got %q, expected %q", payload.Data, "p70")
			} else {
				t.Logf("Right node payload correct: %q", payload.Data)
			}
		}
	}

	treap.mu.Unlock()
}

// TestPayloadFlushThenSearchReload tests that after flushing nodes, searching for
// keys properly reloads them from disk with correct payloads.
func TestPayloadFlushThenSearchReload(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, templateKey, stre)

	// Insert keys
	keys := []int{50, 30, 70, 20, 40, 60, 80}
	for _, k := range keys {
		key := types.IntKey(k)
		payload := MockPayload{Data: fmt.Sprintf("data-%d", k)}
		treap.Insert(&key, payload)
	}

	// Persist
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	t.Logf("Tree persisted, nodes in memory: %d", treap.CountInMemoryNodes())

	// Flush oldest 80%
	flushed, err := treap.FlushOldestPercentile(80)
	if err != nil {
		t.Logf("Flush returned error: %v", err)
	}
	t.Logf("Flushed %d nodes, remaining in memory: %d", flushed, treap.CountInMemoryNodes())

	// Now search for all keys - should reload from disk
	for _, k := range keys {
		searchKey := types.IntKey(k)
		result := treap.Search(&searchKey)
		if result == nil || result.IsNil() {
			t.Errorf("Failed to find key %d after flush", k)
		} else {
			payload := result.GetPayload()
			expectedData := fmt.Sprintf("data-%d", k)
			if payload.Data != expectedData {
				t.Errorf("Payload mismatch for key %d: got %q, expected %q", k, payload.Data, expectedData)
			} else {
				t.Logf("Key %d found with correct payload %q", k, payload.Data)
			}
		}
	}

	t.Logf("After searches, nodes in memory: %d", treap.CountInMemoryNodes())
}

// TestPayloadFlushPersistValidateCycle tests the cycle: insert -> persist -> flush -> validate
// to ensure data integrity is maintained through the flush/persist cycle.
func TestPayloadFlushPersistValidateCycle(t *testing.T) {
	dir, stre, cleanup := testutil.SetupTestStore(t)
	defer cleanup()
	t.Logf("Test store created at: %s", dir)

	templateKey := types.IntKey(0).New()
	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, templateKey, stre)

	// Insert initial data
	const size = 50
	for i := 0; i < size; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("item-%d", i)}
		treap.Insert(&key, payload)
	}

	// Cycle: persist -> flush -> insert -> persist -> validate
	for cycle := 0; cycle < 5; cycle++ {
		t.Logf("=== Cycle %d ===", cycle)

		// Persist
		if err := treap.Persist(); err != nil {
			t.Fatalf("Cycle %d: Persist failed: %v", cycle, err)
		}
		t.Logf("Persisted, nodes in memory: %d", treap.CountInMemoryNodes())

		// Flush 50%
		flushed, err := treap.FlushOldestPercentile(50)
		if err != nil {
			t.Logf("Cycle %d: Flush error (may be partial): %v", cycle, err)
		}
		t.Logf("Flushed %d nodes, remaining: %d", flushed, treap.CountInMemoryNodes())

		// Insert new data
		for i := 0; i < 5; i++ {
			key := types.IntKey(size + cycle*5 + i)
			payload := MockPayload{Data: fmt.Sprintf("item-%d", size+cycle*5+i)}
			treap.Insert(&key, payload)
		}
		t.Logf("Inserted 5 new keys")

		// Persist again
		if err := treap.Persist(); err != nil {
			t.Fatalf("Cycle %d: Second persist failed: %v", cycle, err)
		}

		// Validate all original keys
		missingCount := 0
		payloadMismatchCount := 0
		for i := 0; i < size; i++ {
			key := types.IntKey(i)
			result := treap.Search(&key)
			if result == nil || result.IsNil() {
				missingCount++
			} else {
				payload := result.GetPayload()
				expectedData := fmt.Sprintf("item-%d", i)
				if payload.Data != expectedData {
					payloadMismatchCount++
					if payloadMismatchCount <= 3 {
						t.Errorf("Cycle %d: Payload mismatch for key %d: got %q, expected %q", cycle, i, payload.Data, expectedData)
					}
				}
			}
		}

		if missingCount > 0 {
			t.Errorf("Cycle %d: %d keys missing", cycle, missingCount)
		}
		if payloadMismatchCount > 0 {
			t.Errorf("Cycle %d: %d payload mismatches", cycle, payloadMismatchCount)
		}
	}

	t.Logf("All cycles completed successfully")
}
