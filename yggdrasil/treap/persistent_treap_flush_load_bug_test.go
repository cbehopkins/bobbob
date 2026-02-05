package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestMinimalFlushLoadBug is a minimal reproducer for the bug where
// InOrderVisit does not load flushed nodes from disk.
//
// Steps:
// 1. Create a small treap with 10 nodes
// 2. Persist the entire tree to disk
// 3. Manually flush some nodes (clear pointers, keep objectIds)
// 4. Call InOrderVisit which should reload flushed nodes via GetLeft()/GetRight()
// 5. Verify all 10 nodes are yielded
//
// Expected: All 10 nodes yielded
// Actual (BUG): Only in-memory nodes yielded, flushed nodes not loaded
func TestMinimalFlushLoadBug(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	// Insert 10 nodes
	keys := []types.IntKey{50, 30, 70, 20, 40, 60, 80, 10, 25, 35}
	for _, k := range keys {
		key := k
		treap.Insert(&key)
	}

	t.Logf("Inserted %d nodes", len(keys))

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
	rootNode, ok := treap.root.(*PersistentTreapNode[types.IntKey])
	if !ok {
		treap.mu.Unlock()
		t.Fatalf("Root is not a PersistentTreapNode")
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

	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key, ok := node.GetKey().(*types.IntKey)
		if !ok {
			return nil
		}
		yieldedKeys = append(yieldedKeys, *key)
		yieldedCount++
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	t.Logf("InOrderVisit yielded %d nodes (expected %d)", yieldedCount, len(keys))
	t.Logf("Yielded keys: %v", yieldedKeys)

	// THE BUG: This assertion fails because InOrderVisit only yields in-memory nodes
	if yieldedCount != len(keys) {
		t.Errorf("BUG REPRODUCED: InOrderVisit yielded only %d of %d nodes", yieldedCount, len(keys))
		t.Errorf("This means flushed nodes were NOT loaded from disk via GetLeft()/GetRight()")
		t.Errorf("In-memory after flush: %d, Yielded: %d", inMemoryAfter, yieldedCount)
	} else {
		t.Logf("SUCCESS: All nodes were yielded (bug is fixed)")
	}
}

// TestManualGetLeftGetRightAfterFlush tests the lazy-loading mechanism directly.
// This verifies that GetLeft()/GetRight() actually load flushed nodes.
func TestManualGetLeftGetRightAfterFlush(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	// Create a simple 3-node tree: 50 (root), 30 (left), 70 (right)
	// Use InsertComplex to control priorities and ensure balanced tree
	k50, k30, k70 := types.IntKey(50), types.IntKey(30), types.IntKey(70)
	treap.InsertComplex(&k50, 100) // Highest priority - will be root
	treap.InsertComplex(&k30, 50)  // Lower priority - will be left child
	treap.InsertComplex(&k70, 50)  // Same priority as left - will be right child

	// Persist
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist: %v", err)
	}

	// Get the root
	treap.mu.Lock()
	rootNode, ok := treap.root.(*PersistentTreapNode[types.IntKey])
	if !ok {
		treap.mu.Unlock()
		t.Fatalf("Root is not a PersistentTreapNode")
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
	}

	// Call GetRight() - it SHOULD reload from disk
	rightNode := rootNode.GetRight()
	if rightNode == nil {
		t.Errorf("BUG: GetRight() returned nil, should have loaded from disk (objectId=%v)", rootNode.rightObjectId)
	} else {
		t.Logf("SUCCESS: GetRight() loaded node from disk")
		rightKey := rightNode.GetKey().(*types.IntKey)
		t.Logf("Loaded right key: %v", *rightKey)
	}

	treap.mu.Unlock()
}
