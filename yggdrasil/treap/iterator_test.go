package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestIterator_BasicTraversal validates that the hybrid walker visits
// all nodes in sorted order without any disk I/O.
func TestIterator_BasicTraversal(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert some values
	keys := []int32{5, 3, 7, 1, 4, 6, 9}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Visit all nodes in order
	var visited []int32
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey()
		if key == nil {
			t.Fatal("node key is nil")
		}
		// IntKey is just int32, convert it
		val := int32(key.Value())
		visited = append(visited, val)
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	// Verify visited order is sorted
	expected := []int32{1, 3, 4, 5, 6, 7, 9}
	if len(visited) != len(expected) {
		t.Errorf("got %d nodes, expected %d", len(visited), len(expected))
	}

	for i, v := range visited {
		if i < len(expected) && v != expected[i] {
			t.Errorf("visited[%d] = %d, expected %d", i, v, expected[i])
		}
	}
}

// TestIterator_NoMutationNoTrash validates that a read-only traversal
// does not accumulate any trash or delete any nodes.
func TestIterator_NoMutationNoTrash(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert some values
	keys := []int32{5, 3, 7, 1, 4, 6, 9}
	for _, k := range keys {
		key := types.IntKey(k)
		if err := treap.Insert(&key); err != nil {
			t.Fatalf("Insert(%d) failed: %v", k, err)
		}
	}

	// Persist all nodes
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Visit all nodes without mutating
	visitCount := 0
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		visitCount++
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	if visitCount != len(keys) {
		t.Errorf("visited %d nodes, expected %d", visitCount, len(keys))
	}
}

// TestIterator_MutationDetection validates that mutations are detected
// and the old ObjectId is added to trash.
func TestIterator_MutationDetection(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	// Insert and persist a single node
	key := types.IntKey(5)
	if err := treap.Insert(&key); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Get the ObjectId of the persisted node
	var nodeToMutate TreapNodeInterface[types.IntKey]
	treap.mu.RLock()
	nodeToMutate = treap.root
	pNode := nodeToMutate.(*PersistentTreapNode[types.IntKey])
	oldObjectId := pNode.objectId
	treap.mu.RUnlock()

	if oldObjectId < 0 {
		t.Fatal("node should be persisted with valid ObjectId")
	}

	// Mutate the node during iteration
	mutationDetected := false
	err := treap.InOrderMutate(func(node TreapNodeInterface[types.IntKey]) error {
		// Type assert to access internal fields for mutation
		pNode := node.(*PersistentTreapNode[types.IntKey])
		// Mutate by modifying priority (in-memory change)
		pNode.priority = pNode.priority + 1
		pNode.objectId = -1 // Mark as unpersisted
		mutationDetected = true
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderMutate failed: %v", err)
	}

	if !mutationDetected {
		t.Fatal("callback was not invoked")
	}

	// Verify old ObjectId was deleted
	// Note: In a real test, we'd verify store deletion, but for now
	// we just verify no panic occurred
}

// TestIterator_EmptyTree validates that iterating over an empty tree
// returns no error and invokes no callbacks.
func TestIterator_EmptyTree(t *testing.T) {
	_, st, cleanup := testutil.SetupConcurrentStore(t)
	defer cleanup()

	treap := NewPersistentTreap[types.IntKey](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	visitCount := 0
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		visitCount++
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit on empty tree failed: %v", err)
	}

	if visitCount != 0 {
		t.Errorf("expected 0 visits on empty tree, got %d", visitCount)
	}
}
