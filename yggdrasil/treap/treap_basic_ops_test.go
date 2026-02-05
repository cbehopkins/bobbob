package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func countTreapNodes[T any](t *Treap[T]) int {
	count := 0
	t.Walk(func(node TreapNodeInterface[T]) {
		if node != nil && !node.IsNil() {
			count++
		}
	})
	return count
}

func TestTreapInsertSingle(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	key := types.IntKey(42)
	treap.Insert(key)

	found := treap.Search(key)
	if found == nil || found.IsNil() {
		t.Fatal("expected to find inserted key")
	}

	if count := countTreapNodes(treap); count != 1 {
		t.Fatalf("expected 1 node, got %d", count)
	}
}

func TestTreapInsertDuplicateNoDup(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	key := types.IntKey(7)
	treap.Insert(key)
	treap.Insert(key)

	if count := countTreapNodes(treap); count != 1 {
		t.Fatalf("expected 1 node after duplicate insert, got %d", count)
	}
}

func TestTreapDeleteNonExistent(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	for i := 0; i < 5; i++ {
		key := types.IntKey(i)
		treap.Insert(key)
	}

	before := countTreapNodes(treap)
	treap.Delete(types.IntKey(999))
	after := countTreapNodes(treap)

	if before != after {
		t.Fatalf("expected node count unchanged, before=%d after=%d", before, after)
	}
}

func TestTreapDeleteSingleElement(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	key := types.IntKey(99)
	treap.Insert(key)

	treap.Delete(key)
	if node := treap.Search(key); node != nil && !node.IsNil() {
		t.Fatal("expected key to be deleted from single-element treap")
	}

	if count := countTreapNodes(treap); count != 0 {
		t.Fatalf("expected 0 nodes after delete, got %d", count)
	}
}

func TestTreapSearchEmpty(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	found := treap.Search(types.IntKey(1))
	if found != nil && !found.IsNil() {
		t.Fatal("expected nil result when searching empty treap")
	}
}

func TestTreapSearchExistingAndMissing(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	found := treap.Search(types.IntKey(15))
	if found == nil || found.IsNil() {
		t.Fatal("expected to find existing key")
	}

	missing := treap.Search(types.IntKey(999))
	if missing != nil && !missing.IsNil() {
		t.Fatal("expected nil result for missing key")
	}
}

func TestTreapDeleteLeafAndRoot(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Delete a leaf (likely 5 or 30 depending on rotations)
	treap.Delete(types.IntKey(5))
	if node := treap.Search(types.IntKey(5)); node != nil && !node.IsNil() {
		t.Fatal("expected leaf key 5 to be deleted")
	}

	// Delete root key (not guaranteed to be actual root, but should be removed)
	treap.Delete(types.IntKey(10))
	if node := treap.Search(types.IntKey(10)); node != nil && !node.IsNil() {
		t.Fatal("expected key 10 to be deleted")
	}
}

func TestTreapDeleteInternalNode(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30, 12, 18}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Delete an internal node (15 should have children in this setup)
	treap.Delete(types.IntKey(15))
	if node := treap.Search(types.IntKey(15)); node != nil && !node.IsNil() {
		t.Fatal("expected internal key 15 to be deleted")
	}
}

func TestTreapInsertMultipleOrders(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	for i := 1; i <= 50; i++ {
		treap.Insert(types.IntKey(i))
	}

	for i := 100; i >= 51; i-- {
		treap.Insert(types.IntKey(i))
	}

	if count := countTreapNodes(treap); count != 100 {
		t.Fatalf("expected 100 nodes after inserts, got %d", count)
	}

	for _, key := range []types.IntKey{1, 25, 50, 75, 100} {
		if node := treap.Search(key); node == nil || node.IsNil() {
			t.Fatalf("expected to find key %d after inserts", key)
		}
	}
}

func TestTreapUpdatePriorityKeepsKey(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)
	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Update priority for an existing key
	treap.UpdatePriority(types.IntKey(15), Priority(999))

	if node := treap.Search(types.IntKey(15)); node == nil || node.IsNil() {
		t.Fatal("expected key 15 to remain after UpdatePriority")
	}
}
