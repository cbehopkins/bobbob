package treap

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func verifyHeapProperty(node TreapNodeInterface[types.IntKey]) error {
	if node == nil || node.IsNil() {
		return nil
	}
	left := node.GetLeft()
	right := node.GetRight()
	if left != nil && !left.IsNil() {
		if node.GetPriority() < left.GetPriority() {
			return fmt.Errorf("heap property violated: parent %d < left %d", node.GetPriority(), left.GetPriority())
		}
		if err := verifyHeapProperty(left); err != nil {
			return err
		}
	}
	if right != nil && !right.IsNil() {
		if node.GetPriority() < right.GetPriority() {
			return fmt.Errorf("heap property violated: parent %d < right %d", node.GetPriority(), right.GetPriority())
		}
		if err := verifyHeapProperty(right); err != nil {
			return err
		}
	}
	return nil
}

func verifyBSTProperty(node TreapNodeInterface[types.IntKey], min *types.IntKey, max *types.IntKey) error {
	if node == nil || node.IsNil() {
		return nil
	}
	key := node.GetKey().Value()
	if min != nil && key <= *min {
		return fmt.Errorf("bst property violated: key %d <= min %d", key, *min)
	}
	if max != nil && key >= *max {
		return fmt.Errorf("bst property violated: key %d >= max %d", key, *max)
	}
	left := node.GetLeft()
	right := node.GetRight()
	if err := verifyBSTProperty(left, min, &key); err != nil {
		return err
	}
	if err := verifyBSTProperty(right, &key, max); err != nil {
		return err
	}
	return nil
}

// TestTreapInOrderIsSorted verifies that in-order traversal yields sorted keys
// after inserting keys in random order.
func TestTreapInOrderIsSorted(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := rand.New(rand.NewSource(12345)).Perm(100)
	for _, k := range keys {
		key := types.IntKey(k)
		treap.Insert(key)
	}

	var walked []types.IntKey
	treap.Walk(func(node TreapNodeInterface[types.IntKey]) {
		walked = append(walked, node.GetKey().Value())
	})

	if len(walked) == 0 {
		t.Fatal("expected at least one key from Walk")
	}

	for i := 1; i < len(walked); i++ {
		if walked[i-1] > walked[i] {
			t.Fatalf("in-order traversal not sorted at index %d: %d > %d", i, walked[i-1], walked[i])
		}
	}
}

// TestTreapHeapPropertyAfterRandomInserts verifies heap property after random inserts.
func TestTreapHeapPropertyAfterRandomInserts(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := rand.New(rand.NewSource(54321)).Perm(200)
	for _, k := range keys {
		key := types.IntKey(k)
		treap.Insert(key)
	}

	if err := verifyHeapProperty(treap.root); err != nil {
		t.Fatalf("heap property check failed: %v", err)
	}
}

// TestTreapBSTPropertyAfterDeletes verifies BST property after deletes.
func TestTreapBSTPropertyAfterDeletes(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	for i := 0; i < 200; i++ {
		key := types.IntKey(i)
		treap.Insert(key)
	}

	rng := rand.New(rand.NewSource(24680))
	for i := 0; i < 50; i++ {
		key := types.IntKey(rng.Intn(200))
		treap.Delete(key)
	}

	if err := verifyBSTProperty(treap.root, nil, nil); err != nil {
		t.Fatalf("bst property check failed: %v", err)
	}
}

// TestTreapUpdatePriorityMaintainsProperties verifies BST/heap properties after UpdatePriority.
func TestTreapUpdatePriorityMaintainsProperties(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	for i := 0; i < 100; i++ {
		key := types.IntKey(i)
		treap.Insert(key)
	}

	// Update priorities for a handful of keys
	for _, k := range []int{5, 20, 50, 75, 99} {
		treap.UpdatePriority(types.IntKey(k), Priority(1000+k))
	}

	if err := verifyBSTProperty(treap.root, nil, nil); err != nil {
		t.Fatalf("bst property check failed: %v", err)
	}
	if err := verifyHeapProperty(treap.root); err != nil {
		t.Fatalf("heap property check failed: %v", err)
	}
}

// TestTreapInsertDeleteCycles verifies properties after insert/delete cycles.
func TestTreapInsertDeleteCycles(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	for i := 0; i < 200; i++ {
		key := types.IntKey(i)
		treap.Insert(key)
	}

	rng := rand.New(rand.NewSource(13579))
	for i := 0; i < 100; i++ {
		key := types.IntKey(rng.Intn(200))
		treap.Delete(key)
	}

	for i := 200; i < 300; i++ {
		key := types.IntKey(i)
		treap.Insert(key)
	}

	if err := verifyBSTProperty(treap.root, nil, nil); err != nil {
		t.Fatalf("bst property check failed: %v", err)
	}
	if err := verifyHeapProperty(treap.root); err != nil {
		t.Fatalf("heap property check failed: %v", err)
	}
}

// TestTreapStressLargeInserts verifies treap behavior on 10,000 inserts.
func TestTreapStressLargeInserts(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := rand.New(rand.NewSource(777)).Perm(10000)
	for _, k := range keys {
		key := types.IntKey(k)
		treap.Insert(key)
	}

	count := 0
	var prev *types.IntKey
	for node := range treap.Iter() {
		key := node.GetKey().Value()
		if prev != nil && key < *prev {
			t.Fatalf("iterator not sorted at key %d", key)
		}
		copyKey := key
		prev = &copyKey
		count++
	}

	if count != 10000 {
		t.Fatalf("expected 10000 nodes after insert, got %d", count)
	}
}
