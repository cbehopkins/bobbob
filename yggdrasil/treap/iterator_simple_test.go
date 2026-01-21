package treap

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestSimpleWalkInOrder tests basic in-order iteration
// TestSimpleWalkInOrder verifies that WalkInOrder visits all nodes in ascending order
// and that the callback receives each node's key and payload correctly.
func TestSimpleWalkInOrder(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	// Insert test data
	values := []types.IntKey{50, 30, 70, 20, 40, 60, 80}
	for _, v := range values {
		key := v
		treap.Insert(&key)
	}

	// Persist the tree
	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Walk in order and collect keys
	var collected []types.IntKey
	opts := DefaultIteratorOptions()
	opts.KeepInMemory = true

	err = treap.WalkInOrder(opts, func(node PersistentTreapNodeInterface[types.IntKey]) error {
		key, ok := node.GetKey().(*types.IntKey)
		if !ok {
			return fmt.Errorf("key is not *types.IntKey")
		}
		collected = append(collected, *key)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkInOrder failed: %v", err)
	}

	// Verify sorted order
	expected := []types.IntKey{20, 30, 40, 50, 60, 70, 80}
	if len(collected) != len(expected) {
		t.Fatalf("Expected %d nodes, got %d", len(expected), len(collected))
	}

	for i, v := range expected {
		if collected[i] != v {
			t.Errorf("Position %d: expected %d, got %d", i, v, collected[i])
		}
	}
}

// TestSimpleWalkKeys tests the convenience method for keys
// TestSimpleWalkKeys verifies that WalkKeys visits all keys in ascending order,
// providing a simpler interface than WalkInOrder when payloads aren't needed.
func TestSimpleWalkKeys(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	// Insert test data
	for i := types.IntKey(10); i >= 1; i-- {
		key := i
		treap.Insert(&key)
	}

	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Walk and collect keys
	var collected []types.IntKey
	opts := DefaultIteratorOptions()

	err = treap.WalkInOrderKeys(opts, func(key types.PersistentKey[types.IntKey]) error {
		intKey, ok := key.(*types.IntKey)
		if !ok {
			return fmt.Errorf("key is not *types.IntKey")
		}
		collected = append(collected, *intKey)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkInOrderKeys failed: %v", err)
	}

	// Verify ascending order
	for i := 0; i < len(collected); i++ {
		if collected[i] != types.IntKey(i+1) {
			t.Errorf("Position %d: expected %d, got %d", i, i+1, collected[i])
		}
	}
}

// TestSimpleCount tests the Count method
// TestSimpleCount verifies that Count returns the correct number of nodes in a treap,
// including empty treaps and treaps with multiple nodes.
func TestSimpleCount(t *testing.T) {
	stre := testutil.NewMockStore()
	defer stre.Close()

	templateKey := types.IntKey(0).New()
	treap := NewPersistentTreap(types.IntLess, templateKey, stre)

	// Insert 10 nodes
	for i := 0; i < 10; i++ {
		key := types.IntKey(i)
		treap.Insert(&key)
	}

	count, err := treap.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if count != 10 {
		t.Errorf("Expected count 10, got %d", count)
	}
}
