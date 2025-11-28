package treap

import (
	"fmt"
	"testing"

	"bobbob/internal/store"
)

// TestSimpleWalkInOrder tests basic in-order iteration
func TestSimpleWalkInOrder(t *testing.T) {
	stre, err := store.NewBasicStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	templateKey := IntKey(0).New()
	treap := NewPersistentTreap(IntLess, templateKey, stre)

	// Insert test data
	values := []IntKey{50, 30, 70, 20, 40, 60, 80}
	for _, v := range values {
		key := v
		treap.Insert(&key)
	}

	// Persist the tree
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Walk in order and collect keys
	var collected []IntKey
	opts := DefaultIteratorOptions()
	opts.KeepInMemory = true

	err = treap.WalkInOrder(opts, func(node PersistentTreapNodeInterface[IntKey]) error {
		key, ok := node.GetKey().(*IntKey)
		if !ok {
			return fmt.Errorf("key is not *IntKey")
		}
		collected = append(collected, *key)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkInOrder failed: %v", err)
	}

	// Verify sorted order
	expected := []IntKey{20, 30, 40, 50, 60, 70, 80}
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
func TestSimpleWalkKeys(t *testing.T) {
	stre, err := store.NewBasicStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	templateKey := IntKey(0).New()
	treap := NewPersistentTreap(IntLess, templateKey, stre)

	// Insert test data
	for i := IntKey(10); i >= 1; i-- {
		key := i
		treap.Insert(&key)
	}

	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Walk and collect keys
	var collected []IntKey
	opts := DefaultIteratorOptions()

	err = treap.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error {
		intKey, ok := key.(*IntKey)
		if !ok {
			return fmt.Errorf("key is not *IntKey")
		}
		collected = append(collected, *intKey)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkInOrderKeys failed: %v", err)
	}

	// Verify ascending order
	for i := 0; i < len(collected); i++ {
		if collected[i] != IntKey(i+1) {
			t.Errorf("Position %d: expected %d, got %d", i, i+1, collected[i])
		}
	}
}

// TestSimpleCount tests the Count method
func TestSimpleCount(t *testing.T) {
	stre, err := store.NewBasicStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer stre.Close()

	templateKey := IntKey(0).New()
	treap := NewPersistentTreap(IntLess, templateKey, stre)

	// Insert 10 nodes
	for i := 0; i < 10; i++ {
		key := IntKey(i)
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
