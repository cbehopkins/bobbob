package yggdrasil

import (
	"math/rand"
	"testing"

	"github.com/cbehopkins/bobbob/internal/store"
)

func setupTestStore(t *testing.T) *store.Store {
    store, err := store.NewStore("test_store.bin")
    if err != nil {
        t.Fatalf("Failed to create store: %v", err)
    }
    return store
}

func TestPersistentTreap(t *testing.T) {
    store := setupTestStore(t)
    defer store.Close()

    treap := NewPersistentTreap(mockIntLess, store)

    keys := []MockIntKey{10, 20, 15, 5, 30}
    for _, key := range keys {
        treap.Insert(key, Priority(rand.Intn(100)))
    }

    for _, key := range keys {
        node := treap.Search(key)
        if node == nil {
            t.Errorf("Expected to find key %d in the treap, but it was not found", key)
        } else if node.GetKey() != key {
            t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey())
        }
    }

    nonExistentKeys := []MockIntKey{1, 3, 6}
    for _, key := range nonExistentKeys {
        node := treap.Search(key)
        if node != nil && !node.IsNil() {
            t.Errorf("Expected not to find key %d in the treap, but it was found", key)
        }
    }
}
