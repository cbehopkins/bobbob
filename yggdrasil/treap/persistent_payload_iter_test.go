package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestIteratePayloadTreapFromDisk verifies that iterating over a persisted
// payload treap correctly loads payload nodes from disk.
func TestIteratePayloadTreapFromDisk(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	// Insert items with unique keys
	const numItems = 100
	for i := 0; i < numItems; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: string(rune('A' + (i % 26)))}
		treap.Insert(&key, payload)
	}

	// Persist to disk
	if err := treap.Persist(); err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	// Force all nodes out of memory by setting root's children to nil
	// (this simulates memory flushing)
	treap.PersistentTreap.mu.Lock()
	rootNode, ok := treap.root.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
	if ok && rootNode != nil {
		rootNode.TreapNode.left = nil
		rootNode.TreapNode.right = nil
	}
	treap.PersistentTreap.mu.Unlock()

	// Verify minimal nodes in memory (just root)
	inMemory := treap.CountInMemoryNodes()
	t.Logf("Nodes in memory after simulated flush: %d", inMemory)

	// Now iterate - should load from disk
	count := 0
	var lastKey int32 = -1
	if err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		k := int32(*pNode.GetKey().(*types.IntKey))

		// Verify keys are in order
		if k <= lastKey {
			t.Errorf("Keys not in order: %d followed by %d", lastKey, k)
		}
		lastKey = k

		// Verify payload is not empty
		payload := pNode.GetPayload()
		if payload.Data == "" {
			t.Errorf("Payload data is empty for key %d", k)
		}

		count++
		return nil
	}); err != nil {
		t.Fatalf("InOrderVisit error: %v", err)
	}

	if count != numItems {
		t.Errorf("Expected %d items, got %d", numItems, count)
	}

	t.Logf("Successfully iterated %d payload nodes from disk", count)
}
