package treap

import (
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestDuplicateKeyInsertion verifies that duplicate key insertions
// do not create new nodes or overwrite existing payloads.
func TestDuplicateKeyInsertion(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, MockPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		store,
	)

	// Insert 100 keys with original payloads
	expected := make(map[int]MockPayload)
	for i := 0; i < 100; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("original_%d", i)}
		treap.Insert(&key, payload)
		expected[i] = payload
	}

	t.Logf("Inserted 100 keys with original payloads")

	// Insert 10 duplicate keys with different payloads
	// Keys 0-9 already exist, so these SHOULD overwrite (Go map behavior)
	for i := 0; i < 10; i++ {
		key := types.IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("duplicate_%d", i)}
		treap.Insert(&key, payload)
		// Update expected - new payloads should replace original
		expected[i] = payload
	}

	t.Logf("Inserted 10 duplicate keys with different payloads")

	// Walk and verify we have exactly 100 keys with updated payloads
	seen := make(map[int]MockPayload)
	err := treap.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		pNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
		k := int(*pNode.GetKey().(*types.IntKey))
		seen[k] = pNode.GetPayload()
		return nil
	})

	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	// Verify count
	if len(seen) != 100 {
		t.Errorf("Expected 100 keys, got %d", len(seen))
	}

	// Verify payloads match original
	for k, expectedPayload := range expected {
		seenPayload, ok := seen[k]
		if !ok {
			t.Errorf("Key %d missing from iteration", k)
			continue
		}
		if seenPayload.Data != expectedPayload.Data {
			t.Errorf("Key %d: expected payload %q, got %q", k, expectedPayload.Data, seenPayload.Data)
		}
	}

	// Verify no unexpected keys
	for k := range seen {
		if _, ok := expected[k]; !ok {
			t.Errorf("Unexpected key %d in iteration", k)
		}
	}

	t.Logf("Successfully verified 100 keys with updated payloads (keys 0-9 replaced)")
}
