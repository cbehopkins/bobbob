package treap

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPayloadTreapInsertAndSearchPayload(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	treap.Insert(types.IntKey(1), "one")
	treap.Insert(types.IntKey(2), "two")
	treap.Insert(types.IntKey(3), "three")

	cases := []struct {
		key     types.IntKey
		payload string
	}{
		{1, "one"},
		{2, "two"},
		{3, "three"},
	}

	for _, c := range cases {
		node := treap.Search(c.key)
		if node == nil || node.IsNil() {
			t.Fatalf("expected to find key %d", c.key)
		}
		payloadNode, ok := node.(*PayloadTreapNode[types.IntKey, string])
		if !ok {
			t.Fatalf("expected PayloadTreapNode for key %d", c.key)
		}
		if payloadNode.GetPayload() != c.payload {
			t.Fatalf("expected payload %q for key %d, got %q", c.payload, c.key, payloadNode.GetPayload())
		}
	}
}

func TestPayloadTreapUpdatePayloadMethod(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	key := types.IntKey(7)
	treap.Insert(key, "before")

	if err := treap.UpdatePayload(key, "after"); err != nil {
		t.Fatalf("UpdatePayload failed: %v", err)
	}

	node := treap.Search(key)
	if node == nil || node.IsNil() {
		t.Fatal("expected to find key after UpdatePayload")
	}
	payloadNode := node.(*PayloadTreapNode[types.IntKey, string])
	if payloadNode.GetPayload() != "after" {
		t.Fatalf("expected payload 'after', got %q", payloadNode.GetPayload())
	}
}

func TestPayloadTreapPayloadPreservedAfterRotations(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	// Insert with explicit priorities to force rotations
	treap.InsertComplex(types.IntKey(1), Priority(10), "one")
	treap.InsertComplex(types.IntKey(2), Priority(30), "two")
	treap.InsertComplex(types.IntKey(3), Priority(20), "three")

	if treap.root == nil || treap.root.IsNil() {
		t.Fatal("expected non-nil root after inserts")
	}

	root := treap.root.(*PayloadTreapNode[types.IntKey, string])
	if root.GetKey().Value() != types.IntKey(2) {
		t.Fatalf("expected root key 2 due to highest priority, got %d", root.GetKey().Value())
	}

	// Ensure payloads are preserved after rotations
	for _, c := range []struct {
		key     types.IntKey
		payload string
	}{
		{1, "one"},
		{2, "two"},
		{3, "three"},
	} {
		node := treap.Search(c.key)
		if node == nil || node.IsNil() {
			t.Fatalf("expected to find key %d", c.key)
		}
		payloadNode := node.(*PayloadTreapNode[types.IntKey, string])
		if payloadNode.GetPayload() != c.payload {
			t.Fatalf("expected payload %q for key %d, got %q", c.payload, c.key, payloadNode.GetPayload())
		}
	}
}

func TestPayloadTreapSearchComplexCallback(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key, "payload")
	}

	var accessed []types.IntKey
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		if node != nil && !node.IsNil() {
			accessed = append(accessed, node.GetKey().Value())
		}
		return nil
	}

	found, err := treap.SearchComplex(types.IntKey(15), callback)
	if err != nil {
		t.Fatalf("SearchComplex returned error: %v", err)
	}
	if found == nil || found.IsNil() {
		t.Fatal("expected to find key 15")
	}
	if len(accessed) == 0 {
		t.Fatal("expected callback to be invoked at least once")
	}
}

func TestPayloadTreapSearchComplexError(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	keys := []types.IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key, "payload")
	}

	boom := errors.New("callback error")
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		_ = node
		return boom
	}

	found, err := treap.SearchComplex(types.IntKey(15), callback)
	if err == nil {
		t.Fatal("expected error from SearchComplex")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected error %q, got %v", boom, err)
	}
	if found != nil {
		t.Fatalf("expected nil node when callback errors, got %v", found)
	}
}

func TestPayloadTreapUpdatePayloadMissingKey(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	if err := treap.UpdatePayload(types.IntKey(404), "missing"); err == nil {
		t.Fatal("expected error when updating missing key")
	}
}

func TestPayloadTreapWalkInOrderSorted(t *testing.T) {
	treap := NewPayloadTreap[types.IntKey, string](types.IntLess)

	keys := []types.IntKey{42, 7, 19, 3, 88, 55, 1}
	expected := make(map[types.IntKey]string, len(keys))
	for _, key := range keys {
		payload := fmt.Sprintf("p%d", key)
		expected[key] = payload
		treap.Insert(key, payload)
	}

	var walked []types.IntKey
	treap.Walk(func(node TreapNodeInterface[types.IntKey]) {
		payloadNode := node.(*PayloadTreapNode[types.IntKey, string])
		walked = append(walked, payloadNode.GetKey().Value())
		if expected[payloadNode.GetKey().Value()] != payloadNode.GetPayload() {
			t.Fatalf("payload mismatch for key %d", payloadNode.GetKey().Value())
		}
	})

	if len(walked) != len(keys) {
		t.Fatalf("expected %d keys from Walk, got %d", len(keys), len(walked))
	}
	for i := 1; i < len(walked); i++ {
		if walked[i-1] > walked[i] {
			t.Fatalf("payload treap walk not sorted at index %d: %d > %d", i, walked[i-1], walked[i])
		}
	}
}
