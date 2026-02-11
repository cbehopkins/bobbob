package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentPayloadTreapWalkInOrderKeys(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, (*types.IntKey)(new(int32)), st)
	keys := []int{10, 20, 15, 5, 30}
	for _, k := range keys {
		key := types.IntKey(k)
		tr.Insert(&key, MockPayload{Data: "p"})
	}

	var walked []types.IntKey
	err := tr.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		key := node.GetKey().(types.PersistentKey[types.IntKey])
		walked = append(walked, key.Value())
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}

	expected := []types.IntKey{5, 10, 15, 20, 30}
	if len(walked) != len(expected) {
		t.Fatalf("expected %d keys, got %d", len(expected), len(walked))
	}
	for i := range expected {
		if walked[i] != expected[i] {
			t.Fatalf("expected key %d at index %d, got %d", expected[i], i, walked[i])
		}
	}
}

func TestPersistentPayloadTreapWalkInOrderWithFlush(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 30; i++ {
		key := types.IntKey(i)
		tr.Insert(&key, MockPayload{Data: "p"})
	}

	count := 0
	err := tr.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}
	if count != 30 {
		t.Fatalf("expected 30 nodes from InOrderVisit, got %d", count)
	}
}
