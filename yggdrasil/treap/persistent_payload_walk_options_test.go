package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

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
