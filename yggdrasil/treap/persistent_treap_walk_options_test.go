package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentTreapWalkInOrderWithFlush(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 50; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	count := 0
	err := tr.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}
	if count != 50 {
		t.Fatalf("expected 50 nodes from InOrderVisit, got %d", count)
	}
}
