package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentTreapInOrderVisit(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 20; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	count := 0
	err := tr.InOrderVisit(func(node TreapNodeInterface[types.IntKey]) error {
		if node == nil || node.IsNil() {
			t.Fatal("expected non-nil node in InOrderVisit")
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderVisit failed: %v", err)
	}
	if count != 20 {
		t.Fatalf("expected 20 nodes visited, got %d", count)
	}
}

func TestPersistentTreapInOrderMutate(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 15; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	count := 0
	err := tr.InOrderMutate(func(node TreapNodeInterface[types.IntKey]) error {
		if node == nil || node.IsNil() {
			t.Fatal("expected non-nil node in InOrderMutate")
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("InOrderMutate failed: %v", err)
	}
	if count != 15 {
		t.Fatalf("expected 15 nodes visited, got %d", count)
	}
}
