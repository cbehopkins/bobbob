package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentTreapCount(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 50; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	count, err := tr.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 50 {
		t.Fatalf("expected Count=50, got %d", count)
	}
}

func TestPersistentTreapCountInMemoryNodes(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 20; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	count := tr.CountInMemoryNodes()
	if count != 20 {
		t.Fatalf("expected CountInMemoryNodes=20, got %d", count)
	}
}
