package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentPayloadTreapCountInMemoryNodes(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 25; i++ {
		key := types.IntKey(i)
		tr.Insert(&key, MockPayload{Data: "p"})
	}

	count := tr.CountInMemoryNodes()
	if count != 25 {
		t.Fatalf("expected CountInMemoryNodes=25, got %d", count)
	}
}
