package treap

import (
	"math/rand"
	"testing"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestTreapIterSorted(t *testing.T) {
	treap := NewTreap[types.IntKey](types.IntLess)

	keys := rand.New(rand.NewSource(424242)).Perm(200)
	for _, k := range keys {
		key := types.IntKey(k)
		treap.Insert(key)
	}

	var prev *types.IntKey
	count := 0
	for node := range treap.Iter() {
		key := node.GetKey().Value()
		if prev != nil && key < *prev {
			t.Fatalf("iterator not sorted: %d < %d", key, *prev)
		}
		copyKey := key
		prev = &copyKey
		count++
	}

	if count != 200 {
		t.Fatalf("expected 200 nodes from iterator, got %d", count)
	}
}
