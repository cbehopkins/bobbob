package treap_test

import (
	"path/filepath"
	"testing"

	collections "github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

// This test lives in an external package to avoid the import cycle between
// multistore (which imports treap for PersistentTreapObjectSizes) and treap
// tests.
func TestPersistentTreapBatchPersistWithMultiStore(t *testing.T) {
	ms, err := collections.NewMultiStore(filepath.Join(t.TempDir(), "multi_batch.bin"), 0)
	if err != nil {
		t.Fatalf("failed to create multistore: %v", err)
	}
	defer ms.Close()

	var keyTemplate *treap.IntKey = (*treap.IntKey)(new(int32))
	pt := treap.NewPersistentTreap[treap.IntKey](treap.IntLess, keyTemplate, ms)

	keys := []*treap.IntKey{(*treap.IntKey)(new(int32)), (*treap.IntKey)(new(int32)), (*treap.IntKey)(new(int32))}
	*keys[0] = 1
	*keys[1] = 2
	*keys[2] = 3

	for _, k := range keys {
		pt.Insert(k)
	}

	if err := pt.BatchPersist(); err != nil {
		t.Fatalf("BatchPersist failed: %v", err)
	}

	rootNode, ok := pt.Root().(treap.PersistentTreapNodeInterface[treap.IntKey])
	if !ok {
		t.Fatalf("root is not persistent node")
	}
	rootId, err := rootNode.ObjectId()
	if err != nil {
		t.Fatalf("root ObjectId failed: %v", err)
	}

	reloaded := treap.NewPersistentTreap[treap.IntKey](treap.IntLess, keyTemplate, ms)
	if err := reloaded.Load(rootId); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	for _, k := range keys {
		n := reloaded.Search(k)
		if n == nil {
			t.Fatalf("expected to find key %d after reload", *k)
		}
	}
}
