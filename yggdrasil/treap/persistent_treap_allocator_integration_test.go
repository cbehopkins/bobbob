package treap_test

import (
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentTreapMultiStoreIntegration(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "multistore.bin")

	ms, err := multistore.NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("failed to create multistore: %v", err)
	}

	tr := treap.NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), ms)
	keys := []int{10, 20, 15, 5, 30}
	for _, k := range keys {
		key := types.IntKey(k)
		tr.Insert(&key)
	}

	if err := tr.Persist(); err != nil {
		_ = ms.Close()
		t.Fatalf("persist failed: %v", err)
	}

	rootId, err := tr.GetRootObjectId()
	if err != nil {
		_ = ms.Close()
		t.Fatalf("GetRootObjectId failed: %v", err)
	}
	if !store.IsValidObjectId(rootId) {
		_ = ms.Close()
		t.Fatalf("expected valid root ObjectId, got %d", rootId)
	}

	_ = ms.Close()

	ms2, err := multistore.LoadMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("failed to load multistore: %v", err)
	}
	defer ms2.Close()

	tr2 := treap.NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), ms2)
	if err := tr2.Load(rootId); err != nil {
		t.Fatalf("failed to load treap: %v", err)
	}

	for _, k := range keys {
		key := types.IntKey(k)
		node := tr2.Search(&key)
		if node == nil || node.IsNil() {
			t.Fatalf("expected to find key %d after reload", k)
		}
	}
}
