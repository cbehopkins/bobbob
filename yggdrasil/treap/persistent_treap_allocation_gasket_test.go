package treap

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

type allocationGasket struct {
	inner store.Storer
	live  map[store.ObjectId]struct{}
}

func newAllocationGasket(inner store.Storer) *allocationGasket {
	return &allocationGasket{
		inner: inner,
		live:  make(map[store.ObjectId]struct{}),
	}
}

func (g *allocationGasket) snapshot() map[store.ObjectId]struct{} {
	copySet := make(map[store.ObjectId]struct{}, len(g.live))
	for id := range g.live {
		copySet[id] = struct{}{}
	}
	return copySet
}

func (g *allocationGasket) track(id store.ObjectId) {
	if store.IsValidObjectId(id) {
		g.live[id] = struct{}{}
	}
}

func (g *allocationGasket) untrack(id store.ObjectId) {
	delete(g.live, id)
}

func (g *allocationGasket) NewObj(size int) (store.ObjectId, error) {
	id, err := g.inner.NewObj(size)
	if err == nil {
		g.track(id)
	}
	return id, err
}

func (g *allocationGasket) PrimeObject(size int) (store.ObjectId, error) {
	id, err := g.inner.PrimeObject(size)
	if err == nil {
		g.track(id)
	}
	return id, err
}

func (g *allocationGasket) DeleteObj(objId store.ObjectId) error {
	err := g.inner.DeleteObj(objId)
	if err == nil {
		g.untrack(objId)
	}
	return err
}

func (g *allocationGasket) Close() error {
	return g.inner.Close()
}

func (g *allocationGasket) LateReadObj(id store.ObjectId) (reader io.Reader, finisher bobbob.Finisher, err error) {
	return g.inner.LateReadObj(id)
}

func (g *allocationGasket) LateWriteNewObj(size int) (store.ObjectId, io.Writer, bobbob.Finisher, error) {
	id, writer, finisher, err := g.inner.LateWriteNewObj(size)
	if err == nil {
		g.track(id)
	}
	return id, writer, finisher, err
}

func (g *allocationGasket) WriteToObj(objectId store.ObjectId) (io.Writer, bobbob.Finisher, error) {
	return g.inner.WriteToObj(objectId)
}

func (g *allocationGasket) WriteBatchedObjs(objIds []store.ObjectId, data []byte, sizes []int) error {
	return g.inner.WriteBatchedObjs(objIds, data, sizes)
}

func setDifference(a, b map[store.ObjectId]struct{}) map[store.ObjectId]struct{} {
	out := make(map[store.ObjectId]struct{})
	for id := range a {
		if _, found := b[id]; !found {
			out[id] = struct{}{}
		}
	}
	return out
}

func collectTreapObjectIds[T any](t *PersistentTreap[T]) (map[store.ObjectId]struct{}, error) {
	ids := make(map[store.ObjectId]struct{})
	_, err := t.RangeOverTreapPostOrder(func(node *PersistentTreapNode[T]) error {
		// Use GetObjectIdNoAlloc to avoid triggering allocations during collection
		id := node.GetObjectIdNoAlloc()
		if store.IsValidObjectId(id) {
			ids[id] = struct{}{}
		}
		// Also collect dependent ObjectIds (key backing objects, etc.)
		for _, depId := range node.DependentObjectIds() {
			if store.IsValidObjectId(depId) {
				ids[depId] = struct{}{}
			}
		}
		return nil
	})
	return ids, err
}

func TestPersistentTreapAllocationGasketTracksNodeObjects(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "gasket_store.bin")
	base, err := store.NewBasicStore(filePath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	gasket := newAllocationGasket(base)
	defer gasket.Close()

	baseline := gasket.snapshot()

	// Create a treap with IntKey (which doesn't allocate separate objects for keys)
	tr := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), gasket)
	for i := 0; i < 10; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	gasketLive := setDifference(gasket.snapshot(), baseline)
	treeIds, err := collectTreapObjectIds(tr)
	if err != nil {
		t.Fatalf("collectTreapObjectIds failed: %v", err)
	}

	// Verify all gasket-tracked allocations are found in the treap
	for id := range gasketLive {
		if _, found := treeIds[id]; !found {
			t.Fatalf("gasket objectId %d not found in treap after insert", id)
		}
	}

	// Track root before deletion
	rootBefore, err := tr.GetRootObjectId()
	if err != nil {
		t.Logf("Warning: Could not get root before deletion: %v", err)
	} else {
		t.Logf("Root ObjectId before deletion: %d", rootBefore)
	}

	// Delete a subset
	t.Logf("Deleting nodes 0, 3, 6, 9...")
	for i := 0; i < 10; i += 3 {
		key := types.IntKey(i)
		// Try to find this node and log its ObjectId
		node := tr.Search(&key)
		if node != nil {
			if pNode, ok := node.(*PersistentTreapNode[types.IntKey]); ok {
				objId := pNode.GetObjectIdNoAlloc()
				t.Logf("  Deleting key %d (ObjectId %d)", i, objId)
			}
		}
		if err := tr.Delete(&key); err != nil {
			t.Logf("  Delete error for key %d: %v", i, err)
		}
	}
	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist after delete failed: %v", err)
	}

	// Track root after deletion
	rootAfter, err := tr.GetRootObjectId()
	if err != nil {
		t.Logf("Warning: Could not get root after deletion: %v", err)
	} else {
		t.Logf("Root ObjectId after deletion: %d (changed: %v)", rootAfter, rootAfter != rootBefore)
	}

	gasketLive = setDifference(gasket.snapshot(), baseline)
	treeIds, err = collectTreapObjectIds(tr)
	if err != nil {
		t.Fatalf("collectTreapObjectIds failed after delete: %v", err)
	}

	// Verify all remaining gasket objects are in the treap
	for id := range gasketLive {
		if _, found := treeIds[id]; !found {
			t.Fatalf("gasket objectId %d not found in treap after delete", id)
		}
	}
}
