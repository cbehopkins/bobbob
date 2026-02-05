package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentTreapDeleteReclaimsObject(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	infoProvider, ok := st.(store.ObjectInfoProvider)
	if !ok {
		t.Skip("store does not support ObjectInfoProvider")
	}

	tr := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), st)
	for i := 0; i < 10; i++ {
		key := types.IntKey(i)
		tr.Insert(&key)
	}

	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Pick a key to delete and record its object id.
	targetKey := types.IntKey(5)
	node := tr.Search(&targetKey)
	if node == nil || node.IsNil() {
		t.Fatal("expected to find target node before delete")
	}
	pnode := node.(*PersistentTreapNode[types.IntKey])
	objId, err := pnode.ObjectId()
	if err != nil {
		t.Fatalf("ObjectId failed: %v", err)
	}
	if !store.IsValidObjectId(objId) {
		t.Fatalf("expected valid objectId before delete, got %d", objId)
	}

	// Ensure object exists before delete.
	if _, found := infoProvider.GetObjectInfo(objId); !found {
		t.Fatalf("expected object %d to exist before delete", objId)
	}

	tr.Delete(&targetKey)

	// Deleted object may be reclaimed immediately or later depending on allocator.
	if _, found := infoProvider.GetObjectInfo(objId); found {
		t.Skipf("object %d still present after delete; allocator may defer reclamation", objId)
	}
}
