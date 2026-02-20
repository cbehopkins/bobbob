package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPersistentPayloadUpdatePayloadInvalidatesObjectId(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	tr := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, (*types.IntKey)(new(int32)), st)
	key := types.IntKey(1)
	tr.Insert(&key, MockPayload{Data: "before"})

	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	node := tr.Search(&key)
	if node == nil || node.IsNil() {
		t.Fatal("expected to find node after persist")
	}
	payloadNode := node.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
	objId, err := payloadNode.ObjectId()
	if err != nil {
		t.Fatalf("ObjectId failed: %v", err)
	}
	if !store.IsValidObjectId(objId) {
		t.Fatalf("expected valid objectId after persist, got %d", objId)
	}

	if err := tr.UpdatePayload(&key, MockPayload{Data: "after"}); err != nil {
		t.Fatalf("UpdatePayload failed: %v", err)
	}

	nodeAfter := tr.Search(&key)
	if nodeAfter == nil || nodeAfter.IsNil() {
		t.Fatal("expected to find node after UpdatePayload")
	}
	payloadNodeAfter := nodeAfter.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
	if !payloadNodeAfter.IsObjectIdInvalid() {
		t.Fatal("expected objectId invalid after UpdatePayload")
	}

	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist after SetPayload failed: %v", err)
	}

	objId2, err := payloadNode.ObjectId()
	if err != nil {
		t.Fatalf("ObjectId failed after re-persist: %v", err)
	}
	if !store.IsValidObjectId(objId2) {
		t.Fatalf("expected valid objectId after re-persist, got %d", objId2)
	}
}
