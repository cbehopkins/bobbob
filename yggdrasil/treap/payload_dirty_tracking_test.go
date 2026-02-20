package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

func TestPayloadDeleteRotationCleansDependentsAndMarksDirty(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, ComplexPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	makeChild := func(b byte) store.ObjectId {
		objId, err := st.NewObj(1)
		if err != nil {
			t.Fatalf("NewObj failed: %v", err)
		}
		if err := store.WriteBytesToObj(st, []byte{b}, objId); err != nil {
			t.Fatalf("WriteBytesToObj failed: %v", err)
		}
		return objId
	}

	key20 := types.IntKey(20)
	key10 := types.IntKey(10)
	key30 := types.IntKey(30)

	child20 := makeChild(20)
	child10 := makeChild(10)
	child30 := makeChild(30)

	treap.InsertComplex(&key20, Priority(100), ComplexPayload{Child: child20, Data: []byte("root")})
	treap.InsertComplex(&key10, Priority(90), ComplexPayload{Child: child10, Data: []byte("left")})
	treap.InsertComplex(&key30, Priority(80), ComplexPayload{Child: child30, Data: []byte("right")})

	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	if node := treap.Search(&key20); node == nil {
		t.Fatalf("Expected key %d to exist before delete", key20)
	} else if !store.IsValidObjectId(node.GetObjectIdNoAlloc()) {
		t.Fatalf("Expected key %d to be persisted before delete", key20)
	}

	treap.Delete(&key20)

	if node := treap.Search(&key20); node != nil {
		t.Fatalf("Expected key %d to be deleted", key20)
	}

	if _, err := store.ReadBytesFromObj(st, child20); err == nil {
		t.Fatalf("Expected child object for key %d to be deleted", key20)
	}
	if _, err := store.ReadBytesFromObj(st, child10); err != nil {
		t.Fatalf("Expected child object for key %d to remain: %v", key10, err)
	}
	if _, err := store.ReadBytesFromObj(st, child30); err != nil {
		t.Fatalf("Expected child object for key %d to remain: %v", key30, err)
	}

	treap.mu.RLock()
	root, ok := treap.root.(*PersistentPayloadTreapNode[types.IntKey, ComplexPayload])
	if !ok || root == nil {
		treap.mu.RUnlock()
		t.Fatalf("Root is not a PersistentPayloadTreapNode")
	}
	rootObjId := root.GetObjectIdNoAlloc()
	treap.mu.RUnlock()

	if store.IsValidObjectId(rootObjId) {
		t.Fatalf("Expected root objectId to be invalid after delete rotation")
	}
}

func TestPayloadUpdateDeletesStaleDependents(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	treap := NewPersistentPayloadTreap[types.IntKey, ComplexPayload](
		types.IntLess,
		(*types.IntKey)(new(int32)),
		st,
	)

	makeChild := func(b byte) store.ObjectId {
		objId, err := st.NewObj(1)
		if err != nil {
			t.Fatalf("NewObj failed: %v", err)
		}
		if err := store.WriteBytesToObj(st, []byte{b}, objId); err != nil {
			t.Fatalf("WriteBytesToObj failed: %v", err)
		}
		return objId
	}

	key := types.IntKey(42)
	childOld := makeChild(1)
	childNew := makeChild(2)

	treap.Insert(&key, ComplexPayload{Child: childOld, Data: []byte("old")})
	if err := treap.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	if err := treap.UpdatePayload(&key, ComplexPayload{Child: childNew, Data: []byte("new")}); err != nil {
		t.Fatalf("UpdatePayload failed: %v", err)
	}

	if _, err := store.ReadBytesFromObj(st, childOld); err == nil {
		t.Fatalf("Expected old child object to be deleted after UpdatePayload")
	}
	if _, err := store.ReadBytesFromObj(st, childNew); err != nil {
		t.Fatalf("Expected new child object to remain: %v", err)
	}

	updated := treap.Search(&key)
	if updated == nil {
		t.Fatalf("Expected key %d to exist after UpdatePayload", key)
	}
	if store.IsValidObjectId(updated.GetObjectIdNoAlloc()) {
		t.Fatalf("Expected node objectId to be invalid after UpdatePayload")
	}
}
