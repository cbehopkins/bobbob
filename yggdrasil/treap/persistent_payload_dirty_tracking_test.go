package treap

import (
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPayloadTreapRangeOver verifies that PersistentPayloadTreapNode
// works with the polymorphic walker interface.
// Since PersistentPayloadTreapNode embeds PersistentTreapNode[K],
// it automatically implements PersistentNodeWalker[K] and can be used
// with the generic walker (once we refactor rangeOverPostOrder to accept interfaces).
//
// Currently, payload treap dirty tracking is not yet implemented with the walker pattern,
// but the infrastructure is in place.
func TestPayloadTreapRangeOver(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	tr := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, keyTemplate, st)

	// Insert 5 nodes with payloads
	keys := []int{10, 20, 15, 5, 30}
	for _, k := range keys {
		key := types.IntKey(k)
		payload := MockPayload{Data: "test"}
		tr.Insert(&key, payload)
	}

	// Verify we can iterate over all nodes
	var iterCount int
	_, err := tr.RangeOverTreapPayloadPostOrder(func(node *PersistentPayloadTreapNode[types.IntKey, MockPayload]) error {
		iterCount++
		return nil
	})
	if err != nil {
		t.Fatalf("RangeOver failed: %v", err)
	}

	if iterCount != 5 {
		t.Fatalf("Expected to iterate over 5 nodes, iterated over %d", iterCount)
	}

	t.Log("SUCCESS: Payload treap iteration works")
}

// TestPayloadTreapWalkerInterface verifies that PersistentPayloadTreapNode[K, P]
// correctly implements the PersistentNodeWalker[K] interface through embedding.
func TestPayloadTreapWalkerInterface(t *testing.T) {
	st := setupTestStore(t)
	defer st.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	tr := NewPersistentPayloadTreap[types.IntKey, MockPayload](types.IntLess, keyTemplate, st)

	// Insert a few nodes
	keys := []int{10, 20, 15}
	for _, k := range keys {
		key := types.IntKey(k)
		payload := MockPayload{Data: "test"}
		tr.Insert(&key, payload)
	}

	// Get the root node (accessed through the embedded PersistentTreap)
	root := tr.root
	if root == nil {
		t.Fatal("Root is nil")
	}

	rootNode, ok := root.(*PersistentPayloadTreapNode[types.IntKey, MockPayload])
	if !ok {
		t.Fatal("Root is not a PersistentPayloadTreapNode")
	}

	if rootNode.IsNil() {
		t.Fatal("Root should not be nil")
	}

	// Verify the root implements PersistentNodeWalker[types.IntKey]
	// by calling the interface methods directly
	var walker PersistentNodeWalker[types.IntKey] = rootNode
	if walker == nil {
		t.Fatal("Root does not implement PersistentNodeWalker interface")
	}

	// Test GetObjectIdNoAlloc
	objId := rootNode.GetObjectIdNoAlloc()
	// ObjectId may be invalid at this point (not yet persisted)
	t.Logf("Root objectId: %d (valid: %v)", objId, store.IsValidObjectId(objId))

	// Test GetLeftChild/GetRightChild
	left := rootNode.GetLeftChild()
	right := rootNode.GetRightChild()
	t.Logf("Root has left child: %v, right child: %v", left != nil, right != nil)

	// Persist and test again
	if err := tr.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	objId = rootNode.GetObjectIdNoAlloc()
	if !store.IsValidObjectId(objId) {
		t.Fatalf("Expected valid objectId after persist, got %d", objId)
	}

	t.Log("SUCCESS: PersistentPayloadTreapNode correctly implements PersistentNodeWalker interface")
}
