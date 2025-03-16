package yggdrasil

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/internal/store"
)

func setupTestStore(t *testing.T) store.Storer {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_store.bin")
	store, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	return store
}

func TestPersistentTreapBasics(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentTreap(mockIntLess, store)

	keys := []*MockIntKey{
		(*MockIntKey)(new(int32)),
		(*MockIntKey)(new(int32)),
		(*MockIntKey)(new(int32)),
		(*MockIntKey)(new(int32)),
		(*MockIntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 15
	*keys[3] = 5
	*keys[4] = 30

	for _, key := range keys {
		treap.Insert(key, Priority(rand.Intn(100)))
	}

	for _, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
		} else if node.GetKey() != key {
			t.Errorf("Expected to find key %d, but found key %d instead", *key, node.GetKey())
		}
	}

	nonExistentKeys := []*MockIntKey{
		(*MockIntKey)(new(int32)),
		(*MockIntKey)(new(int32)),
		(*MockIntKey)(new(int32)),
	}
	*nonExistentKeys[0] = 1
	*nonExistentKeys[1] = 3
	*nonExistentKeys[2] = 6

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %d in the treap, but it was found", *key)
		}
	}

	// Test UpdatePriority
	keyToUpdate := keys[2]
	newPriority := Priority(200)
	treap.UpdatePriority(keyToUpdate, newPriority)
	updatedNode := treap.Search(keyToUpdate)
	if updatedNode == nil {
		t.Errorf("Expected to find key %d in the treap after updating priority, but it was not found", *keyToUpdate)
	} else if updatedNode.GetPriority() != newPriority {
		t.Errorf("Expected priority %d, but got %d", newPriority, updatedNode.GetPriority())
	}
}

func TestPersistentTreapNodeMarshalUnmarshal(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := MockIntKey(42)
	priority := Priority(100)
	node := NewPersistentTreapNode(&key, priority, store)

	// Marshal the node
	data, err := node.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	// Unmarshal the node
	unmarshalledNode := &PersistentTreapNode{Store: store}
	dstKey := MockIntKey(0)
	err = unmarshalledNode.Unmarshal(data, &dstKey)
	if err != nil {
		t.Fatalf("Failed to unmarshal node: %v", err)
	}
	tmpKey := unmarshalledNode.GetKey().(*MockIntKey)
	// Check if the unmarshalled node is equal to the original node
	if *tmpKey != key {
		t.Errorf("Expected key %d, got %d", key, *tmpKey)
	}
	if unmarshalledNode.GetPriority() != priority {
		t.Errorf("Expected priority %d, got %d", priority, unmarshalledNode.GetPriority())
	}
	if unmarshalledNode.left != nil {
		t.Errorf("Expected left child to be nil, got %v", unmarshalledNode.left)
	}
	if unmarshalledNode.right != nil {
		t.Errorf("Expected right child to be nil, got %v", unmarshalledNode.right)
	}
}

// Here we want to test that is we add a child to a node, the ObjectId of the node is invalidated.
func TestPersistentTreapNodeInvalidateObjectId(t *testing.T) {
	stre := setupTestStore(t)
	defer stre.Close()

	key := MockIntKey(42)
	priority := Priority(100)
	node := NewPersistentTreapNode(&key, priority, stre)

	// Initially, the ObjectId should be store.ObjNotAllocated
	if node.objectId != store.ObjNotAllocated {
		t.Fatalf("Expected initial ObjectId to be store.ObjNotAllocated, got %d", node.ObjectId())
	}

	// Persist the node to assign an ObjectId
	err := node.Persist()
	if err != nil {
		t.Fatalf("Failed to persist node: %v", err)
	}

	// Check that the ObjectId is now valid (not store.ObjNotAllocated)
	if node.objectId == store.ObjNotAllocated {
		t.Fatalf("Expected ObjectId to be valid after persisting, got store.ObjNotAllocated")
	}

	// Add a left child and check if ObjectId is invalidated
	leftKey := MockIntKey(21)
	leftNode := NewPersistentTreapNode(&leftKey, Priority(50), stre)
	node.SetLeft(leftNode)

	if node.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting left child, got %d", node.ObjectId())
	}

	// Persist the node again to assign a new ObjectId
	err = node.Persist()
	if err != nil {
		t.Fatalf("Failed to persist node: %v", err)
	}

	// Check that the ObjectId is now valid (not store.ObjNotAllocated)
	if node.objectId == store.ObjNotAllocated {
		t.Fatalf("Expected ObjectId to be valid after persisting, got store.ObjNotAllocated")
	}

	// Add a right child and check if ObjectId is invalidated
	rightKey := MockIntKey(63)
	rightNode := NewPersistentTreapNode(&rightKey, Priority(75), stre)
	node.SetRight(rightNode)

	if node.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting right child, got %d", node.ObjectId())
	}
	node.Persist()
	if node.objectId == store.ObjNotAllocated {
		t.Fatalf("Expected ObjectId to be valid after persisting, got store.ObjNotAllocated")
	}
	previousObjectId := node.objectId

	rightNode.SetPriority(Priority(80))
	if rightNode.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting right child's priority, got %d", rightNode.ObjectId())
	}
	node.Persist()

	if node.objectId == previousObjectId || node.objectId == store.ObjNotAllocated {
		t.Errorf("Expected ObjectId updated after setting right child's priority, got %d", node.objectId)
	}
}
