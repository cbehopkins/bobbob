package yggdrasil

import (
	"math/rand"
	"path/filepath"
	"testing"

	"bobbob/internal/store"
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
	var keyTemplate *IntKey = (*IntKey)(new(int32))
	treap := NewPersistentTreap[IntKey](IntLess, keyTemplate, store)

	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
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

	nonExistentKeys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
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

	key := IntKey(42)
	priority := Priority(100)
	treap := NewPersistentTreap[IntKey](IntLess, (*IntKey)(new(int32)), store)
	node := NewPersistentTreapNode[IntKey](&key, priority, store, treap)

	// Marshal the node
	data, err := node.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	// Unmarshal the node
	unmarshalledNode := &PersistentTreapNode[IntKey]{Store: store, parent: treap}
	dstKey := IntKey(0)
	err = unmarshalledNode.unmarshal(data, &dstKey)
	if err != nil {
		t.Fatalf("Failed to unmarshal node: %v", err)
	}
	tmpKey := unmarshalledNode.GetKey().(*IntKey)
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

	key := IntKey(42)
	priority := Priority(100)
	treap := NewPersistentTreap[IntKey](IntLess, (*IntKey)(new(int32)), stre)
	node := NewPersistentTreapNode[IntKey](&key, priority, stre, treap)

	// Initially, the ObjectId should be store.ObjNotAllocated
	if node.objectId != store.ObjNotAllocated {
		t.Fatalf("Expected initial ObjectId to be store.ObjNotAllocated, got %d", node.ObjectId())
	}

	// Persist the node to assign an ObjectId
	err := node.persist()
	if err != nil {
		t.Fatalf("Failed to persist node: %v", err)
	}

	// Check that the ObjectId is now valid (not store.ObjNotAllocated)
	if node.objectId == store.ObjNotAllocated {
		t.Fatalf("Expected ObjectId to be valid after persisting, got store.ObjNotAllocated")
	}

	// Add a left child and check if ObjectId is invalidated
	leftKey := IntKey(21)
	leftNode := NewPersistentTreapNode[IntKey](&leftKey, Priority(50), stre, treap)
	node.SetLeft(leftNode)

	if node.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting left child, got %d", node.ObjectId())
	}

	// Persist the node again to assign a new ObjectId
	err = node.persist()
	if err != nil {
		t.Fatalf("Failed to persist node: %v", err)
	}

	// Check that the ObjectId is now valid (not store.ObjNotAllocated)
	if node.objectId == store.ObjNotAllocated {
		t.Fatalf("Expected ObjectId to be valid after persisting, got store.ObjNotAllocated")
	}

	// Add a right child and check if ObjectId is invalidated
	rightKey := IntKey(63)
	rightNode := NewPersistentTreapNode[IntKey](&rightKey, Priority(75), stre, treap)
	node.SetRight(rightNode)

	if node.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting right child, got %d", node.ObjectId())
	}
	node.persist()
	if node.objectId == store.ObjNotAllocated {
		t.Fatalf("Expected ObjectId to be valid after persisting, got store.ObjNotAllocated")
	}
	previousObjectId := node.objectId

	rightNode.SetPriority(Priority(80))
	if rightNode.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting right child's priority, got %d", rightNode.ObjectId())
	}
	node.persist()

	if node.objectId == previousObjectId || node.objectId == store.ObjNotAllocated {
		t.Errorf("Expected ObjectId updated after setting right child's priority, got %d", node.objectId)
	}
}

func TestPersistentTreapPersistence(t *testing.T) {
	// Create the store and treap
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_store.bin")
	store0, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	var keyTemplate IntKey
	treap := NewPersistentTreap[IntKey](IntLess, &keyTemplate, store0)

	// Insert data into the treap
	keys := make([]*IntKey, 100)
	for i := 0; i < 100; i++ {
		keys[i] = (*IntKey)(new(int32))
		*keys[i] = IntKey(i)
		treap.Insert(keys[i], Priority(rand.Intn(100)))
	}

	// Persist the treap
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	// Simplification for this test
	// We will implement an object lookup mechanism later
	var treapObjectId store.ObjectId
	treapObjectId = treap.root.(*PersistentTreapNode[IntKey]).ObjectId()
	var bob PersistentTreap[IntKey]
	bob.keyTemplate = (*IntKey)(new(int32))
	bob.Store = store0
	bobNode, err := NewFromObjectId(treapObjectId, &bob, store0)
	if err != nil {
		t.Fatalf("Failed to read treap: %v", err)
	}
	if bobNode == nil {
		t.Fatalf("Failed to read treap: %v", err)
	}
	if !store.IsValidObjectId(bobNode.leftObjectId) {
		t.Fatalf("Failed to read treap, invalid left node: %v", err)
	}
	if !store.IsValidObjectId(bobNode.rightObjectId) {
		t.Fatalf("Failed to read treap, invalid right node: %v", err)
	}

	// Close the store
	store0.Close()

	// Create a new store loading the data from the file
	store1, err := store.LoadBaseStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store1.Close()

	// Create a new treap with the loaded store
	treap = NewPersistentTreap[IntKey](IntLess, (*IntKey)(new(int32)), store1)
	err = treap.Load(treapObjectId)
	if err != nil {
		t.Fatalf("Failed to load treap: %v", err)
	}
	// Test that the data is reloaded correctly
	for _, key := range keys {
		node := treap.Search(key)
		if node.IsNil() {
			t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
		} else if !key.Equals(node.GetKey().Value()) {
			t.Errorf("Expected to find key %d, but found key %d instead", *key, node.GetKey())
		}
	}
}

func TestPersistentTreapNodeMarshalUnmarshalWithChildren(t *testing.T) {
	store0 := setupTestStore(t)
	defer store0.Close()

	var keyTemplate IntKey
	parent := &PersistentTreap[IntKey]{keyTemplate: &keyTemplate, Store: store0}
	rootKey := IntKey(100)
	leftKey := IntKey(50)
	rightKey := IntKey(150)
	root := NewPersistentTreapNode[IntKey](&rootKey, 10, store0, parent)
	left := NewPersistentTreapNode[IntKey](&leftKey, 5, store0, parent)
	right := NewPersistentTreapNode[IntKey](&rightKey, 15, store0, parent)

	root.SetLeft(left)
	root.SetRight(right)

	err := root.Persist()
	if err != nil {
		t.Fatalf("Failed to persist root: %v", err)
	}
	if root.GetLeft().GetKey().Value() != left.GetKey().Value() {
		t.Errorf("Expected left child to be %v, got %v", left, root.GetLeft())
	}
	if root.GetRight().GetKey().Value() != right.GetKey().Value() {
		t.Errorf("Expected right child to be %v, got %v", right, root.GetRight())
	}
}
