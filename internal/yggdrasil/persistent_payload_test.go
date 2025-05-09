package yggdrasil

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"bobbob/internal/store"
)

// MockPayload is a mock implementation of PersistentPayload for testing.
type MockPayload struct {
	Data string
}

func (p MockPayload) Marshal() ([]byte, error) {
	return []byte(p.Data), nil
}

func (p MockPayload) Unmarshal(data []byte) (MockPayload, error) {
	p.Data = string(data)
	return p, nil
}

func TestPersistentPayloadTreapNodeMarshalUnmarshal(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	var key IntKey = 42
	priority := Priority(100)
	payload := MockPayload{Data: "test_payload"}
	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)
	node := NewPersistentPayloadTreapNode[IntKey, MockPayload](&key, priority, payload, store, treap)

	// Marshal the node
	data, err := node.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	// Unmarshal the node
	unmarshalledNode := &PersistentPayloadTreapNode[IntKey, MockPayload]{}
	unmarshalledNode.payload = MockPayload{}
	err = unmarshalledNode.unmarshal(data, &key)
	if err != nil {
		t.Fatalf("Failed to unmarshal node: %v", err)
	}

	// Check if the unmarshalled node is equal to the original node
	if unmarshalledNode.GetKey().Value() != node.GetKey().Value() {
		t.Errorf("Expected key %d, got %d", *node.GetKey().(*IntKey), *unmarshalledNode.GetKey().(*IntKey))
	}
	if unmarshalledNode.GetPriority() != node.GetPriority() {
		t.Errorf("Expected priority %d, got %d", node.GetPriority(), unmarshalledNode.GetPriority())
	}
	if unmarshalledNode.GetPayload().Data != node.GetPayload().Data {
		t.Errorf("Expected payload %s, got %s", node.GetPayload().Data, unmarshalledNode.GetPayload().Data)
	}
}

func TestPersistentPayloadTreapInsertAndSearch(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 15

	payloads := []MockPayload{
		{Data: "payload_10"},
		{Data: "payload_20"},
		{Data: "payload_15"},
	}

	for i, key := range keys {
		treap.Insert(key, Priority(rand.Intn(100)), payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil || node.IsNil() {
			t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
		} else {
			payloadNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
			if payloadNode.GetPayload().Data != payloads[i].Data {
				t.Errorf("Expected payload %s, got %s", payloads[i].Data, payloadNode.GetPayload().Data)
			}
		}
	}
}

func TestPersistentPayloadTreapUpdatePayload(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	key := IntKey(42)
	payload := MockPayload{Data: "initial_payload"}
	treap.Insert(&key, Priority(50), payload)

	// Update the payload
	newPayload := MockPayload{Data: "updated_payload"}
	treap.UpdatePayload(&key, newPayload)

	// Verify the updated payload
	node := treap.Search(&key)
	if node == nil || node.IsNil() {
		t.Fatalf("Expected to find key %d in the treap, but it was not found", key)
	}
	payloadNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
	if payloadNode.GetPayload().Data != newPayload.Data {
		t.Errorf("Expected payload %s, got %s", newPayload.Data, payloadNode.GetPayload().Data)
	}
}

func TestPersistentPayloadTreapInsertLargeNumberOfPairs(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	const numPairs = 10000
	keys := make([]*IntKey, numPairs)
	payloads := make([]MockPayload, numPairs)

	// Insert a large number of key/value pairs
	for i := 0; i < numPairs; i++ {
		key := IntKey(i)
		keys[i] = &key
		payload := MockPayload{Data: fmt.Sprintf("payload_%d", i)}
		payloads[i] = payload
		treap.Insert(&key, Priority(rand.Intn(100)), payload)
	}

	// Update each key with a new payload
	for i := 0; i < numPairs; i++ {
		newPayload := MockPayload{Data: fmt.Sprintf("updated_payload_%d", i)}
		treap.UpdatePayload(keys[i], newPayload)
		payloads[i] = newPayload
	}

	// Verify that each key returns the most up-to-date payload
	for i, key := range keys {
		node := treap.Search(key)
		if node == nil || node.IsNil() {
			t.Fatalf("Expected to find key %d in the treap, but it was not found", *key)
		}
		payloadNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
		if payloadNode.GetPayload().Data != payloads[i].Data {
			t.Errorf("Expected payload %s, got %s", payloads[i].Data, payloadNode.GetPayload().Data)
		}
	}
}

func TestPersistentPayloadTreapLargeScaleUpdateAndVerify(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	const numPairs = 5000
	keys := make([]*IntKey, numPairs)
	payloads := make([]MockPayload, numPairs)

	// Insert key-value pairs
	for i := 0; i < numPairs; i++ {
		key := IntKey(i)
		keys[i] = &key
		payload := MockPayload{Data: fmt.Sprintf("initial_payload_%d", i)}
		payloads[i] = payload
		treap.Insert(&key, Priority(rand.Intn(100)), payload)
	}

	// Update payloads
	for i := 0; i < numPairs; i++ {
		newPayload := MockPayload{Data: fmt.Sprintf("updated_payload_%d", i)}
		treap.UpdatePayload(keys[i], newPayload)
		payloads[i] = newPayload
	}

	// Verify updated payloads
	for i, key := range keys {
		node := treap.Search(key)
		if node == nil || node.IsNil() {
			t.Fatalf("Expected to find key %d in the treap, but it was not found", *key)
		}
		payloadNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
		if payloadNode.GetPayload().Data != payloads[i].Data {
			t.Errorf("Expected payload %s, got %s", payloads[i].Data, payloadNode.GetPayload().Data)
		}
	}
}

func TestPersistentPayloadTreapInsertDeleteAndVerify(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	const numPairs = 5000
	keys := make([]*IntKey, numPairs)
	payloads := make([]MockPayload, numPairs)

	// Insert key-value pairs
	for i := 0; i < numPairs; i++ {
		key := IntKey(i)
		keys[i] = &key
		payload := MockPayload{Data: fmt.Sprintf("payload_%d", i)}
		payloads[i] = payload
		treap.Insert(&key, Priority(rand.Intn(100)), payload)
	}

	// Delete some keys
	for i := 0; i < numPairs; i += 2 { // Delete every second key
		treap.Delete(keys[i])
	}

	// Verify remaining keys
	for i, key := range keys {
		node := treap.Search(key)
		if i%2 == 0 { // Deleted keys
			if node != nil && !node.IsNil() {
				t.Errorf("Expected key %d to be deleted, but it was found", *key)
			}
		} else { // Remaining keys
			if node == nil || node.IsNil() {
				t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
			} else {
				payloadNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
				if payloadNode.GetPayload().Data != payloads[i].Data {
					t.Errorf("Expected payload %s, got %s", payloads[i].Data, payloadNode.GetPayload().Data)
				}
			}
		}
	}
}

func TestPersistentPayloadTreapPersistence(t *testing.T) {
	// Create the store and treap
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_store.bin")
	store0, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	keyTemplate := (*IntKey)(new(int32))
	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, keyTemplate, store0)

	// Insert data into the treap
	keys := make([]*IntKey, 100)
	for i := 0; i < 100; i++ {
		keys[i] = (*IntKey)(new(int32))
		*keys[i] = IntKey(i)
		payload := MockPayload{Data: fmt.Sprintf("payload_%d", i)}
		treap.Insert(keys[i], Priority(rand.Intn(100)), payload)
	}

	// Persist the treap
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	// Simplification for this test
	// We will implement an object lookup mechanism later
	var treapObjectId store.ObjectId
	treapObjectId = treap.root.(*PersistentPayloadTreapNode[IntKey, MockPayload]).ObjectId()
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
	treap = NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store1)
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
