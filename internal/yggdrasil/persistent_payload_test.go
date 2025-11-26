package yggdrasil

import (
	"fmt"
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

func (p MockPayload) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	p.Data = string(data)
	return p, nil
}

func (p MockPayload) SizeInBytes() int {
	return len(p.Data)
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
		treap.Insert(key, payloads[i])
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
	treap.InsertComplex(&key, Priority(50), payload)

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
		treap.Insert(&key, payload)
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
		treap.Insert(&key, payload)
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
		treap.Insert(&key, payload)
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

func TestPersistentPayloadTreapPersistenceOne(t *testing.T) {
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
	key := IntKey(42)
	payload := MockPayload{Data: fmt.Sprintf("payload_%d", 42)}
	treap.Insert(&key, payload)

	// Persist the treap
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	// Simplification for this test
	// We will implement an object lookup mechanism later
	var treapObjectId store.ObjectId
	treapObjectId, _ = treap.root.(*PersistentPayloadTreapNode[IntKey, MockPayload]).ObjectId()

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
	node := treap.Search(&key)
	if node == nil || node.IsNil() {
		t.Errorf("Expected to find key %d in the treap, but it was not found", key)
	} else if !key.Equals(node.GetKey().Value()) {
		t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey())
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
		treap.Insert(keys[i], payload)
	}

	// Persist the treap
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}
	// Verify that the data is still accessible
	for _, key := range keys {
		node := treap.Search(key)
		if node == nil || node.IsNil() {
			t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
		} else if !key.Equals(node.GetKey().Value()) {
			t.Errorf("Expected to find key %d, but found key %d instead", *key, node.GetKey())
		}
	}

	// Simplification for this test
	// We will implement an object lookup mechanism later
	var treapObjectId store.ObjectId
	treapObjectId, _ = treap.root.(*PersistentPayloadTreapNode[IntKey, MockPayload]).ObjectId()
	var bob PersistentTreap[IntKey]
	bob.keyTemplate = (*IntKey)(new(int32))
	bob.Store = store0
	bobNode, err := NewPayloadFromObjectId[IntKey, MockPayload](treapObjectId, &bob, store0)
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
		if node == nil || node.IsNil() {
			t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
		} else if !key.Equals(node.GetKey().Value()) {
			t.Errorf("Expected to find key %d, but found key %d instead", *key, node.GetKey())
		}
	}
}

func TestPersistentPayloadTreapNodeMarshalToObjectId(t *testing.T) {
	stre := setupTestStore(t)
	defer stre.Close()

	// Create a DummyPayload node
	var key IntKey = 99
	priority := Priority(150)
	payload := MockPayload{Data: "dummy_payload_data"}
	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), stre)
	node := NewPersistentPayloadTreapNode[IntKey, MockPayload](&key, priority, payload, stre, treap)

	// Test MarshalToObjectId - this should write the node to the store
	objId, err := node.MarshalToObjectId(stre)
	if err != nil {
		t.Fatalf("Failed to marshal node to ObjectId: %v", err)
	}

	// Verify that we got a valid ObjectId
	if !store.IsValidObjectId(objId) {
		t.Fatalf("Expected valid ObjectId, got: %v", objId)
	}

	// Unmarshal from the ObjectId using NewPayloadFromObjectId (read from store)
	newNode, err := NewPayloadFromObjectId[IntKey, MockPayload](objId, &treap.PersistentTreap, stre)
	if err != nil {
		t.Fatalf("Failed to unmarshal node from ObjectId: %v", err)
	}

	// Verify the unmarshalled data matches the original
	if newNode.GetKey().Value() != node.GetKey().Value() {
		t.Errorf("Expected key %d, got %d", node.GetKey().Value(), newNode.GetKey().Value())
	}
	if newNode.GetPriority() != node.GetPriority() {
		t.Errorf("Expected priority %d, got %d", node.GetPriority(), newNode.GetPriority())
	}
	if newNode.GetPayload().Data != node.GetPayload().Data {
		t.Errorf("Expected payload data %s, got %s", node.GetPayload().Data, newNode.GetPayload().Data)
	}
}

// TestPersistentPayloadTreapLazyLoading verifies that loading a persisted treap from disk
// only loads the root node, not the entire tree structure.
func TestPersistentPayloadTreapLazyLoading(t *testing.T) {
	stre := setupTestStore(t)
	defer stre.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), stre)

	// Insert nodes with controlled priorities to create a predictable tree structure
	// Root will be key=50 (highest priority=100)
	// Left child will be key=30 (priority=80)
	// Right child will be key=70 (priority=90)
	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*keys[0] = 50
	*keys[1] = 30
	*keys[2] = 70

	priorities := []Priority{100, 80, 90}
	payloads := []MockPayload{
		{Data: "payload_50"},
		{Data: "payload_30"},
		{Data: "payload_70"},
	}

	// Insert in order to create the tree structure
	for i, key := range keys {
		treap.InsertComplex(key, priorities[i], payloads[i])
	}

	// Persist the entire tree to disk
	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	// Get the root object ID
	rootObjectId, _ := treap.root.(*PersistentPayloadTreapNode[IntKey, MockPayload]).ObjectId()

	// Create a new treap and load only the root from disk
	treap2 := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), stre)
	err = treap2.Load(rootObjectId)
	if err != nil {
		t.Fatalf("Failed to load treap from ObjectId: %v", err)
	}

	// Now verify that only the root is loaded, not its children
	rootNode := treap2.root.(*PersistentPayloadTreapNode[IntKey, MockPayload])

	// Verify the root node data is correct
	if rootNode.GetKey().Value() != IntKey(50) {
		t.Errorf("Expected root key 50, got %d", rootNode.GetKey().Value())
	}

	// Critical test: verify that child pointers are nil but their ObjectIds are valid
	// This proves lazy loading is working - the children are not loaded yet
	if rootNode.TreapNode.left != nil {
		t.Errorf("Expected left child pointer to be nil (not loaded yet), but it was not nil")
	}
	if !store.IsValidObjectId(rootNode.leftObjectId) {
		t.Errorf("Expected left child ObjectId to be valid, but got: %v", rootNode.leftObjectId)
	}

	if rootNode.TreapNode.right != nil {
		t.Errorf("Expected right child pointer to be nil (not loaded yet), but it was not nil")
	}
	if !store.IsValidObjectId(rootNode.rightObjectId) {
		t.Errorf("Expected right child ObjectId to be valid, but got: %v", rootNode.rightObjectId)
	}

	// Now verify that accessing the children via GetLeft()/GetRight() triggers lazy loading
	leftNode := rootNode.GetLeft()
	if leftNode == nil {
		t.Fatalf("Expected GetLeft() to load and return left child, but got nil")
	}
	if leftNode.GetKey().Value() != IntKey(30) {
		t.Errorf("Expected left child key 30, got %d", leftNode.GetKey().Value())
	}

	// After calling GetLeft(), the left pointer should now be populated
	if rootNode.TreapNode.left == nil {
		t.Errorf("Expected left child pointer to be populated after GetLeft(), but it was nil")
	}

	// Similarly for the right child
	rightNode := rootNode.GetRight()
	if rightNode == nil {
		t.Fatalf("Expected GetRight() to load and return right child, but got nil")
	}
	if rightNode.GetKey().Value() != IntKey(70) {
		t.Errorf("Expected right child key 70, got %d", rightNode.GetKey().Value())
	}

	// After calling GetRight(), the right pointer should now be populated
	if rootNode.TreapNode.right == nil {
		t.Errorf("Expected right child pointer to be populated after GetRight(), but it was nil")
	}
}

func TestPersistentPayloadTreapTimestamps(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	// Insert some keys with payloads
	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 30

	payloads := []MockPayload{
		{Data: "payload_10"},
		{Data: "payload_20"},
		{Data: "payload_30"},
	}

	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	// Search for a key - this should update timestamps
	node := treap.Search(keys[1])
	if node == nil {
		t.Fatalf("Expected to find key %d", *keys[1])
	}

	// Check that the node has a timestamp
	pNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
	timestamp := pNode.GetLastAccessTime()
	if timestamp == 0 {
		t.Errorf("Expected lastAccessTime to be set after search, but got 0")
	}

	// Verify the payload is correct
	if pNode.GetPayload().Data != payloads[1].Data {
		t.Errorf("Expected payload %s, got %s", payloads[1].Data, pNode.GetPayload().Data)
	}

	// Search for all keys to ensure they all have timestamps
	for _, key := range keys {
		treap.Search(key)
	}

	// Get all in-memory nodes
	inMemoryNodes := treap.GetInMemoryNodes()
	if len(inMemoryNodes) == 0 {
		t.Errorf("Expected to find in-memory nodes, but got none")
	}

	// Verify that all nodes now have timestamps (after being searched)
	for _, nodeInfo := range inMemoryNodes {
		if nodeInfo.LastAccessTime == 0 {
			t.Errorf("Expected node with key %v to have a timestamp after search", nodeInfo.Key)
		}
	}
}

func TestPersistentPayloadTreapGetInMemoryNodes(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	// Insert and search for keys
	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 5
	*keys[3] = 15

	payloads := []MockPayload{
		{Data: "payload_10"},
		{Data: "payload_20"},
		{Data: "payload_5"},
		{Data: "payload_15"},
	}

	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	// Search for all keys to ensure they're in memory
	for _, key := range keys {
		treap.Search(key)
	}

	// Get in-memory nodes
	inMemoryNodes := treap.GetInMemoryNodes()

	// We should have all the nodes we inserted
	if len(inMemoryNodes) != len(keys) {
		t.Errorf("Expected %d in-memory nodes, got %d", len(keys), len(inMemoryNodes))
	}

	// Verify each key is present
	keyMap := make(map[int32]bool)
	for _, nodeInfo := range inMemoryNodes {
		key := nodeInfo.Key.(*IntKey)
		keyMap[int32(*key)] = true
	}

	for _, key := range keys {
		if !keyMap[int32(*key)] {
			t.Errorf("Expected to find key %d in in-memory nodes", *key)
		}
	}
}

func TestPersistentPayloadTreapFlushOlderThan(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	// Insert keys with payloads
	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 30

	payloads := []MockPayload{
		{Data: "payload_10"},
		{Data: "payload_20"},
		{Data: "payload_30"},
	}

	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	// Search for all keys to set timestamps
	for _, key := range keys {
		treap.Search(key)
	}

	// Persist the tree
	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Get the current time
	currentTime := currentUnixTime()

	// All nodes should be in memory
	initialNodes := treap.GetInMemoryNodes()
	if len(initialNodes) != len(keys) {
		t.Errorf("Expected %d nodes in memory before flush, got %d", len(keys), len(initialNodes))
	}

	// Flush nodes older than current time + 1 second (should flush all)
	flushedCount, err := treap.FlushOlderThan(currentTime + 1)
	if err != nil {
		t.Fatalf("Failed to flush old nodes: %v", err)
	}

	if flushedCount == 0 {
		t.Errorf("Expected to flush some nodes, but flushed %d", flushedCount)
	}

	// After flushing, we should have fewer nodes in memory
	afterFlushNodes := treap.GetInMemoryNodes()
	if len(afterFlushNodes) >= len(initialNodes) {
		t.Errorf("Expected fewer nodes after flush. Before: %d, After: %d", len(initialNodes), len(afterFlushNodes))
	}

	// But we should still be able to search for all keys (they'll be loaded from disk)
	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d after flush, but it was not found", *key)
		} else {
			// Verify the payload is still correct
			pNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
			if pNode.GetPayload().Data != payloads[i].Data {
				t.Errorf("Expected payload %s for key %d, got %s", payloads[i].Data, *key, pNode.GetPayload().Data)
			}
		}
	}
}

func TestPersistentPayloadTreapSelectiveFlush(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	// Insert keys with payloads
	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 30

	payloads := []MockPayload{
		{Data: "payload_10"},
		{Data: "payload_20"},
		{Data: "payload_30"},
	}

	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	// Search for first two keys
	treap.Search(keys[0])
	treap.Search(keys[1])

	// Record the timestamp after searching for the first two
	midTimestamp := currentUnixTime()

	// Search for the third key and manually set a newer timestamp
	node2 := treap.Search(keys[2])
	pNode2 := node2.(*PersistentPayloadTreapNode[IntKey, MockPayload])
	pNode2.SetLastAccessTime(midTimestamp + 10) // Manually set a newer timestamp

	// Persist everything
	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Flush nodes older than midTimestamp + 5 (should flush keys[0] and keys[1], but not keys[2])
	flushedCount, err := treap.FlushOlderThan(midTimestamp + 5)
	if err != nil {
		t.Fatalf("Failed to flush old nodes: %v", err)
	}

	if flushedCount == 0 {
		t.Errorf("Expected to flush some nodes, got %d", flushedCount)
	}

	// The third key should still be in memory with its newer timestamp
	inMemoryNodes := treap.GetInMemoryNodes()
	foundKey2 := false
	for _, nodeInfo := range inMemoryNodes {
		key := nodeInfo.Key.(*IntKey)
		if *key == *keys[2] {
			foundKey2 = true
			if nodeInfo.LastAccessTime < midTimestamp+5 {
				t.Errorf("Expected key %d to have timestamp >= %d, got %d", *key, midTimestamp+5, nodeInfo.LastAccessTime)
			}
		}
	}

	if !foundKey2 {
		t.Logf("Key %d was flushed (or not found), which is acceptable depending on tree structure", *keys[2])
	}

	// All keys should still be searchable and have correct payloads
	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d (keys[%d]) after selective flush", *key, i)
		} else {
			pNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
			if pNode.GetPayload().Data != payloads[i].Data {
				t.Errorf("Expected payload %s for key %d, got %s", payloads[i].Data, *key, pNode.GetPayload().Data)
			}
		}
	}
}

func TestPersistentPayloadTreapFlushAndReload(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	// Insert a larger number of keys
	const numKeys = 20
	keys := make([]*IntKey, numKeys)
	payloads := make([]MockPayload, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = (*IntKey)(new(int32))
		*keys[i] = IntKey(i * 10)
		payloads[i] = MockPayload{Data: fmt.Sprintf("payload_%d", i*10)}
		treap.Insert(keys[i], payloads[i])
	}

	// Search for all keys to load them into memory and set timestamps
	for _, key := range keys {
		treap.Search(key)
	}

	// Persist the tree
	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Record how many nodes are in memory before flush
	beforeFlush := treap.GetInMemoryNodes()
	t.Logf("Nodes in memory before flush: %d", len(beforeFlush))

	// Flush all nodes
	currentTime := currentUnixTime()
	flushedCount, err := treap.FlushOlderThan(currentTime + 1)
	if err != nil {
		t.Fatalf("Failed to flush nodes: %v", err)
	}

	t.Logf("Flushed %d nodes", flushedCount)

	// Verify fewer nodes are in memory
	afterFlush := treap.GetInMemoryNodes()
	t.Logf("Nodes in memory after flush: %d", len(afterFlush))

	if len(afterFlush) >= len(beforeFlush) {
		t.Errorf("Expected fewer nodes after flush. Before: %d, After: %d", len(beforeFlush), len(afterFlush))
	}

	// Search for a subset of keys - they should be reloaded from disk
	searchIndices := []int{5, 10, 15}
	for _, idx := range searchIndices {
		node := treap.Search(keys[idx])
		if node == nil {
			t.Errorf("Expected to find key %d after flush and reload", *keys[idx])
		} else {
			pNode := node.(*PersistentPayloadTreapNode[IntKey, MockPayload])
			if pNode.GetPayload().Data != payloads[idx].Data {
				t.Errorf("Expected payload %s for key %d, got %s", payloads[idx].Data, *keys[idx], pNode.GetPayload().Data)
			}
			// Verify the timestamp was updated by the search
			if pNode.GetLastAccessTime() == 0 {
				t.Errorf("Expected timestamp to be set after reload for key %d", *keys[idx])
			}
		}
	}

	// Now we should have more nodes in memory again (the ones we just searched for)
	afterSearch := treap.GetInMemoryNodes()
	t.Logf("Nodes in memory after searching: %d", len(afterSearch))

	if len(afterSearch) <= len(afterFlush) {
		t.Logf("Note: Expected more nodes after searching, but got %d (was %d). This may be OK depending on tree structure.", len(afterSearch), len(afterFlush))
	}
}

func TestPersistentPayloadTreapFlushWithNoNodes(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	// Try to flush when there are no nodes
	currentTime := currentUnixTime()
	flushedCount, err := treap.FlushOlderThan(currentTime)
	if err != nil {
		t.Fatalf("Expected no error when flushing empty tree, got: %v", err)
	}

	if flushedCount != 0 {
		t.Errorf("Expected to flush 0 nodes from empty tree, got %d", flushedCount)
	}
}

func TestPersistentPayloadTreapFlushNoneOlderThan(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	treap := NewPersistentPayloadTreap[IntKey, MockPayload](IntLess, (*IntKey)(new(int32)), store)

	// Insert keys
	keys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20

	payloads := []MockPayload{
		{Data: "payload_10"},
		{Data: "payload_20"},
	}

	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	// Search to set timestamps
	for _, key := range keys {
		treap.Search(key)
	}

	// Persist
	err := treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist tree: %v", err)
	}

	// Try to flush with a cutoff time in the past (no nodes should be flushed)
	pastTime := currentUnixTime() - 3600 // 1 hour ago
	flushedCount, err := treap.FlushOlderThan(pastTime)
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	if flushedCount != 0 {
		t.Errorf("Expected to flush 0 nodes (all newer than cutoff), got %d", flushedCount)
	}

	// All nodes should still be in memory
	inMemoryNodes := treap.GetInMemoryNodes()
	if len(inMemoryNodes) != len(keys) {
		t.Errorf("Expected %d nodes still in memory, got %d", len(keys), len(inMemoryNodes))
	}
}
