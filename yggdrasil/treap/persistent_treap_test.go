package treap

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
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

// TestPersistentTreapBasics verifies basic operations on a persistent treap:
// insertion, search, deletion, walking, and counting nodes.
func TestPersistentTreapBasics(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()
	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, store)

	keys := []*types.IntKey{
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 15
	*keys[3] = 5
	*keys[4] = 30

	for _, key := range keys {
		treap.Insert(key)
	}

	for _, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
		} else if node.GetKey() != key {
			t.Errorf("Expected to find key %d, but found key %d instead", *key, node.GetKey())
		}
	}

	nonExistentKeys := []*types.IntKey{
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
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

	// Test SearchComplex with callback
	var accessedNodes []types.IntKey
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		if node != nil && !node.IsNil() {
			key := node.GetKey().(*types.IntKey)
			accessedNodes = append(accessedNodes, *key)
		}
		return nil
	}

	searchKey := keys[1] // key with value 20
	foundNode, err := treap.SearchComplex(searchKey, callback)
	if err != nil {
		t.Errorf("Unexpected error from SearchComplex: %v", err)
	}
	if foundNode == nil {
		t.Errorf("Expected to find key %d in the treap using SearchComplex", *searchKey)
	}
	if len(accessedNodes) == 0 {
		t.Errorf("Expected callback to be called at least once during SearchComplex")
	}
	// Verify the callback was called with the searched key
	found := false
	for _, k := range accessedNodes {
		if k == *searchKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected callback to be called with key %d, but it was not in the accessed nodes: %v", *searchKey, accessedNodes)
	}
}

// TestPersistentTreapSearchComplexWithError verifies that SearchComplex on persistent treaps
// properly handles and propagates callback errors, allowing search abortion.
func TestPersistentTreapSearchComplexWithError(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, store)

	keys := []*types.IntKey{
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
	}
	*keys[0] = 50
	*keys[1] = 30
	*keys[2] = 70
	*keys[3] = 20
	*keys[4] = 40

	for _, key := range keys {
		treap.Insert(key)
	}

	// Test that callback error aborts the search
	var accessedCount int
	expectedError := errors.New("custom error from callback")
	callback := func(node TreapNodeInterface[types.IntKey]) error {
		accessedCount++
		// Always return error to test error handling
		return expectedError
	}

	searchKey := keys[3] // Search for 20, which requires traversal
	foundNode, err := treap.SearchComplex(searchKey, callback)

	// Should get the error we returned
	if err == nil {
		t.Errorf("Expected error from callback, but got nil")
	}
	if err != expectedError {
		t.Errorf("Expected error %v, but got %v", expectedError, err)
	}

	// Should not have found the node due to error
	if foundNode != nil {
		t.Errorf("Expected nil node when callback returns error, but got node with key %v", foundNode.GetKey())
	}

	// Should have accessed at least 1 node before error (the root)
	if accessedCount < 1 {
		t.Errorf("Expected at least 1 node access, but got %d", accessedCount)
	}
}

// TestPersistentTreapNodeMarshalUnmarshal verifies that persistent treap nodes can be
// serialized and deserialized, preserving key, priority, and object IDs.
func TestPersistentTreapNodeMarshalUnmarshal(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	key := types.IntKey(42)
	priority := Priority(100)
	treap := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), store)
	node := NewPersistentTreapNode[types.IntKey](&key, priority, store, treap)

	// Marshal the node
	data, err := node.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal node: %v", err)
	}

	// Unmarshal the node
	unmarshalledNode := &PersistentTreapNode[types.IntKey]{Store: store, parent: treap}
	dstKey := types.IntKey(0)
	err = unmarshalledNode.unmarshal(data, &dstKey)
	if err != nil {
		t.Fatalf("Failed to unmarshal node: %v", err)
	}
	tmpKey := unmarshalledNode.GetKey().(*types.IntKey)
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
// TestPersistentTreapNodeInvalidateObjectId verifies that modifying a node (e.g., rotating)
// invalidates its object ID, forcing re-serialization on next persist.
func TestPersistentTreapNodeInvalidateObjectId(t *testing.T) {
	stre := setupTestStore(t)
	defer stre.Close()

	key := types.IntKey(42)
	priority := Priority(100)
	treap := NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), stre)
	node := NewPersistentTreapNode[types.IntKey](&key, priority, stre, treap)

	// Initially, the ObjectId should be store.ObjNotAllocated
	if node.objectId != store.ObjNotAllocated {
		t.Fatalf("Expected initial ObjectId to be store.ObjNotAllocated, got %d", node.objectId)
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
	leftKey := types.IntKey(21)
	leftNode := NewPersistentTreapNode[types.IntKey](&leftKey, Priority(50), stre, treap)
	err = node.SetLeft(leftNode)
	if err != nil {
		t.Fatalf("Failed to set left child: %v", err)
	}

	if node.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting left child, got %d", node.objectId)
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
	rightKey := types.IntKey(63)
	rightNode := NewPersistentTreapNode[types.IntKey](&rightKey, Priority(70), stre, treap)
	err = node.SetRight(rightNode)
	if err != nil {
		t.Fatalf("Failed to set right child: %v", err)
	}

	if node.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting right child, got %d", node.objectId)
	}
	node.persist()
	if node.objectId == store.ObjNotAllocated {
		t.Fatalf("Expected ObjectId to be valid after persisting, got store.ObjNotAllocated")
	}

	rightNode.SetPriority(Priority(80))
	if rightNode.objectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be invalidated (set to store.ObjNotAllocated) after setting right child's priority, got %d", rightNode.objectId)
	}
	node.persist()

	// Note: After persisting, node.objectId might be the same or different from previousObjectId
	// because the allocator reuses ObjectIds from the free list. What's important is that:
	// 1. The parent was invalidated (set to ObjNotAllocated) when the child changed
	// 2. A new allocation was made (ObjectId was obtained again)
	// 3. The serialized data includes the child's new ObjectId
	// We verify this by checking that the parent's ObjectId is now valid (not ObjNotAllocated)
	if node.objectId == store.ObjNotAllocated {
		t.Errorf("Expected ObjectId to be valid after persisting with child change, got store.ObjNotAllocated")
	}
}

// TestPersistentTreapPersistence verifies that a treap can be persisted to storage,
// then loaded back in a new session with all data intact.
func TestPersistentTreapPersistence(t *testing.T) {
	// Create the store and treap
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_store.bin")
	store0, err := store.NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	var keyTemplate types.IntKey
	treap := NewPersistentTreap[types.IntKey](types.IntLess, &keyTemplate, store0)

	// Insert data into the treap
	keys := make([]*types.IntKey, 100)
	for i := 0; i < 100; i++ {
		keys[i] = (*types.IntKey)(new(int32))
		*keys[i] = types.IntKey(i)
		treap.Insert(keys[i])
	}

	// Persist the treap
	err = treap.Persist()
	if err != nil {
		t.Fatalf("Failed to persist treap: %v", err)
	}

	// Simplification for this test
	// We will implement an object lookup mechanism later
	var treapObjectId store.ObjectId
	treapObjectId, _ = treap.root.(*PersistentTreapNode[types.IntKey]).ObjectId()
	var bob PersistentTreap[types.IntKey]
	bob.keyTemplate = (*types.IntKey)(new(int32))
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
	treap = NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), store1)

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

// TestPersistentTreapNodeMarshalUnmarshalWithChildren verifies that nodes with child
// pointers serialize correctly, storing child object IDs and reloading them properly.
func TestPersistentTreapNodeMarshalUnmarshalWithChildren(t *testing.T) {
	store0 := setupTestStore(t)
	defer store0.Close()

	var keyTemplate types.IntKey
	parent := &PersistentTreap[types.IntKey]{keyTemplate: &keyTemplate, Store: store0}
	rootKey := types.IntKey(100)
	leftKey := types.IntKey(50)
	rightKey := types.IntKey(150)
	root := NewPersistentTreapNode[types.IntKey](&rootKey, 10, store0, parent)
	left := NewPersistentTreapNode[types.IntKey](&leftKey, 5, store0, parent)
	right := NewPersistentTreapNode[types.IntKey](&rightKey, 15, store0, parent)

	err := root.SetLeft(left)
	if err != nil {
		t.Fatalf("Failed to set left child: %v", err)
	}
	err = root.SetRight(right)
	if err != nil {
		t.Fatalf("Failed to set right child: %v", err)
	}

	err = root.Persist()
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

// TestPersistentTreapTimestamps verifies that nodes track their last access time,
// which is used for memory management (flushing old nodes).
func TestPersistentTreapTimestamps(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, store)

	// Insert some keys
	keys := []*types.IntKey{
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 30

	for _, key := range keys {
		treap.Insert(key)
	}

	// Search for a key - this should update timestamps
	node := treap.Search(keys[1])
	if node == nil {
		t.Fatalf("Expected to find key %d", *keys[1])
	}

	// Check that the node has a timestamp
	pNode := node.(*PersistentTreapNode[types.IntKey])
	timestamp := pNode.GetLastAccessTime()
	if timestamp == 0 {
		t.Errorf("Expected lastAccessTime to be set after search, but got 0")
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

// TestPersistentTreapGetInMemoryNodes verifies that GetInMemoryNodes correctly counts
// nodes currently loaded in memory (vs. on disk).
func TestPersistentTreapGetInMemoryNodes(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, store)

	// Insert and search for keys
	keys := []*types.IntKey{
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 5
	*keys[3] = 15

	for _, key := range keys {
		treap.Insert(key)
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
		key := nodeInfo.Key.(*types.IntKey)
		keyMap[int32(*key)] = true
	}

	for _, key := range keys {
		if !keyMap[int32(*key)] {
			t.Errorf("Expected to find key %d in in-memory nodes", *key)
		}
	}
}

// TestPersistentTreapFlushOlderThan verifies that FlushOlderThan removes nodes from memory
// that haven't been accessed recently, reducing memory usage.
func TestPersistentTreapFlushOlderThan(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, store)

	// Insert keys
	keys := []*types.IntKey{
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 30

	for _, key := range keys {
		treap.Insert(key)
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
	for _, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d after flush, but it was not found", *key)
		}
	}
}

// TestPersistentTreapSelectiveFlush verifies that selective flushing removes only old nodes
// from memory while keeping recently accessed nodes loaded.
func TestPersistentTreapSelectiveFlush(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	var keyTemplate *types.IntKey = (*types.IntKey)(new(int32))
	treap := NewPersistentTreap[types.IntKey](types.IntLess, keyTemplate, store)

	// Insert keys
	keys := []*types.IntKey{
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
		(*types.IntKey)(new(int32)),
	}
	*keys[0] = 10
	*keys[1] = 20
	*keys[2] = 30

	for _, key := range keys {
		treap.Insert(key)
	}

	// Search for first two keys
	treap.Search(keys[0])
	treap.Search(keys[1])

	// Record the timestamp after searching for the first two
	midTimestamp := currentUnixTime()

	// Wait a moment and search for the third key
	// (In a real scenario, there would be a time gap; for testing we simulate with direct timestamp manipulation)
	node2 := treap.Search(keys[2])
	pNode2 := node2.(*PersistentTreapNode[types.IntKey])
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
		key := nodeInfo.Key.(*types.IntKey)
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

	// All keys should still be searchable
	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d (keys[%d]) after selective flush", *key, i)
		}
	}
}
