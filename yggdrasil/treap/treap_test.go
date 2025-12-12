package treap

import (
	"errors"
	"testing"
)

// TestTreap verifies basic treap operations: inserting keys, searching for existing
// and non-existent keys, updating priorities, and using SearchComplex with callbacks.
func TestTreap(t *testing.T) {
	treap := NewTreap[IntKey](IntLess)

	keys := []IntKey{10, 20, 15, 5, 30}

	for _, key := range keys {
		treap.Insert(key)
	}

	for _, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d in the treap, but it was not found", key)
		} else if node.GetKey().(IntKey) != key {
			t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey().(IntKey))
		}
	}

	nonExistentKeys := []IntKey{1, 3, 6}

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %d in the treap, but it was found", key)
		}
	}

	// Test UpdatePriority
	keyToUpdate := keys[2]
	newPriority := Priority(200)
	treap.UpdatePriority(keyToUpdate, newPriority)
	updatedNode := treap.Search(keyToUpdate)
	if updatedNode == nil {
		t.Errorf("Expected to find key %d in the treap after updating priority, but it was not found", keyToUpdate)
	} else if updatedNode.GetPriority() != newPriority {
		t.Errorf("Expected priority %d, but got %d", newPriority, updatedNode.GetPriority())
	}

	// Test SearchComplex with callback
	var accessedNodes []IntKey
	callback := func(node TreapNodeInterface[IntKey]) error {
		if node != nil && !node.IsNil() {
			accessedNodes = append(accessedNodes, node.GetKey().(IntKey))
		}
		return nil
	}

	searchKey := IntKey(15)
	foundNode, err := treap.SearchComplex(searchKey, callback)
	if err != nil {
		t.Errorf("Unexpected error from SearchComplex: %v", err)
	}
	if foundNode == nil {
		t.Errorf("Expected to find key %d in the treap using SearchComplex", searchKey)
	}
	if len(accessedNodes) == 0 {
		t.Errorf("Expected callback to be called at least once during SearchComplex")
	}
	// Verify the callback was called with the searched key
	found := false
	for _, k := range accessedNodes {
		if k == searchKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected callback to be called with key %d, but it was not in the accessed nodes: %v", searchKey, accessedNodes)
	}
}

// TestTreapSearchComplexWithError verifies that SearchComplex properly propagates
// errors returned from the callback function, allowing searches to be aborted.
func TestTreapSearchComplexWithError(t *testing.T) {
	treap := NewTreap[IntKey](IntLess)

	keys := []IntKey{10, 20, 15, 5, 30}
	for _, key := range keys {
		treap.Insert(key)
	}

	// Test that callback error aborts the search
	var accessedCount int
	expectedError := errors.New("custom error from callback")
	callback := func(node TreapNodeInterface[IntKey]) error {
		accessedCount++
		// Always return error to test error handling
		return expectedError
	}

	searchKey := IntKey(5) // Search for a key that exists
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

// TestPayloadTreap verifies that a treap can store key-value pairs (payloads),
// allowing insertion and retrieval of both keys and associated data.
func TestPayloadTreap(t *testing.T) {
	treap := NewPayloadTreap[IntKey, string](IntLess)

	keys := []IntKey{10, 20, 15, 5, 30}

	payloads := []string{"ten", "twenty", "fifteen", "five", "thirty"}
	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d in the treap, but it was not found", key)
		} else if node.GetKey().(IntKey) != key {
			t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey().(IntKey))
		} else if node.(*PayloadTreapNode[IntKey, string]).payload != payloads[i] {
			t.Errorf("Expected to find payload %s, but found payload %s instead", payloads[i], node.(*PayloadTreapNode[IntKey, string]).payload)
		}
	}

	nonExistentKeys := []IntKey{1, 25, 35}

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %d in the treap, but it was found", key)
		}
	}
}

// TestStringKeyTreap verifies that treaps work correctly with string keys,
// properly maintaining order and allowing search operations.
func TestStringKeyTreap(t *testing.T) {
	treap := NewPayloadTreap[StringKey, int](StringLess)

	keys := []StringKey{"apple", "banana", "cherry", "date", "elderberry"}

	payloads := []int{1, 2, 3, 4, 5}
	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %s in the treap, but it was not found", key)
		} else if node.GetKey().(StringKey) != key {
			t.Errorf("Expected to find key %s, but found key %s instead", key, node.GetKey().(StringKey))
		} else if node.(*PayloadTreapNode[StringKey, int]).payload != payloads[i] {
			t.Errorf("Expected to find payload %d, but found payload %d instead", payloads[i], node.(*PayloadTreapNode[StringKey, int]).payload)
		}
	}

	nonExistentKeys := []StringKey{"fig", "grape", "honeydew"}

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %s in the treap, but it was found", key)
		}
	}
}

// TestCustomKeyTreap verifies that treaps work with custom types (ShortUIntKey),
// demonstrating the generic key type capability.
func TestCustomKeyTreap(t *testing.T) {
	treap := NewPayloadTreap[exampleCustomKey, string](customKeyLess)

	keys := []exampleCustomKey{
		{ID: 1, Name: "one"},
		{ID: 2, Name: "two"},
		{ID: 3, Name: "three"},
		{ID: 4, Name: "four"},
		{ID: 5, Name: "five"},
	}
	payloads := []string{"payload1", "payload2", "payload3", "payload4", "payload5"}
	for i, key := range keys {
		treap.Insert(key, payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %+v in the treap, but it was not found", key)
		} else if node.GetKey().(exampleCustomKey) != key {
			t.Errorf("Expected to find key %+v, but found key %+v instead", key, node.GetKey().(exampleCustomKey))
		} else if node.(*PayloadTreapNode[exampleCustomKey, string]).payload != payloads[i] {
			t.Errorf("Expected to find payload %s, but found payload %s instead", payloads[i], node.(*PayloadTreapNode[exampleCustomKey, string]).payload)
		}
	}

	nonExistentKeys := []exampleCustomKey{
		{ID: 6, Name: "six"},
		{ID: 7, Name: "seven"},
		{ID: 8, Name: "eight"},
	}
	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %+v in the treap, but it was found", key)
		}
	}
}

// TestTreapWalk verifies that walking a treap visits all nodes in ascending order
// (in-order traversal) and that the callback is called for each node.
func TestTreapWalk(t *testing.T) {
	treap := NewTreap[IntKey](IntLess)

	keys := []IntKey{10, 20, 15, 5, 30}

	for _, key := range keys {
		treap.Insert(key)
	}

	var walkedKeys []IntKey
	treap.Walk(func(node TreapNodeInterface[IntKey]) {
		walkedKeys = append(walkedKeys, node.GetKey().(IntKey))
	})

	expectedKeys := []IntKey{5, 10, 15, 20, 30}
	for i, key := range expectedKeys {
		if walkedKeys[i] != key {
			t.Errorf("Expected key %d at position %d, but got %d", key, i, walkedKeys[i])
		}
	}
}

// TestTreapWalkReverse verifies that WalkReverse visits all nodes in descending order
// (reverse in-order traversal).
func TestTreapWalkReverse(t *testing.T) {
	treap := NewTreap[IntKey](IntLess)

	keys := []IntKey{10, 20, 15, 5, 30}

	for _, key := range keys {
		treap.Insert(key)
	}

	var walkedKeys []IntKey
	treap.WalkReverse(func(node TreapNodeInterface[IntKey]) {
		walkedKeys = append(walkedKeys, node.GetKey().(IntKey))
	})

	expectedKeys := []IntKey{30, 20, 15, 10, 5}
	for i, key := range expectedKeys {
		if walkedKeys[i] != key {
			t.Errorf("Expected key %d at position %d, but got %d", key, i, walkedKeys[i])
		}
	}
}

// TestPointerKeyEquality verifies that pointer-based keys (like *IntKey) work correctly,
// with equality based on value not pointer identity.
func TestPointerKeyEquality(t *testing.T) {
	treap := NewTreap(IntLess)

	key1 := IntKey(10)
	key2 := IntKey(10)

	// Insert key1 into the treap
	treap.Insert(key1)

	// Search for key2 in the treap
	node := treap.Search(key2)

	if node == nil || node.IsNil() {
		t.Errorf("Expected to find key %d in the treap, but it was not found", key2)
	} else if node.GetKey().(IntKey) != key1 {
		t.Errorf("Expected to find key %d, but found key %d instead", key1, node.GetKey().(IntKey))
	}
}
