package yggdrasil

import (
	"testing"
)

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
	callback := func(node TreapNodeInterface[IntKey]) {
		if node != nil && !node.IsNil() {
			accessedNodes = append(accessedNodes, node.GetKey().(IntKey))
		}
	}

	searchKey := IntKey(15)
	foundNode := treap.SearchComplex(searchKey, callback)
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
