package yggdrasil

import (
	"math/rand"
	"testing"
)



func TestTreap(t *testing.T) {
	treap := NewTreap(IntLess)

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

func TestPayloadTreap(t *testing.T) {

	treap := NewPayloadTreap(IntLess)

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

	payloads := []string{"ten", "twenty", "fifteen", "five", "thirty"}
	for i, key := range keys {
		treap.Insert(key, Priority(rand.Intn(100)), payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %d in the treap, but it was not found", *key)
		} else if node.GetKey() != key {
			t.Errorf("Expected to find key %d, but found key %d instead", *key, node.GetKey())
		} else if node.(*PayloadTreapNode).payload != payloads[i] {
			t.Errorf("Expected to find payload %s, but found payload %s instead", payloads[i], node.(*PayloadTreapNode).payload)
		}
	}

	nonExistentKeys := []*IntKey{
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
		(*IntKey)(new(int32)),
	}
	*nonExistentKeys[0] = 1
	*nonExistentKeys[1] = 25
	*nonExistentKeys[2] = 35

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %d in the treap, but it was found", *key)
		}
	}
}



func TestStringKeyTreap(t *testing.T) {
	treap := NewPayloadTreap(StringLess)

	keys := []*StringKey{
		(*StringKey)(new(string)),
		(*StringKey)(new(string)),
		(*StringKey)(new(string)),
		(*StringKey)(new(string)),
		(*StringKey)(new(string)),
	}
	*keys[0] = "apple"
	*keys[1] = "banana"
	*keys[2] = "cherry"
	*keys[3] = "date"
	*keys[4] = "elderberry"

	payloads := []int{1, 2, 3, 4, 5}
	for i, key := range keys {
		treap.Insert(key, Priority(rand.Intn(100)), payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %s in the treap, but it was not found", *key)
		} else if node.GetKey() != key {
			t.Errorf("Expected to find key %s, but found key %s instead", *key, node.GetKey())
		} else if node.(*PayloadTreapNode).payload != payloads[i] {
			t.Errorf("Expected to find payload %d, but found payload %d instead", payloads[i], node.(*PayloadTreapNode).payload)
		}
	}

	nonExistentKeys := []*StringKey{
		(*StringKey)(new(string)),
		(*StringKey)(new(string)),
		(*StringKey)(new(string)),
	}
	*nonExistentKeys[0] = "fig"
	*nonExistentKeys[1] = "grape"
	*nonExistentKeys[2] = "honeydew"

	for _, key := range nonExistentKeys {
		node := treap.Search(key)
		if node != nil && !node.IsNil() {
			t.Errorf("Expected not to find key %s in the treap, but it was found", *key)
		}
	}
}

func TestCustomKeyTreap(t *testing.T) {
	treap := NewPayloadTreap(customKeyLess)

	keys := []*exampleCustomKey{
		{ID: 1, Name: "one"},
		{ID: 2, Name: "two"},
		{ID: 3, Name: "three"},
		{ID: 4, Name: "four"},
		{ID: 5, Name: "five"},
	}
	payloads := []string{"payload1", "payload2", "payload3", "payload4", "payload5"}
	for i, key := range keys {
		treap.Insert(key, Priority(rand.Intn(100)), payloads[i])
	}

	for i, key := range keys {
		node := treap.Search(key)
		if node == nil {
			t.Errorf("Expected to find key %+v in the treap, but it was not found", key)
		} else if node.GetKey() != key {
			t.Errorf("Expected to find key %+v, but found key %+v instead", key, node.GetKey())
		} else if node.(*PayloadTreapNode).payload != payloads[i] {
			t.Errorf("Expected to find payload %s, but found payload %s instead", payloads[i], node.(*PayloadTreapNode).payload)
		}
	}

	nonExistentKeys := []*exampleCustomKey{
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
	treap := NewTreap(IntLess)

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

	var walkedKeys []IntKey
	treap.Walk(func(node TreapNodeInterface) {
		walkedKeys = append(walkedKeys, *node.GetKey().(*IntKey))
	})

	expectedKeys := []IntKey{5, 10, 15, 20, 30}
	for i, key := range expectedKeys {
		if walkedKeys[i] != key {
			t.Errorf("Expected key %d at position %d, but got %d", key, i, walkedKeys[i])
		}
	}
}

func TestTreapWalkReverse(t *testing.T) {
	treap := NewTreap(IntLess)

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

	var walkedKeys []IntKey
	treap.WalkReverse(func(node TreapNodeInterface) {
		walkedKeys = append(walkedKeys, *node.GetKey().(*IntKey))
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
    treap.Insert(&key1, Priority(rand.Intn(100)))

    // Search for key2 in the treap
    node := treap.Search(&key2)

    if node == nil || node.IsNil() {
        t.Errorf("Expected to find key %d in the treap, but it was not found", key2)
    } else if node.GetKey() != &key1 {
        t.Errorf("Expected to find key %d, but found key %d instead", key1, node.GetKey())
    }
}