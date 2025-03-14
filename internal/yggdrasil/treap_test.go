package yggdrasil

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/rand"
	"testing"

	"github.com/cbehopkins/bobbob/internal/store"
)

func mockIntLess(a, b any) bool {
return a.(MockIntKey) < b.(MockIntKey)
}
type MockIntKey int32
func (k MockIntKey) SizeInBytes() int {
    return 4
}
func (k MockIntKey) GetObjectId(s *store.Store) store.ObjectId {
    return store.ObjectId(k)
}
func (k MockIntKey) Marshal() ([]byte, error) {
    buf := make([]byte, 4)
    binary.LittleEndian.PutUint32(buf, uint32(k))
    return buf, nil
}
func (k MockIntKey) Unmarshal(data []byte) (Key, error) {
    if len(data) != 4 {
        return MockIntKey(0), errors.New("invalid data length for MockKey")
    }
    return  MockIntKey(binary.LittleEndian.Uint32(data)), nil
}

type MockStringKey string

func (k MockStringKey) SizeInBytes() int {
    return store.ObjectId(0).SizeInBytes()
}

func (k MockStringKey) GetObjectId(s *store.Store) store.ObjectId {
    return store.ObjectId(0) // FIXME
}

func (k MockStringKey) Marshal() ([]byte, error) {
    return []byte(k), nil
}

func (k MockStringKey) Unmarshal(data []byte) (Key, error) {
    return MockStringKey(data), nil
}

func TestTreap(t *testing.T) {
    treap := NewTreap(mockIntLess)

    keys := []MockIntKey{10, 20, 15, 5, 30}
    for _, key := range keys {
        treap.Insert(key, Priority(rand.Intn(100)))
    }

    for _, key := range keys {
        node := treap.Search(key)
        if node == nil {
            t.Errorf("Expected to find key %d in the treap, but it was not found", key)
        } else if node.GetKey() != key {
            t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey())
        }
    }

    nonExistentKeys := []MockIntKey{1, 3, 6}
    for _, key := range nonExistentKeys {
        node := treap.Search(key)
        if node != nil  && !node.IsNil() {
            t.Errorf("Expected not to find key %d in the treap, but it was found", key)
        }
    }
}


func TestPayloadTreap(t *testing.T) {

    treap := NewPayloadTreap(mockIntLess)

    keys := []MockIntKey{10, 20, 15, 5, 30}
    payloads := []string{"ten", "twenty", "fifteen", "five", "thirty"}
    for i, key := range keys {
        treap.Insert(key, Priority(rand.Intn(100)), payloads[i])
    }

    for i, key := range keys {
        node := treap.Search(key)
        if node == nil {
            t.Errorf("Expected to find key %d in the treap, but it was not found", key)
        } else if node.GetKey() != key {
            t.Errorf("Expected to find key %d, but found key %d instead", key, node.GetKey())
        } else if node.(*PayloadTreapNode).payload != payloads[i] {
            t.Errorf("Expected to find payload %s, but found payload %s instead", payloads[i], node.(*PayloadTreapNode).payload)
        }
    }

    nonExistentKeys := []MockIntKey{1, 25, 35}
    for _, key := range nonExistentKeys {
        node := treap.Search(key)
        if node != nil && !node.IsNil() {
            t.Errorf("Expected not to find key %d in the treap, but it was found", key)
        }
    }
}

// Custom struct for testing
type CustomKey struct {
    ID   int
    Name string
}

func (k CustomKey) SizeInBytes() int {
    return 8
}

func (k CustomKey) GetObjectId(s *store.Store) store.ObjectId {
    return store.ObjectId(0) // FIXME
}


func (k CustomKey) Marshal() ([]byte, error) {
    return json.Marshal(k)
}

func (k CustomKey) Unmarshal(data []byte) (Key, error) {
    var key CustomKey
    err := json.Unmarshal(data, &key)
    if err != nil {
        return nil, err
    }
    return key, nil
}

func customKeyLess(a, b any) bool {
    ka := a.(CustomKey)
    kb := b.(CustomKey)
    if ka.ID == kb.ID {
        return ka.Name < kb.Name
    }
    return ka.ID < kb.ID
}

func stringLess(a, b any) bool {
    return a.(MockStringKey) < b.(MockStringKey)
}

func TestStringKeyTreap(t *testing.T) {
    treap := NewPayloadTreap(stringLess)

    keys := []MockStringKey{"apple", "banana", "cherry", "date", "elderberry"}
    payloads := []int{1, 2, 3, 4, 5}
    for i, key := range keys {
        treap.Insert(key, Priority(rand.Intn(100)), payloads[i])
    }

    for i, key := range keys {
        node := treap.Search(key)
        if node == nil {
            t.Errorf("Expected to find key %s in the treap, but it was not found", key)
        } else if node.GetKey() != key {
            t.Errorf("Expected to find key %s, but found key %s instead", key, node.GetKey())
        } else if node.(*PayloadTreapNode).payload != payloads[i] {
            t.Errorf("Expected to find payload %d, but found payload %d instead", payloads[i], node.(*PayloadTreapNode).payload)
        }
    }

    nonExistentKeys := []MockStringKey{"fig", "grape", "honeydew"}
    for _, key := range nonExistentKeys {
        node := treap.Search(key)
        if node != nil && !node.IsNil() {
            t.Errorf("Expected not to find key %s in the treap, but it was found", key)
        }
    }
}

func TestCustomKeyTreap(t *testing.T) {
    treap := NewPayloadTreap(customKeyLess)

    keys := []CustomKey{
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

    nonExistentKeys := []CustomKey{
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
    treap := NewTreap(mockIntLess)

    keys := []MockIntKey{10, 20, 15, 5, 30}
    for _, key := range keys {
        treap.Insert(key, Priority(rand.Intn(100)))
    }

    var walkedKeys []MockIntKey
    treap.Walk(func(node TreapNodeInterface) {
        walkedKeys = append(walkedKeys, node.GetKey().(MockIntKey))
    })

    expectedKeys := []MockIntKey{5, 10, 15, 20, 30}
    for i, key := range expectedKeys {
        if walkedKeys[i] != key {
            t.Errorf("Expected key %d at position %d, but got %d", key, i, walkedKeys[i])
        }
    }
}

func TestTreapWalkReverse(t *testing.T) {
    treap := NewTreap(mockIntLess)

    keys := []MockIntKey{10, 20, 15, 5, 30}
    for _, key := range keys {
        treap.Insert(key, Priority(rand.Intn(100)))
    }

    var walkedKeys []MockIntKey
    treap.WalkReverse(func(node TreapNodeInterface) {
        walkedKeys = append(walkedKeys, node.GetKey().(MockIntKey))
    })

    expectedKeys := []MockIntKey{30, 20, 15, 10, 5}
    for i, key := range expectedKeys {
        if walkedKeys[i] != key {
            t.Errorf("Expected key %d at position %d, but got %d", key, i, walkedKeys[i])
        }
    }
}