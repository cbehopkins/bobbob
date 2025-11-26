package yggdrasil

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"bobbob/internal/store"
)

// Example demonstrates creating a basic in-memory treap.
func Example() {
	// Create a treap with integer keys
	treap := NewTreap[IntKey](IntLess)

	// Insert some key-priority pairs
	treap.Insert(IntKey(10), 50)
	treap.Insert(IntKey(5), 30)
	treap.Insert(IntKey(15), 40)

	// Search for a key
	node := treap.Search(IntKey(10))
	if node != nil && !node.IsNil() {
		fmt.Printf("Found key: %d\n", node.GetKey().(IntKey))
	}
	// Output: Found key: 10
}

// ExampleTreap_Walk demonstrates iterating through treap nodes in order.
func ExampleTreap_Walk() {
	treap := NewTreap[IntKey](IntLess)

	// Insert keys with priorities
	treap.Insert(IntKey(30), 1)
	treap.Insert(IntKey(10), 2)
	treap.Insert(IntKey(20), 3)
	treap.Insert(IntKey(40), 4)

	// Walk through in sorted order
	treap.Walk(func(node TreapNodeInterface[IntKey]) {
		fmt.Printf("%d ", node.GetKey().(IntKey))
	})
	fmt.Println()
	// Output: 10 20 30 40
}

// ExamplePayloadTreap demonstrates using a treap with data payloads.
func ExamplePayloadTreap() {
	// Create a payload treap mapping strings to integers
	treap := NewPayloadTreap[StringKey, int](StringLess)

	// Insert key-value pairs
	treap.Insert(StringKey("age"), 100, 25)
	treap.Insert(StringKey("score"), 200, 95)
	treap.Insert(StringKey("level"), 150, 7)

	// Search and retrieve payload
	node := treap.Search(StringKey("score"))
	if node != nil && !node.IsNil() {
		payloadNode := node.(*PayloadTreapNode[StringKey, int])
		fmt.Printf("score = %d\n", payloadNode.GetPayload())
	}
	// Output: score = 95
}

// SimplePayload is a simple integer payload for demonstration.
type SimplePayload int

func (p SimplePayload) Marshal() ([]byte, error) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(p))
	return data, nil
}

func (p SimplePayload) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	val := SimplePayload(binary.LittleEndian.Uint64(data))
	return val, nil
}

func (p SimplePayload) SizeInBytes() int {
	return 8
}

// ExamplePersistentPayloadTreap demonstrates a persistent treap backed by storage.
func ExamplePersistentPayloadTreap() {
	tmpFile := filepath.Join(os.TempDir(), "example_treap.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	// Create a persistent treap
	treap := NewPersistentPayloadTreap[IntKey, SimplePayload](IntLess, (*IntKey)(new(int32)), s)

	// Create keys for persistent interface
	key100, key200, key300 := IntKey(100), IntKey(200), IntKey(300)

	// Insert key-value pairs
	treap.Insert(&key100, 1, SimplePayload(42))
	treap.Insert(&key200, 2, SimplePayload(99))
	treap.Insert(&key300, 3, SimplePayload(17))

	// Persist to storage
	treap.Persist()

	// Search for a value
	node := treap.Search(&key200)
	if node != nil && !node.IsNil() {
		fmt.Printf("Value at key 200: %d\n", node.GetPayload())
	}
	// Output: Value at key 200: 99
}

// ExamplePersistentPayloadTreap_Load demonstrates loading a persisted treap.
func ExamplePersistentPayloadTreap_Load() {
	tmpFile := filepath.Join(os.TempDir(), "example_load_treap.bin")
	defer os.Remove(tmpFile)

	var rootObjId store.ObjectId

	// Create and persist a treap
	{
		s, _ := store.NewBasicStore(tmpFile)

		treap := NewPersistentPayloadTreap[IntKey, SimplePayload](IntLess, (*IntKey)(new(int32)), s)

		key42 := IntKey(42)
		treap.Insert(&key42, 1, SimplePayload(123))
		treap.Persist()

		// Get the root object ID for later loading
		rootNode := treap.root.(*PersistentPayloadTreapNode[IntKey, SimplePayload])
		rootObjId = rootNode.ObjectId()

		s.Close()
	}

	// Load the treap from storage
	{
		s, _ := store.LoadBaseStore(tmpFile)
		defer s.Close()

		treap := NewPersistentPayloadTreap[IntKey, SimplePayload](IntLess, (*IntKey)(new(int32)), s)
		treap.Load(rootObjId)

		// Access the loaded data
		searchKey := IntKey(42)
		node := treap.Search(&searchKey)
		if node != nil && !node.IsNil() {
			fmt.Printf("Loaded value: %d\n", node.GetPayload())
		}
	}
	// Output: Loaded value: 123
}

// ExampleTypeMap demonstrates the type mapping system.
func ExampleTypeMap() {
	// Create an empty TypeMap without built-in types
	tm := &TypeMap{}

	// Register custom types (all strings get same type name)
	tm.AddType("User")
	tm.AddType("Post")
	tm.AddType("Comment")

	// All three strings map to the same "string" type
	fmt.Printf("Registered %d types\n", tm.NextShortCode)
	// Output: Registered 1 types
}

// ExampleTreap_Delete demonstrates removing nodes from a treap.
func ExampleTreap_Delete() {
	treap := NewTreap[IntKey](IntLess)

	// Insert keys
	treap.Insert(IntKey(10), 1)
	treap.Insert(IntKey(20), 2)
	treap.Insert(IntKey(30), 3)

	// Delete a node
	treap.Delete(IntKey(20))

	// Walk to verify deletion
	treap.Walk(func(node TreapNodeInterface[IntKey]) {
		fmt.Printf("%d ", node.GetKey().(IntKey))
	})
	fmt.Println()
	// Output: 10 30
}

// ExamplePersistentTreap demonstrates using a persistent treap without payloads.
func ExamplePersistentTreap() {
	tmpFile := filepath.Join(os.TempDir(), "example_persistent.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	treap := NewPersistentTreap[IntKey](IntLess, (*IntKey)(new(int32)), s)

	// Create and insert keys with priorities
	key5, key3, key7 := IntKey(5), IntKey(3), IntKey(7)
	treap.Insert(&key5, 100)
	treap.Insert(&key3, 200)
	treap.Insert(&key7, 150)

	// Persist to disk
	treap.Persist()

	// Walk through persisted treap
	treap.Walk(func(node TreapNodeInterface[IntKey]) {
		fmt.Printf("%d ", *node.GetKey().(*IntKey))
	})
	fmt.Println()
	// Output: 3 5 7
}

// ExampleTreap_UpdatePriority demonstrates changing node priorities.
func ExampleTreap_UpdatePriority() {
	treap := NewTreap[IntKey](IntLess)

	// Insert with initial priority
	treap.Insert(IntKey(100), 50)

	// Update priority (causes rebalancing)
	treap.UpdatePriority(IntKey(100), 250)

	// Node is still accessible with new priority
	node := treap.Search(IntKey(100))
	if node != nil && !node.IsNil() {
		fmt.Printf("Key %d has new priority %d\n",
			node.GetKey().(IntKey),
			node.GetPriority())
	}
	// Output: Key 100 has new priority 250
}
