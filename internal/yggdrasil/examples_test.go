package yggdrasil

import (
	"encoding/binary"
	"errors"
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
	treap.InsertComplex(IntKey(10), 50)
	treap.InsertComplex(IntKey(5), 30)
	treap.InsertComplex(IntKey(15), 40)

	// Search for a key
	node := treap.Search(IntKey(10))
	if node != nil && !node.IsNil() {
		fmt.Printf("Found key: %d\n", node.GetKey().Value())
	}
	// Output: Found key: 10
}

// ExampleTreap_Walk demonstrates iterating through treap nodes in order.
func ExampleTreap_Walk() {
	treap := NewTreap[IntKey](IntLess)

	// Insert keys with priorities
	treap.InsertComplex(IntKey(30), 1)
	treap.InsertComplex(IntKey(10), 2)
	treap.InsertComplex(IntKey(20), 3)
	treap.InsertComplex(IntKey(40), 4)

	// Walk through in sorted order
	treap.Walk(func(node TreapNodeInterface[IntKey]) {
		fmt.Printf("%d ", node.GetKey().Value())
	})
	fmt.Println()
	// Output: 10 20 30 40
}

// ExampleTreap_SearchComplex demonstrates using SearchComplex with a callback
// that can return an error to abort the search. This is useful for implementing
// access control, rate limiting, or conditional searches.
func ExampleTreap_SearchComplex() {
	treap := NewTreap[IntKey](IntLess)

	// Insert some keys
	treap.InsertComplex(IntKey(10), 1)
	treap.InsertComplex(IntKey(20), 2)
	treap.InsertComplex(IntKey(30), 3)
	treap.InsertComplex(IntKey(40), 4)
	treap.InsertComplex(IntKey(50), 5)

	// Example 1: Track which nodes are accessed during search
	var accessedNodes []int
	callback := func(node TreapNodeInterface[IntKey]) error {
		key := int(node.GetKey().(IntKey))
		accessedNodes = append(accessedNodes, key)
		return nil
	}

	node, err := treap.SearchComplex(IntKey(30), callback)
	if err == nil && node != nil {
		fmt.Printf("Found: %d\n", node.GetKey().(IntKey))
		fmt.Printf("Accessed nodes: %v\n", accessedNodes)
	}

	// Example 2: Abort search based on a condition
	accessCount := 0
	limitCallback := func(node TreapNodeInterface[IntKey]) error {
		accessCount++
		if accessCount >= 3 {
			return errors.New("access limit exceeded")
		}
		return nil
	}

	node, err = treap.SearchComplex(IntKey(10), limitCallback)
	if err != nil {
		fmt.Printf("Search aborted: %v\n", err)
	}

	// Output:
	// Found: 30
	// Accessed nodes: [50 40 30]
	// Search aborted: access limit exceeded
}

// ExamplePayloadTreap demonstrates using a treap with data payloads.
func ExamplePayloadTreap() {
	// Create a payload treap mapping strings to integers
	treap := NewPayloadTreap[StringKey, int](StringLess)

	// Insert key-value pairs
	treap.InsertComplex(StringKey("age"), 100, 25)
	treap.InsertComplex(StringKey("score"), 200, 95)
	treap.InsertComplex(StringKey("level"), 150, 7)

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
	treap.InsertComplex(&key100, 1, SimplePayload(42))
	treap.InsertComplex(&key200, 2, SimplePayload(99))
	treap.InsertComplex(&key300, 3, SimplePayload(17))

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
		treap.InsertComplex(&key42, 1, SimplePayload(123))
		treap.Persist()

		// Get the root object ID for later loading
		rootNode := treap.root.(*PersistentPayloadTreapNode[IntKey, SimplePayload])
		rootObjId, _ = rootNode.ObjectId()

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
	treap.InsertComplex(IntKey(10), 1)
	treap.InsertComplex(IntKey(20), 2)
	treap.InsertComplex(IntKey(30), 3)

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
	treap.InsertComplex(&key5, 100)
	treap.InsertComplex(&key3, 200)
	treap.InsertComplex(&key7, 150)

	// Persist to disk
	treap.Persist()

	// Walk through persisted treap
	treap.Walk(func(node TreapNodeInterface[IntKey]) {
		fmt.Printf("%d ", *node.GetKey().(*IntKey))
	})
	fmt.Println()
	// Output: 3 5 7
}

// ExamplePersistentTreap_SearchComplex demonstrates using SearchComplex with a persistent treap
// to track which nodes are accessed during a search operation.
func ExamplePersistentTreap_SearchComplex() {
	tmpFile := filepath.Join(os.TempDir(), "example_search_complex.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	treap := NewPersistentTreap[IntKey](IntLess, (*IntKey)(new(int32)), s)

	// Insert some keys
	for _, val := range []int32{10, 20, 30, 40, 50} {
		key := IntKey(val)
		treap.InsertComplex(&key, Priority(val))
	}
	treap.Persist()

	// Track which nodes are accessed during search
	fmt.Println("Tracking accessed nodes:")
	accessedKeys := []IntKey{}
	searchKey := IntKey(40)
	node, err := treap.SearchComplex(&searchKey, func(n TreapNodeInterface[IntKey]) error {
		accessedKeys = append(accessedKeys, *n.GetKey().(*IntKey))
		return nil
	})
	if err == nil && node != nil {
		fmt.Printf("Found: %d\n", searchKey)
		fmt.Printf("Accessed nodes: %v\n", accessedKeys)
	}

	// Example with error: abort search after accessing too many nodes
	fmt.Println("\nAborting search:")
	accessCount := 0
	searchKey2 := IntKey(10)
	_, err = treap.SearchComplex(&searchKey2, func(n TreapNodeInterface[IntKey]) error {
		accessCount++
		if accessCount > 1 {
			return errors.New("access limit exceeded")
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Search aborted: %v\n", err)
	}

	// Output:
	// Tracking accessed nodes:
	// Found: 40
	// Accessed nodes: [50 40]
	//
	// Aborting search:
	// Search aborted: access limit exceeded
}

// ExampleTreap_UpdatePriority demonstrates changing node priorities.
func ExampleTreap_UpdatePriority() {
	treap := NewTreap[IntKey](IntLess)

	// Insert with initial priority
	treap.InsertComplex(IntKey(100), 50)

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

// FileInfo represents metadata about a file.
type FileInfo struct {
	Name string
	Type string
	Size int
}

// Marshal serializes FileInfo to bytes.
func (f FileInfo) Marshal() ([]byte, error) {
	// Calculate size: 4 bytes for name length + name + 4 bytes for type length + type + 8 bytes for size
	nameBytes := []byte(f.Name)
	typeBytes := []byte(f.Type)
	totalSize := 4 + len(nameBytes) + 4 + len(typeBytes) + 8

	data := make([]byte, totalSize)
	offset := 0

	// Write name length and name
	binary.LittleEndian.PutUint32(data[offset:], uint32(len(nameBytes)))
	offset += 4
	copy(data[offset:], nameBytes)
	offset += len(nameBytes)

	// Write type length and type
	binary.LittleEndian.PutUint32(data[offset:], uint32(len(typeBytes)))
	offset += 4
	copy(data[offset:], typeBytes)
	offset += len(typeBytes)

	// Write size
	binary.LittleEndian.PutUint64(data[offset:], uint64(f.Size))

	return data, nil
}

// Unmarshal deserializes FileInfo from bytes.
func (f FileInfo) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	offset := 0

	// Read name
	nameLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	name := string(data[offset : offset+int(nameLen)])
	offset += int(nameLen)

	// Read type
	typeLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	fileType := string(data[offset : offset+int(typeLen)])
	offset += int(typeLen)

	// Read size
	size := int(binary.LittleEndian.Uint64(data[offset:]))

	return FileInfo{
		Name: name,
		Type: fileType,
		Size: size,
	}, nil
}

// SizeInBytes returns the serialized size of FileInfo.
func (f FileInfo) SizeInBytes() int {
	return 4 + len(f.Name) + 4 + len(f.Type) + 8
}

// ExamplePersistentPayloadTreap_stringToStruct demonstrates using a persistent treap
// to store a map from strings to structs with custom serialization.
// For a simpler approach using JSON, see ExampleJsonPayload.
func ExamplePersistentPayloadTreap_stringToStruct() {
	tmpFile := filepath.Join(os.TempDir(), "file_metadata.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	// Create a persistent treap mapping file paths (StringKey) to FileInfo
	treap := NewPersistentPayloadTreap[StringKey, FileInfo](
		StringLess,
		(*StringKey)(new(string)),
		s,
	)

	// Add file metadata entries
	docPath := StringKey("/home/user/document.txt")
	imgPath := StringKey("/home/user/photo.jpg")
	vidPath := StringKey("/home/user/video.mp4")

	treap.InsertComplex(&docPath, 100, FileInfo{
		Name: "document.txt",
		Type: "text/plain",
		Size: 2048,
	})

	treap.InsertComplex(&imgPath, 200, FileInfo{
		Name: "photo.jpg",
		Type: "image/jpeg",
		Size: 524288,
	})

	treap.InsertComplex(&vidPath, 150, FileInfo{
		Name: "video.mp4",
		Type: "video/mp4",
		Size: 10485760,
	})

	// Persist to disk
	treap.Persist()

	// Look up metadata by path
	searchPath := StringKey("/home/user/photo.jpg")
	node := treap.Search(&searchPath)
	if node != nil && !node.IsNil() {
		info := node.GetPayload()
		fmt.Printf("File: %s, Type: %s, Size: %d bytes\n",
			info.Name, info.Type, info.Size)
	}

	// Walk through all entries in sorted order
	fmt.Println("All files:")
	treap.Walk(func(node TreapNodeInterface[StringKey]) {
		path := *node.GetKey().(*StringKey)
		payloadNode := node.(*PersistentPayloadTreapNode[StringKey, FileInfo])
		info := payloadNode.GetPayload()
		fmt.Printf("  %s: %s (%d bytes)\n", path, info.Type, info.Size)
	})

	// Output:
	// File: photo.jpg, Type: image/jpeg, Size: 524288 bytes
	// All files:
	//   /home/user/document.txt: text/plain (2048 bytes)
	//   /home/user/photo.jpg: image/jpeg (524288 bytes)
	//   /home/user/video.mp4: video/mp4 (10485760 bytes)
}

// Product represents a simple product struct for demonstration.
type Product struct {
	Name  string
	Price float64
	Stock int
}

// ExampleJsonPayload demonstrates using JsonPayload to avoid implementing Marshal/Unmarshal.
// This is the simplest way to use persistent treaps with custom structs.
func ExampleJsonPayload() {
	tmpFile := filepath.Join(os.TempDir(), "products.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	// Create a persistent treap using JsonPayload wrapper
	// No need to implement Marshal/Unmarshal for Product!
	treap := NewPersistentPayloadTreap[StringKey, JsonPayload[Product]](
		StringLess,
		(*StringKey)(new(string)),
		s,
	)

	// Add products
	laptop := StringKey("laptop")
	mouse := StringKey("mouse")
	keyboard := StringKey("keyboard")

	treap.InsertComplex(&laptop, 100, JsonPayload[Product]{
		Value: Product{Name: "Gaming Laptop", Price: 1299.99, Stock: 5},
	})

	treap.InsertComplex(&mouse, 200, JsonPayload[Product]{
		Value: Product{Name: "Wireless Mouse", Price: 29.99, Stock: 50},
	})

	treap.InsertComplex(&keyboard, 150, JsonPayload[Product]{
		Value: Product{Name: "Mechanical Keyboard", Price: 89.99, Stock: 20},
	})

	// Persist to disk
	treap.Persist()

	// Search for a product
	searchKey := StringKey("mouse")
	node := treap.Search(&searchKey)
	if node != nil && !node.IsNil() {
		product := node.GetPayload().Value
		fmt.Printf("Product: %s, Price: $%.2f, Stock: %d\n",
			product.Name, product.Price, product.Stock)
	}

	// Output: Product: Wireless Mouse, Price: $29.99, Stock: 50
}

// ExamplePersistentPayloadTreap_flushOlderThan demonstrates using FlushOlderThan
// to manage memory by flushing nodes from memory while keeping them on disk.
// This is useful for large treaps where you want to reduce memory usage by
// flushing nodes that can be reloaded from disk when needed.
func ExamplePersistentPayloadTreap_flushOlderThan() {
	tmpFile := filepath.Join(os.TempDir(), "cache_data.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	// Create a persistent treap for caching data
	treap := NewPersistentPayloadTreap[StringKey, JsonPayload[Product]](
		StringLess,
		(*StringKey)(new(string)),
		s,
	)

	// Add cache entries
	item1 := StringKey("item1")
	item2 := StringKey("item2")
	item3 := StringKey("item3")

	// Note using insertComplex to specify priorities to get the expected tree structure
	treap.InsertComplex(&item1, 100, JsonPayload[Product]{
		Value: Product{Name: "Widget A", Price: 9.99, Stock: 5},
	})

	treap.InsertComplex(&item2, 200, JsonPayload[Product]{
		Value: Product{Name: "Widget B", Price: 19.99, Stock: 10},
	})

	treap.InsertComplex(&item3, 300, JsonPayload[Product]{
		Value: Product{Name: "Widget C", Price: 29.99, Stock: 15},
	})

	// Check how many nodes are in memory initially
	beforeNodes := treap.GetInMemoryNodes()
	fmt.Printf("Nodes in memory before flush: %d\n", len(beforeNodes))

	// Flush all nodes from memory (using a future timestamp)
	// This persists them to disk and removes them from RAM
	futureTime := int64(9999999999) // Far future timestamp
	flushedCount, err := treap.FlushOlderThan(futureTime)
	if err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		return
	}

	fmt.Printf("Flushed %d node(s) from memory\n", flushedCount)

	// Check memory usage after flush
	afterNodes := treap.GetInMemoryNodes()
	fmt.Printf("Nodes in memory after flush: %d\n", len(afterNodes))

	// Items are still accessible (automatically loaded from disk)
	node := treap.Search(&item1)
	if node != nil && !node.IsNil() {
		product := node.GetPayload().Value
		fmt.Printf("Item still accessible: %s\n", product.Name)
	}

	// Output:
	// Nodes in memory before flush: 3
	// Flushed 3 node(s) from memory
	// Nodes in memory after flush: 1
	// Item still accessible: Widget A
}

// ExampleJsonPayload_loadAndUpdate demonstrates loading a persisted treap
// and updating values using JsonPayload.
func ExampleJsonPayload_loadAndUpdate() {
	tmpFile := filepath.Join(os.TempDir(), "inventory.bin")
	defer os.Remove(tmpFile)

	var rootObjId store.ObjectId

	// Create and persist initial inventory
	{
		s, _ := store.NewBasicStore(tmpFile)

		treap := NewPersistentPayloadTreap[StringKey, JsonPayload[Product]](
			StringLess,
			(*StringKey)(new(string)),
			s,
		)

		// Add initial product
		sku := StringKey("SKU-001")
		treap.InsertComplex(&sku, 100, JsonPayload[Product]{
			Value: Product{Name: "Widget", Price: 19.99, Stock: 100},
		})

		treap.Persist()

		// Save root for later loading
		rootNode := treap.root.(*PersistentPayloadTreapNode[StringKey, JsonPayload[Product]])
		rootObjId, _ = rootNode.ObjectId()

		s.Close()
	}

	// Load and update inventory
	{
		s, _ := store.LoadBaseStore(tmpFile)
		defer s.Close()

		treap := NewPersistentPayloadTreap[StringKey, JsonPayload[Product]](
			StringLess,
			(*StringKey)(new(string)),
			s,
		)
		treap.Load(rootObjId)

		// Update stock after sale
		sku := StringKey("SKU-001")
		node := treap.Search(&sku)
		if node != nil && !node.IsNil() {
			product := node.GetPayload().Value
			product.Stock -= 10 // Sold 10 units

			// Update the treap with new stock
			treap.UpdatePayload(&sku, JsonPayload[Product]{Value: product})

			fmt.Printf("Updated: %s, New Stock: %d\n", product.Name, product.Stock)
		}
	}
	// Output: Updated: Widget, New Stock: 90
}
