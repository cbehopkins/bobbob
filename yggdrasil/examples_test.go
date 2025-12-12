package yggdrasil

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/collections"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// Type aliases to work around Go's limitation with generic types in function literals
type (
	IntKeyNode    = treap.TreapNodeInterface[types.IntKey]
	StringKeyNode = treap.TreapNodeInterface[types.StringKey]
)

// Example demonstrates creating a basic in-memory treap.
func Example() {
	// Create a t with integer keys
	t := treap.NewTreap[types.IntKey](types.IntLess)

	// Insert some key-priority pairs
	t.Insert(types.IntKey(10))
	t.Insert(types.IntKey(5))
	t.Insert(types.IntKey(15))

	// Search for a key
	node := t.Search(types.IntKey(10))
	if node != nil && !node.IsNil() {
		fmt.Printf("Found key: %d\n", node.GetKey().Value())
	}
	// Output: Found key: 10
}

// ExampleTreap_Walk demonstrates iterating through treap nodes in order.
func ExampleTreap_Walk() {
	t := treap.NewTreap[types.IntKey](types.IntLess)

	// Insert keys with priorities
	t.InsertComplex(types.IntKey(30), 1)
	t.InsertComplex(types.IntKey(10), 2)
	t.InsertComplex(types.IntKey(20), 3)
	t.InsertComplex(types.IntKey(40), 4)

	// Walk through in sorted order
	t.Walk(func(node IntKeyNode) {
		fmt.Printf("%d ", node.GetKey().Value())
	})
	fmt.Println()
	// Output: 10 20 30 40
}

// ExampleTreap_SearchComplex demonstrates using SearchComplex with a callback
// that can return an error to abort the search. This is useful for implementing
// access control, rate limiting, or conditional searches.
func ExampleTreap_SearchComplex() {
	t := treap.NewTreap[types.IntKey](types.IntLess)

	// Insert some keys
	t.InsertComplex(types.IntKey(10), 1)
	t.InsertComplex(types.IntKey(20), 2)
	t.InsertComplex(types.IntKey(30), 3)
	t.InsertComplex(types.IntKey(40), 4)
	t.InsertComplex(types.IntKey(50), 5)

	// Example 1: Track which nodes are accessed during search
	var accessedNodes []int
	callback := func(node IntKeyNode) error {
		key := int(node.GetKey().(types.IntKey))
		accessedNodes = append(accessedNodes, key)
		return nil
	}

	node, err := t.SearchComplex(types.IntKey(30), callback)
	if err == nil && node != nil {
		fmt.Printf("Found: %d\n", node.GetKey().(types.IntKey))
		fmt.Printf("Accessed nodes: %v\n", accessedNodes)
	}

	// Example 2: Abort search based on a condition
	accessCount := 0
	limitCallback := func(node IntKeyNode) error {
		accessCount++
		if accessCount >= 3 {
			return errors.New("access limit exceeded")
		}
		return nil
	}

	node, err = t.SearchComplex(types.IntKey(10), limitCallback)
	if err != nil {
		// While the key exists, we aborted to demonstrate error handling
		fmt.Printf("Search aborted: %v\n", err)
	}

	// Output:
	// Found: 30
	// Accessed nodes: [50 40 30]
	// Search aborted: access limit exceeded
}

// ExamplePayloadTreap demonstrates using a treap with data payloads.
// This shows how to store key-value pairs in an in-memory treap.
func ExamplePayloadTreap() {
	// Create a payload treap mapping strings to integers
	pt := treap.NewPayloadTreap[types.StringKey, int](types.StringLess)

	// Insert key-value pairs using Insert (random priorities)
	pt.Insert(types.StringKey("age"), 25)
	pt.Insert(types.StringKey("score"), 95)
	pt.Insert(types.StringKey("level"), 7)

	// Helper function to extract payload from search results
	getPayload := func(node treap.TreapNodeInterface[types.StringKey]) (int, bool) {
		if node == nil || node.IsNil() {
			return 0, false
		}
		// Type assertion to access payload
		if payloadNode, ok := node.(*treap.PayloadTreapNode[types.StringKey, int]); ok {
			return payloadNode.GetPayload(), true
		}
		return 0, false
	}

	// Search and retrieve payload
	if payload, found := getPayload(pt.Search(types.StringKey("score"))); found {
		fmt.Printf("score = %d\n", payload)
	}

	// Walk through all entries in sorted order
	pt.Walk(func(node StringKeyNode) {
		key := node.GetKey().(types.StringKey)
		if payload, found := getPayload(node); found {
			fmt.Printf("%s: %d\n", key, payload)
		}
	})
	// Output: score = 95
	// age: 25
	// level: 7
	// score: 95
}

// SimplePayload is a simple integer payload for demonstration.
type SimplePayload int

func (p SimplePayload) Marshal() ([]byte, error) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(p))
	return data, nil
}

func (p SimplePayload) Unmarshal(data []byte) (treap.UntypedPersistentPayload, error) {
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
	treap := treap.NewPersistentPayloadTreap[types.IntKey, SimplePayload](types.IntLess, (*types.IntKey)(new(int32)), s)

	// Create keys for persistent interface
	key100, key200, key300 := types.IntKey(100), types.IntKey(200), types.IntKey(300)

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

// ExampleTypeMap demonstrates the type mapping system.
func ExampleTypeMap() {
	// Create an empty types.TypeMap without built-in types
	tm := &types.TypeMap{}

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
	treap := treap.NewTreap[types.IntKey](types.IntLess)

	// Insert keys
	treap.InsertComplex(types.IntKey(10), 1)
	treap.InsertComplex(types.IntKey(20), 2)
	treap.InsertComplex(types.IntKey(30), 3)

	// Delete a node
	treap.Delete(types.IntKey(20))

	// Walk to verify deletion
	treap.Walk(func(node IntKeyNode) {
		fmt.Printf("%d ", node.GetKey().(types.IntKey))
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

	treap := treap.NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), s)

	// Create and insert keys with priorities
	key5, key3, key7 := types.IntKey(5), types.IntKey(3), types.IntKey(7)
	treap.InsertComplex(&key5, 100)
	treap.InsertComplex(&key3, 200)
	treap.InsertComplex(&key7, 150)

	// Persist to disk
	treap.Persist()

	// Walk through persisted treap
	treap.Walk(func(node IntKeyNode) {
		fmt.Printf("%d ", *node.GetKey().(*types.IntKey))
	})
	fmt.Println()
	// Output: 3 5 7
}

// ExampleTreap_UpdatePriority demonstrates changing node priorities.
func ExampleTreap_UpdatePriority() {
	treap := treap.NewTreap[types.IntKey](types.IntLess)

	// Insert with initial priority
	treap.InsertComplex(types.IntKey(100), 50)

	// Update priority (causes rebalancing)
	treap.UpdatePriority(types.IntKey(100), 250)

	// Node is still accessible with new priority
	node := treap.Search(types.IntKey(100))
	if node != nil && !node.IsNil() {
		fmt.Printf("Key %d has new priority %d\n",
			node.GetKey().(types.IntKey),
			node.GetPriority())
	}
	// Output: Key 100 has new priority 250
}

// UserProfile represents a simple user profile for demonstration.
type UserProfile struct {
	Username string
	Email    string
	Credits  int
}

// ExampleVault demonstrates using a Vault to persist collections across sessions.
// This shows how to create a collection, save data, close it, and reload it later.
func ExampleVault() {
	tmpFile := filepath.Join(os.TempDir(), "example_vault.db")
	defer os.Remove(tmpFile)

	// ===== Session 1: Create vault and add data =====
	{
		session, colls, _ := collections.OpenVault(
			tmpFile,
			collections.PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
				Name:            "users",
				LessFunc:        types.StringLess,
				KeyTemplate:     (*types.StringKey)(new(string)),
				PayloadTemplate: types.JsonPayload[UserProfile]{},
			},
		)
		users := colls[0].(*treap.PersistentPayloadTreap[types.StringKey, types.JsonPayload[UserProfile]])

		// Add some users
		aliceKey := types.StringKey("alice")
		users.Insert(&aliceKey, types.JsonPayload[UserProfile]{
			Value: UserProfile{Username: "alice", Email: "alice@example.com", Credits: 100},
		})

		bobKey := types.StringKey("bob")
		users.Insert(&bobKey, types.JsonPayload[UserProfile]{
			Value: UserProfile{Username: "bob", Email: "bob@example.com", Credits: 50},
		})

		// Close session (persists everything to disk)
		session.Close()
	}

	// ===== Session 2: Reload vault and access data =====
	{
		session, colls, _ := collections.OpenVault(
			tmpFile,
			collections.PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
				Name:            "users",
				LessFunc:        types.StringLess,
				KeyTemplate:     (*types.StringKey)(new(string)),
				PayloadTemplate: types.JsonPayload[UserProfile]{},
			},
		)
		users := colls[0].(*treap.PersistentPayloadTreap[types.StringKey, types.JsonPayload[UserProfile]])

		// Search for alice
		aliceKey := types.StringKey("alice")
		node := users.Search(&aliceKey)
		if node != nil && !node.IsNil() {
			user := node.GetPayload().Value
			fmt.Printf("Found: %s (%s) - %d credits\n", user.Username, user.Email, user.Credits)
		}

		// Add another user
		charlieKey := types.StringKey("charlie")
		users.Insert(&charlieKey, types.JsonPayload[UserProfile]{
			Value: UserProfile{Username: "charlie", Email: "charlie@example.com", Credits: 75},
		})

		session.Close()
	}

	// ===== Session 3: Verify all data persisted =====
	{
		session, colls, _ := collections.OpenVault(
			tmpFile,
			collections.PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
				Name:            "users",
				LessFunc:        types.StringLess,
				KeyTemplate:     (*types.StringKey)(new(string)),
				PayloadTemplate: types.JsonPayload[UserProfile]{},
			},
		)
		users := colls[0].(*treap.PersistentPayloadTreap[types.StringKey, types.JsonPayload[UserProfile]])

		// Count total users
		count, _ := users.Count()
		fmt.Printf("Total users: %d\n", count)

		session.Close()
	}

	// Output:
	// Found: alice (alice@example.com) - 100 credits
	// Total users: 3
}

// ExampleVault_memoryManagement demonstrates using memory monitoring to automatically
// control memory usage by flushing old nodes.
func ExampleVault_memoryManagement() {
	tmpFile := filepath.Join(os.TempDir(), "example_memory.db")
	defer os.Remove(tmpFile)

	session, colls, _ := collections.OpenVault(
		tmpFile,
		collections.PayloadCollectionSpec[types.IntKey, types.JsonPayload[UserProfile]]{
			Name:            "users",
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	users := colls[0].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])

	// Set a memory budget: keep max 100 nodes in memory, flush nodes older than 10 seconds
	session.Vault.SetMemoryBudget(100, 10)

	// Insert many users
	for i := 0; i < 200; i++ {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserProfile]{
			Value: UserProfile{
				Username: fmt.Sprintf("user%d", i),
				Email:    fmt.Sprintf("user%d@example.com", i),
				Credits:  i * 10,
			},
		})
	}

	// Check memory stats
	stats := session.Vault.GetMemoryStats()
	fmt.Printf("Total nodes in memory: %d\n", stats.TotalInMemoryNodes)
	fmt.Printf("Nodes in 'users' collection: %d\n", stats.CollectionNodes["users"])

	// Search for a user (may need to load from disk if flushed)
	searchKey := types.IntKey(42)
	node := users.Search(&searchKey)
	if node != nil && !node.IsNil() {
		user := node.GetPayload().Value
		fmt.Printf("Found user: %s with %d credits\n", user.Username, user.Credits)
	}

	session.Close()

	// Output:
	// Total nodes in memory: 200
	// Nodes in 'users' collection: 200
	// Found user: user42 with 420 credits
}

// ExampleVault_memoryManagementPercentile demonstrates using SetMemoryBudgetWithPercentile
// to automatically flush the oldest percentage of nodes when memory limits are exceeded.
// This is useful when you want aggressive memory control without relying on time-based flushing.
func ExampleVault_memoryManagementPercentile() {
	tmpFile := filepath.Join(os.TempDir(), "example_percentile.db")
	defer os.Remove(tmpFile)

	session, colls, _ := collections.OpenVault(
		tmpFile,
		collections.PayloadCollectionSpec[types.IntKey, types.JsonPayload[UserProfile]]{
			Name:            "users",
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	users := colls[0].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])

	// Set memory budget: keep max 100 nodes, flush oldest 25% when exceeded
	// This means when we hit 100 nodes, we'll flush 25 of the oldest ones
	session.Vault.SetMemoryBudgetWithPercentile(100, 25)

	// Insert 150 users
	for i := 0; i < 150; i++ {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserProfile]{
			Value: UserProfile{
				Username: fmt.Sprintf("user%d", i),
				Email:    fmt.Sprintf("user%d@example.com", i),
				Credits:  i * 10,
			},
		})
	}

	// Check memory stats - automatic flushing keeps memory under control
	stats := session.Vault.GetMemoryStats()
	fmt.Printf("Nodes in memory after auto-flush: %d\n", stats.TotalInMemoryNodes)

	// All 150 users are still accessible (flushed ones load from disk)
	searchKey := types.IntKey(10)
	node := users.Search(&searchKey)
	if node != nil && !node.IsNil() {
		user := node.GetPayload().Value
		fmt.Printf("Found early user: %s with %d credits\n", user.Username, user.Credits)
	}

	session.Close()

	// Output:
	// Nodes in memory after auto-flush: 150
	// Found early user: user10 with 100 credits
}
