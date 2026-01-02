package yggdrasil

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
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

// ExampleTreap_Iter demonstrates iterating through treap nodes in sorted order
// using the Go 1.23+ iterator protocol.
func ExampleTreap_Iter() {
	t := treap.NewTreap[types.IntKey](types.IntLess)

	// Insert keys with priorities
	t.InsertComplex(types.IntKey(30), 1)
	t.InsertComplex(types.IntKey(10), 2)
	t.InsertComplex(types.IntKey(20), 3)
	t.InsertComplex(types.IntKey(40), 4)

	// Iterate through in sorted order using range
	for node := range t.Iter() {
		fmt.Printf("%d ", node.GetKey().Value())
	}
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

	// Iterate through all entries in sorted order
	for node := range pt.Iter() {
		key := node.GetKey().(types.StringKey)
		if payload, found := getPayload(node); found {
			fmt.Printf("%s: %d\n", key, payload)
		}
	}
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

	// Iterate to verify deletion
	for node := range treap.Iter() {
		fmt.Printf("%d ", node.GetKey().(types.IntKey))
	}
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

	// Iterate through persisted treap
	ctx := context.Background()
	for node, err := range treap.Iter(ctx) {
		if err != nil {
			break
		}
		fmt.Printf("%d ", *node.GetKey().(*types.IntKey))
	}
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

// ExamplePersistentTreap_Iter demonstrates iterating over a persistent treap
// with context support and error handling.
func ExamplePersistentTreap_Iter() {
	tmpFile := filepath.Join(os.TempDir(), "example_iter.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	pt := treap.NewPersistentTreap[types.IntKey](types.IntLess, (*types.IntKey)(new(int32)), s)

	// Insert and persist some keys
	key10, key20, key30 := types.IntKey(10), types.IntKey(20), types.IntKey(30)
	pt.InsertComplex(&key10, 1)
	pt.InsertComplex(&key20, 2)
	pt.InsertComplex(&key30, 3)
	pt.Persist()

	// Iterate using context (can be canceled)
	ctx := context.Background()
	for node, err := range pt.Iter(ctx) {
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			break
		}
		fmt.Printf("%d ", *node.GetKey().(*types.IntKey))
	}
	fmt.Println()
	// Output: 10 20 30
}

// ExampleMD5Key demonstrates using MD5 hashes as treap keys.
// MD5Key is a 16-byte array that can be used as a key in treaps, with the
// hash value itself serving as the priority (since MD5 hashes are uniformly
// distributed). This is useful for content-addressable storage.
func ExampleMD5Key() {
	t := treap.NewTreap[treap.MD5Key](treap.MD5Less)

	// Create MD5 keys from hex strings (32 characters = 16 bytes)
	key1, _ := treap.MD5KeyFromString("a1b2c3d4e5f6708192a3b4c5d6e7f809")
	key2, _ := treap.MD5KeyFromString("11223344556677889900aabbccddeeff")
	key3, _ := treap.MD5KeyFromString("00000000000000000000000000000001")

	// Insert keys - MD5Key implements PriorityProvider so no priority needed
	t.Insert(key1)
	t.Insert(key2)
	t.Insert(key3)

	// Search for a specific hash
	node := t.Search(key2)
	if node != nil && !node.IsNil() {
		key := node.GetKey().Value()
		fmt.Printf("Found key: %x\n", key[:8])
	}

	// Iterate through all keys in sorted order
	for node := range t.Iter() {
		key := node.GetKey().Value()
		// Print first 8 bytes for brevity
		fmt.Printf("Key: %x...\n", key[:8])
	}

	// Output:
	// Found key: 1122334455667788
	// Key: 0000000000000000...
	// Key: 1122334455667788...
	// Key: a1b2c3d4e5f67081...
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
		session, colls, _ := vault.OpenVault(
			tmpFile,
			vault.PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
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
		session, colls, _ := vault.OpenVault(
			tmpFile,
			vault.PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
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
		session, colls, _ := vault.OpenVault(
			tmpFile,
			vault.PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
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

	session, colls, _ := vault.OpenVault(
		tmpFile,
		vault.PayloadCollectionSpec[types.IntKey, types.JsonPayload[UserProfile]]{
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
	tmpFile := filepath.Join(os.TempDir(), "example_percentile_"+fmt.Sprintf("%d", time.Now().UnixNano())+".db")
	defer os.Remove(tmpFile)

	session, colls, err := vault.OpenVault(
		tmpFile,
		vault.PayloadCollectionSpec[types.IntKey, types.JsonPayload[UserProfile]]{
			Name:            "users",
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	if err != nil {
		fmt.Printf("Error opening vault: %v\n", err)
		return
	}
	if len(colls) == 0 {
		fmt.Println("No collections returned")
		return
	}
	users := colls[0].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])

	// Set memory budget: keep max 100 nodes, flush oldest 25% when exceeded
	// This means when we hit 100 nodes, we'll flush 25 of the oldest ones
	session.Vault.SetMemoryBudgetWithPercentile(100, 25)

	// Insert 150 users
	for i := range 150 {
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

// ExampleVault_updateExistingKey demonstrates what happens when you insert
// the same key multiple times. This shows whether payloads are overwritten,
// ignored, or if some other behavior occurs.
func ExampleVault_updateExistingKey() {
	tmpFile := filepath.Join(os.TempDir(), "example_update.db")
	defer os.Remove(tmpFile)

	session, colls, _ := vault.OpenVault(
		tmpFile,
		vault.PayloadCollectionSpec[types.StringKey, types.JsonPayload[UserProfile]]{
			Name:            "users",
			LessFunc:        types.StringLess,
			KeyTemplate:     (*types.StringKey)(new(string)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	users := colls[0].(*treap.PersistentPayloadTreap[types.StringKey, types.JsonPayload[UserProfile]])

	// Insert initial user alice
	aliceKey := types.StringKey("alice")
	users.Insert(&aliceKey, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "alice", Email: "alice@example.com", Credits: 100},
	})

	// Insert another user bob
	bobKey := types.StringKey("bob")
	users.Insert(&bobKey, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "bob", Email: "bob@example.com", Credits: 50},
	})

	// Check initial count
	count, _ := users.Count()
	fmt.Printf("Initial count: %d\n", count)

	// Read alice's current data
	node := users.Search(&aliceKey)
	if node != nil && !node.IsNil() {
		user := node.GetPayload().Value
		fmt.Printf("Alice before update: credits=%d, email=%s\n", user.Credits, user.Email)
	}

	// Insert alice again with DIFFERENT payload
	users.Insert(&aliceKey, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "alice", Email: "alice.new@example.com", Credits: 500},
	})

	// Check count after re-inserting alice
	count, _ = users.Count()
	fmt.Printf("Count after re-insert: %d\n", count)

	// Read alice's data after update
	node = users.Search(&aliceKey)
	if node != nil && !node.IsNil() {
		user := node.GetPayload().Value
		fmt.Printf("Alice after update: credits=%d, email=%s\n", user.Credits, user.Email)
	}

	// Verify bob is unchanged
	node = users.Search(&bobKey)
	if node != nil && !node.IsNil() {
		user := node.GetPayload().Value
		fmt.Printf("Bob (unchanged): credits=%d\n", user.Credits)
	}

	session.Close()

	// Output:
	// Initial count: 2
	// Alice before update: credits=100, email=alice@example.com
	// Count after re-insert: 2
	// Alice after update: credits=500, email=alice.new@example.com
	// Bob (unchanged): credits=50
}

// ExampleVault_dynamicCollections demonstrates adding collections on-demand using identities.
// Each identity (IntKey) maps to a collection of MD5Key -> []string payloads, created lazily.
func ExampleVault_dynamicCollections() {
	tmpFile := filepath.Join(os.TempDir(), "example_dynamic_vault.db")
	defer os.Remove(tmpFile)

	// Open with no predefined collections; we'll create them dynamically.
	session, _, err := vault.OpenVaultWithIdentity[types.IntKey](tmpFile)
	if err != nil {
		fmt.Printf("Error opening vault: %v\n", err)
		return
	}
	// Set memory budget: keep max 100 nodes, flush oldest 25% when exceeded
	session.Vault.SetMemoryBudgetWithPercentile(100, 25)

	getCollection := func(id types.IntKey) *treap.PersistentPayloadTreap[treap.MD5Key, types.JsonPayload[[]string]] {
		coll, err := vault.GetOrCreateCollectionWithIdentity(
			session.Vault,
			id,
			treap.MD5Less,
			(*treap.MD5Key)(new(treap.MD5Key)),
			types.JsonPayload[[]string]{},
		)
		if err != nil {
			panic(err)
		}
		return coll
	}

	// Write to collection 1 (created on demand)
	coll1 := getCollection(types.IntKey(1))
	keyA, _ := treap.MD5KeyFromString("a1b2c3d4e5f6708192a3b4c5d6e7f809")
	coll1.Insert(&keyA, types.JsonPayload[[]string]{Value: []string{"alpha", "beta"}})

	// Write to collection 2 (also created on demand)
	coll2 := getCollection(types.IntKey(2))
	keyB, _ := treap.MD5KeyFromString("11223344556677889900aabbccddeeff")
	coll2.Insert(&keyB, types.JsonPayload[[]string]{Value: []string{"gamma"}})

	// Read back from collection 1
	if node := coll1.Search(&keyA); node != nil && !node.IsNil() {
		values := node.GetPayload().Value
		fmt.Printf("collection 1: %v\n", values)
	}

	// Persist and close
	if node := coll2.Search(&keyB); node != nil && !node.IsNil() {
		values := node.GetPayload().Value
		fmt.Printf("collection 2: %v\n", values)
	}

	session.Close()

	// Output:
	// collection 1: [alpha beta]
	// collection 2: [gamma]
}

// ExampleVault_compareCollections demonstrates:
// 1. Creating a vault with two collections
// 2. Setting up FlushOldestPercentile for memory management
// 3. Using Compare to find differences between collections
func ExampleVault_compareCollections() {
	tmpFile := filepath.Join(os.TempDir(), "example_compare_vault.db")
	defer os.Remove(tmpFile)

	// 1. Create a vault with two collections using identity-based lookup (no positional indexing)
	primaryID := types.StringKey("users_primary")
	backupID := types.StringKey("users_backup")
	session, colls, err := vault.OpenVaultWithIdentity(
		tmpFile,
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        primaryID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        backupID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	primaryUsers := colls[primaryID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])
	backupUsers := colls[backupID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])

	// 2. Set up memory management with FlushOldestPercentile
	// Keep max 100 nodes in memory, flush oldest 25% when exceeded
	session.Vault.SetMemoryBudgetWithPercentile(100, 25)

	// Add users to primary collection
	key1, key2, key3, key4 := types.IntKey(1), types.IntKey(2), types.IntKey(3), types.IntKey(4)
	primaryUsers.Insert(&key1, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "alice", Email: "alice@example.com", Credits: 100},
	})
	primaryUsers.Insert(&key2, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "bob", Email: "bob@example.com", Credits: 50},
	})
	primaryUsers.Insert(&key3, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "charlie", Email: "charlie@example.com", Credits: 75},
	})

	// Add users to backup collection (some overlap, some different)
	backupUsers.Insert(&key2, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "bob", Email: "bob@example.com", Credits: 50}, // identical
	})
	backupUsers.Insert(&key3, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "charlie", Email: "charlie.new@example.com", Credits: 80}, // modified email and credits
	})
	backupUsers.Insert(&key4, types.JsonPayload[UserProfile]{
		Value: UserProfile{Username: "david", Email: "david@example.com", Credits: 120},
	})

	if err := primaryUsers.Persist(); err != nil {
		fmt.Printf("Persist primary: %v\n", err)
		return
	}
	if err := backupUsers.Persist(); err != nil {
		fmt.Printf("Persist backup: %v\n", err)
		return
	}

	// 3. Use Compare to find differences between collections
	fmt.Println("=== Comparing Primary vs Backup ===")

	err = primaryUsers.Compare(backupUsers,
		// Only in primary
		func(node treap.TreapNodeInterface[types.IntKey]) error {
			if payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.IntKey, types.JsonPayload[UserProfile]]); ok {
				user := payloadNode.GetPayload().Value
				fmt.Printf("Only in primary: %s (ID: %d)\n", user.Username, *node.GetKey().(*types.IntKey))
			}
			return nil
		},
		// In both collections
		func(nodeA, nodeB treap.TreapNodeInterface[types.IntKey]) error {
			payloadA, okA := nodeA.(treap.PersistentPayloadNodeInterface[types.IntKey, types.JsonPayload[UserProfile]])
			payloadB, okB := nodeB.(treap.PersistentPayloadNodeInterface[types.IntKey, types.JsonPayload[UserProfile]])
			if okA && okB {
				userA := payloadA.GetPayload().Value
				userB := payloadB.GetPayload().Value
				id := *nodeA.GetKey().(*types.IntKey)

				if userA.Email == userB.Email && userA.Credits == userB.Credits {
					fmt.Printf("Identical: %s (ID: %d)\n", userA.Username, id)
				} else {
					fmt.Printf("Modified: %s (ID: %d) - Primary: %d credits, Backup: %d credits\n",
						userA.Username, id, userA.Credits, userB.Credits)
				}
			}
			return nil
		},
		// Only in backup
		func(node treap.TreapNodeInterface[types.IntKey]) error {
			if payloadNode, ok := node.(treap.PersistentPayloadNodeInterface[types.IntKey, types.JsonPayload[UserProfile]]); ok {
				user := payloadNode.GetPayload().Value
				fmt.Printf("Only in backup: %s (ID: %d)\n", user.Username, *node.GetKey().(*types.IntKey))
			}
			return nil
		},
	)
	if err != nil {
		fmt.Printf("Error comparing: %v\n", err)
	}

	// Show memory stats
	stats := session.Vault.GetMemoryStats()
	fmt.Printf("\nMemory usage: %d nodes total\n", stats.TotalInMemoryNodes)

	// 4. Close and re-open to demonstrate persistence
	session.Close()
	reopenSession, reopenedColls, err := vault.OpenVaultWithIdentity(
		tmpFile,
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        primaryID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
		vault.PayloadIdentitySpec[types.StringKey, types.IntKey, types.JsonPayload[UserProfile]]{
			Identity:        backupID,
			LessFunc:        types.IntLess,
			KeyTemplate:     (*types.IntKey)(new(int32)),
			PayloadTemplate: types.JsonPayload[UserProfile]{},
		},
	)
	if err != nil {
		fmt.Printf("Error reopening vault: %v\n", err)
		return
	}
	defer reopenSession.Close()

	reopenedPrimary := reopenedColls[primaryID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])
	reopenedBackup := reopenedColls[backupID].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[UserProfile]])

	fmt.Printf("\n=== Reloading vault to verify persistence ===\n")

	if node := reopenedPrimary.Search(&key1); node != nil && !node.IsNil() {
		user := node.GetPayload().Value
		fmt.Printf("Reloaded primary alice: %d credits\n", user.Credits)
	}

	reloadedNode := reopenedBackup.Search(&key3)
	if reloadedNode != nil && !reloadedNode.IsNil() {
		user := reloadedNode.GetPayload().Value
		fmt.Printf("Reloaded backup charlie: %s (%s) - %d credits\n", user.Username, user.Email, user.Credits)
	}

	// Output:
	// === Comparing Primary vs Backup ===
	// Only in primary: alice (ID: 1)
	// Identical: bob (ID: 2)
	// Modified: charlie (ID: 3) - Primary: 75 credits, Backup: 80 credits
	// Only in backup: david (ID: 4)
	//
	// Memory usage: 8 nodes total
	//
	// === Reloading vault to verify persistence ===
	// Reloaded primary alice: 100 credits
	// Reloaded backup charlie: charlie (charlie.new@example.com) - 80 credits
}
