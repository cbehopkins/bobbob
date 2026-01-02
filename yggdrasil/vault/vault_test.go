package vault

import (
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/collections"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// Test data types for the example
type UserData struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Age      int    `json:"age"`
}

type ProductData struct {
	Name  string  `json:"name"`
	Price float64 `json:"price"`
	Stock int     `json:"stock"`
}

// TestVaultMultipleCollections demonstrates storing multiple collections
// (users and products) in a single vault/store file.
func TestVaultMultipleCollections(t *testing.T) {
	// Setup: Create a new store
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "multi_collection.db")
	stre, err := store.NewBasicStore(storePath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Create a vault
	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	// Register all types we'll use (in a consistent order!)
	// This order must be the same across sessions for consistency
	v.RegisterType((*types.StringKey)(new(string)))
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})
	v.RegisterType(types.JsonPayload[ProductData]{})

	// Create/get the "users" collection
	users, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[UserData]](
		v,
		"users",
		types.StringLess,
		(*types.StringKey)(new(string)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Create/get the "products" collection
	products, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[ProductData]](
		v,
		"products",
		types.StringLess,
		(*types.StringKey)(new(string)),
	)
	if err != nil {
		t.Fatalf("Failed to create products collection: %v", err)
	}

	// Insert users
	user1Key := types.StringKey("user:alice")
	user1 := types.JsonPayload[UserData]{
		Value: UserData{
			Username: "alice",
			Email:    "alice@example.com",
			Age:      30,
		},
	}
	users.Insert(&user1Key, user1)

	user2Key := types.StringKey("user:bob")
	user2 := types.JsonPayload[UserData]{
		Value: UserData{
			Username: "bob",
			Email:    "bob@example.com",
			Age:      25,
		},
	}
	users.Insert(&user2Key, user2)

	// Insert products
	prod1Key := types.StringKey("product:widget")
	prod1 := types.JsonPayload[ProductData]{
		Value: ProductData{
			Name:  "Widget",
			Price: 19.99,
			Stock: 100,
		},
	}
	products.Insert(&prod1Key, prod1)

	prod2Key := types.StringKey("product:gadget")
	prod2 := types.JsonPayload[ProductData]{
		Value: ProductData{
			Name:  "Gadget",
			Price: 29.99,
			Stock: 50,
		},
	}
	products.Insert(&prod2Key, prod2)

	// Search in users collection
	aliceNode := users.Search(&user1Key)
	if aliceNode == nil {
		t.Fatal("Expected to find alice in users collection")
	}
	if aliceNode.GetPayload().Value.Username != "alice" {
		t.Errorf("Expected username 'alice', got '%s'", aliceNode.GetPayload().Value.Username)
	}
	if aliceNode.GetPayload().Value.Age != 30 {
		t.Errorf("Expected age 30, got %d", aliceNode.GetPayload().Value.Age)
	}

	// Search in products collection
	widgetNode := products.Search(&prod1Key)
	if widgetNode == nil {
		t.Fatal("Expected to find widget in products collection")
	}
	if widgetNode.GetPayload().Value.Name != "Widget" {
		t.Errorf("Expected product name 'Widget', got '%s'", widgetNode.GetPayload().Value.Name)
	}
	if widgetNode.GetPayload().Value.Price != 19.99 {
		t.Errorf("Expected price 19.99, got %f", widgetNode.GetPayload().Value.Price)
	}

	// Verify collections are distinct
	productInUsers := users.Search(&prod1Key)
	if productInUsers != nil {
		t.Error("Product key should not be found in users collection")
	}

	userInProducts := products.Search(&user1Key)
	if userInProducts != nil {
		t.Error("User key should not be found in products collection")
	}

	// List collections
	collections := v.ListCollections()
	if len(collections) != 2 {
		t.Errorf("Expected 2 collections, got %d", len(collections))
	}

	// Close the vault
	err = v.Close()
	if err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}

// TestVaultKeyOnlyCollection demonstrates creating a collection
// that stores only keys (no payloads) - useful for sets or indexes.
func TestVaultKeyOnlyCollection(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "key_only.db")
	stre, err := store.NewBasicStore(storePath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	// Register types
	v.RegisterType((*types.StringKey)(new(string)))

	// Create a key-only collection (e.g., a set of active user IDs)
	activeUsers, err := GetOrCreateKeyOnlyCollection[types.StringKey](
		v,
		"active_users",
		types.StringLess,
		(*types.StringKey)(new(string)),
	)
	if err != nil {
		t.Fatalf("Failed to create active_users collection: %v", err)
	}

	// Insert some keys
	userId1 := types.StringKey("user:123")
	activeUsers.Insert(&userId1)

	userId2 := types.StringKey("user:456")
	activeUsers.Insert(&userId2)

	userId3 := types.StringKey("user:789")
	activeUsers.Insert(&userId3)

	// Search for keys
	node1 := activeUsers.Search(&userId1)
	if node1 == nil {
		t.Fatal("Expected to find user:123 in active_users")
	}

	node2 := activeUsers.Search(&userId2)
	if node2 == nil {
		t.Fatal("Expected to find user:456 in active_users")
	}

	// Search for non-existent key
	nonExistentKey := types.StringKey("user:999")
	node := activeUsers.Search(&nonExistentKey)
	if node != nil {
		t.Error("Should not find user:999 in active_users")
	}

	err = v.Close()
	if err != nil {
		t.Fatalf("Failed to close vault: %v", err)
	}
}

// TestCollectionRegistry verifies the basic functionality of the CollectionRegistry:
// registering collections, updating root object IDs, listing collections, and
// serialization/deserialization.
func TestCollectionRegistry(t *testing.T) {
	registry := collections.NewCollectionRegistry()

	// Register a collection
	collId, err := registry.RegisterCollection("users", 100, 1, 2)
	if err != nil {
		t.Fatalf("Failed to register collection: %v", err)
	}
	if collId != 1 {
		t.Errorf("Expected collection ID 1, got %d", collId)
	}

	// Verify registration
	info, exists := registry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection")
	}
	if info.Name != "users" {
		t.Errorf("Expected collection name 'users', got '%s'", info.Name)
	}
	if info.RootObjectId != 100 {
		t.Errorf("Expected root object ID 100, got %d", info.RootObjectId)
	}

	// Update root object ID
	err = registry.UpdateRootObjectId("users", 200)
	if err != nil {
		t.Fatalf("Failed to update root object ID: %v", err)
	}

	// Verify update
	info, exists = registry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection after update")
	}
	if info.RootObjectId != 200 {
		t.Errorf("Expected updated root object ID 200, got %d", info.RootObjectId)
	}

	// List collections
	collectionNames := registry.ListCollections()
	if len(collectionNames) != 1 {
		t.Errorf("Expected 1 collection, got %d", len(collectionNames))
	}
	if collectionNames[0] != "users" {
		t.Errorf("Expected collection 'users', got '%s'", collectionNames[0])
	}

	// Test serialization
	data, err := registry.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal registry: %v", err)
	}

	// Deserialize
	newRegistry := collections.NewCollectionRegistry()
	err = newRegistry.Unmarshal(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal registry: %v", err)
	}

	// Verify deserialized registry
	info, exists = newRegistry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection in deserialized registry")
	}
	if info.RootObjectId != 200 {
		t.Errorf("Expected root object ID 200 in deserialized registry, got %d", info.RootObjectId)
	}
}

// TestVaultPersistenceAcrossSessions tests that a vault can be saved,
// closed, and then reopened with all collections intact.
func TestVaultPersistenceAcrossSessions(t *testing.T) {
	// Setup: Create a new store
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "persistent.db")

	// ===== SESSION 1: Create vault and add data =====
	{
		stre, err := store.NewBasicStore(storePath)
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}

		v, err := LoadVault(stre)
		if err != nil {
			t.Fatalf("Failed to load vault: %v", err)
		}

		// Register types (in consistent order!)
		v.RegisterType((*types.StringKey)(new(string)))
		v.RegisterType((*types.IntKey)(new(int32)))
		v.RegisterType(types.JsonPayload[UserData]{})
		v.RegisterType(types.JsonPayload[ProductData]{})

		// Create collections
		users, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[UserData]](
			v, "users", types.StringLess, (*types.StringKey)(new(string)),
		)
		if err != nil {
			t.Fatalf("Failed to create users collection: %v", err)
		}

		products, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[ProductData]](
			v, "products", types.StringLess, (*types.StringKey)(new(string)),
		)
		if err != nil {
			t.Fatalf("Failed to create products collection: %v", err)
		}

		// Insert data
		aliceKey := types.StringKey("user:alice")
		users.Insert(&aliceKey, types.JsonPayload[UserData]{
			Value: UserData{Username: "alice", Email: "alice@example.com", Age: 30},
		})

		bobKey := types.StringKey("user:bob")
		users.Insert(&bobKey, types.JsonPayload[UserData]{
			Value: UserData{Username: "bob", Email: "bob@example.com", Age: 25},
		})

		widgetKey := types.StringKey("product:widget")
		products.Insert(&widgetKey, types.JsonPayload[ProductData]{
			Value: ProductData{Name: "Widget", Price: 19.99, Stock: 100},
		})

		// Close the vault (this persists everything)
		err = v.Close()
		if err != nil {
			t.Fatalf("Failed to close vault: %v", err)
		}
	}

	// ===== SESSION 2: Reopen vault and verify data =====
	{
		stre, err := store.LoadBaseStore(storePath)
		if err != nil {
			t.Fatalf("Failed to load store: %v", err)
		}

		// Load the existing vault
		v, err := LoadVault(stre)
		if err != nil {
			t.Fatalf("Failed to load vault: %v", err)
		}

		// Re-register types in THE SAME ORDER
		v.RegisterType((*types.StringKey)(new(string)))
		v.RegisterType((*types.IntKey)(new(int32)))
		v.RegisterType(types.JsonPayload[UserData]{})
		v.RegisterType(types.JsonPayload[ProductData]{})

		// Verify collections are listed
		collections := v.ListCollections()
		t.Logf("Found %d collections: %v", len(collections), collections)
		if len(collections) != 2 {
			t.Fatalf("Expected 2 collections, got %d", len(collections))
		}

		// Load users collection
		users, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[UserData]](
			v, "users", types.StringLess, (*types.StringKey)(new(string)),
		)
		if err != nil {
			t.Fatalf("Failed to get users collection: %v", err)
		}

		// Verify alice's data
		aliceKey := types.StringKey("user:alice")
		aliceNode := users.Search(&aliceKey)
		if aliceNode == nil {
			t.Fatal("Expected to find alice, but she was not found")
		}
		if aliceNode.GetPayload().Value.Username != "alice" {
			t.Errorf("Expected username alice, got %s", aliceNode.GetPayload().Value.Username)
		}
		if aliceNode.GetPayload().Value.Age != 30 {
			t.Errorf("Expected age 30, got %d", aliceNode.GetPayload().Value.Age)
		}

		// Verify bob's data
		bobKey := types.StringKey("user:bob")
		bobNode := users.Search(&bobKey)
		if bobNode == nil {
			t.Fatal("Expected to find bob, but he was not found")
		}
		if bobNode.GetPayload().Value.Username != "bob" {
			t.Errorf("Expected username bob, got %s", bobNode.GetPayload().Value.Username)
		}

		// Load products collection
		products, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[ProductData]](
			v, "products", types.StringLess, (*types.StringKey)(new(string)),
		)
		if err != nil {
			t.Fatalf("Failed to get products collection: %v", err)
		}

		// Verify widget's data
		widgetKey := types.StringKey("product:widget")
		widgetNode := products.Search(&widgetKey)
		if widgetNode == nil {
			t.Fatal("Expected to find widget, but it was not found")
		}
		if widgetNode.GetPayload().Value.Name != "Widget" {
			t.Errorf("Expected product name Widget, got %s", widgetNode.GetPayload().Value.Name)
		}
		if widgetNode.GetPayload().Value.Price != 19.99 {
			t.Errorf("Expected price 19.99, got %f", widgetNode.GetPayload().Value.Price)
		}

		// Add more data in session 2
		charlieKey := types.StringKey("user:charlie")
		users.Insert(&charlieKey, types.JsonPayload[UserData]{
			Value: UserData{Username: "charlie", Email: "charlie@example.com", Age: 35},
		})

		gadgetKey := types.StringKey("product:gadget")
		products.Insert(&gadgetKey, types.JsonPayload[ProductData]{
			Value: ProductData{Name: "Gadget", Price: 29.99, Stock: 50},
		})

		// Close vault
		err = v.Close()
		if err != nil {
			t.Fatalf("Failed to close vault in session 2: %v", err)
		}
	}

	// ===== SESSION 3: Reopen and verify all data (from sessions 1 and 2) =====
	{
		stre, err := store.LoadBaseStore(storePath)
		if err != nil {
			t.Fatalf("Failed to load store in session 3: %v", err)
		}

		v, err := LoadVault(stre)
		if err != nil {
			t.Fatalf("Failed to load vault in session 3: %v", err)
		}

		// Re-register types
		v.RegisterType((*types.StringKey)(new(string)))
		v.RegisterType((*types.IntKey)(new(int32)))
		v.RegisterType(types.JsonPayload[UserData]{})
		v.RegisterType(types.JsonPayload[ProductData]{})

		// Load collections
		users, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[UserData]](
			v, "users", types.StringLess, (*types.StringKey)(new(string)),
		)
		if err != nil {
			t.Fatalf("Failed to get users collection in session 3: %v", err)
		}

		products, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[ProductData]](
			v, "products", types.StringLess, (*types.StringKey)(new(string)),
		)
		if err != nil {
			t.Fatalf("Failed to get products collection in session 3: %v", err)
		}

		// Verify all users (alice, bob from session 1, charlie from session 2)
		aliceKey := types.StringKey("user:alice")
		if users.Search(&aliceKey) == nil {
			t.Error("Expected to find alice in session 3")
		}

		bobKey := types.StringKey("user:bob")
		if users.Search(&bobKey) == nil {
			t.Error("Expected to find bob in session 3")
		}

		charlieKey := types.StringKey("user:charlie")
		charlieNode := users.Search(&charlieKey)
		if charlieNode == nil {
			t.Error("Expected to find charlie in session 3")
		} else if charlieNode.GetPayload().Value.Age != 35 {
			t.Errorf("Expected charlie's age to be 35, got %d", charlieNode.GetPayload().Value.Age)
		}

		// Verify all products
		widgetKey := types.StringKey("product:widget")
		if products.Search(&widgetKey) == nil {
			t.Error("Expected to find widget in session 3")
		}

		gadgetKey := types.StringKey("product:gadget")
		gadgetNode := products.Search(&gadgetKey)
		if gadgetNode == nil {
			t.Error("Expected to find gadget in session 3")
		} else if gadgetNode.GetPayload().Value.Stock != 50 {
			t.Errorf("Expected gadget stock to be 50, got %d", gadgetNode.GetPayload().Value.Stock)
		}

		err = v.Close()
		if err != nil {
			t.Fatalf("Failed to close vault in session 3: %v", err)
		}
	}
}
