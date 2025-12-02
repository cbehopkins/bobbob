package collections

import (
	"testing"

	"bobbob/internal/store"
)

// TestNewCollectionRegistry verifies that a new registry is created with properly
// initialized maps and default values (NextCollectionId starts at 1, maps are non-nil).
func TestNewCollectionRegistry(t *testing.T) {
	registry := NewCollectionRegistry()

	if registry == nil {
		t.Fatal("Expected non-nil registry")
	}
	if registry.Collections == nil {
		t.Error("Expected initialized Collections map")
	}
	if registry.CollectionById == nil {
		t.Error("Expected initialized CollectionById map")
	}
	if registry.NextCollectionId != 1 {
		t.Errorf("Expected NextCollectionId to be 1, got %d", registry.NextCollectionId)
	}
	if len(registry.Collections) != 0 {
		t.Errorf("Expected empty Collections map, got %d entries", len(registry.Collections))
	}
}

// TestRegisterCollection verifies that registering a collection assigns it a unique ID,
// stores all the collection metadata (name, root object ID, type codes), and allows
// retrieval by name.
func TestRegisterCollection(t *testing.T) {
	registry := NewCollectionRegistry()

	// Register first collection
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
	if info.CollectionId != 1 {
		t.Errorf("Expected collection ID 1, got %d", info.CollectionId)
	}
	if info.RootObjectId != 100 {
		t.Errorf("Expected root object ID 100, got %d", info.RootObjectId)
	}
	if info.KeyTypeShortCode != 1 {
		t.Errorf("Expected key type short code 1, got %d", info.KeyTypeShortCode)
	}
	if info.PayloadTypeShortCode != 2 {
		t.Errorf("Expected payload type short code 2, got %d", info.PayloadTypeShortCode)
	}
}

// TestRegisterMultipleCollections verifies that multiple collections can be registered,
// each receives a sequential ID, and all are accessible in the registry.
func TestRegisterMultipleCollections(t *testing.T) {
	registry := NewCollectionRegistry()

	// Register multiple collections
	collId1, err := registry.RegisterCollection("users", 100, 1, 2)
	if err != nil {
		t.Fatalf("Failed to register users collection: %v", err)
	}

	collId2, err := registry.RegisterCollection("products", 200, 3, 4)
	if err != nil {
		t.Fatalf("Failed to register products collection: %v", err)
	}

	collId3, err := registry.RegisterCollection("orders", 300, 5, 6)
	if err != nil {
		t.Fatalf("Failed to register orders collection: %v", err)
	}

	// Verify IDs are sequential
	if collId1 != 1 {
		t.Errorf("Expected first collection ID 1, got %d", collId1)
	}
	if collId2 != 2 {
		t.Errorf("Expected second collection ID 2, got %d", collId2)
	}
	if collId3 != 3 {
		t.Errorf("Expected third collection ID 3, got %d", collId3)
	}

	// Verify all collections exist
	if len(registry.Collections) != 3 {
		t.Errorf("Expected 3 collections, got %d", len(registry.Collections))
	}

	// Verify NextCollectionId is incremented
	if registry.NextCollectionId != 4 {
		t.Errorf("Expected NextCollectionId to be 4, got %d", registry.NextCollectionId)
	}
}

// TestRegisterCollectionDuplicate verifies that re-registering an existing collection
// updates its metadata while preserving its collection ID.
func TestRegisterCollectionDuplicate(t *testing.T) {
	registry := NewCollectionRegistry()

	// Register a collection
	collId1, err := registry.RegisterCollection("users", 100, 1, 2)
	if err != nil {
		t.Fatalf("Failed to register collection: %v", err)
	}

	// Register the same collection name again (should update, not error)
	collId2, err := registry.RegisterCollection("users", 200, 3, 4)
	if err != nil {
		t.Fatalf("Failed to re-register collection: %v", err)
	}

	// Should return the same collection ID
	if collId1 != collId2 {
		t.Errorf("Expected same collection ID on re-registration, got %d and %d", collId1, collId2)
	}

	// Verify the collection was updated
	info, exists := registry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection")
	}
	if info.RootObjectId != 200 {
		t.Errorf("Expected updated root object ID 200, got %d", info.RootObjectId)
	}
	if info.KeyTypeShortCode != 3 {
		t.Errorf("Expected updated key type 3, got %d", info.KeyTypeShortCode)
	}
	if info.PayloadTypeShortCode != 4 {
		t.Errorf("Expected updated payload type 4, got %d", info.PayloadTypeShortCode)
	}
}

// TestGetCollection verifies that collections can be retrieved by name,
// returning the correct metadata for existing collections and false for non-existent ones.
func TestGetCollection(t *testing.T) {
	registry := NewCollectionRegistry()
	registry.RegisterCollection("users", 100, 1, 2)

	// Get existing collection
	info, exists := registry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection")
	}
	if info.Name != "users" {
		t.Errorf("Expected collection name 'users', got '%s'", info.Name)
	}

	// Get non-existent collection
	_, exists = registry.GetCollection("nonexistent")
	if exists {
		t.Error("Expected not to find nonexistent collection")
	}
}

// TestGetCollectionById verifies that collections can be retrieved by their numeric ID,
// enabling reverse lookup from ID to collection metadata.
func TestGetCollectionById(t *testing.T) {
	registry := NewCollectionRegistry()
	collId, _ := registry.RegisterCollection("users", 100, 1, 2)

	// Get by ID
	info, exists := registry.GetCollectionById(collId)
	if !exists {
		t.Fatal("Expected to find collection by ID")
	}
	if info.Name != "users" {
		t.Errorf("Expected collection name 'users', got '%s'", info.Name)
	}

	// Get by non-existent ID
	_, exists = registry.GetCollectionById(999)
	if exists {
		t.Error("Expected not to find collection with ID 999")
	}
}

// TestUpdateRootObjectId verifies that a collection's root object ID can be updated
// after registration, and that updating a non-existent collection returns an error.
func TestUpdateRootObjectId(t *testing.T) {
	registry := NewCollectionRegistry()
	registry.RegisterCollection("users", 100, 1, 2)

	// Update root object ID
	err := registry.UpdateRootObjectId("users", 200)
	if err != nil {
		t.Fatalf("Failed to update root object ID: %v", err)
	}

	// Verify update
	info, exists := registry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection after update")
	}
	if info.RootObjectId != 200 {
		t.Errorf("Expected updated root object ID 200, got %d", info.RootObjectId)
	}

	// Update non-existent collection
	err = registry.UpdateRootObjectId("nonexistent", 300)
	if err == nil {
		t.Error("Expected error when updating non-existent collection")
	}
}

// TestListCollections verifies that ListCollections returns all registered collection names,
// handling both empty registries and registries with multiple collections.
func TestListCollections(t *testing.T) {
	registry := NewCollectionRegistry()

	// Empty registry
	collections := registry.ListCollections()
	if len(collections) != 0 {
		t.Errorf("Expected 0 collections in empty registry, got %d", len(collections))
	}

	// Add collections
	registry.RegisterCollection("users", 100, 1, 2)
	registry.RegisterCollection("products", 200, 3, 4)
	registry.RegisterCollection("orders", 300, 5, 6)

	collections = registry.ListCollections()
	if len(collections) != 3 {
		t.Errorf("Expected 3 collections, got %d", len(collections))
	}

	// Verify all names are present (order not guaranteed)
	names := make(map[string]bool)
	for _, name := range collections {
		names[name] = true
	}

	if !names["users"] {
		t.Error("Expected to find 'users' in collection list")
	}
	if !names["products"] {
		t.Error("Expected to find 'products' in collection list")
	}
	if !names["orders"] {
		t.Error("Expected to find 'orders' in collection list")
	}
}

// TestMarshalUnmarshal verifies that a registry can be serialized and deserialized,
// preserving all collection metadata and the NextCollectionId counter.
func TestMarshalUnmarshal(t *testing.T) {
	registry := NewCollectionRegistry()

	// Add some collections
	registry.RegisterCollection("users", 100, 1, 2)
	registry.RegisterCollection("products", 200, 3, 4)
	registry.RegisterCollection("orders", 300, 5, 6)

	// Marshal
	data, err := registry.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal registry: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty marshaled data")
	}

	// Unmarshal into new registry
	newRegistry := NewCollectionRegistry()
	err = newRegistry.Unmarshal(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal registry: %v", err)
	}

	// Verify all collections are present
	if len(newRegistry.Collections) != 3 {
		t.Errorf("Expected 3 collections in unmarshaled registry, got %d", len(newRegistry.Collections))
	}

	// Verify specific collection
	info, exists := newRegistry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection in unmarshaled registry")
	}
	if info.RootObjectId != 100 {
		t.Errorf("Expected root object ID 100 in unmarshaled registry, got %d", info.RootObjectId)
	}
	if info.KeyTypeShortCode != 1 {
		t.Errorf("Expected key type 1 in unmarshaled registry, got %d", info.KeyTypeShortCode)
	}
	if info.PayloadTypeShortCode != 2 {
		t.Errorf("Expected payload type 2 in unmarshaled registry, got %d", info.PayloadTypeShortCode)
	}

	// Verify NextCollectionId is preserved
	if newRegistry.NextCollectionId != 4 {
		t.Errorf("Expected NextCollectionId 4 in unmarshaled registry, got %d", newRegistry.NextCollectionId)
	}
}

// TestMarshalUnmarshalEmpty verifies that an empty registry can be marshaled and
// unmarshaled without errors, maintaining its empty state.
func TestMarshalUnmarshalEmpty(t *testing.T) {
	registry := NewCollectionRegistry()

	// Marshal empty registry
	data, err := registry.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal empty registry: %v", err)
	}

	// Unmarshal into new registry
	newRegistry := &CollectionRegistry{}
	err = newRegistry.Unmarshal(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal empty registry: %v", err)
	}

	// Verify it's empty
	if len(newRegistry.Collections) != 0 {
		t.Errorf("Expected 0 collections in unmarshaled empty registry, got %d", len(newRegistry.Collections))
	}
}

// TestCollectionRegistryWithKeyOnlyCollection verifies that collections without payloads
// (key-only, like sets) can be registered with PayloadTypeShortCode = 0.
func TestCollectionRegistryWithKeyOnlyCollection(t *testing.T) {
	registry := NewCollectionRegistry()

	// Register a key-only collection (payload type = 0)
	collId, err := registry.RegisterCollection("active_users", 100, 1, 0)
	if err != nil {
		t.Fatalf("Failed to register key-only collection: %v", err)
	}

	info, exists := registry.GetCollection("active_users")
	if !exists {
		t.Fatal("Expected to find active_users collection")
	}
	if info.PayloadTypeShortCode != 0 {
		t.Errorf("Expected payload type 0 for key-only collection, got %d", info.PayloadTypeShortCode)
	}
	if info.CollectionId != collId {
		t.Errorf("Expected collection ID %d, got %d", collId, info.CollectionId)
	}
}

// TestCollectionRegistryUpdatePreservesOtherFields verifies that re-registering a collection
// updates all metadata fields while preserving the original collection ID.
func TestCollectionRegistryUpdatePreservesOtherFields(t *testing.T) {
	registry := NewCollectionRegistry()

	// Register initial collection
	collId, _ := registry.RegisterCollection("users", 100, 1, 2)

	// Re-register with different values
	newCollId, _ := registry.RegisterCollection("users", 200, 3, 4)

	// Verify ID is preserved
	if newCollId != collId {
		t.Errorf("Expected collection ID to be preserved, got %d instead of %d", newCollId, collId)
	}

	// Verify all fields are updated
	info, _ := registry.GetCollection("users")
	if info.RootObjectId != 200 {
		t.Errorf("Expected root object ID 200, got %d", info.RootObjectId)
	}
	if info.KeyTypeShortCode != 3 {
		t.Errorf("Expected key type 3, got %d", info.KeyTypeShortCode)
	}
	if info.PayloadTypeShortCode != 4 {
		t.Errorf("Expected payload type 4, got %d", info.PayloadTypeShortCode)
	}
}

// TestCollectionRegistryBidirectionalLookup verifies that collections can be looked up
// both by name and by ID, with both lookups returning consistent data.
func TestCollectionRegistryBidirectionalLookup(t *testing.T) {
	registry := NewCollectionRegistry()

	// Register collections
	collId1, _ := registry.RegisterCollection("users", 100, 1, 2)
	collId2, _ := registry.RegisterCollection("products", 200, 3, 4)

	// Verify forward lookup (name -> info)
	info1, exists := registry.GetCollection("users")
	if !exists || info1.CollectionId != collId1 {
		t.Error("Forward lookup failed for users")
	}

	// Verify reverse lookup (id -> info)
	info2, exists := registry.GetCollectionById(collId1)
	if !exists || info2.Name != "users" {
		t.Error("Reverse lookup failed for users")
	}

	// Verify both lookups return same data
	if info1.RootObjectId != info2.RootObjectId {
		t.Error("Forward and reverse lookups returned different data")
	}

	// Verify for second collection
	info3, exists := registry.GetCollectionById(collId2)
	if !exists || info3.Name != "products" {
		t.Error("Reverse lookup failed for products")
	}
}

// TestCollectionRegistryWithObjNotAllocated verifies that collections can be registered
// with ObjNotAllocated (for new collections not yet persisted) and later updated to a real ObjectId.
func TestCollectionRegistryWithObjNotAllocated(t *testing.T) {
	registry := NewCollectionRegistry()

	// Register collection with ObjNotAllocated (common for new collections)
	collId, err := registry.RegisterCollection("users", store.ObjNotAllocated, 1, 2)
	if err != nil {
		t.Fatalf("Failed to register collection with ObjNotAllocated: %v", err)
	}

	info, exists := registry.GetCollection("users")
	if !exists {
		t.Fatal("Expected to find users collection")
	}
	if info.RootObjectId != store.ObjNotAllocated {
		t.Errorf("Expected ObjNotAllocated, got %d", info.RootObjectId)
	}

	// Update to a real ObjectId
	err = registry.UpdateRootObjectId("users", 100)
	if err != nil {
		t.Fatalf("Failed to update from ObjNotAllocated: %v", err)
	}

	info, _ = registry.GetCollection("users")
	if info.RootObjectId != 100 {
		t.Errorf("Expected root object ID 100, got %d", info.RootObjectId)
	}

	// Verify ID didn't change
	if info.CollectionId != collId {
		t.Errorf("Expected collection ID to remain %d, got %d", collId, info.CollectionId)
	}
}
