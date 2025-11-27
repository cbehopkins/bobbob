package yggdrasil

import (
	"encoding/binary"
	"fmt"

	"bobbob/internal/store"
)

// Reserved ObjectIds for vault metadata
// VaultMetadataObjectId is typically the first object allocated (ObjectId 1)
// since ObjectId 0 is used by the store's internal objectMap
const (
	// VaultMetadataObjectId stores references to TypeMap and CollectionRegistry
	VaultMetadataObjectId store.ObjectId = 1
)

// VaultMetadata contains the ObjectIds of the TypeMap and CollectionRegistry.
// This is stored at a known location (VaultMetadataObjectId) so we can
// find the other metadata when loading.
type VaultMetadata struct {
	TypeMapObjectId            store.ObjectId
	CollectionRegistryObjectId store.ObjectId
}

// Marshal serializes the metadata to bytes.
func (vm *VaultMetadata) Marshal() []byte {
	buf := make([]byte, 16) // 2 ObjectIds * 8 bytes each
	binary.LittleEndian.PutUint64(buf[0:8], uint64(vm.TypeMapObjectId))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(vm.CollectionRegistryObjectId))
	return buf
}

// Unmarshal deserializes the metadata from bytes.
func (vm *VaultMetadata) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("metadata too short: %d bytes", len(data))
	}
	vm.TypeMapObjectId = store.ObjectId(binary.LittleEndian.Uint64(data[0:8]))
	vm.CollectionRegistryObjectId = store.ObjectId(binary.LittleEndian.Uint64(data[8:16]))
	return nil
}

// Vault is the top-level abstraction for working with multiple collections
// (treaps) in a single persistent store. It manages:
// - A single persistent store file
// - A TypeMap for efficient type serialization
// - A CollectionRegistry for tracking all collections
//
// Usage pattern:
//  1. Create/open a vault
//  2. Register all your types (in a consistent order)
//  3. Get or create collections by name
//  4. Work with collections (insert, search, delete)
//  5. Close the vault to persist everything
type Vault struct {
	// Store is the underlying persistent storage
	Store store.Storer

	// TypeMap manages type-to-short-code mappings
	TypeMap *TypeMap

	// CollectionRegistry tracks all collections in this vault
	CollectionRegistry *CollectionRegistry

	// ActiveCollections caches loaded collections
	// Maps collection name to the actual treap instance
	activeCollections map[string]interface{}
}

// NewVault creates a new vault with the given store.
// The store should be newly created (not loaded from disk).
func NewVault(stre store.Storer) *Vault {
	// Allocate the prime object (ObjectId 1) first to ensure it's available
	// for storing vault metadata when Close() is called.
	// This must be done before any other objects are allocated.
	_, _ = stre.PrimeObject(16)

	return &Vault{
		Store:              stre,
		TypeMap:            NewTypeMap(),
		CollectionRegistry: NewCollectionRegistry(),
		activeCollections:  make(map[string]interface{}),
	}
}

// LoadVault loads an existing vault from a store.
// It reads the TypeMap and CollectionRegistry from the store's reserved objects.
// After loading, you should register your types in the same order as when the vault was created.
// The loaded TypeMap will be merged with your registered types.
func LoadVault(stre store.Storer) (*Vault, error) {
	vault := &Vault{
		Store:              stre,
		TypeMap:            NewTypeMap(),
		CollectionRegistry: NewCollectionRegistry(),
		activeCollections:  make(map[string]interface{}),
	}

	// Get the prime object (ObjectId 1) where vault metadata is stored
	primeObjectId, err := stre.PrimeObject(16)
	if err != nil {
		return nil, fmt.Errorf("failed to get prime object: %w", err)
	}

	// Try to load the metadata object
	metadataBytes, err := store.ReadBytesFromObj(stre, primeObjectId)
	if err != nil {
		// No metadata found - this is a new vault
		return vault, nil
	}

	var metadata VaultMetadata
	if err := metadata.Unmarshal(metadataBytes); err != nil {
		return vault, nil // Ignore errors, treat as new vault
	}

	// Load TypeMap
	if store.IsValidObjectId(metadata.TypeMapObjectId) {
		typeMapData, err := store.ReadBytesFromObj(stre, metadata.TypeMapObjectId)
		if err == nil {
			loadedTypeMap := NewTypeMap()
			if err := loadedTypeMap.Unmarshal(typeMapData); err == nil {
				// Use the loaded TypeMap
				vault.TypeMap = loadedTypeMap
			}
		}
	}

	// Load CollectionRegistry
	if store.IsValidObjectId(metadata.CollectionRegistryObjectId) {
		registryData, err := store.ReadBytesFromObj(stre, metadata.CollectionRegistryObjectId)
		if err == nil {
			loadedRegistry := NewCollectionRegistry()
			if err := loadedRegistry.Unmarshal(registryData); err == nil {
				vault.CollectionRegistry = loadedRegistry
			}
		}
	}

	return vault, nil
}

// RegisterType adds a type to the vault's type map.
// You should call this for all types you'll use in collections,
// in a consistent order across sessions.
func (v *Vault) RegisterType(t any) {
	v.TypeMap.AddType(t)
}

// GetOrCreateCollection retrieves or creates a collection with the given name.
// The collection stores keys of type K and payloads of type P.
// If the collection already exists, it loads it from the store.
// If it doesn't exist, it creates a new empty collection.
//
// Important: You must register the key and payload types before calling this.
//
// Example:
//
//	v.RegisterType(StringKey(""))
//	v.RegisterType(UserData{})
//	users := GetOrCreateCollection[string, JsonPayload[UserData]](v, "users", StringLess, (*StringKey)(new(string)))
func GetOrCreateCollection[K any, P PersistentPayload[P]](
	v *Vault,
	collectionName string,
	lessFunc func(a, b K) bool,
	keyTemplate PersistentKey[K],
) (*PersistentPayloadTreap[K, P], error) {
	// Check if we already have this collection loaded
	if cached, exists := v.activeCollections[collectionName]; exists {
		if treap, ok := cached.(*PersistentPayloadTreap[K, P]); ok {
			return treap, nil
		}
		return nil, fmt.Errorf("collection %s exists but has wrong type", collectionName)
	}

	// Check if the collection exists in the registry
	collInfo, exists := v.CollectionRegistry.GetCollection(collectionName)
	if exists {
		// Load the existing collection from the store
		treap := NewPersistentPayloadTreap[K, P](lessFunc, keyTemplate, v.Store)
		if store.IsValidObjectId(collInfo.RootObjectId) {
			err := treap.Load(collInfo.RootObjectId)
			if err != nil {
				return nil, fmt.Errorf("failed to load collection %s: %w", collectionName, err)
			}
		}
		// Cache it
		v.activeCollections[collectionName] = treap
		return treap, nil
	}

	// Create a new collection
	treap := NewPersistentPayloadTreap[K, P](lessFunc, keyTemplate, v.Store)

	// Get the type short codes
	var zeroP P

	// Use the template to get the short code (not the generic K)
	keyShortCode, err := v.TypeMap.getShortCode(keyTemplate)
	if err != nil {
		return nil, fmt.Errorf("key type not registered: %w", err)
	}

	payloadShortCode, err := v.TypeMap.getShortCode(zeroP)
	if err != nil {
		return nil, fmt.Errorf("payload type not registered: %w", err)
	}

	// Register in the collection registry
	_, err = v.CollectionRegistry.RegisterCollection(
		collectionName,
		store.ObjNotAllocated, // No root yet
		keyShortCode,
		payloadShortCode,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register collection: %w", err)
	}

	// Cache it
	v.activeCollections[collectionName] = treap
	return treap, nil
}

// GetOrCreateKeyOnlyCollection retrieves or creates a collection that stores only keys (no payloads).
// This is useful for sets or indexes.
func GetOrCreateKeyOnlyCollection[K any](
	v *Vault,
	collectionName string,
	lessFunc func(a, b K) bool,
	keyTemplate PersistentKey[K],
) (*PersistentTreap[K], error) {
	// Check if we already have this collection loaded
	if cached, exists := v.activeCollections[collectionName]; exists {
		if treap, ok := cached.(*PersistentTreap[K]); ok {
			return treap, nil
		}
		return nil, fmt.Errorf("collection %s exists but has wrong type", collectionName)
	}

	// Check if the collection exists in the registry
	collInfo, exists := v.CollectionRegistry.GetCollection(collectionName)
	if exists {
		// Load the existing collection from the store
		treap := NewPersistentTreap[K](lessFunc, keyTemplate, v.Store)
		if store.IsValidObjectId(collInfo.RootObjectId) {
			err := treap.Load(collInfo.RootObjectId)
			if err != nil {
				return nil, fmt.Errorf("failed to load collection %s: %w", collectionName, err)
			}
		}
		// Cache it
		v.activeCollections[collectionName] = treap
		return treap, nil
	}

	// Create a new collection
	treap := NewPersistentTreap[K](lessFunc, keyTemplate, v.Store)

	// Get the type short code for the key (use the template, not the generic K)
	keyShortCode, err := v.TypeMap.getShortCode(keyTemplate)
	if err != nil {
		return nil, fmt.Errorf("key type not registered: %w", err)
	}

	// Register in the collection registry (payload short code is 0 for key-only)
	_, err = v.CollectionRegistry.RegisterCollection(
		collectionName,
		store.ObjNotAllocated, // No root yet
		keyShortCode,
		0, // No payload type
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register collection: %w", err)
	}

	// Cache it
	v.activeCollections[collectionName] = treap
	return treap, nil
}

// PersistCollection saves a collection's current root to the registry.
// Call this after making changes to a collection and before closing the vault.
func (v *Vault) PersistCollection(collectionName string) error {
	// Get the collection from the cache
	cached, exists := v.activeCollections[collectionName]
	if !exists {
		return fmt.Errorf("collection %s not loaded", collectionName)
	}

	// Try to get the root ObjectId depending on the collection type
	var rootObjectId store.ObjectId = store.ObjNotAllocated
	var err error

	// Try as PersistentPayloadTreap first
	if treap, ok := cached.(interface {
		Persist() error
		GetRootObjectId() (store.ObjectId, error)
	}); ok {
		err = treap.Persist()
		if err != nil {
			return fmt.Errorf("failed to persist collection %s: %w", collectionName, err)
		}
		rootObjectId, err = treap.GetRootObjectId()
		if err != nil {
			return fmt.Errorf("failed to get root object ID for collection %s: %w", collectionName, err)
		}
	} else {
		return fmt.Errorf("collection %s has unknown type", collectionName)
	}

	// Update the registry
	return v.CollectionRegistry.UpdateRootObjectId(collectionName, rootObjectId)
}

// Close persists all active collections and closes the underlying store.
// You should call this when you're done with the vault.
func (v *Vault) Close() error {
	// Persist all active collections
	for name := range v.activeCollections {
		err := v.PersistCollection(name)
		if err != nil {
			return fmt.Errorf("failed to persist collection %s: %w", name, err)
		}
	}

	// Save TypeMap to a new object
	typeMapData, err := v.TypeMap.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal TypeMap: %w", err)
	}
	typeMapObjId, err := store.WriteNewObjFromBytes(v.Store, typeMapData)
	if err != nil {
		return fmt.Errorf("failed to write TypeMap: %w", err)
	}

	// Save CollectionRegistry to a new object
	registryData, err := v.CollectionRegistry.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal CollectionRegistry: %w", err)
	}
	registryObjId, err := store.WriteNewObjFromBytes(v.Store, registryData)
	if err != nil {
		return fmt.Errorf("failed to write CollectionRegistry: %w", err)
	}

	// Get the prime object (ObjectId 1) where we'll store vault metadata
	primeObjectId, err := v.Store.PrimeObject(16)
	if err != nil {
		return fmt.Errorf("failed to get prime object: %w", err)
	}

	// Create and save metadata pointing to TypeMap and CollectionRegistry
	metadata := VaultMetadata{
		TypeMapObjectId:            typeMapObjId,
		CollectionRegistryObjectId: registryObjId,
	}
	metadataBytes := metadata.Marshal()
	err = store.WriteBytesToObj(v.Store, metadataBytes, primeObjectId)
	if err != nil {
		return fmt.Errorf("failed to write metadata to prime object: %w", err)
	}

	// Close the store
	return v.Store.Close()
} // ListCollections returns the names of all collections in the vault.
func (v *Vault) ListCollections() []string {
	return v.CollectionRegistry.ListCollections()
}
