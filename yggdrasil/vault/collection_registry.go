package vault

import (
	"github.com/cbehopkins/bobbob/store"
	"encoding/json"
	"fmt"
)

// CollectionId is a unique identifier for a collection within a vault.
// It's a short code (uint16) to save space during serialization.
type CollectionId = ShortCodeType

// CollectionInfo holds metadata about a single collection (treap).
type CollectionInfo struct {
	// Name is a human-readable name for the collection (e.g., "users", "products")
	Name string `json:"name"`

	// CollectionId is a unique numeric identifier for this collection
	CollectionId CollectionId `json:"collection_id"`

	// RootObjectId is the ObjectId of the root node of this treap
	RootObjectId store.ObjectId `json:"root_object_id"`

	// KeyTypeShortCode identifies the key type used by this collection
	KeyTypeShortCode ShortCodeType `json:"key_type_short_code"`

	// PayloadTypeShortCode identifies the payload type used by this collection
	// Set to 0 if this is a key-only collection (no payloads)
	PayloadTypeShortCode ShortCodeType `json:"payload_type_short_code"`
}

// CollectionRegistry maintains a mapping of all collections in a vault.
// It provides a way to store multiple treaps in a single store file and
// retrieve them by name or ID.
type CollectionRegistry struct {
	// Collections maps collection names to their metadata
	Collections map[string]CollectionInfo `json:"collections"`

	// CollectionById provides reverse lookup by CollectionId
	CollectionById map[CollectionId]string `json:"collection_by_id"`

	// NextCollectionId is the next available collection ID
	NextCollectionId CollectionId `json:"next_collection_id"`
}

// NewCollectionRegistry creates a new empty collection registry.
func NewCollectionRegistry() *CollectionRegistry {
	return &CollectionRegistry{
		Collections:      make(map[string]CollectionInfo),
		CollectionById:   make(map[CollectionId]string),
		NextCollectionId: 1, // Start at 1, reserve 0 for special purposes
	}
}

// RegisterCollection adds a new collection to the registry.
// Returns the assigned CollectionId and any error.
func (cr *CollectionRegistry) RegisterCollection(
	name string,
	rootObjectId store.ObjectId,
	keyTypeShortCode ShortCodeType,
	payloadTypeShortCode ShortCodeType,
) (CollectionId, error) {
	if cr.Collections == nil {
		cr.Collections = make(map[string]CollectionInfo)
		cr.CollectionById = make(map[CollectionId]string)
	}

	// Check if collection already exists
	if existing, exists := cr.Collections[name]; exists {
		// Update the existing collection
		existing.RootObjectId = rootObjectId
		existing.KeyTypeShortCode = keyTypeShortCode
		existing.PayloadTypeShortCode = payloadTypeShortCode
		cr.Collections[name] = existing
		return existing.CollectionId, nil
	}

	// Assign a new collection ID
	collectionId := cr.NextCollectionId
	cr.NextCollectionId++

	// Create the collection info
	info := CollectionInfo{
		Name:                 name,
		CollectionId:         collectionId,
		RootObjectId:         rootObjectId,
		KeyTypeShortCode:     keyTypeShortCode,
		PayloadTypeShortCode: payloadTypeShortCode,
	}

	cr.Collections[name] = info
	cr.CollectionById[collectionId] = name

	return collectionId, nil
}

// GetCollection retrieves collection info by name.
func (cr *CollectionRegistry) GetCollection(name string) (CollectionInfo, bool) {
	info, exists := cr.Collections[name]
	return info, exists
}

// GetCollectionById retrieves collection info by CollectionId.
func (cr *CollectionRegistry) GetCollectionById(id CollectionId) (CollectionInfo, bool) {
	name, exists := cr.CollectionById[id]
	if !exists {
		return CollectionInfo{}, false
	}
	return cr.GetCollection(name)
}

// UpdateRootObjectId updates the root ObjectId for a collection.
// This is called when the treap's root changes and needs to be persisted.
func (cr *CollectionRegistry) UpdateRootObjectId(name string, rootObjectId store.ObjectId) error {
	info, exists := cr.Collections[name]
	if !exists {
		return fmt.Errorf("collection %s not found", name)
	}

	info.RootObjectId = rootObjectId
	cr.Collections[name] = info
	return nil
}

// ListCollections returns a slice of all collection names.
func (cr *CollectionRegistry) ListCollections() []string {
	names := make([]string, 0, len(cr.Collections))
	for name := range cr.Collections {
		names = append(names, name)
	}
	return names
}

// Marshal serializes the collection registry to JSON.
func (cr *CollectionRegistry) Marshal() ([]byte, error) {
	return json.Marshal(cr)
}

// Unmarshal deserializes the collection registry from JSON.
func (cr *CollectionRegistry) Unmarshal(data []byte) error {
	return json.Unmarshal(data, cr)
}
