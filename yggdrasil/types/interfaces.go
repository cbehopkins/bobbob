package types

import "github.com/cbehopkins/bobbob/store"

// Key represents a key in a treap-like data structure.
// Implementations should be simple value types with deterministic sizing.
type Key[T any] interface {
	SizeInBytes() int
	Equals(T) bool
	Value() T
}

// PersistentKey extends Key with persistence capabilities.
type PersistentKey[T any] interface {
	Key[T]
	// New returns a new zero-value instance of this key type as a concrete pointer.
	New() PersistentKey[T]
	// MarshalToObjectId stores the key in the store and returns its ObjectId.
	MarshalToObjectId(store.Storer) (store.ObjectId, error)
	// UnmarshalFromObjectId loads the key from the given ObjectId.
	UnmarshalFromObjectId(store.ObjectId, store.Storer) error
}

// PriorityProvider is an optional interface keys can implement to supply a priority.
type PriorityProvider interface {
	Priority() uint32
}

// UntypedPersistentPayload represents a payload that can be persisted without type parameters.
type UntypedPersistentPayload interface {
	PersistentPayload[UntypedPersistentPayload]
	Unmarshal([]byte) (UntypedPersistentPayload, error)
}

// PersistentPayload represents a payload that can be marshaled and persisted.
type PersistentPayload[T any] interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) (UntypedPersistentPayload, error)
	SizeInBytes() int
}
