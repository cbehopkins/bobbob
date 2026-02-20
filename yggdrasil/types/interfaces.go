package types

import (
	"github.com/cbehopkins/bobbob"
)

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
	// LateMarshal stores the key in the store and returns its ObjectId and logical size.
	LateMarshal(bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher)
	// LateUnmarshal loads the key from the given ObjectId.
	LateUnmarshal(id bobbob.ObjectId, size int, s bobbob.Storer) bobbob.Finisher
	// DeleteDependents deletes any dependent objects owned by this key.
	// For keys that allocate separate storage (e.g., StringKey, MD5Key), this should
	// delete the backing object. For keys stored inline (e.g., IntKey), this is a no-op.
	DeleteDependents(bobbob.Storer) error
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
