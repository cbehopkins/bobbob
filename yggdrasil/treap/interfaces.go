package treap

import "github.com/cbehopkins/bobbob/store"

// Key represents a key in a treap data structure.
// It provides methods to determine size, equality, and value retrieval.
type Key[T any] interface {
	// SizeInBytes returns the size in bytes required to store the key.
	// This should be constant for all objects of the same type.
	SizeInBytes() int
	// Equals reports whether this key equals another.
	Equals(T) bool
	// Value returns the underlying value of the key.
	Value() T
}

// PersistentKey extends Key with persistence capabilities.
// It can be marshaled to and unmarshaled from a store.
type PersistentKey[T any] interface {
	Key[T]
	// New creates a new instance of this key type.
	New() PersistentKey[T]
	// MarshalToObjectId stores the key in the store and returns its ObjectId.
	MarshalToObjectId(store.Storer) (store.ObjectId, error)
	// UnmarshalFromObjectId loads the key from the given ObjectId in the store.
	UnmarshalFromObjectId(store.ObjectId, store.Storer) error
}

// PriorityProvider is an optional interface that keys can implement to provide
// their own priority value for the treap. This is useful for keys that have
// inherently well-distributed values (like hash values) that can serve as
// priorities, avoiding the need to generate random priorities.
// If a key implements this interface, the Insert method will use the provided
// priority instead of generating a random one.
type PriorityProvider interface {
	// Priority returns the priority value to use for this key in the treap.
	Priority() Priority
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

// TreapNodeInterface defines the interface for treap nodes.
type TreapNodeInterface[T any] interface {
	GetKey() Key[T]
	GetPriority() Priority
	SetPriority(Priority)
	GetLeft() TreapNodeInterface[T]
	GetRight() TreapNodeInterface[T]
	SetLeft(TreapNodeInterface[T]) error
	SetRight(TreapNodeInterface[T]) error
	IsNil() bool
}

// PersistentTreapNodeInterface extends TreapNodeInterface with persistence methods.
type PersistentTreapNodeInterface[T any] interface {
	TreapNodeInterface[T]
	ObjectId() (store.ObjectId, error) // Returns the object ID of the node, allocating one if necessary
	SetObjectId(store.ObjectId)        // Sets the object ID of the node
	Persist() error                    // Persist the node and its children to the store
	Flush() error                      // Flush the node and its children from memory
}

// PersistentPayloadTreapInterface defines the interface for persistent payload treaps.
type PersistentPayloadTreapInterface[T any, P any] interface {
	Insert(key PersistentKey[T], payload P)
	InsertComplex(key PersistentKey[T], priority Priority, payload P)
	Search(key PersistentKey[T]) PersistentPayloadNodeInterface[T, P]
	SearchComplex(key PersistentKey[T], callback func(TreapNodeInterface[T]) error) (PersistentPayloadNodeInterface[T, P], error)
	UpdatePriority(key PersistentKey[T], newPriority Priority)
	UpdatePayload(key PersistentKey[T], newPayload P) error
	Persist() error
	Load(objId store.ObjectId) error
	Marshal() ([]byte, error)
}

// PersistentPayloadNodeInterface defines the interface for persistent payload treap nodes.
type PersistentPayloadNodeInterface[T any, P any] interface {
	PersistentTreapNodeInterface[T]
	GetPayload() P
	SetPayload(P)
}
