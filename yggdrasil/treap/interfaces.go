package treap

import (
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TreapNodeInterface defines the interface for treap nodes.
type TreapNodeInterface[T any] interface {
	GetKey() types.Key[T]
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
	ObjectId() (store.ObjectId, error)  // Returns the object ID of the node, allocating one if necessary
	SetObjectId(store.ObjectId)         // Sets the object ID of the node
	IsObjectIdInvalid() bool            // Returns true if the node's ObjectId has been invalidated (is negative)
	GetObjectIdNoAlloc() store.ObjectId // Returns the object ID without allocating (may be invalid)
	Persist() error                     // Persist the node and its children to the store
	Flush() error                       // Flush the node and its children from memory
}

// PersistentPayloadTreapInterface defines the interface for persistent payload treaps.
type PersistentPayloadTreapInterface[T any, P any] interface {
	Insert(key types.PersistentKey[T], payload P)
	InsertComplex(key types.PersistentKey[T], priority Priority, payload P)
	Search(key types.PersistentKey[T]) PersistentPayloadNodeInterface[T, P]
	SearchComplex(key types.PersistentKey[T], callback func(TreapNodeInterface[T]) error) (PersistentPayloadNodeInterface[T, P], error)
	UpdatePriority(key types.PersistentKey[T], newPriority Priority)
	UpdatePayload(key types.PersistentKey[T], newPayload P) error
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
