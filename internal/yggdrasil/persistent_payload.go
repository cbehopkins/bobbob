package yggdrasil

import (
	"fmt"

	"bobbob/internal/store"
)

type UntypedPersistentPayload interface {
	PersistentPayload[UntypedPersistentPayload]
	Unmarshal([]byte) (UntypedPersistentPayload, error)
}
type PersistentPayload[T any] interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) (UntypedPersistentPayload, error)
	SizeInBytes() int
}

// PersistentPayloadTreapNode represents a node in the persistent payload treap.
type PersistentPayloadTreapNode[K any, P PersistentPayload[P]] struct {
	PersistentTreapNode[K]
	payload P
}

// GetPayload returns the payload of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetPayload() P {
	return n.payload
}

// SetPayload sets the payload of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetPayload(payload P) {
	n.payload = payload
	n.objectId = store.ObjNotAllocated
}

// SetLeft sets the left child of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetLeft(left TreapNodeInterface[K]) {
	if left == nil {
		n.PersistentTreapNode.SetLeft(nil)
		return
	}
	tmp := left.(*PersistentPayloadTreapNode[K, P])
	n.PersistentTreapNode.SetLeft(tmp)
}

// SetRight sets the right child of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetRight(right TreapNodeInterface[K]) {
	if right == nil {
		n.PersistentTreapNode.SetRight(nil)
		return
	}
	tmp := right.(*PersistentPayloadTreapNode[K, P])
	n.PersistentTreapNode.SetRight(tmp)
}

// GetLeft returns the left child of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetLeft() TreapNodeInterface[K] {
	// Check if we need to load the left child from storage
	if n.TreapNode.left == nil && store.IsValidObjectId(n.leftObjectId) {
		tmp, err := NewPayloadFromObjectId[K, P](n.leftObjectId, n.parent, n.Store)
		if err != nil {
			return nil
		}
		n.TreapNode.left = tmp
	}
	return n.TreapNode.left
}

// GetRight returns the right child of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetRight() TreapNodeInterface[K] {
	// Check if we need to load the right child from storage
	if n.TreapNode.right == nil && store.IsValidObjectId(n.rightObjectId) {
		tmp, err := NewPayloadFromObjectId[K, P](n.rightObjectId, n.parent, n.Store)
		if err != nil {
			return nil
		}
		n.TreapNode.right = tmp
	}
	return n.TreapNode.right
}

// Marshal overrides the Marshal method to include the payload.
func (n *PersistentPayloadTreapNode[K, P]) Marshal() ([]byte, error) {
	baseData, err := n.PersistentTreapNode.Marshal()
	if err != nil {
		return nil, err
	}

	payloadData, err := n.payload.Marshal()
	if err != nil {
		return nil, err
	}

	return append(baseData, payloadData...), nil
}

// unmarshal overrides the Unmarshal method to include the payload.
func (n *PersistentPayloadTreapNode[K, P]) unmarshal(data []byte, key PersistentKey[K]) error {
	// Unmarshal the base PersistentTreapNode
	err := n.PersistentTreapNode.unmarshal(data, key)
	if err != nil {
		return err
	}

	// Calculate the offset for the payload data
	payloadOffset := n.PersistentTreapNode.sizeInBytes()

	// Unmarshal the payload
	val, err := n.payload.Unmarshal(data[payloadOffset:])
	if err != nil {
		return err
	}
	payload, ok := val.(P)
	if !ok {
		return fmt.Errorf("failed to unmarshal payload")
	}
	n.payload = payload
	return nil
}

func (n *PersistentPayloadTreapNode[K, P]) Unmarshal(data []byte) error {
	return n.unmarshal(data, n.PersistentTreapNode.parent.keyTemplate)
}

// SetObjectId sets the object ID of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetObjectId(id store.ObjectId) {
	n.objectId = id
}

type PersistentPayloadTreapInterface[T any, P any] interface {
	Insert(key PersistentKey[T], priority Priority, payload P)
	Search(key PersistentKey[T]) PersistentPayloadNodeInterface[T, P]
	UpdatePriority(key PersistentKey[T], newPriority Priority)
	UpdatePayload(key PersistentKey[T], newPayload P)
	Persist() error
	Load(objId store.ObjectId) error
	Marshal() ([]byte, error)
}
type PersistentPayloadNodeInterface[T any, P any] interface {
	PersistentTreapNodeInterface[T]
	GetPayload() P
	SetPayload(P)
}

// PersistentPayloadTreap represents a persistent treap with payloads.
type PersistentPayloadTreap[K any, P PersistentPayload[P]] struct {
	PersistentTreap[K]
}

// NewPersistentPayloadTreapNode creates a new PersistentPayloadTreapNode with the given key, priority, and payload.
func NewPersistentPayloadTreapNode[K any, P PersistentPayload[P]](key PersistentKey[K], priority Priority, payload P, stre store.Storer, parent *PersistentPayloadTreap[K, P]) *PersistentPayloadTreapNode[K, P] {
	return &PersistentPayloadTreapNode[K, P]{
		PersistentTreapNode: PersistentTreapNode[K]{
			TreapNode: TreapNode[K]{
				key:      key,
				priority: priority,
			},
			objectId: store.ObjNotAllocated,
			Store:    stre,
			parent:   &parent.PersistentTreap,
		},
		payload: payload,
	}
}

// NewPersistentPayloadTreap creates a new PersistentPayloadTreap with the given comparison function and store reference.
func NewPersistentPayloadTreap[K any, P PersistentPayload[P]](lessFunc func(a, b K) bool, keyTemplate PersistentKey[K], store store.Storer) *PersistentPayloadTreap[K, P] {
	return &PersistentPayloadTreap[K, P]{
		PersistentTreap: PersistentTreap[K]{
			Treap: Treap[K]{
				root: nil,
				Less: lessFunc,
			},
			keyTemplate: keyTemplate,
			Store:       store,
		},
	}
}

// Insert inserts a new node with the given key, priority, and payload into the persistent payload treap.
func (t *PersistentPayloadTreap[K, P]) Insert(key PersistentKey[K], priority Priority, payload P) {
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t.Store, t)
	var tmp TreapNodeInterface[K]
	tmp = t.insert(t.root, newNode)
	t.root = tmp
}

// NewPayloadFromObjectId creates a PersistentPayloadTreapNode from the given object ID.
// Reading it in fron the store if it exuists.
func NewPayloadFromObjectId[T any, P PersistentPayload[P]](objId store.ObjectId, parent *PersistentTreap[T], stre store.Storer) (*PersistentPayloadTreapNode[T, P], error) {
	tmp := &PersistentPayloadTreapNode[T, P]{
		PersistentTreapNode: PersistentTreapNode[T]{
			Store:  stre,
			parent: parent,
		},
	}
	err := store.ReadGeneric(stre, tmp, objId)
	if err != nil {
		return nil, err
	}
	return tmp, nil
}

func (t *PersistentPayloadTreap[K, P]) Load(objId store.ObjectId) error {
	var err error
	t.root, err = NewPayloadFromObjectId[K, P](objId, &t.PersistentTreap, t.Store)
	return err
}

// Search searches for the node with the given key in the persistent treap.
func (t *PersistentPayloadTreap[K, P]) Search(key PersistentKey[K]) PersistentPayloadNodeInterface[K, P] {
	node := t.search(t.root, key.Value())
	if node == nil {
		return nil
	}
	n, ok := node.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		panic("Invalid type assertion in Search")
	}
	return n
}

// UpdatePayload updates the payload of the node with the given key.
func (t *PersistentPayloadTreap[K, P]) UpdatePayload(key PersistentKey[K], newPayload P) {
	node := t.Search(key)
	if node != nil && !node.IsNil() {
		payloadNode := node.(*PersistentPayloadTreapNode[K, P])
		payloadNode.SetPayload(newPayload)
		payloadNode.Persist()
	}
}

func (t *PersistentPayloadTreap[K, P]) Persist() error {
	if t.root != nil {
		rootNode := t.root.(*PersistentPayloadTreapNode[K, P])
		return rootNode.Persist()
	}
	return nil
}

func (n *PersistentPayloadTreapNode[K, P]) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	marshalled, err := n.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, marshalled)
}

func (k *PersistentPayloadTreapNode[K, P]) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}

func (n *PersistentPayloadTreapNode[K, P]) Persist() error {
	// Persist children first so their object IDs are available when marshaling the parent
	if n.GetLeft() != nil {
		leftNode := n.GetLeft().(*PersistentPayloadTreapNode[K, P])
		err := leftNode.Persist()
		if err != nil {
			return err
		}
	}
	if n.GetRight() != nil {
		rightNode := n.GetRight().(*PersistentPayloadTreapNode[K, P])
		err := rightNode.Persist()
		if err != nil {
			return err
		}
	}

	// Now marshal and persist this node
	objId, err := n.MarshalToObjectId(n.Store)
	if err != nil {
		return err
	}
	n.objectId = objId
	return nil
}

func (n *PersistentPayloadTreapNode[K, P]) sizeInBytes() int {
	objectIdSize := n.objectId.SizeInBytes()
	keySize := objectIdSize
	prioritySize := n.priority.SizeInBytes()
	leftSize := objectIdSize
	rightSize := objectIdSize
	selfSize := objectIdSize
	payloadSize := n.payload.SizeInBytes()
	return keySize + prioritySize + leftSize + rightSize + selfSize + payloadSize
}

// ObjectId returns the object ID of the node, allocating one if necessary.
// Messy Implementation - this is a repeat of the one for PersistentTreapNode
func (n *PersistentPayloadTreapNode[K, P]) ObjectId() store.ObjectId {
	if n == nil {
		return store.ObjNotAllocated
	}
	if n.objectId < 0 {
		objId, err := n.Store.NewObj(n.sizeInBytes())
		if err != nil {
			// FIXME Refactor method signature to return an error
			panic(err) // This should never happen
		}
		n.objectId = objId
	}
	return n.objectId
}

// Marshal should Return some byte slice representing the payload treap
func (t *PersistentPayloadTreap[K, P]) Marshal() ([]byte, error) {
	root := t.root.(*PersistentPayloadTreapNode[K, P])
	return root.objectId.Marshal()
}

// SizeInBytes returns the size in bytes of the marshalled treap (just the root ObjectId)
func (t *PersistentPayloadTreap[K, P]) SizeInBytes() int {
	var objId store.ObjectId
	return objId.SizeInBytes()
}

// Unmarshal implements the UntypedPersistentPayload interface
func (t *PersistentPayloadTreap[K, P]) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	var rootId store.ObjectId
	err := rootId.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	t.root, _ = NewPayloadFromObjectId[K, P](rootId, &t.PersistentTreap, t.Store)
	return t, nil
}
