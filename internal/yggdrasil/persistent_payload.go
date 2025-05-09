package yggdrasil

import (
	"bobbob/internal/store"
)

type PersistentPayload[T any] interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) (T, error) // Updated to return the concrete type
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
	leftNode := n.PersistentTreapNode.GetLeft()
	if leftNode == nil {
		return nil
	}
	return leftNode.(*PersistentPayloadTreapNode[K, P])
}

// GetRight returns the right child of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetRight() TreapNodeInterface[K] {
	rightNode := n.PersistentTreapNode.GetRight()
	if rightNode == nil {
		return nil
	}
	tmp, ok := rightNode.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		panic("Invalid type assertion for right child")
	}
	return tmp
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

// Unmarshal overrides the Unmarshal method to include the payload.
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
	n.payload = val

	return nil
}

func (n *PersistentPayloadTreapNode[K, P]) Unmarshal(data []byte) error {
	return n.unmarshal(data, n.PersistentTreapNode.parent.keyTemplate)
}

// SetObjectId sets the object ID of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetObjectId(id store.ObjectId) {
	n.objectId = id
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
	t.root = t.insert(t.root, newNode)
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
