package treap

import (
	"fmt"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// PayloadTreapNode represents a node in the PayloadTreap.
type PayloadTreapNode[K any, P any] struct {
	TreapNode[K]
	left    *PayloadTreapNode[K, P]
	right   *PayloadTreapNode[K, P]
	payload P
}

// GetLeft returns the left child of the node.
func (n *PayloadTreapNode[K, P]) GetLeft() TreapNodeInterface[K] {
	return n.left
}

// GetRight returns the right child of the node.
func (n *PayloadTreapNode[K, P]) GetRight() TreapNodeInterface[K] {
	return n.right
}

// SetLeft sets the left child of the node.
func (n *PayloadTreapNode[K, P]) SetLeft(left TreapNodeInterface[K]) error {
	if left == nil {
		n.left = nil
		return nil
	}
	tmp, ok := left.(*PayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("left child is not a PayloadTreapNode")
	}
	n.left = tmp
	return nil
}

// SetRight sets the right child of the node.
func (n *PayloadTreapNode[K, P]) SetRight(right TreapNodeInterface[K]) error {
	if right == nil {
		n.right = nil
		return nil
	}
	tmp, ok := right.(*PayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("right child is not a PayloadTreapNode")
	}
	n.right = tmp
	return nil
}

// IsNil checks if the node is nil.
func (n *PayloadTreapNode[K, P]) IsNil() bool {
	return n == nil
}

// GetPayload returns the payload of the node.
func (n *PayloadTreapNode[K, P]) GetPayload() P {
	return n.payload
}

// SetPayload sets the payload of the node.
func (n *PayloadTreapNode[K, P]) SetPayload(payload P) {
	n.payload = payload
}

// PayloadTreap represents a treap with payloads.
type PayloadTreap[K any, P any] struct {
	Treap[K]
}

// NewPayloadTreapNode creates a new PayloadTreapNode with the given key, priority, and payload.
func NewPayloadTreapNode[K any, P any](key types.Key[K], priority Priority, payload P) *PayloadTreapNode[K, P] {
	return &PayloadTreapNode[K, P]{
		TreapNode: TreapNode[K]{
			key:      key,
			priority: priority,
		},
		left:    nil,
		right:   nil,
		payload: payload,
	}
}

// NewPayloadTreap creates a new PayloadTreap with the given comparison function.
func NewPayloadTreap[K any, P any](lessFunc func(a, b K) bool) *PayloadTreap[K, P] {
	return &PayloadTreap[K, P]{
		Treap: Treap[K]{
			root: nil,
			Less: lessFunc,
		},
	}
}

// InsertComplex inserts a new node with the given value, priority, and payload into the treap.
// The type K must implement the types.Key[K] interface (e.g., IntKey, StringKey).
// Use this method when you need to specify a custom priority value.
func (t *PayloadTreap[K, P]) InsertComplex(value K, priority Priority, payload P) {
	// Since K implements types.Key[K], convert to use as key
	key := any(value).(types.Key[K])
	if existing := SearchNode(t.Treap.root, key.Value(), t.Treap.Less); existing != nil && !existing.IsNil() {
		if payloadNode, ok := existing.(*PayloadTreapNode[K, P]); ok {
			payloadNode.SetPayload(payload)
			return
		}
	}
	newNode := NewPayloadTreapNode(key, priority, payload)
	inserted, err := InsertNode(t.Treap.root, newNode, t.Treap.Less, nil)
	if err != nil {
		panic(err)
	}
	t.Treap.root = inserted
}

// Insert inserts a new node with the given value and payload into the treap with a random priority.
// This is the preferred method for most use cases.
func (t *PayloadTreap[K, P]) Insert(value K, payload P) {
	t.InsertComplex(value, randomPriority(), payload)
}

// Delete removes the node with the given value from the payload treap.
func (t *PayloadTreap[K, P]) Delete(value K) {
	key := any(value).(types.Key[K])
	deleted, err := DeleteNode(t.Treap.root, key.Value(), t.Treap.Less, nil)
	if err != nil {
		panic(err)
	}
	t.Treap.root = deleted
}

// SearchComplex searches for the node with the given value in the payload treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback can return an error to abort the search.
func (t *PayloadTreap[K, P]) SearchComplex(value K, callback func(TreapNodeInterface[K]) error) (TreapNodeInterface[K], error) {
	key := any(value).(types.Key[K])
	return SearchNodeComplex(t.Treap.root, key.Value(), t.Treap.Less, callback)
}

// Search searches for the node with the given value in the payload treap.
// It calls SearchComplex with a nil callback.
func (t *PayloadTreap[K, P]) Search(value K) TreapNodeInterface[K] {
	result, _ := t.SearchComplex(value, nil)
	return result
}

// UpdatePayload updates the payload for the given key.
// Returns an error if the key is not found.
func (t *PayloadTreap[K, P]) UpdatePayload(value K, newPayload P) error {
	key := any(value).(types.Key[K])
	node := SearchNode(t.Treap.root, key.Value(), t.Treap.Less)
	if node == nil || node.IsNil() {
		return fmt.Errorf("key not found")
	}
	if payloadNode, ok := node.(*PayloadTreapNode[K, P]); ok {
		payloadNode.SetPayload(newPayload)
		return nil
	}
	return fmt.Errorf("node type mismatch")
}
