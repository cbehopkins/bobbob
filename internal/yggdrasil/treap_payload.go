package yggdrasil

import "fmt"

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
func NewPayloadTreapNode[K any, P any](key Key[K], priority Priority, payload P) *PayloadTreapNode[K, P] {
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
// The type K must implement the Key[K] interface (e.g., IntKey, StringKey).
// Use this method when you need to specify a custom priority value.
func (t *PayloadTreap[K, P]) InsertComplex(value K, priority Priority, payload P) {
	// Since K implements Key[K], convert to use as key
	key := any(value).(Key[K])
	newNode := NewPayloadTreapNode(key, priority, payload)
	t.Treap.root = t.Treap.insert(t.Treap.root, newNode)
}

// Insert inserts a new node with the given value and payload into the treap with a random priority.
// This is the preferred method for most use cases.
func (t *PayloadTreap[K, P]) Insert(value K, payload P) {
	t.InsertComplex(value, randomPriority(), payload)
}
