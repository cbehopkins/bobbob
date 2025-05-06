package yggdrasil

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
func (n *PayloadTreapNode[K, P]) SetLeft(left TreapNodeInterface[K]) {
	n.left = left.(*PayloadTreapNode[K, P])
}

// SetRight sets the right child of the node.
func (n *PayloadTreapNode[K, P]) SetRight(right TreapNodeInterface[K]) {
	n.right = right.(*PayloadTreapNode[K, P])
}

// IsNil checks if the node is nil.
func (n *PayloadTreapNode[K, P]) IsNil() bool {
	return n == nil
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

// Insert inserts a new node with the given key, priority, and payload into the treap.
func (t *PayloadTreap[K, P]) Insert(key Key[K], priority Priority, payload P) {
	newNode := NewPayloadTreapNode(key, priority, payload)
	t.Treap.root = t.Treap.insert(t.Treap.root, newNode)
}
