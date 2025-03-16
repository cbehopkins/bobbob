package yggdrasil

type Payload any

// PayloadTreapNode represents a node in the PayloadTreap.
type PayloadTreapNode struct {
	TreapNode
	left    *PayloadTreapNode
	right   *PayloadTreapNode
	payload Payload
}

// GetLeft returns the left child of the node.
func (n *PayloadTreapNode) GetLeft() TreapNodeInterface {
	return n.left
}

// GetRight returns the right child of the node.
func (n *PayloadTreapNode) GetRight() TreapNodeInterface {
	return n.right
}

// SetLeft sets the left child of the node.
func (n *PayloadTreapNode) SetLeft(left TreapNodeInterface) {
	n.left = left.(*PayloadTreapNode)
}

// SetRight sets the right child of the node.
func (n *PayloadTreapNode) SetRight(right TreapNodeInterface) {
	n.right = right.(*PayloadTreapNode)
}

// IsNil checks if the node is nil.
func (n *PayloadTreapNode) IsNil() bool {
	return n == nil
}

// PayloadTreap represents a treap with payloads.
type PayloadTreap struct {
	Treap
}

// NewPayloadTreapNode creates a new PayloadTreapNode with the given key, priority, and payload.
func NewPayloadTreapNode(key Key, priority Priority, payload Payload) *PayloadTreapNode {
	return &PayloadTreapNode{
		TreapNode: TreapNode{
			key:      key,
			priority: priority,
		},
		left:    nil,
		right:   nil,
		payload: payload,
	}
}

// NewPayloadTreap creates a new PayloadTreap with the given comparison function.
func NewPayloadTreap(lessFunc func(a, b any) bool) *PayloadTreap {
	return &PayloadTreap{
		Treap: Treap{
			root: nil,
			Less: lessFunc,
		},
	}
}

// Insert inserts a new node with the given key, priority, and payload into the treap.
func (t *PayloadTreap) Insert(key Key, priority Priority, payload Payload) {
	newNode := NewPayloadTreapNode(key, priority, payload)
	t.root = t.insert(t.root, newNode)
}
