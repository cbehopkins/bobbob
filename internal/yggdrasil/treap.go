package yggdrasil

import (
	"encoding/binary"
	"errors"
)

type Priority uint32

func (p Priority) SizeInBytes() int {
	return 4
}

func (p Priority) Marshal() ([]byte, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(p))
	return buf, nil
}

func (p *Priority) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("invalid data length for Priority")
	}
	*p = Priority(binary.LittleEndian.Uint32(data[:4]))
	return nil
}

// TreapNodeInterface defines the interface for a node in the treap.
type TreapNodeInterface[T any] interface {
	GetKey() Key[T]
	GetPriority() Priority
	SetPriority(Priority)
	GetLeft() TreapNodeInterface[T]
	GetRight() TreapNodeInterface[T]
	SetLeft(TreapNodeInterface[T])
	SetRight(TreapNodeInterface[T])
	IsNil() bool
}

// TreapNode represents a node in the treap.
type TreapNode[T any] struct {
	key      Key[T]
	priority Priority
	left     *TreapNode[T]
	right    *TreapNode[T]
}

// GetKey returns the key of the node.
func (n *TreapNode[T]) GetKey() Key[T] {
	return n.key
}

// GetPriority returns the priority of the node.
func (n *TreapNode[T]) GetPriority() Priority {
	return n.priority
}

func (n *TreapNode[T]) SetPriority(p Priority) {
	n.priority = p
}

// GetLeft returns the left child of the node.
func (n *TreapNode[T]) GetLeft() TreapNodeInterface[T] {
	return n.left
}

// GetRight returns the right child of the node.
func (n *TreapNode[T]) GetRight() TreapNodeInterface[T] {
	return n.right
}

// SetLeft sets the left child of the node.
func (n *TreapNode[T]) SetLeft(left TreapNodeInterface[T]) {
	if left == nil {
		n.left = nil
		return
	}
	n.left = left.(*TreapNode[T])
}

// SetRight sets the right child of the node.
func (n *TreapNode[T]) SetRight(right TreapNodeInterface[T]) {
	if right == nil {
		n.right = nil
		return
	}
	n.right = right.(*TreapNode[T])
}

// IsNil checks if the node is nil.
func (n *TreapNode[T]) IsNil() bool {
	return n == nil
}

// Treap represents a treap data structure.
type Treap[T any] struct {
	root TreapNodeInterface[T]
	Less func(a, b T) bool
}

// NewTreapNode creates a new TreapNode with the given key and priority.
func NewTreapNode[T any](key Key[T], priority Priority) *TreapNode[T] {
	return &TreapNode[T]{
		key:      key,
		priority: priority,
		left:     nil,
		right:    nil,
	}
}

// NewTreap creates a new Treap with the given comparison function.
func NewTreap[T any](lessFunc func(a, b T) bool) *Treap[T] {
	return &Treap[T]{
		root: nil,
		Less: lessFunc,
	}
}

// rotateRight performs a right rotation on the given node.
func (t *Treap[T]) rotateRight(node TreapNodeInterface[T]) TreapNodeInterface[T] {
	newRoot := node.GetLeft()
	node.SetLeft(newRoot.GetRight())
	newRoot.SetRight(node)
	return newRoot
}

// rotateLeft performs a left rotation on the given node.
func (t *Treap[T]) rotateLeft(node TreapNodeInterface[T]) TreapNodeInterface[T] {
	newRoot := node.GetRight()
	node.SetRight(newRoot.GetLeft())
	newRoot.SetLeft(node)
	return newRoot
}

// insert is a helper function that inserts a new node into the treap.
func (t *Treap[T]) insert(node TreapNodeInterface[T], newNode TreapNodeInterface[T]) TreapNodeInterface[T] {
	if node == nil || node.IsNil() {
		return newNode
	}

	if t.Less(newNode.GetKey().Value(), node.GetKey().Value()) {
		node.SetLeft(t.insert(node.GetLeft(), newNode))
		if node.GetLeft() != nil && node.GetLeft().GetPriority() > node.GetPriority() {
			node = t.rotateRight(node)
		}
	} else {
		node.SetRight(t.insert(node.GetRight(), newNode))
		if node.GetRight() != nil && node.GetRight().GetPriority() > node.GetPriority() {
			node = t.rotateLeft(node)
		}
	}

	return node
}

// delete removes the node with the given key from the treap.
func (t *Treap[T]) delete(node TreapNodeInterface[T], key T) TreapNodeInterface[T] {
	if node == nil || node.IsNil() {
		return nil
	}

	if t.Less(key, node.GetKey().Value()) {
		node.SetLeft(t.delete(node.GetLeft(), key))
	} else if t.Less(node.GetKey().Value(), key) {
		node.SetRight(t.delete(node.GetRight(), key))
	} else {
		left := node.GetLeft().(*TreapNode[T])
		right := node.GetRight().(*TreapNode[T])

		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		leftPriority := left.GetPriority()
		rightPriority := right.GetPriority()
		if leftPriority > rightPriority {
			node = t.rotateRight(node)
			node.SetRight(t.delete(node.GetRight(), key))
		} else {
			node = t.rotateLeft(node)
			node.SetLeft(t.delete(node.GetLeft(), key))
		}
	}

	return node
}

// search searches for the node with the given key in the treap.
func (t *Treap[T]) search(node TreapNodeInterface[T], key T) TreapNodeInterface[T] {
	if node.IsNil() {
		return node
	}
	currentKey := node.GetKey()
	if currentKey.Equals(key) {
		return node
	}

	if t.Less(key, currentKey.Value()) {
		return t.search(node.GetLeft(), key)
	} else {
		return t.search(node.GetRight(), key)
	}
}

// Insert inserts a new node with the given key and priority into the treap.
func (t *Treap[T]) Insert(key Key[T], priority Priority) {
	newNode := NewTreapNode(key, priority)
	t.root = t.insert(t.root, newNode)
}

// Delete removes the node with the given key from the treap.
func (t *Treap[T]) Delete(key Key[T]) {
	t.root = t.delete(t.root, key.Value())
}

// Search searches for the node with the given key in the treap.
func (t *Treap[T]) Search(key Key[T]) TreapNodeInterface[T] {
	return t.search(t.root, key.Value())
}

// UpdatePriority updates the priority of the node with the given key.
func (t *Treap[T]) UpdatePriority(key Key[T], newPriority Priority) {
	node := t.Search(key)
	if node != nil && !node.IsNil() {
		node.SetPriority(newPriority)
		t.Delete(key)
		t.Insert(key, newPriority)
	}
}

// Walk traverses the treap in order and calls the callback function on each node.
func (t *Treap[T]) Walk(callback func(TreapNodeInterface[T])) {
	var walk func(node TreapNodeInterface[T])
	walk = func(node TreapNodeInterface[T]) {
		if node == nil || node.IsNil() {
			return
		}
		walk(node.GetLeft())
		callback(node)
		walk(node.GetRight())
	}
	walk(t.root)
}

// WalkReverse traverses the treap in reverse order and calls the callback function on each node.
func (t *Treap[T]) WalkReverse(callback func(TreapNodeInterface[T])) {
	var walkReverse func(node TreapNodeInterface[T])
	walkReverse = func(node TreapNodeInterface[T]) {
		if node == nil || node.IsNil() {
			return
		}
		walkReverse(node.GetRight())
		callback(node)
		walkReverse(node.GetLeft())
	}
	walkReverse(t.root)
}
