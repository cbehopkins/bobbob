package yggdrasil

import (
	"encoding/binary"
	"errors"
	"math/rand"
)

// Priority represents the heap priority for a node in the treap.
// Higher priority nodes are rotated towards the root during insertion.
type Priority uint32

// SizeInBytes returns the size of the priority value in bytes.
func (p Priority) SizeInBytes() int {
	return 4
}

// Marshal encodes the priority to a 4-byte little-endian representation.
func (p Priority) Marshal() ([]byte, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(p))
	return buf, nil
}

// Unmarshal decodes the priority from a 4-byte little-endian representation.
func (p *Priority) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("invalid data length for Priority")
	}
	*p = Priority(binary.LittleEndian.Uint32(data[:4]))
	return nil
}

// randomPriority generates a random priority value using math/rand.
func randomPriority() Priority {
	return Priority(rand.Uint32())
}

// TreapNodeInterface defines the interface for a node in the treap data structure.
// A treap combines properties of a binary search tree (ordered by key) and a heap (ordered by priority).
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

// TreapNode represents a node in an in-memory treap.
// It maintains both a key (for BST ordering) and a priority (for heap ordering).
type TreapNode[T any] struct {
	key      Key[T]
	priority Priority
	left     TreapNodeInterface[T]
	right    TreapNodeInterface[T]
}

// GetKey returns the key of the node.
func (n *TreapNode[T]) GetKey() Key[T] {
	return n.key
}

// GetPriority returns the priority of the node.
func (n *TreapNode[T]) GetPriority() Priority {
	return n.priority
}

// SetPriority updates the priority of the node.
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
	n.left = left
}

// SetRight sets the right child of the node.
func (n *TreapNode[T]) SetRight(right TreapNodeInterface[T]) {
	n.right = right
}

// IsNil checks if the node is nil.
func (n *TreapNode[T]) IsNil() bool {
	return n == nil
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

// Treap represents a treap data structure.
type Treap[T any] struct {
	root TreapNodeInterface[T]
	Less func(a, b T) bool
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
		left := node.GetLeft()
		right := node.GetRight()

		if left == nil || left.IsNil() {
			return right
		}
		if right == nil || right.IsNil() {
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

// searchComplex searches for the node with the given key in the treap.
// It accepts an optional callback that is called when a node is accessed during the search.
func (t *Treap[T]) searchComplex(node TreapNodeInterface[T], key T, callback func(TreapNodeInterface[T])) TreapNodeInterface[T] {
	if node == nil || node.IsNil() {
		return node
	}

	// Call the callback if provided
	if callback != nil {
		callback(node)
	}

	currentKey := node.GetKey()
	if currentKey.Equals(key) {
		return node
	}

	if t.Less(key, currentKey.Value()) {
		return t.searchComplex(node.GetLeft(), key, callback)
	} else {
		return t.searchComplex(node.GetRight(), key, callback)
	}
}

// search searches for the node with the given key in the treap.
// It calls searchComplex with a nil callback.
func (t *Treap[T]) search(node TreapNodeInterface[T], key T) TreapNodeInterface[T] {
	return t.searchComplex(node, key, nil)
}

// InsertComplex inserts a new node with the given value and priority into the treap.
// The type T must implement the Key[T] interface (e.g., IntKey, StringKey).
// Use this method when you need to specify a custom priority value.
func (t *Treap[T]) InsertComplex(value T, priority Priority) {
	// Since T implements Key[T], we can use the value directly as the key
	key := any(value).(Key[T])
	newNode := NewTreapNode(key, priority)
	t.root = t.insert(t.root, newNode)
}

// Insert inserts a new node with the given value into the treap with a random priority.
// The type T must implement the Key[T] interface (e.g., IntKey, StringKey).
// This is the preferred method for most use cases.
func (t *Treap[T]) Insert(value T) {
	t.InsertComplex(value, randomPriority())
}

// Delete removes the node with the given value from the treap.
func (t *Treap[T]) Delete(value T) {
	// Since T implements Key[T], convert to get the comparable value
	key := any(value).(Key[T])
	t.root = t.delete(t.root, key.Value())
}

// SearchComplex searches for the node with the given value in the treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback receives the node that was accessed, allowing for custom operations
// such as updating access times for LRU caching.
func (t *Treap[T]) SearchComplex(value T, callback func(TreapNodeInterface[T])) TreapNodeInterface[T] {
	// Since T implements Key[T], convert to get the comparable value
	key := any(value).(Key[T])
	return t.searchComplex(t.root, key.Value(), callback)
}

// Search searches for the node with the given value in the treap.
// It calls SearchComplex with a nil callback.
func (t *Treap[T]) Search(value T) TreapNodeInterface[T] {
	return t.SearchComplex(value, nil)
}

// UpdatePriority updates the priority of the node with the given value.
// It does this by deleting the old node and inserting a new one with the new priority.
func (t *Treap[T]) UpdatePriority(value T, newPriority Priority) {
	node := t.Search(value)
	if node != nil && !node.IsNil() {
		node.SetPriority(newPriority)
		t.Delete(value)
		t.InsertComplex(value, newPriority)
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
