package yggdrasil

import (
	"encoding/binary"
	"errors"

	"github.com/cbehopkins/bobbob/internal/store"
)

type Key interface {
	SizeInBytes() int
	GetObjectId(store.Storer) store.ObjectId
}

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
type TreapNodeInterface interface {
	GetKey() Key
	GetPriority() Priority
	SetPriority(Priority)
	GetLeft() TreapNodeInterface
	GetRight() TreapNodeInterface
	SetLeft(TreapNodeInterface)
	SetRight(TreapNodeInterface)
    IsNil() bool
}

// TreapNode represents a node in the treap.
type TreapNode struct {
	key      Key
	priority Priority
	left     *TreapNode
	right    *TreapNode
}

// GetKey returns the key of the node.
func (n *TreapNode) GetKey() Key {
	return n.key
}

// GetPriority returns the priority of the node.
func (n *TreapNode) GetPriority() Priority {
	return n.priority
}
func (n *TreapNode) SetPriority(p Priority) {
    n.priority = p
}

// GetLeft returns the left child of the node.
func (n *TreapNode) GetLeft() TreapNodeInterface {
	return n.left
}

// GetRight returns the right child of the node.
func (n *TreapNode) GetRight() TreapNodeInterface {
	return n.right
}

// SetLeft sets the left child of the node.
func (n *TreapNode) SetLeft(left TreapNodeInterface) {
    if left == nil {
        n.left = nil
        return
    }
	n.left = left.(*TreapNode)
}

// SetRight sets the right child of the node.
func (n *TreapNode) SetRight(right TreapNodeInterface) {
    if right == nil {
        n.right = nil
        return
    }
	n.right = right.(*TreapNode)
}

// IsNil checks if the node is nil.
func (n *TreapNode) IsNil() bool {
	return n == nil
}

// Treap represents a treap data structure.
type Treap struct {
	root TreapNodeInterface
	Less func(a, b any) bool
}

// NewTreapNode creates a new TreapNode with the given key and priority.
func NewTreapNode(key Key, priority Priority) *TreapNode {
	return &TreapNode{
		key:      key,
		priority: priority,
		left:     nil,
		right:    nil,
	}
}

// NewTreap creates a new Treap with the given comparison function.
func NewTreap(lessFunc func(a, b any) bool) *Treap {
	return &Treap{
		root: nil,
		Less: lessFunc,
	}
}

// rotateRight performs a right rotation on the given node.
func (t *Treap) rotateRight(node TreapNodeInterface) TreapNodeInterface {
	newRoot := node.GetLeft()
	node.SetLeft(newRoot.GetRight())
	newRoot.SetRight(node)
	return newRoot
}

// rotateLeft performs a left rotation on the given node.
func (t *Treap) rotateLeft(node TreapNodeInterface) TreapNodeInterface {
	newRoot := node.GetRight()
	node.SetRight(newRoot.GetLeft())
	newRoot.SetLeft(node)
	return newRoot
}

// insert is a helper function that inserts a new node into the treap.
func (t *Treap) insert(node TreapNodeInterface, newNode TreapNodeInterface) TreapNodeInterface {
	if node == nil || node.IsNil() {
		return newNode
	}

	if t.Less(newNode.GetKey(), node.GetKey()) {
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
func (t *Treap) delete(node TreapNodeInterface, key any) TreapNodeInterface {
	if node == nil || node.IsNil() {
		return nil
	}

	if t.Less(key, node.GetKey()) {
		node.SetLeft(t.delete(node.GetLeft(), key))
	} else if t.Less(node.GetKey(), key) {
		node.SetRight(t.delete(node.GetRight(), key))
	} else {
        left := node.GetLeft().(*TreapNode)
        right := node.GetRight().(*TreapNode)

		if left == nil {
			return right
		} 
        if right == nil {
			return left
		}
        leftPriority:= left.GetPriority()
        rightPriority:= right.GetPriority()
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
func (t *Treap) search(node TreapNodeInterface, key any) TreapNodeInterface {
	if node == nil || node.IsNil() {
		return node
	}
	currentKey := node.GetKey()
	if currentKey == key {
		return node
	}

	if t.Less(key, currentKey) {
		return t.search(node.GetLeft(), key)
	} else {
		return t.search(node.GetRight(), key)
	}
}

// Insert inserts a new node with the given key and priority into the treap.
func (t *Treap) Insert(key Key, priority Priority) {
	newNode := NewTreapNode(key, priority)
	t.root = t.insert(t.root, newNode)
}

// Delete removes the node with the given key from the treap.
func (t *Treap) Delete(key Key) {
	t.root = t.delete(t.root, key)
}

// Search searches for the node with the given key in the treap.
func (t *Treap) Search(key Key) TreapNodeInterface {
	return t.search(t.root, key)
}

// UpdatePriority updates the priority of the node with the given key.
func (t *Treap) UpdatePriority(key Key, newPriority Priority) {
	node := t.Search(key)
	if node != nil && !node.IsNil() {
		node.SetPriority(newPriority)
		t.Delete(key)
		t.Insert(key, newPriority)
	}
}

// Walk traverses the treap in order and calls the callback function on each node.
func (t *Treap) Walk(callback func(TreapNodeInterface)) {
	var walk func(node TreapNodeInterface)
	walk = func(node TreapNodeInterface) {
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
func (t *Treap) WalkReverse(callback func(TreapNodeInterface)) {
	var walkReverse func(node TreapNodeInterface)
	walkReverse = func(node TreapNodeInterface) {
		if node == nil || node.IsNil() {
			return
		}
		walkReverse(node.GetRight())
		callback(node)
		walkReverse(node.GetLeft())
	}
	walkReverse(t.root)
}
