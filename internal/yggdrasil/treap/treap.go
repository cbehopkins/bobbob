// Package yggdrasil provides persistent treap (tree + heap) data structures
// with support for multiple collections, type safety, and efficient serialization.
//
// # Core Components
//
// Treap: A randomized binary search tree that uses heap properties for balancing.
// Each node has a key (for BST ordering) and a random priority (for heap property).
//
// PersistentTreap: A treap that stores nodes in a persistent store (file-backed).
// Supports efficient storage and retrieval of tree structures.
//
// PersistentPayloadTreap: A treap where each node can store an associated payload.
// Useful for key-value stores where values can be complex types.
//
// Vault: A multi-collection manager that coordinates multiple treaps in a single
// store file, with type registration and metadata management.
//
// # Type System
//
// TypeMap: Maps Go types to compact short codes for efficient serialization.
// Ensures type safety when loading data across sessions.
//
// Key Types: IntKey, StringKey, ShortUIntKey - wrapper types that implement
// PersistentKey interface for storage in treaps.
//
// # Usage Example
//
//	type UserData struct {
//	    Username string
//	    Email    string
//	}
//
//	// Create a vault with a store
//	stre, _ := store.NewBasicStore("data.db")
//	vault, _ := yggdrasil.LoadVault(stre)
//
//	// Register types
//	vault.RegisterType((*yggdrasil.StringKey)(new(string)))
//	vault.RegisterType(yggdrasil.JsonPayload[UserData]{})
//
//	// Create a collection
//	users, _ := yggdrasil.GetOrCreateCollection[yggdrasil.StringKey, yggdrasil.JsonPayload[UserData]](
//	    vault, "users", yggdrasil.StringLess, (*yggdrasil.StringKey)(new(string)),
//	)
//
//	// Insert data
//	key := yggdrasil.StringKey("alice")
//	users.Insert(&key, yggdrasil.JsonPayload[UserData]{
//	    Value: UserData{Username: "alice", Email: "alice@example.com"},
//	})
//
//	// Save and close
//	vault.Close()
package treap

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

// TreapNodeInterface has been moved to interfaces.go

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
func (n *TreapNode[T]) SetLeft(left TreapNodeInterface[T]) error {
	n.left = left
	return nil
}

// SetRight sets the right child of the node.
func (n *TreapNode[T]) SetRight(right TreapNodeInterface[T]) error {
	n.right = right
	return nil
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
	_ = node.SetLeft(newRoot.GetRight())
	_ = newRoot.SetRight(node)
	return newRoot
}

// rotateLeft performs a left rotation on the given node.
func (t *Treap[T]) rotateLeft(node TreapNodeInterface[T]) TreapNodeInterface[T] {
	newRoot := node.GetRight()
	_ = node.SetRight(newRoot.GetLeft())
	_ = newRoot.SetLeft(node)
	return newRoot
}

// insert is a helper function that inserts a new node into the treap.
func (t *Treap[T]) insert(node TreapNodeInterface[T], newNode TreapNodeInterface[T]) TreapNodeInterface[T] {
	if node == nil || node.IsNil() {
		return newNode
	}

	if t.Less(newNode.GetKey().Value(), node.GetKey().Value()) {
		_ = node.SetLeft(t.insert(node.GetLeft(), newNode))
		if node.GetLeft() != nil && node.GetLeft().GetPriority() > node.GetPriority() {
			node = t.rotateRight(node)
		}
	} else {
		_ = node.SetRight(t.insert(node.GetRight(), newNode))
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
		_ = node.SetLeft(t.delete(node.GetLeft(), key))
	} else if t.Less(node.GetKey().Value(), key) {
		_ = node.SetRight(t.delete(node.GetRight(), key))
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
			_ = node.SetRight(t.delete(node.GetRight(), key))
		} else {
			node = t.rotateLeft(node)
			_ = node.SetLeft(t.delete(node.GetLeft(), key))
		}
	}

	return node
}

// searchComplex searches for the node with the given key in the treap.
// It accepts an optional callback that is called when a node is accessed during the search.
// The callback can return an error to abort the search.
func (t *Treap[T]) searchComplex(node TreapNodeInterface[T], key T, callback func(TreapNodeInterface[T]) error) (TreapNodeInterface[T], error) {
	if node == nil || node.IsNil() {
		return node, nil
	}

	// Call the callback if provided
	if callback != nil {
		err := callback(node)
		if err != nil {
			return nil, err
		}
	}

	currentKey := node.GetKey()
	if currentKey.Equals(key) {
		return node, nil
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
	result, _ := t.searchComplex(node, key, nil)
	return result
} // InsertComplex inserts a new node with the given value and priority into the treap.
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
// The callback can return an error to abort the search.
func (t *Treap[T]) SearchComplex(value T, callback func(TreapNodeInterface[T]) error) (TreapNodeInterface[T], error) {
	// Since T implements Key[T], convert to get the comparable value
	key := any(value).(Key[T])
	return t.searchComplex(t.root, key.Value(), callback)
}

// Search searches for the node with the given value in the treap.
// It calls SearchComplex with a nil callback.
func (t *Treap[T]) Search(value T) TreapNodeInterface[T] {
	result, _ := t.SearchComplex(value, nil)
	return result
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
