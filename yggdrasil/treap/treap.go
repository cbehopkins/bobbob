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

// TreeCallback is a callback function invoked during tree traversal.
// It receives a node and can return an error to halt traversal.
type TreeCallback[T any] func(TreapNodeInterface[T]) error

// inOrderWalk is a shared helper that performs in-order traversal of a tree.
// It works with any tree that implements TreapNodeInterface[T].
// The callback is invoked for each node; returning an error halts traversal.
func inOrderWalk[T any](node TreapNodeInterface[T], callback TreeCallback[T]) error {
	if node == nil || node.IsNil() {
		return nil
	}

	// In-order: left, node, right
	if err := inOrderWalk(node.GetLeft(), callback); err != nil {
		return err
	}

	if err := callback(node); err != nil {
		return err
	}

	if err := inOrderWalk(node.GetRight(), callback); err != nil {
		return err
	}

	return nil
}

// reverseOrderWalk is a shared helper that performs reverse in-order traversal of a tree.
// It works with any tree that implements TreapNodeInterface[T].
// The callback is invoked for each node; returning an error halts traversal.
func reverseOrderWalk[T any](node TreapNodeInterface[T], callback TreeCallback[T]) error {
	if node == nil || node.IsNil() {
		return nil
	}

	// Reverse in-order: right, node, left
	if err := reverseOrderWalk(node.GetRight(), callback); err != nil {
		return err
	}

	if err := callback(node); err != nil {
		return err
	}

	if err := reverseOrderWalk(node.GetLeft(), callback); err != nil {
		return err
	}

	return nil
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

	newKey := newNode.GetKey().Value()
	nodeKey := node.GetKey().Value()

	// Check for exact key match - don't insert duplicate
	if !t.Less(newKey, nodeKey) && !t.Less(nodeKey, newKey) {
		// Keys are equal - return existing node without adding duplicate
		return node
	}

	if t.Less(newKey, nodeKey) {
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

// Insert inserts a new node with the given value into the treap.
// The type T must implement the Key[T] interface (e.g., IntKey, StringKey).
// If the value implements PriorityProvider, its Priority() method is used;
// otherwise, a random priority is generated.
// This is the preferred method for most use cases.
func (t *Treap[T]) Insert(value T) {
	var priority Priority
	if pp, ok := any(value).(PriorityProvider); ok {
		priority = pp.Priority()
	} else {
		priority = randomPriority()
	}
	t.InsertComplex(value, priority)
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
	// Ignore errors since callback doesn't return error
	_ = inOrderWalk(t.root, func(node TreapNodeInterface[T]) error {
		callback(node)
		return nil
	})
}

// WalkReverse traverses the treap in reverse order and calls the callback function on each node.
func (t *Treap[T]) WalkReverse(callback func(TreapNodeInterface[T])) {
	// Ignore errors since callback doesn't return error
	_ = reverseOrderWalk(t.root, func(node TreapNodeInterface[T]) error {
		callback(node)
		return nil
	})
}

// Compare compares this treap with another treap and invokes callbacks for keys that are:
// - Only in this treap (onlyInA)
// - In both treaps (inBoth)
// - Only in the other treap (onlyInB)
//
// The comparison is done by traversing both treaps in sorted order.
// All three callbacks are optional (can be nil).
//
// Example usage:
//
//	treapA.Compare(treapB,
//	    func(node TreapNodeInterface[T]) error {
//	        fmt.Printf("Only in A: %v\n", node.GetKey().Value())
//	        return nil
//	    },
//	    func(nodeA, nodeB TreapNodeInterface[T]) error {
//	        fmt.Printf("In both: %v\n", nodeA.GetKey().Value())
//	        return nil
//	    },
//	    func(node TreapNodeInterface[T]) error {
//	        fmt.Printf("Only in B: %v\n", node.GetKey().Value())
//	        return nil
//	    },
//	)
func (t *Treap[T]) Compare(
	other *Treap[T],
	onlyInA func(TreapNodeInterface[T]) error,
	inBoth func(nodeA, nodeB TreapNodeInterface[T]) error,
	onlyInB func(TreapNodeInterface[T]) error,
) error {
	// Stream both trees via in-order walks without buffering entire trees.
	nodeChA, errChA, cancelA := newInOrderStream(t.root)
	nodeChB, errChB, cancelB := newInOrderStream(other.root)
	defer cancelA()
	defer cancelB()

	nextA := channelNext(nodeChA, errChA)
	nextB := channelNext(nodeChB, errChB)

	return mergeOrdered(nextA, nextB, t.Less, onlyInA, inBoth, onlyInB)
}

var errWalkCanceled = errors.New("walk canceled")

// channelNext adapts a streamed node channel into an iterator-style next function.
// It surfaces walker errors (including cancellation-as-nil) when the channel is exhausted.
func channelNext[T any](nodeCh <-chan TreapNodeInterface[T], errCh <-chan error) func() (TreapNodeInterface[T], bool, error) {
	var done bool
	return func() (TreapNodeInterface[T], bool, error) {
		if done {
			return nil, false, nil
		}
		node, ok := <-nodeCh
		if ok {
			return node, true, nil
		}
		done = true
		err := <-errCh
		return nil, false, err
	}
}

// mergeOrdered drives a merge-style comparison over two in-order iterators.
// Iterators must yield nodes in ascending key order. less is the key comparator.
func mergeOrdered[T any](
	nextA func() (TreapNodeInterface[T], bool, error),
	nextB func() (TreapNodeInterface[T], bool, error),
	less func(a, b T) bool,
	onlyInA func(TreapNodeInterface[T]) error,
	inBoth func(nodeA, nodeB TreapNodeInterface[T]) error,
	onlyInB func(TreapNodeInterface[T]) error,
) error {
	nodeA, okA, err := nextA()
	if err != nil {
		return err
	}
	nodeB, okB, err := nextB()
	if err != nil {
		return err
	}

	for okA || okB {
		switch {
		case !okA:
			if onlyInB != nil {
				if err := onlyInB(nodeB); err != nil {
					return err
				}
			}
			nodeB, okB, err = nextB()
			if err != nil {
				return err
			}
		case !okB:
			if onlyInA != nil {
				if err := onlyInA(nodeA); err != nil {
					return err
				}
			}
			nodeA, okA, err = nextA()
			if err != nil {
				return err
			}
		default:
			keyA := nodeA.GetKey()
			keyB := nodeB.GetKey()

			if keyA.Equals(keyB.Value()) {
				if inBoth != nil {
					if err := inBoth(nodeA, nodeB); err != nil {
						return err
					}
				}
				nodeA, okA, err = nextA()
				if err != nil {
					return err
				}
				nodeB, okB, err = nextB()
				if err != nil {
					return err
				}
			} else if less(keyA.Value(), keyB.Value()) {
				if onlyInA != nil {
					if err := onlyInA(nodeA); err != nil {
						return err
					}
				}
				nodeA, okA, err = nextA()
				if err != nil {
					return err
				}
			} else {
				if onlyInB != nil {
					if err := onlyInB(nodeB); err != nil {
						return err
					}
				}
				nodeB, okB, err = nextB()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// newInOrderStream starts an in-order walk and streams nodes over a channel.
// Cancellation is respected via the returned cancel function.
func newInOrderStream[T any](root TreapNodeInterface[T]) (chan TreapNodeInterface[T], chan error, func()) {
	nodeCh := make(chan TreapNodeInterface[T], 1)
	errCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(nodeCh)
		defer close(errCh)

		err := inOrderWalk(root, func(node TreapNodeInterface[T]) error {
			select {
			case <-done:
				return errWalkCanceled
			case nodeCh <- node:
				return nil
			}
		})

		if err == errWalkCanceled {
			err = nil
		}

		errCh <- err
	}()

	cancel := func() { close(done) }
	return nodeCh, errCh, cancel
}
