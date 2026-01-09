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
// types.PersistentKey interface for storage in treaps.
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
	"iter"
	"math/rand"
	"sync"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
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
	key      types.Key[T]
	priority Priority
	left     TreapNodeInterface[T]
	right    TreapNodeInterface[T]
}

// GetKey returns the key of the node.
func (n *TreapNode[T]) GetKey() types.Key[T] {
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
func NewTreapNode[T any](key types.Key[T], priority Priority) *TreapNode[T] {
	return &TreapNode[T]{
		key:      key,
		priority: priority,
		left:     nil,
		right:    nil,
	}
}

// Treap represents a treap data structure.
type Treap[T any] struct {
	root     TreapNodeInterface[T]
	Less     func(a, b T) bool
	nodePool sync.Pool
}

// NewTreap creates a new Treap with the given comparison function.
func NewTreap[T any](lessFunc func(a, b T) bool) *Treap[T] {
	return &Treap[T]{
		root:     nil,
		Less:     lessFunc,
		nodePool: sync.Pool{New: func() any { return new(TreapNode[T]) }},
	}
}

func (t *Treap[T]) newNode(key types.Key[T], priority Priority) *TreapNode[T] {
	v := t.nodePool.Get()
	n, _ := v.(*TreapNode[T])
	if n == nil {
		n = &TreapNode[T]{}
	}
	n.key = key
	n.priority = priority
	n.left = nil
	n.right = nil
	return n
}

func (t *Treap[T]) releasePlainNode(n *TreapNode[T]) {
	if n == nil {
		return
	}
	n.key = nil
	n.priority = 0
	n.left = nil
	n.right = nil
	t.nodePool.Put(n)
}

func (t *Treap[T]) releaseNode(node TreapNodeInterface[T]) {
	if node == nil || node.IsNil() {
		return
	}
	if plain, ok := node.(*TreapNode[T]); ok {
		t.releasePlainNode(plain)
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
		// Return newNode to pool if it's a pooled type
		t.releaseNode(newNode)
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
			// Release the removed node back to pool
			t.releaseNode(node)
			return right
		}
		if right == nil || right.IsNil() {
			// Release the removed node back to pool
			t.releaseNode(node)
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
// The type T must implement the types.Key[T] interface (e.g., IntKey, StringKey).
// Use this method when you need to specify a custom priority value.
func (t *Treap[T]) InsertComplex(value T, priority Priority) {
	// Since T implements types.Key[T], we can use the value directly as the key
	key := any(value).(types.Key[T])
	newNode := t.newNode(key, priority)
	t.root = t.insert(t.root, newNode)
}

// Insert inserts a new node with the given value into the treap.
// The type T must implement the types.Key[T] interface (e.g., IntKey, StringKey).
// If the value implements types.PriorityProvider, its Priority() method is used;
// otherwise, a random priority is generated.
// This is the preferred method for most use cases.
func (t *Treap[T]) Insert(value T) {
	var priority Priority
	if pp, ok := any(value).(types.PriorityProvider); ok {
		priority = Priority(pp.Priority())
	} else {
		priority = randomPriority()
	}
	t.InsertComplex(value, priority)
}

// Delete removes the node with the given value from the treap.
func (t *Treap[T]) Delete(value T) {
	// Since T implements types.Key[T], convert to get the comparable value
	key := any(value).(types.Key[T])
	t.root = t.delete(t.root, key.Value())
}

// SearchComplex searches for the node with the given value in the treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback receives the node that was accessed, allowing for custom operations
// such as updating access times for LRU caching.
// The callback can return an error to abort the search.
func (t *Treap[T]) SearchComplex(value T, callback func(TreapNodeInterface[T]) error) (TreapNodeInterface[T], error) {
	// Since T implements types.Key[T], convert to get the comparable value
	key := any(value).(types.Key[T])
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
	// Stream both trees via in-order iterators without buffering entire trees.
	nextA, cancelA := seqNext(t.Iter())
	nextB, cancelB := seqNext(other.Iter())
	defer cancelA()
	defer cancelB()

	return mergeOrdered(nextA, nextB, t.Less, onlyInA, inBoth, onlyInB)
}

var errWalkCanceled = errors.New("walk canceled")

// seqNext adapts an iter.Seq into an iterator-style next function with cancellation.
// Cancellation prevents goroutine leaks when the consumer returns early.
func seqNext[T any](seq iter.Seq[T]) (func() (T, bool, error), func()) {
	done := make(chan struct{})
	itemCh := make(chan T, 1)

	go func() {
		defer close(itemCh)
		seq(func(v T) bool {
			select {
			case <-done:
				return false
			case itemCh <- v:
				return true
			}
		})
	}()

	next := func() (T, bool, error) {
		v, ok := <-itemCh
		if !ok {
			var zero T
			return zero, false, nil
		}
		return v, true, nil
	}

	cancel := func() { close(done) }
	return next, cancel
}

// seq2Next adapts an iter.Seq2 that yields values and errors into a next function.
// If the sequence yields a non-nil error, iteration stops and that error is returned.
func seq2Next[T any](seq iter.Seq2[T, error]) (func() (T, bool, error), func()) {
	done := make(chan struct{})
	itemCh := make(chan T, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(itemCh)
		defer close(errCh)

		seq(func(v T, err error) bool {
			if err != nil {
				errCh <- err
				return false
			}
			select {
			case <-done:
				return false
			case itemCh <- v:
				return true
			}
		})

		// No error encountered during iteration
		errCh <- nil
	}()

	next := func() (T, bool, error) {
		v, ok := <-itemCh
		if ok {
			return v, true, nil
		}
		err := <-errCh
		var zero T
		return zero, false, err
	}

	cancel := func() { close(done) }
	return next, cancel
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

// Iter returns an in-order iterator over the treap using the Go iterator protocol.
// Traversal is O(height) space via an explicit stack.
func (t *Treap[T]) Iter() iter.Seq[TreapNodeInterface[T]] {
	return func(yield func(TreapNodeInterface[T]) bool) {
		stack := initInOrderStack(t.root)
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if right := n.GetRight(); right != nil && !right.IsNil() {
				pushLeftmost(right, &stack)
			}

			if !yield(n) {
				return
			}
		}
	}
}

// initInOrderStack initializes a stack with the leftmost path.
func initInOrderStack[T any](root TreapNodeInterface[T]) []TreapNodeInterface[T] {
	var stack []TreapNodeInterface[T]
	pushLeftmost(root, &stack)
	return stack
}

// pushLeftmost pushes a node and all its left descendants onto the stack.
func pushLeftmost[T any](node TreapNodeInterface[T], stack *[]TreapNodeInterface[T]) {
	current := node
	for current != nil && !current.IsNil() {
		*stack = append(*stack, current)
		current = current.GetLeft()
	}
}
