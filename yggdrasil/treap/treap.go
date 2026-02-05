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

// RotateRight performs a right rotation on the given node.
// This is a package-level function that works with any TreapNodeInterface[T].
func RotateRight[T any](node TreapNodeInterface[T]) (TreapNodeInterface[T], error) {
	newRoot := node.GetLeft()
	if err := node.SetLeft(newRoot.GetRight()); err != nil {
		return node, err
	}
	if err := newRoot.SetRight(node); err != nil {
		return node, err
	}
	return newRoot, nil
}

// RotateLeft performs a left rotation on the given node.
// This is a package-level function that works with any TreapNodeInterface[T].
func RotateLeft[T any](node TreapNodeInterface[T]) (TreapNodeInterface[T], error) {
	newRoot := node.GetRight()
	if err := node.SetRight(newRoot.GetLeft()); err != nil {
		return node, err
	}
	if err := newRoot.SetLeft(node); err != nil {
		return node, err
	}
	return newRoot, nil
}

// NodeFactory is a function that creates a new treap node with the given key and priority.
// Different treap implementations can provide their own factories to manage node allocation.
type NodeFactory[T any] func(key types.Key[T], priority Priority) TreapNodeInterface[T]

// NodeReleaser is a function that releases a node back to a pool or performs cleanup.
// It may be nil if no cleanup is needed.
type NodeReleaser[T any] func(node TreapNodeInterface[T])

// DirtyTracker is a callback function that marks a node as dirty (modified) for persistence.
// Used by persistent treap implementations to track which nodes need re-serialization.
type DirtyTracker[T any] func(node TreapNodeInterface[T])

// RotateRightTracked performs a right rotation and tracks dirty nodes via callback.
// This is used by persistent treaps to track modifications for later invalidation.
func RotateRightTracked[T any](node TreapNodeInterface[T], trackDirty DirtyTracker[T]) (TreapNodeInterface[T], error) {
	newRoot, err := RotateRight(node)
	if err != nil {
		return node, err
	}
	if trackDirty != nil {
		trackDirty(node)
		trackDirty(newRoot)
	}
	return newRoot, nil
}

// RotateLeftTracked performs a left rotation and tracks dirty nodes via callback.
// This is used by persistent treaps to track modifications for later invalidation.
func RotateLeftTracked[T any](node TreapNodeInterface[T], trackDirty DirtyTracker[T]) (TreapNodeInterface[T], error) {
	newRoot, err := RotateLeft(node)
	if err != nil {
		return node, err
	}
	if trackDirty != nil {
		trackDirty(node)
		trackDirty(newRoot)
	}
	return newRoot, nil
}

// DuplicateKeyHandler is called when inserting a duplicate key.
// It should update the existing node and return true if handled, or false to use default behavior.
type DuplicateKeyHandler[T any] func(existingNode, newNode TreapNodeInterface[T]) bool

// InsertNodeTracked inserts a node with dirty tracking for persistent treaps.
// The trackDirty callback is invoked for each modified node during insertion.
// The duplicateHandler is called when a duplicate key is encountered (optional).
func InsertNodeTracked[T any](
	node TreapNodeInterface[T],
	newNode TreapNodeInterface[T],
	less func(a, b T) bool,
	releaser NodeReleaser[T],
	trackDirty DirtyTracker[T],
	duplicateHandler DuplicateKeyHandler[T],
) (TreapNodeInterface[T], error) {
	if node == nil || node.IsNil() {
		return newNode, nil
	}

	newKey := newNode.GetKey().Value()
	nodeKey := node.GetKey().Value()

	// Check for duplicate key
	if !less(newKey, nodeKey) && !less(nodeKey, newKey) {
		// Allow custom duplicate handling (for persistent treaps that replace keys)
		if duplicateHandler != nil && duplicateHandler(node, newNode) {
			if trackDirty != nil {
				trackDirty(node)
			}
			if releaser != nil {
				releaser(newNode)
			}
			return node, nil
		}
		// Default: don't insert duplicate
		if releaser != nil {
			releaser(newNode)
		}
		return node, nil
	}

	// Recursive insertion
	if less(newKey, nodeKey) {
		left, err := InsertNodeTracked(node.GetLeft(), newNode, less, releaser, trackDirty, duplicateHandler)
		if err != nil {
			return node, err
		}
		if err := node.SetLeft(left); err != nil {
			return node, err
		}
		if trackDirty != nil {
			trackDirty(node)
		}
		if node.GetLeft() != nil && node.GetLeft().GetPriority() > node.GetPriority() {
			return RotateRightTracked(node, trackDirty)
		}
	} else {
		right, err := InsertNodeTracked(node.GetRight(), newNode, less, releaser, trackDirty, duplicateHandler)
		if err != nil {
			return node, err
		}
		if err := node.SetRight(right); err != nil {
			return node, err
		}
		if trackDirty != nil {
			trackDirty(node)
		}
		if node.GetRight() != nil && node.GetRight().GetPriority() > node.GetPriority() {
			return RotateLeftTracked(node, trackDirty)
		}
	}

	return node, nil
}

// DeleteNodeTracked removes a node with dirty tracking for persistent treaps.
// The trackDirty callback is invoked for each modified node during deletion.
// The cleanupRemoved callback is called before releasing a deleted node (optional).
func DeleteNodeTracked[T any](
	node TreapNodeInterface[T],
	key T,
	less func(a, b T) bool,
	releaser NodeReleaser[T],
	trackDirty DirtyTracker[T],
	cleanupRemoved func(TreapNodeInterface[T], DirtyTracker[T]),
) (TreapNodeInterface[T], error) {
	if node == nil || node.IsNil() {
		return nil, nil
	}

	nodeKey := node.GetKey().Value()

	// Recursive deletion
	if less(key, nodeKey) {
		left, err := DeleteNodeTracked(node.GetLeft(), key, less, releaser, trackDirty, cleanupRemoved)
		if err != nil {
			return node, err
		}
		if err := node.SetLeft(left); err != nil {
			return node, err
		}
		if trackDirty != nil {
			trackDirty(node)
		}
	} else if less(nodeKey, key) {
		right, err := DeleteNodeTracked(node.GetRight(), key, less, releaser, trackDirty, cleanupRemoved)
		if err != nil {
			return node, err
		}
		if err := node.SetRight(right); err != nil {
			return node, err
		}
		if trackDirty != nil {
			trackDirty(node)
		}
	} else {
		// Found the node to delete
		left := node.GetLeft()
		right := node.GetRight()

		if left == nil || left.IsNil() {
			if cleanupRemoved != nil {
				cleanupRemoved(node, trackDirty)
			}
			if releaser != nil {
				releaser(node)
			}
			return right, nil
		}
		if right == nil || right.IsNil() {
			if cleanupRemoved != nil {
				cleanupRemoved(node, trackDirty)
			}
			if releaser != nil {
				releaser(node)
			}
			return left, nil
		}

		// Both children exist - rotate down and recurse
		if left.GetPriority() > right.GetPriority() {
			rotated, err := RotateRightTracked(node, trackDirty)
			if err != nil {
				return node, err
			}
			node = rotated
			newRight, err := DeleteNodeTracked(node.GetRight(), key, less, releaser, trackDirty, cleanupRemoved)
			if err != nil {
				return node, err
			}
			if err := node.SetRight(newRight); err != nil {
				return node, err
			}
			if trackDirty != nil {
				trackDirty(node)
			}
		} else {
			rotated, err := RotateLeftTracked(node, trackDirty)
			if err != nil {
				return node, err
			}
			node = rotated
			newLeft, err := DeleteNodeTracked(node.GetLeft(), key, less, releaser, trackDirty, cleanupRemoved)
			if err != nil {
				return node, err
			}
			if err := node.SetLeft(newLeft); err != nil {
				return node, err
			}
			if trackDirty != nil {
				trackDirty(node)
			}
		}
	}

	return node, nil
}

// InsertNode is a package-level function that inserts a new node into the treap.
// It takes a comparison function and optional node releaser for managing duplicates.
func InsertNode[T any](node TreapNodeInterface[T], newNode TreapNodeInterface[T], less func(a, b T) bool, releaser NodeReleaser[T]) (TreapNodeInterface[T], error) {
	if node == nil || node.IsNil() {
		return newNode, nil
	}

	newKey := newNode.GetKey().Value()
	nodeKey := node.GetKey().Value()

	// Check for exact key match - don't insert duplicate
	if !less(newKey, nodeKey) && !less(nodeKey, newKey) {
		// Keys are equal - return existing node without adding duplicate
		// Release newNode if releaser is provided
		if releaser != nil {
			releaser(newNode)
		}
		return node, nil
	}

	if less(newKey, nodeKey) {
		left, err := InsertNode(node.GetLeft(), newNode, less, releaser)
		if err != nil {
			return node, err
		}
		if err := node.SetLeft(left); err != nil {
			return node, err
		}
		if node.GetLeft() != nil && node.GetLeft().GetPriority() > node.GetPriority() {
			rotated, err := RotateRight(node)
			if err != nil {
				return node, err
			}
			node = rotated
		}
	} else {
		right, err := InsertNode(node.GetRight(), newNode, less, releaser)
		if err != nil {
			return node, err
		}
		if err := node.SetRight(right); err != nil {
			return node, err
		}
		if node.GetRight() != nil && node.GetRight().GetPriority() > node.GetPriority() {
			rotated, err := RotateLeft(node)
			if err != nil {
				return node, err
			}
			node = rotated
		}
	}

	return node, nil
}

// DeleteNode removes the node with the given key from the treap.
// It takes a comparison function and optional node releaser for cleanup.
func DeleteNode[T any](node TreapNodeInterface[T], key T, less func(a, b T) bool, releaser NodeReleaser[T]) (TreapNodeInterface[T], error) {
	if node == nil || node.IsNil() {
		return nil, nil
	}

	if less(key, node.GetKey().Value()) {
		left, err := DeleteNode(node.GetLeft(), key, less, releaser)
		if err != nil {
			return node, err
		}
		if err := node.SetLeft(left); err != nil {
			return node, err
		}
	} else if less(node.GetKey().Value(), key) {
		right, err := DeleteNode(node.GetRight(), key, less, releaser)
		if err != nil {
			return node, err
		}
		if err := node.SetRight(right); err != nil {
			return node, err
		}
	} else {
		left := node.GetLeft()
		right := node.GetRight()

		if left == nil || left.IsNil() {
			// Release the removed node if releaser is provided
			if releaser != nil {
				releaser(node)
			}
			return right, nil
		}
		if right == nil || right.IsNil() {
			// Release the removed node if releaser is provided
			if releaser != nil {
				releaser(node)
			}
			return left, nil
		}
		leftPriority := left.GetPriority()
		rightPriority := right.GetPriority()
		if leftPriority > rightPriority {
			rotated, err := RotateRight(node)
			if err != nil {
				return node, err
			}
			node = rotated
			right, err := DeleteNode(node.GetRight(), key, less, releaser)
			if err != nil {
				return node, err
			}
			if err := node.SetRight(right); err != nil {
				return node, err
			}
		} else {
			rotated, err := RotateLeft(node)
			if err != nil {
				return node, err
			}
			node = rotated
			left, err := DeleteNode(node.GetLeft(), key, less, releaser)
			if err != nil {
				return node, err
			}
			if err := node.SetLeft(left); err != nil {
				return node, err
			}
		}
	}

	return node, nil
}

// SearchNodeComplex searches for the node with the given key in the treap.
// It accepts an optional callback that is called when a node is accessed during the search.
// The callback can return an error to abort the search.
func SearchNodeComplex[T any](node TreapNodeInterface[T], key T, less func(a, b T) bool, callback func(TreapNodeInterface[T]) error) (TreapNodeInterface[T], error) {
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

	if less(key, currentKey.Value()) {
		return SearchNodeComplex(node.GetLeft(), key, less, callback)
	} else {
		return SearchNodeComplex(node.GetRight(), key, less, callback)
	}
}

// SearchNode searches for the node with the given key in the treap.
// It calls SearchNodeComplex with a nil callback.
func SearchNode[T any](node TreapNodeInterface[T], key T, less func(a, b T) bool) TreapNodeInterface[T] {
	result, _ := SearchNodeComplex(node, key, less, nil)
	return result
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

// NodeFactory returns a factory function that allocates treap nodes using the treap's pool.
func (t *Treap[T]) NodeFactory() NodeFactory[T] {
	return func(key types.Key[T], priority Priority) TreapNodeInterface[T] {
		return t.newNode(key, priority)
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

// InsertComplex inserts a new node with the given value and priority into the treap.
// The type T must implement the types.Key[T] interface (e.g., IntKey, StringKey).
// Use this method when you need to specify a custom priority value.
func (t *Treap[T]) InsertComplex(value T, priority Priority) {
	// Since T implements types.Key[T], we can use the value directly as the key
	key := any(value).(types.Key[T])
	newNode := t.NodeFactory()(key, priority)
	inserted, err := InsertNode(t.root, newNode, t.Less, t.releaseNode)
	if err != nil {
		panic(err)
	}
	t.root = inserted
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
	deleted, err := DeleteNode(t.root, key.Value(), t.Less, t.releaseNode)
	if err != nil {
		panic(err)
	}
	t.root = deleted
}

// SearchComplex searches for the node with the given value in the treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback receives the node that was accessed, allowing for custom operations
// such as updating access times for LRU caching.
// The callback can return an error to abort the search.
func (t *Treap[T]) SearchComplex(value T, callback func(TreapNodeInterface[T]) error) (TreapNodeInterface[T], error) {
	// Since T implements types.Key[T], convert to get the comparable value
	key := any(value).(types.Key[T])
	return SearchNodeComplex(t.root, key.Value(), t.Less, callback)
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
	return CompareTreaps(t.root, other.root, t.Less, onlyInA, inBoth, onlyInB)
}

// CompareTreaps compares two treap roots and invokes callbacks for keys that are:
// - Only in A (onlyInA)
// - In both (inBoth)
// - Only in B (onlyInB)
// The comparison is done by traversing both treaps in sorted order.
// All three callbacks are optional (can be nil).
func CompareTreaps[T any](
	rootA TreapNodeInterface[T],
	rootB TreapNodeInterface[T],
	less func(a, b T) bool,
	onlyInA func(TreapNodeInterface[T]) error,
	inBoth func(nodeA, nodeB TreapNodeInterface[T]) error,
	onlyInB func(TreapNodeInterface[T]) error,
) error {
	// Collect nodes in-order from both trees
	var nodesA, nodesB []TreapNodeInterface[T]

	var collectA func(TreapNodeInterface[T]) error
	collectA = func(node TreapNodeInterface[T]) error {
		if node == nil || node.IsNil() {
			return nil
		}
		if err := collectA(node.GetLeft()); err != nil {
			return err
		}
		nodesA = append(nodesA, node)
		if err := collectA(node.GetRight()); err != nil {
			return err
		}
		return nil
	}

	var collectB func(TreapNodeInterface[T]) error
	collectB = func(node TreapNodeInterface[T]) error {
		if node == nil || node.IsNil() {
			return nil
		}
		if err := collectB(node.GetLeft()); err != nil {
			return err
		}
		nodesB = append(nodesB, node)
		if err := collectB(node.GetRight()); err != nil {
			return err
		}
		return nil
	}

	if err := collectA(rootA); err != nil {
		return err
	}
	if err := collectB(rootB); err != nil {
		return err
	}

	// Create simple index-based iterators
	idxA, idxB := 0, 0
	nextA := func() (TreapNodeInterface[T], bool, error) {
		if idxA < len(nodesA) {
			node := nodesA[idxA]
			idxA++
			return node, true, nil
		}
		var zero TreapNodeInterface[T]
		return zero, false, nil
	}
	nextB := func() (TreapNodeInterface[T], bool, error) {
		if idxB < len(nodesB) {
			node := nodesB[idxB]
			idxB++
			return node, true, nil
		}
		var zero TreapNodeInterface[T]
		return zero, false, nil
	}

	return mergeOrdered(nextA, nextB, less, onlyInA, inBoth, onlyInB)
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
