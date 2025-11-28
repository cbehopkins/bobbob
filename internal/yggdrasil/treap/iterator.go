package treap

import (
	"fmt"

	"bobbob/internal/store"
)

// IteratorOptions controls the behavior of treap iteration.
type IteratorOptions struct {
	// KeepInMemory determines whether visited nodes should remain in memory
	// after being yielded. If false, nodes are flushed after their subtree
	// has been fully visited (memory-efficient mode).
	KeepInMemory bool

	// LoadPayloads determines whether to load payloads for payload treaps.
	// If false, only keys and structure are loaded (faster, less memory).
	LoadPayloads bool
}

// DefaultIteratorOptions returns options that minimize memory usage.
func DefaultIteratorOptions() IteratorOptions {
	return IteratorOptions{
		KeepInMemory: false,
		LoadPayloads: true,
	}
}

// IteratorState tracks the traversal state for in-order iteration.
// It maintains a stack of nodes representing the current path from root to current position.
type IteratorState[K any] struct {
	stack       []TreapNodeInterface[K]
	current     TreapNodeInterface[K]
	lastVisited TreapNodeInterface[K] // Track last visited node for flushing
	done        bool
	opts        IteratorOptions
}

// WalkInOrder iterates through all nodes in the treap in sorted key order.
// It yields each node to the callback function.
//
// Memory efficiency:
// - With KeepInMemory=false: Only maintains nodes on the current path (O(log n) memory)
// - With KeepInMemory=true: Nodes remain in memory after visiting (O(n) memory worst case)
//
// The callback receives the node and should return:
// - nil to continue iteration
// - an error to stop iteration early
func (t *PersistentTreap[K]) WalkInOrder(opts IteratorOptions, callback func(node PersistentTreapNodeInterface[K]) error) error {
	if t.root == nil {
		return nil
	}

	state := &IteratorState[K]{
		stack: make([]TreapNodeInterface[K], 0, 32), // Pre-allocate for typical tree depth
		opts:  opts,
	}

	// Start with the root
	state.current = t.root

	// Enhanced iterative in-order traversal with aggressive flushing
	for state.current != nil || len(state.stack) > 0 {
		// Go to the leftmost node
		for state.current != nil && !state.current.IsNil() {
			state.stack = append(state.stack, state.current)
			state.current = state.current.GetLeft()
		}

		if len(state.stack) == 0 {
			break
		}

		// Peek at the top of stack (don't pop yet)
		state.current = state.stack[len(state.stack)-1]

		// Check if we're coming back from the right subtree
		// (i.e., we've already visited this node and processed its right child)
		if state.lastVisited != nil && state.current.GetRight() != nil &&
			!state.current.GetRight().IsNil() && state.current.GetRight() == state.lastVisited {
			// We've finished both left and right subtrees, now we can flush
			if !state.opts.KeepInMemory {
				if pNode, ok := state.current.(*PersistentTreapNode[K]); ok {
					if store.IsValidObjectId(pNode.objectId) {
						_ = pNode.Flush() // Ignore flush errors during iteration
					}
				}
			}

			// Pop this node and mark it as visited
			state.stack = state.stack[:len(state.stack)-1]
			state.lastVisited = state.current
			state.current = nil // Move back up the tree
			continue
		}

		// If we haven't visited the right subtree yet, pop and visit this node
		state.stack = state.stack[:len(state.stack)-1]

		// Convert to persistent node
		pNode, ok := state.current.(*PersistentTreapNode[K])
		if !ok {
			return fmt.Errorf("node is not a PersistentTreapNode")
		}

		// Yield this node to the callback
		err := callback(pNode)
		if err != nil {
			return err
		}

		// Mark this as the last visited node
		state.lastVisited = state.current

		// If this node has no right subtree, we can flush it immediately
		if !state.opts.KeepInMemory && (state.current.GetRight() == nil || state.current.GetRight().IsNil()) {
			if store.IsValidObjectId(pNode.objectId) {
				_ = pNode.Flush() // Ignore flush errors during iteration
			}
		}

		// Move to right subtree
		state.current = state.current.GetRight()
	}

	return nil
}

// WalkInOrderKeys is a convenience method that yields only the keys.
// This is more memory efficient as it doesn't require loading payloads.
func (t *PersistentTreap[K]) WalkInOrderKeys(opts IteratorOptions, callback func(key PersistentKey[K]) error) error {
	return t.WalkInOrder(opts, func(node PersistentTreapNodeInterface[K]) error {
		key, ok := node.GetKey().(PersistentKey[K])
		if !ok {
			return fmt.Errorf("node key is not a PersistentKey")
		}
		return callback(key)
	})
}

// PayloadIteratorCallback is called for each node during payload treap iteration.
// If loadPayload is false, the payload parameter will be the zero value.
type PayloadIteratorCallback[K any, P PersistentPayload[P]] func(key PersistentKey[K], payload P, loadPayload bool) error

// WalkInOrder iterates through all nodes in the payload treap in sorted key order.
//
// Memory efficiency:
// - With KeepInMemory=false: Only maintains nodes on the current path (O(log n) memory)
// - With LoadPayloads=false: Payloads are not loaded, only keys (saves memory & I/O)
//
// The callback receives:
// - key: The node's key
// - payload: The node's payload (zero value if LoadPayloads=false)
// - loadPayload: Whether the payload was actually loaded
func (t *PersistentPayloadTreap[K, P]) WalkInOrder(opts IteratorOptions, callback PayloadIteratorCallback[K, P]) error {
	if t.root == nil {
		return nil
	}

	state := &IteratorState[K]{
		stack: make([]TreapNodeInterface[K], 0, 32),
		opts:  opts,
	}

	state.current = t.root

	// Enhanced iterative in-order traversal with aggressive flushing
	for state.current != nil || len(state.stack) > 0 {
		// Go to the leftmost node
		for state.current != nil && !state.current.IsNil() {
			state.stack = append(state.stack, state.current)
			state.current = state.current.GetLeft()
		}

		if len(state.stack) == 0 {
			break
		}

		// Peek at the top of stack
		state.current = state.stack[len(state.stack)-1]

		// Check if we're coming back from the right subtree
		if state.lastVisited != nil && state.current.GetRight() != nil &&
			!state.current.GetRight().IsNil() && state.current.GetRight() == state.lastVisited {
			// We've finished both left and right subtrees, flush this node
			if !state.opts.KeepInMemory {
				if pNode, ok := state.current.(*PersistentPayloadTreapNode[K, P]); ok {
					if store.IsValidObjectId(pNode.objectId) {
						_ = pNode.Flush()
					}
				}
			}

			// Pop and mark as visited
			state.stack = state.stack[:len(state.stack)-1]
			state.lastVisited = state.current
			state.current = nil
			continue
		}

		// Pop and visit this node
		state.stack = state.stack[:len(state.stack)-1]

		// Convert to persistent payload node
		pNode, ok := state.current.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return fmt.Errorf("node is not a PersistentPayloadTreapNode")
		}

		// Get key
		key, ok := pNode.GetKey().(PersistentKey[K])
		if !ok {
			return fmt.Errorf("node key is not a PersistentKey")
		}

		// Get payload if requested
		var payload P
		if opts.LoadPayloads {
			payload = pNode.GetPayload()
		}

		// Yield to callback
		err := callback(key, payload, opts.LoadPayloads)
		if err != nil {
			return err
		}

		// Mark as last visited
		state.lastVisited = state.current

		// If no right subtree, flush immediately
		if !opts.KeepInMemory && (state.current.GetRight() == nil || state.current.GetRight().IsNil()) {
			if store.IsValidObjectId(pNode.objectId) {
				_ = pNode.Flush()
			}
		}

		// Move to right subtree
		state.current = state.current.GetRight()
	}

	return nil
}

// WalkInOrderKeys is a convenience method that yields only the keys from a payload treap.
// This is more memory efficient as it doesn't require loading payloads.
func (t *PersistentPayloadTreap[K, P]) WalkInOrderKeys(opts IteratorOptions, callback func(key PersistentKey[K]) error) error {
	// Override LoadPayloads to false for efficiency
	opts.LoadPayloads = false
	return t.WalkInOrder(opts, func(key PersistentKey[K], _ P, _ bool) error {
		return callback(key)
	})
}

// Count returns the total number of nodes in the treap.
// This uses WalkInOrder with minimal memory usage (keys only, no retention).
func (t *PersistentTreap[K]) Count() (int, error) {
	count := 0
	opts := IteratorOptions{
		KeepInMemory: false,
		LoadPayloads: false,
	}
	err := t.WalkInOrder(opts, func(node PersistentTreapNodeInterface[K]) error {
		count++
		return nil
	})
	return count, err
}

// Count returns the total number of nodes in the payload treap.
func (t *PersistentPayloadTreap[K, P]) Count() (int, error) {
	count := 0
	opts := IteratorOptions{
		KeepInMemory: false,
		LoadPayloads: false,
	}
	err := t.WalkInOrder(opts, func(_ PersistentKey[K], _ P, _ bool) error {
		count++
		return nil
	})
	return count, err
}
