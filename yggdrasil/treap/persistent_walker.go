package treap

import (
	"fmt"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

// PersistentNodeWalker defines the interface for nodes that can be walked in post-order
// by rangeOverPostOrder. This abstraction allows different node types (PersistentTreapNode,
// PersistentPayloadTreapNode, etc.) to use the same walker implementation.
//
// Any persistent node type can implement this interface to gain automatic dirty tracking
// and polymorphic post-order traversal support.
type PersistentNodeWalker[T any] interface {
	// IsNil checks if the node is nil/empty (replaces pointer nil checks).
	IsNil() bool

	// GetObjectIdNoAlloc returns the node's object ID without allocating a new one if invalid.
	GetObjectIdNoAlloc() bobbob.ObjectId

	// GetLeftChild returns the cached left child pointer (may be nil if flushed).
	// Does not load from disk; use GetTransientLeftChild for that.
	GetLeftChild() TreapNodeInterface[T]

	// GetRightChild returns the cached right child pointer (may be nil if flushed).
	// Does not load from disk; use GetTransientRightChild for that.
	GetRightChild() TreapNodeInterface[T]

	// GetTransientLeftChild loads the left child from disk if needed, without caching.
	// Returns the child node and error (may be nil if no child exists).
	GetTransientLeftChild() (PersistentNodeWalker[T], error)

	// GetTransientRightChild loads the right child from disk if needed, without caching.
	// Returns the child node and error (may be nil if no child exists).
	GetTransientRightChild() (PersistentNodeWalker[T], error)

	// GetStore returns the backing store for this node.
	GetStore() bobbob.Storer
}

// rangeOverPostOrder is the generic post-order walker that works with any PersistentNodeWalker[T].
// This is the polymorphic implementation that allows different node types to use the same walker.
//
// The walker automatically tracks which nodes become dirty during callback execution:
// - If a node's objectId goes from valid → invalid, the node was modified
// - When a node becomes dirty, all its ancestors (on the stack) also become dirty
//
// The traversal order is post-order: left subtree, right subtree, node.
//
// Parameters:
//   - root: The root node to start traversal from
//   - callback: Function called for each node; receives node and should return error to stop
//
// Returns:
//   - A slice of dirty nodes (modified during traversal)
//   - An error if callback returns error or I/O error occurs
//
// Example:
//
//	dirtyNodes, err := rangeOverPostOrder(root, func(node *PersistentTreapNode[int]) error {
//	    if needsUpdate(node) {
//	        node.objectId = bobbob.ObjNotAllocated  // Mark dirty
//	    }
//	    return node.persist()
//	})
func rangeOverPostOrder[T any, N PersistentNodeWalker[T]](root N, callback func(node N) error) ([]N, error) {
	if any(root) == nil {
		return nil, nil
	}

	type frame struct {
		node    N
		visited bool
	}

	stack := []frame{{node: root, visited: false}}
	dirtySet := make(map[PersistentNodeWalker[T]]struct{}) // Track dirty nodes (order not guaranteed)
	dirtyOldIds := make(map[bobbob.ObjectId]struct{})
	var path []N

	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if any(f.node) == nil || f.node.IsNil() {
			continue
		}

		if f.visited {
			// Capture objectId before callback
			oldObjectId := f.node.GetObjectIdNoAlloc()

			// Execute callback
			if err := callback(f.node); err != nil {
				return nil, err
			}

			// Check if node became dirty (valid → invalid)
			if store.IsValidObjectId(oldObjectId) && !store.IsValidObjectId(f.node.GetObjectIdNoAlloc()) {
				// This node was modified - it's dirty
				dirtySet[f.node] = struct{}{}
				if store.IsValidObjectId(oldObjectId) {
					dirtyOldIds[oldObjectId] = struct{}{}
				}

				// All ancestors in the current traversal path are now dirty too
				// (their cached child objectIds are stale).
				for i := len(path) - 2; i >= 0; i-- {
					ancestor := path[i]
					if any(ancestor) != nil {
						dirtySet[ancestor] = struct{}{}
					}
					ancestorObjId := ancestor.GetObjectIdNoAlloc()
					if store.IsValidObjectId(ancestorObjId) {
						dirtyOldIds[ancestorObjId] = struct{}{}
					}
				}
			}

			// Pop this node from the current traversal path
			if len(path) > 0 {
				path = path[:len(path)-1]
			}
			continue
		}

		// Post-order: left, right, node
		// Push this node onto the current traversal path
		path = append(path, f.node)
		stack = append(stack, frame{node: f.node, visited: true})

		rightChild, err := f.node.GetTransientRightChild()
		if err != nil {
			return nil, err
		}
		if rightChild != nil && !rightChild.IsNil() {
			rightNode, ok := any(rightChild).(N)
			if !ok {
				return nil, fmt.Errorf("right child is not expected node type")
			}
			stack = append(stack, frame{node: rightNode, visited: false})
		}

		leftChild, err := f.node.GetTransientLeftChild()
		if err != nil {
			return nil, err
		}
		if leftChild != nil && !leftChild.IsNil() {
			leftNode, ok := any(leftChild).(N)
			if !ok {
				return nil, fmt.Errorf("left child is not expected node type")
			}
			stack = append(stack, frame{node: leftNode, visited: false})
		}
	}

	// Queue old ObjectIds for deletion after traversal completes.
	// Prefer to queue via the treap parent when available so deletions are
	// deferred and centralized; fall back to immediate deletion on the store
	// if no treap parent can be located.
	// Delete old ObjectIds for all dirty nodes after traversal completes.
	rootStore := root.GetStore()
	for objId := range dirtyOldIds {
		if store.IsValidObjectId(objId) {
			_ = rootStore.DeleteObj(objId)
		}
	}

	// Build dirty node slice from set (order not guaranteed)
	dirtyNodes := make([]N, 0, len(dirtySet))
	for node := range dirtySet {
		if n, ok := any(node).(N); ok {
			dirtyNodes = append(dirtyNodes, n)
		}
	}

	return dirtyNodes, nil
}

// rangeOverPostOrderInMemory walks only in-memory nodes in post-order.
// It never loads children from disk (GetTransient*), so it is safe for
// persistence that should not rehydrate flushed subtrees.
func rangeOverPostOrderInMemory[T any, N PersistentNodeWalker[T]](root N, callback func(node N) error) ([]N, error) {
	if any(root) == nil {
		return nil, nil
	}

	type frame struct {
		node    N
		visited bool
	}

	stack := []frame{{node: root, visited: false}}
	dirtySet := make(map[PersistentNodeWalker[T]]struct{})
	dirtyOldIds := make(map[bobbob.ObjectId]struct{})
	var path []N

	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if any(f.node) == nil || f.node.IsNil() {
			continue
		}

		if f.visited {
			oldObjectId := f.node.GetObjectIdNoAlloc()

			if err := callback(f.node); err != nil {
				return nil, err
			}

			if store.IsValidObjectId(oldObjectId) && !store.IsValidObjectId(f.node.GetObjectIdNoAlloc()) {
				dirtySet[f.node] = struct{}{}
				if store.IsValidObjectId(oldObjectId) {
					dirtyOldIds[oldObjectId] = struct{}{}
				}

				for i := len(path) - 2; i >= 0; i-- {
					ancestor := path[i]
					if any(ancestor) != nil {
						dirtySet[ancestor] = struct{}{}
					}
					ancestorObjId := ancestor.GetObjectIdNoAlloc()
					if store.IsValidObjectId(ancestorObjId) {
						dirtyOldIds[ancestorObjId] = struct{}{}
					}
				}
			}

			if len(path) > 0 {
				path = path[:len(path)-1]
			}
			continue
		}

		path = append(path, f.node)
		stack = append(stack, frame{node: f.node, visited: true})

		rightChild := f.node.GetRightChild()
		if rightChild != nil && !rightChild.IsNil() {
			rightNode, ok := any(rightChild).(N)
			if !ok {
				return nil, fmt.Errorf("right child is not expected node type")
			}
			stack = append(stack, frame{node: rightNode, visited: false})
		}

		leftChild := f.node.GetLeftChild()
		if leftChild != nil && !leftChild.IsNil() {
			leftNode, ok := any(leftChild).(N)
			if !ok {
				return nil, fmt.Errorf("left child is not expected node type")
			}
			stack = append(stack, frame{node: leftNode, visited: false})
		}
	}

	rootStore := root.GetStore()
	for objId := range dirtyOldIds {
		if store.IsValidObjectId(objId) {
			_ = rootStore.DeleteObj(objId)
		}
	}

	dirtyNodes := make([]N, 0, len(dirtySet))
	for node := range dirtySet {
		if n, ok := any(node).(N); ok {
			dirtyNodes = append(dirtyNodes, n)
		}
	}

	return dirtyNodes, nil
}
