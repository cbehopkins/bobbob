package treap

import (
	"github.com/cbehopkins/bobbob/store"
)

func isNilNode[K any](node TreapNodeInterface[K]) bool {
	if node == nil {
		return true
	}
	return node.IsNil()
}

// VisitCallback is a read-only iterator callback for treap nodes.
type VisitCallback[T any] func(node TreapNodeInterface[T]) error

// MutatingCallback is an iterator callback that can mutate nodes.
// The iterator detects mutations via ObjectId transitions (valid→invalid)
// and automatically adds old ObjectIds to the trash list.
type MutatingCallback[T any] func(node TreapNodeInterface[T]) error

// InOrderVisit performs a read-only in-order traversal of the treap.
// Multiple concurrent readers can call this method simultaneously (uses RLock).
//
// The callback is invoked for each node in sorted order.
// Return an error to halt iteration early.
func (t *PersistentTreap[K]) InOrderVisit(callback VisitCallback[K]) error {
	if t == nil || t.root == nil || callback == nil {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	_, err := hybridWalkInOrder(t.root, t.Store, func(node TreapNodeInterface[K]) error {
		return callback(node)
	}, false)
	return err
}

// InOrderMutate performs an in-order traversal allowing mutations.
// This method acquires an exclusive write lock.
//
// The iterator automatically detects when nodes are mutated (valid→invalid ObjectId transitions)
// and accumulates their old ObjectIds for deletion. All trash is deleted at the end of iteration.
//
// When a mutation is detected, ancestor childObjectIds are automatically invalidated
// to prevent stale disk references.
func (t *PersistentTreap[K]) InOrderMutate(callback MutatingCallback[K]) error {
	if t == nil || t.root == nil || callback == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	trashList, err := hybridWalkInOrder(t.root, t.Store, func(node TreapNodeInterface[K]) error {
		return callback(node)
	}, true)
	if err != nil {
		return err
	}

	// Queue all accumulated trash for deletion and flush pending deletes.
	for _, objId := range trashList {
		if store.IsValidObjectId(objId) {
			t.queueDelete(objId)
		}
	}

	t.flushPendingDeletes()
	return nil
}

// hybridWalkInOrder performs a stack-based in-order traversal of the treap.
// It follows in-memory pointers as the primary mechanism, falling back to
// loading from disk when a pointer is nil but the ObjectId is valid.
//
// If trackMutations is true:
// - Records ObjectId before callback
// - Detects valid→invalid transitions after callback
// - Invalidates ancestor childObjectIds when mutations detected
// - Accumulates trash ObjectIds for deletion
//
// Returns the list of ObjectIds to delete (empty if trackMutations=false).
func hybridWalkInOrder[T any](
	root TreapNodeInterface[T],
	st store.Storer,
	callback func(TreapNodeInterface[T]) error,
	trackMutations bool,
) ([]store.ObjectId, error) {
	if root == nil {
		return nil, nil
	}

	type stackFrame struct {
		node TreapNodeInterface[T]
		// Track which child pointer to process next:
		// 0=left not yet visited, 1=visiting, 2=right not yet visited, 3=done
		phase int
	}

	stack := make([]stackFrame, 0, 32)
	trashList := make([]store.ObjectId, 0)
	current := root
	phase := 0

	for current != nil || len(stack) > 0 {
		if current != nil && isNilNode(current) {
			current = nil
			phase = 0
			continue
		}
		if current != nil && phase == 0 {
			// Visit left subtree
			stack = append(stack, stackFrame{node: current, phase: 1})
			current = current.GetLeft()
			phase = 0
		} else if len(stack) > 0 {
			frame := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if frame.node == nil || isNilNode(frame.node) {
				current = nil
				phase = 0
				continue
			}

			if frame.phase == 1 {
				// Visit node
				var oldObjectId store.ObjectId = -1

				// Record ObjectId before callback for mutation detection
				if trackMutations {
					if pNode, ok := frame.node.(*PersistentTreapNode[T]); ok {
						oldObjectId = pNode.objectId
					}
				}

				// Invoke callback
				if err := callback(frame.node); err != nil {
					return trashList, err
				}

				// Detect mutations (valid→invalid transition)
				if trackMutations {
					if pNode, ok := frame.node.(*PersistentTreapNode[T]); ok {
						if oldObjectId >= 0 && pNode.objectId < 0 {
							// Mutation detected: node was persisted but now in-memory only
							trashList = append(trashList, oldObjectId)

							// Invalidate ancestor childObjectIds
							// Convert stack to generic helper type for the helper function
							helperStack := make([]*stackFrameHelper[T], len(stack))
							for j, s := range stack {
								helperStack[j] = &stackFrameHelper[T]{node: s.node, phase: s.phase}
							}
							invalidateAncestorsHelper(helperStack, frame.node)
						}
					}
				}

				// Continue with right subtree
				stack = append(stack, stackFrame{node: frame.node, phase: 2})
				current = frame.node.GetRight()
				phase = 0
			} else if frame.phase == 2 {
				// Right subtree already visited, node is done
				current = nil
			}
		} else {
			break
		}
	}

	return trashList, nil
}

// invalidateAncestorsHelper walks the ancestor stack and invalidates the childObjectId
// that corresponds to the mutated node.
func invalidateAncestorsHelper[T any](stack []*stackFrameHelper[T], mutatedNode TreapNodeInterface[T]) {
	if len(stack) == 0 {
		return
	}

	// Walk stack from top (most recent ancestor) backwards
	for i := len(stack) - 1; i >= 0; i-- {
		ancestor := stack[i].node
		if ancestor == nil {
			continue
		}

		// Determine if mutatedNode is a left or right child of ancestor
		if ancestor.GetLeft() == mutatedNode {
			// mutatedNode is the left child
			if pAncestor, ok := ancestor.(*PersistentTreapNode[T]); ok {
				pAncestor.leftObjectId = -1
			}
		} else if ancestor.GetRight() == mutatedNode {
			// mutatedNode is the right child
			if pAncestor, ok := ancestor.(*PersistentTreapNode[T]); ok {
				pAncestor.rightObjectId = -1
			}
		}

		// Update mutatedNode reference for next ancestor
		mutatedNode = ancestor
	}
}

// stackFrameHelper is used internally for stack-based traversal.
type stackFrameHelper[T any] struct {
	node  TreapNodeInterface[T]
	phase int
}

// Count returns the total number of nodes in the treap.
func (t *PersistentTreap[K]) Count() (int, error) {
	count := 0
	err := t.InOrderVisit(func(node TreapNodeInterface[K]) error {
		count++
		return nil
	})
	return count, err
}

// Count returns the total number of nodes in the payload treap.
func (t *PersistentPayloadTreap[K, P]) Count() (int, error) {
	count := 0
	err := t.InOrderVisit(func(node TreapNodeInterface[K]) error {
		count++
		return nil
	})
	return count, err
}
