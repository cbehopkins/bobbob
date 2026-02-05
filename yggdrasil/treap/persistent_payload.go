package treap

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

type persistWorker = struct {
	workloads <-chan func() error
	errorChan chan<- error
}

type persistWorkerPool struct {
	workers   []persistWorker
	wg        sync.WaitGroup
	workloads chan<- func() error
	errorChan <-chan error
}

func (p *persistWorkerPool) Submit(workload func() error) error {
	// If the pool is nil, run the workload synchronously.
	// Calling a method with a nil pointer receiver is valid in Go;
	// just avoid dereferencing receiver fields on this path.
	if workload == nil {
		return nil
	}
	if p == nil {
		return errors.New("persist worker pool is nil")
	}
	p.workloads <- workload
	return nil
}
func (p *persistWorkerPool) Close() error {
	close(p.workloads)
	p.wg.Wait()
	err, ok := <-p.errorChan
	if !ok {
		return nil
	}
	return err
}

func newPersistWorkerPool(workerCount int) *persistWorkerPool {
	workloads := make(chan func() error, workerCount*2)
	errorChan := make(chan error, workerCount*2)
	pool := &persistWorkerPool{
		workers:   make([]persistWorker, workerCount),
		workloads: workloads,
		errorChan: errorChan,
	}
	for i := range workerCount {
		worker := persistWorker{
			workloads: workloads,
			errorChan: errorChan,
		}
		pool.workers[i] = worker
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for workload := range worker.workloads {
				if err := workload(); err != nil {
					worker.errorChan <- err
				}
			}
		}()
	}
	go func() {
		pool.wg.Wait()
		close(errorChan)
	}()
	return pool
}

// UntypedPersistentPayload and types.PersistentPayload interfaces have been moved to interfaces.go

// PersistentPayloadTreapNode represents a node in the persistent payload treap.
// It embeds the payload along with the persistent treap node functionality.
// That is when you persist this node, both the treap structure and the payload are persisted together.
type PersistentPayloadTreapNode[K any, P types.PersistentPayload[P]] struct {
	PersistentTreapNode[K]
	payload P
}

// payloadDeleter is an optional interface payloads can implement to clean up
// any dependent ObjectIds they own before the treap frees the node itself.
// Implementors should delete any child objects they allocated.
type payloadDeleter interface {
	DeleteDependents(store.Storer) error
}

// GetPayload returns the payload of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetPayload() P {
	return n.payload
}

// SetPayload sets the payload of the node.
// It first cleans up the old payload's dependent objects before setting the new payload.
func (n *PersistentPayloadTreapNode[K, P]) SetPayload(payload P) {
	// Clean up old payload's dependent objects
	if deleter, ok := any(n.payload).(payloadDeleter); ok {
		_ = deleter.DeleteDependents(n.Store)
	}

	n.payload = payload
	// Mark objectId as invalid so node will be re-persisted
	// CRITICAL: Do NOT call DeleteObj here! Parent nodes may still reference
	// this objectId in their leftObjectId/rightObjectId fields or on disk.
	// Deleting the object would orphan the node and cause data loss.
	if store.IsValidObjectId(n.objectId) {
		n.parent.queueDelete(n.objectId)
	}
	n.objectId = bobbob.ObjNotAllocated
}

// deleteDependents best-effort deletes the key object and lets the payload
// delete any child objects it owns before the node itself is freed.
func (n *PersistentPayloadTreapNode[K, P]) deleteDependents() {
	keyObjId, err := n.keyObjectIdFromStore()
	if err != nil {
		// Failed to read key object ID - this is expected if the key is inline
		return
	}
	if store.IsValidObjectId(keyObjId) {
		_ = n.Store.DeleteObj(keyObjId)
	}

	if deleter, ok := any(n.payload).(payloadDeleter); ok {
		_ = deleter.DeleteDependents(n.Store)
	}
}

// keyObjectIdFromStore reads this node's serialized bytes to recover the key's
// backing ObjectId (first field in the marshaled layout).
func (n *PersistentPayloadTreapNode[K, P]) keyObjectIdFromStore() (store.ObjectId, error) {
	if !store.IsValidObjectId(n.objectId) {
		return bobbob.ObjNotAllocated, fmt.Errorf("invalid node object id: %d", n.objectId)
	}

	data, err := store.ReadBytesFromObj(n.Store, n.objectId)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}
	if len(data) < 8 {
		return bobbob.ObjNotAllocated, fmt.Errorf("node object %d too small to contain key id", n.objectId)
	}

	var keyObjId store.ObjectId
	if err := keyObjId.Unmarshal(data[:8]); err != nil {
		return bobbob.ObjNotAllocated, err
	}

	return keyObjId, nil
}

// SetLeft sets the left child of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetLeft(left TreapNodeInterface[K]) error {
	n.TreapNode.left = left

	// Always sync leftObjectId to match what the pointer points to
	// IMPORTANT: Don't call ObjectId() here as it allocates storage prematurely!
	// Just read the existing objectId field without allocating.
	if left != nil {
		if pChild, ok := left.(*PersistentPayloadTreapNode[K, P]); ok {
			// Read the objectId field directly without allocating
			n.leftObjectId = pChild.objectId // May be -1 (invalid), persist() will allocate/write later
			//fmt.Printf("[SetLeft] Node key=%v now has left child (objectId=%d), pointer=%p\n", n.GetKey(), n.leftObjectId, left)
		} else {
			n.leftObjectId = bobbob.ObjNotAllocated
			//fmt.Printf("[SetLeft] Node key=%v has non-PersistentPayloadTreapNode left child\n", n.GetKey())
		}
	} else {
		// Setting to nil - invalidate
		n.leftObjectId = bobbob.ObjNotAllocated
		//fmt.Printf("[SetLeft] Node key=%v left set to nil\n", n.GetKey())
	}

	// Mark as dirty so it gets re-persisted, but DON'T delete the old ObjectId
	// Other nodes (particularly the parent) may still reference it in their persisted data.
	// Deleting it would cause rehydration failures.
	// Dirty tracking will handle invalidation at operation end.
	if store.IsValidObjectId(n.objectId) {
		n.parent.queueDelete(n.objectId)
	}
	n.objectId = bobbob.ObjNotAllocated
	return nil
}

// SetRight sets the right child of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetRight(right TreapNodeInterface[K]) error {
	n.TreapNode.right = right

	// Always sync rightObjectId to match what the pointer points to
	// IMPORTANT: Don't call ObjectId() here as it allocates storage prematurely!
	// Just read the existing objectId field without allocating.
	if right != nil {
		if pChild, ok := right.(*PersistentPayloadTreapNode[K, P]); ok {
			// Read the objectId field directly without allocating
			n.rightObjectId = pChild.objectId // May be -1 (invalid), persist() will allocate/write later
		} else {
			n.rightObjectId = bobbob.ObjNotAllocated
		}
	} else {
		// Setting to nil - invalidate
		n.rightObjectId = bobbob.ObjNotAllocated
	}

	// Mark as dirty so it gets re-persisted, but DON'T delete the old ObjectId
	// Other nodes (particularly the parent) may still reference it in their persisted data.
	// Deleting it would cause rehydration failures.
	// Dirty tracking will handle invalidation at operation end.
	if store.IsValidObjectId(n.objectId) {
		n.parent.queueDelete(n.objectId)
	}
	n.objectId = bobbob.ObjNotAllocated
	return nil
}

// IsNil checks if the node is nil.
// This is explicitly defined to avoid nil deref when called via interface on a typed-nil payload node.
// Even though PersistentTreapNode[K] has the same method, Go's type system requires this override
// because a nil *PersistentPayloadTreapNode[K,P] wrapped in an interface is not nil (typed-nil issue).
func (n *PersistentPayloadTreapNode[K, P]) IsNil() bool {
	return n == nil
}

// GetLeftChild returns the cached left child pointer (may be nil if flushed).
// Does not load from disk; use GetTransientLeftChild for that.
func (n *PersistentPayloadTreapNode[K, P]) GetLeftChild() TreapNodeInterface[K] {
	if n == nil {
		return nil
	}
	return n.TreapNode.left
}

// GetRightChild returns the cached right child pointer (may be nil if flushed).
// Does not load from disk; use GetTransientRightChild for that.
func (n *PersistentPayloadTreapNode[K, P]) GetRightChild() TreapNodeInterface[K] {
	if n == nil {
		return nil
	}
	return n.TreapNode.right
}

// GetTransientLeftChild loads the left child from disk if needed, without caching.
// Returns the child node and error (may be nil if no child exists).
func (n *PersistentPayloadTreapNode[K, P]) GetTransientLeftChild() (PersistentNodeWalker[K], error) {
	return getPayloadChildNodeTransient(n, true)
}

// GetTransientRightChild loads the right child from disk if needed, without caching.
// Returns the child node and error (may be nil if no child exists).
func (n *PersistentPayloadTreapNode[K, P]) GetTransientRightChild() (PersistentNodeWalker[K], error) {
	return getPayloadChildNodeTransient(n, false)
}

// GetLeft returns the left child of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetLeft() TreapNodeInterface[K] {
	if n == nil {
		return nil
	}
	// Check if we need to load the left child from storage
	if n.TreapNode.left == nil && store.IsValidObjectId(n.leftObjectId) {
		tmp, err := NewPayloadFromObjectId[K, P](n.leftObjectId, n.parent, n.Store)
		if err != nil {
			return nil
		}
		n.TreapNode.left = tmp
	}
	return n.TreapNode.left
}

// GetRight returns the right child of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetRight() TreapNodeInterface[K] {
	if n == nil {
		return nil
	}
	// Check if we need to load the right child from storage
	if n.TreapNode.right == nil && store.IsValidObjectId(n.rightObjectId) {
		tmp, err := NewPayloadFromObjectId[K, P](n.rightObjectId, n.parent, n.Store)
		if err != nil {
			return nil
		}
		n.TreapNode.right = tmp
	}
	return n.TreapNode.right
}

func (n *PersistentPayloadTreapNode[K, P]) toPayloadData() ([]byte, error) {
	if lateMarshalPayload, ok := any(n.payload).(bobbob.LateMarshaler); ok {
		// Payload supports LateMarshal
		objId, finisher := lateMarshalPayload.LateMarshal(n.Store)
		payloadData, err := objId.Marshal()
		// FIXME add this to worker pool
		finisher()
		return payloadData, err
	}
	if lateMarshalPayload, ok := any(&n.payload).(bobbob.LateMarshaler); ok {
		// Payload supports LateMarshal (pointer receiver)
		objId, finisher := lateMarshalPayload.LateMarshal(n.Store)
		payloadData, err := objId.Marshal()
		// FIXME add this to worker pool
		finisher()
		return payloadData, err
	}
	return n.payload.Marshal()
}

// Marshal overrides the Marshal method to include the payload.
func (n *PersistentPayloadTreapNode[K, P]) Marshal() ([]byte, error) {
	baseData, err := n.PersistentTreapNode.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal base treap node: %w", err)
	}

	payloadData, err := n.toPayloadData()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	return append(baseData, payloadData...), nil
}

// unmarshal overrides the Unmarshal method to include the payload.
func (n *PersistentPayloadTreapNode[K, P]) unmarshal(data []byte, key types.PersistentKey[K]) error {
	// Unmarshal the base PersistentTreapNode
	err := n.PersistentTreapNode.unmarshal(data, key)
	if err != nil {
		return fmt.Errorf("failed to unmarshal base treap node: %w", err)
	}

	// Calculate the offset for the payload data
	payloadOffset := n.PersistentTreapNode.sizeInBytes()

	// Validate we have enough data for the payload
	if len(data) < payloadOffset {
		return fmt.Errorf("data too short for PersistentPayloadTreapNode: got %d bytes, need at least %d for payload offset", len(data), payloadOffset)
	}

	// Unmarshal the payload
	return n.fromPayloadData(data[payloadOffset:])
}

func (n *PersistentPayloadTreapNode[K, P]) fromPayloadData(data []byte) error {
	if lateUnmarshalPayload, ok := any(&n.payload).(bobbob.LateUnmarshaler); ok {
		// Payload supports LateUnmarshal (pointer receiver)
		var objId store.ObjectId
		err := objId.Unmarshal(data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal payload object id: %w", err)
		}
		finisher := lateUnmarshalPayload.LateUnmarshal(objId, n.Store)
		return finisher()
	}
	if lateUnmarshalPayload, ok := any(n.payload).(bobbob.LateUnmarshaler); ok {
		// Payload supports LateUnmarshal
		var objId store.ObjectId
		err := objId.Unmarshal(data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal payload object id: %w", err)
		}
		finisher := lateUnmarshalPayload.LateUnmarshal(objId, n.Store)
		return finisher()
	}

	val, err := n.payload.Unmarshal(data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal payload data: %w", err)
	}
	payload, ok := val.(P)
	if !ok {
		return fmt.Errorf("unmarshalled payload is not of expected type P")
	}
	n.payload = payload
	return nil
}

func (n *PersistentPayloadTreapNode[K, P]) Unmarshal(data []byte) error {
	return n.unmarshal(data, n.PersistentTreapNode.parent.keyTemplate)
}

// SetObjectId sets the object ID of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetObjectId(id store.ObjectId) {
	n.objectId = id
}

// IsObjectIdInvalid returns true if the node's ObjectId has been invalidated (is negative).
func (n *PersistentPayloadTreapNode[K, P]) IsObjectIdInvalid() bool {
	if n == nil {
		return true
	}
	return n.objectId < 0
}

// Persist persists the node and its subtree to disk.
// This overrides the base PersistentTreapNode.Persist() to handle payload node types correctly.
func (n *PersistentPayloadTreapNode[K, P]) Persist() error {
	if n == nil {
		return nil
	}
	_, err := rangeOverPostOrder[K](n, func(node *PersistentPayloadTreapNode[K, P]) error {
		// Sync left child ObjectId (if pointer exists)
		if node.TreapNode.left != nil {
			leftNode, ok := node.TreapNode.left.(*PersistentPayloadTreapNode[K, P])
			if !ok {
				return fmt.Errorf("left child is not a PersistentPayloadTreapNode")
			}
			leftObjId := leftNode.objectId
			if store.IsValidObjectId(leftObjId) && leftObjId != node.leftObjectId {
				node.leftObjectId = leftObjId
				node.objectId = bobbob.ObjNotAllocated
			}
		}
		// Sync right child ObjectId (if pointer exists)
		if node.TreapNode.right != nil {
			rightNode, ok := node.TreapNode.right.(*PersistentPayloadTreapNode[K, P])
			if !ok {
				return fmt.Errorf("right child is not a PersistentPayloadTreapNode")
			}
			rightObjId := rightNode.objectId
			if store.IsValidObjectId(rightObjId) && rightObjId != node.rightObjectId {
				node.rightObjectId = rightObjId
				node.objectId = bobbob.ObjNotAllocated
			}
		}
		return node.persist()
	})
	return err
}

// PersistentPayloadTreapInterface and PersistentPayloadNodeInterface have been moved to interfaces.go

// PersistentPayloadTreap represents a persistent treap with payloads.
type PersistentPayloadTreap[K any, P types.PersistentPayload[P]] struct {
	PersistentTreap[K]
	payloadPool sync.Pool // Pool for *PersistentPayloadTreapNode[K,P]
}

// NewPersistentPayloadTreapNode creates a new PersistentPayloadTreapNode with the given key, priority, and payload.
func NewPersistentPayloadTreapNode[K any, P types.PersistentPayload[P]](key types.PersistentKey[K], priority Priority, payload P, stre store.Storer, parent *PersistentPayloadTreap[K, P]) *PersistentPayloadTreapNode[K, P] {
	v := parent.payloadPool.Get()
	n, _ := v.(*PersistentPayloadTreapNode[K, P])
	if n == nil {
		n = &PersistentPayloadTreapNode[K, P]{}
	}
	n.TreapNode.key = key
	n.TreapNode.priority = priority
	n.TreapNode.left = nil
	n.TreapNode.right = nil
	n.objectId = bobbob.ObjNotAllocated
	n.leftObjectId = bobbob.ObjNotAllocated
	n.rightObjectId = bobbob.ObjNotAllocated
	n.Store = stre
	n.parent = &parent.PersistentTreap
	var zero P
	n.payload = zero
	n.payload = payload
	return n
}

// releasePayloadNode zeroes and returns a payload node to the pool.
func (t *PersistentPayloadTreap[K, P]) releasePayloadNode(n *PersistentPayloadTreapNode[K, P]) {
	if n == nil {
		return
	}
	n.TreapNode.left = nil
	n.TreapNode.right = nil
	n.TreapNode.key = nil
	n.TreapNode.priority = 0
	n.objectId = bobbob.ObjNotAllocated
	n.leftObjectId = bobbob.ObjNotAllocated
	n.rightObjectId = bobbob.ObjNotAllocated
	var zero P
	n.payload = zero
	n.Store = nil
	n.parent = nil
	t.payloadPool.Put(n)
}

// NewPersistentPayloadTreap creates a new PersistentPayloadTreap with the given comparison function and store reference.
func NewPersistentPayloadTreap[K any, P types.PersistentPayload[P]](lessFunc func(a, b K) bool, keyTemplate types.PersistentKey[K], store store.Storer) *PersistentPayloadTreap[K, P] {
	t := &PersistentPayloadTreap[K, P]{
		PersistentTreap: PersistentTreap[K]{
			Treap: Treap[K]{
				root: nil,
				Less: lessFunc,
			},
			keyTemplate: keyTemplate,
			Store:       store,
		},
	}
	payloadNodeCreate := func() any { return new(PersistentPayloadTreapNode[K, P]) }
	t.payloadPool = sync.Pool{New: payloadNodeCreate}
	return t
}

// insertTracked overrides the base insert method to handle payload updates for duplicate keys,
// while tracking modified nodes for invalidation.
func (t *PersistentPayloadTreap[K, P]) insertTracked(node TreapNodeInterface[K], newNode TreapNodeInterface[K], dirty *[]PersistentTreapNodeInterface[K]) (TreapNodeInterface[K], error) {
	releaseNode := func(n TreapNodeInterface[K]) {
		if payloadNode, ok := n.(*PersistentPayloadTreapNode[K, P]); ok {
			t.releasePayloadNode(payloadNode)
		}
	}

	trackDirty := func(n TreapNodeInterface[K]) {
		t.PersistentTreap.trackDirty(dirty, n)
	}

	duplicateHandler := func(existingNode, incomingNode TreapNodeInterface[K]) bool {
		payloadNode, ok := existingNode.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return false
		}
		newPayloadNode, ok := incomingNode.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return false
		}

		payloadNode.SetPayload(newPayloadNode.GetPayload())
		// CRITICAL: After returning from this handler, ancestors must still be marked dirty
		// because this node's objectId was invalidated by SetPayload. The caller will
		// track the parent node through trackDirty during recursion.
		return true
	}

	return InsertNodeTracked(
		node,
		newNode,
		t.Less,
		releaseNode,
		trackDirty,
		duplicateHandler,
	)
}

// InsertComplex inserts a new node with the given key, priority, and payload into the persistent payload treap.
// Use this method when you need to specify a custom priority value.
// If a key already exists, this will update its payload instead of creating a duplicate.
func (t *PersistentPayloadTreap[K, P]) InsertComplex(key types.PersistentKey[K], priority Priority, payload P) {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t.Store, t)
	dirty := make([]PersistentTreapNodeInterface[K], 0, 32)
	inserted, err := t.insertTracked(t.root, newNode, &dirty)
	if err != nil {
		panic(err)
	}
	t.root = inserted
	t.PersistentTreap.invalidateDirty(dirty)
}

// Insert inserts a new node with the given key and payload into the persistent payload treap.
// If the key implements types.PriorityProvider, its Priority() method is used;
// otherwise, a random priority is generated.
// If a key already exists, this will update its payload instead of creating a duplicate.
// This is the preferred method for most use cases.
func (t *PersistentPayloadTreap[K, P]) Insert(key types.PersistentKey[K], payload P) {
	var priority Priority
	if pp, ok := any(key).(types.PriorityProvider); ok {
		priority = Priority(pp.Priority())
	} else {
		priority = randomPriority()
	}
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t.Store, t)
	dirty := make([]PersistentTreapNodeInterface[K], 0, 32)
	inserted, err := t.insertTracked(t.root, newNode, &dirty)
	if err != nil {
		panic(err)
	}
	t.root = inserted
	t.PersistentTreap.invalidateDirty(dirty)
}

// Delete removes the node with the given key and frees any dependent objects
// (key object and payload-owned objects) even when the subtree root becomes nil.
func (t *PersistentPayloadTreap[K, P]) Delete(key types.PersistentKey[K]) {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()

	var target *PersistentPayloadTreapNode[K, P]
	var targetObjectId store.ObjectId = bobbob.ObjNotAllocated
	if found, _ := SearchNodeComplex(t.root, key.Value(), t.PersistentTreap.Less, nil); found != nil {
		if pNode, ok := found.(*PersistentPayloadTreapNode[K, P]); ok {
			target = pNode
			// Save the objectId BEFORE deleteTracked, which may invalidate it during rotations
			targetObjectId = pNode.objectId
		}
	}

	dirty := make([]PersistentTreapNodeInterface[K], 0, 32)
	deleted, err := t.deleteTracked(t.root, key.Value(), &dirty)
	if err != nil {
		panic(err)
	}
	t.root = deleted

	// After removing from tree, delete all dependent objects if the node was actually removed
	// CRITICAL: Do this BEFORE invalidateDirty, which will set objectId to -1
	if target != nil {
		// Only release/cleanup the node if it was actually removed from the tree.
		// deleteTracked rotates and returns new subtrees; if for any reason the
		// key remains, releasing here would corrupt the live tree.
		if found, _ := SearchNodeComplex(t.root, key.Value(), t.PersistentTreap.Less, nil); found == nil {
			// Temporarily restore objectId so deleteDependents can read from disk
			savedObjectId := target.objectId
			target.objectId = targetObjectId

			// Delete all dependent objects (key backing object + payload-owned objects)
			// Safe to do immediately since node is fully removed from tree
			target.deleteDependents()

			// Restore the invalidated objectId
			target.objectId = savedObjectId

			// Also delete the node's own objectId since it's not in the dirty list
			if store.IsValidObjectId(targetObjectId) {
				_ = t.Store.DeleteObj(targetObjectId)
			}

			// Return node to pool after cleanup
			t.releasePayloadNode(target)
		}
	}

	// Invalidate dirty nodes from the deletion operation
	// These are nodes that were rotated during delete - their stale ObjectIds
	// will be queued and deleted after the next persist, NOT immediately.
	t.PersistentTreap.invalidateDirty(dirty)
}

// NewPayloadFromObjectId creates a PersistentPayloadTreapNode from the given object ID.
// Reading it in fron the store if it exuists.
func NewPayloadFromObjectId[T any, P types.PersistentPayload[P]](objId store.ObjectId, parent *PersistentTreap[T], stre store.Storer) (*PersistentPayloadTreapNode[T, P], error) {
	tmp := &PersistentPayloadTreapNode[T, P]{
		PersistentTreapNode: PersistentTreapNode[T]{
			Store:  stre,
			parent: parent,
		},
	}
	err := store.ReadGeneric(stre, tmp, objId)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload node from store (objectId=%d): %w", objId, err)
	}
	// Restore the accurate objectId when reloading from disk
	tmp.SetObjectId(objId)
	return tmp, nil
}

func (t *PersistentPayloadTreap[K, P]) Load(objId store.ObjectId) error {
	var err error
	t.root, err = NewPayloadFromObjectId[K, P](objId, &t.PersistentTreap, t.Store)
	if err != nil {
		return fmt.Errorf("failed to load root node from objId=%d: %w", objId, err)
	}
	return nil
}

// SearchComplex searches for the node with the given key in the persistent treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback receives the node that was accessed, allowing for custom operations
// such as updating access times for LRU caching or flushing stale nodes.
// This method automatically updates the lastAccessTime on each accessed node.
// The callback can return an error to abort the search.
func (t *PersistentPayloadTreap[K, P]) SearchComplex(key types.PersistentKey[K], callback func(TreapNodeInterface[K]) error) (PersistentPayloadNodeInterface[K, P], error) {
	t.PersistentTreap.mu.RLock()
	defer t.PersistentTreap.mu.RUnlock()
	// Create a wrapper callback that updates the access time
	wrappedCallback := func(node TreapNodeInterface[K]) error {
		// Update the access time if this is a persistent node
		if pNode, ok := node.(*PersistentPayloadTreapNode[K, P]); ok {
			pNode.TouchAccessTime()
		}
		// Call the user's callback if provided
		if callback != nil {
			return callback(node)
		}
		return nil
	}

	node, err := SearchNodeComplex(t.root, key.Value(), t.PersistentTreap.Less, wrappedCallback)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}
	n, ok := node.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return nil, fmt.Errorf("node is not a PersistentPayloadTreapNode")
	}
	return n, nil
}

// Search searches for the node with the given key in the persistent treap.
// It calls SearchComplex with a nil callback.
func (t *PersistentPayloadTreap[K, P]) Search(key types.PersistentKey[K]) PersistentPayloadNodeInterface[K, P] {
	t.PersistentTreap.mu.RLock()
	defer t.PersistentTreap.mu.RUnlock()
	// Create a wrapper callback that updates the access time
	wrappedCallback := func(node TreapNodeInterface[K]) error {
		// Update the access time if this is a persistent node
		if pNode, ok := node.(*PersistentPayloadTreapNode[K, P]); ok {
			pNode.TouchAccessTime()
		}
		return nil
	}

	node, _ := SearchNodeComplex(t.root, key.Value(), t.PersistentTreap.Less, wrappedCallback)
	if node == nil {
		return nil
	}
	n, _ := node.(*PersistentPayloadTreapNode[K, P])
	return n
} // UpdatePayload updates the payload of the node with the given key.
func (t *PersistentPayloadTreap[K, P]) UpdatePayload(key types.PersistentKey[K], newPayload P) error {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()

	path := make([]*PersistentPayloadTreapNode[K, P], 0, 32)
	var findPath func(node TreapNodeInterface[K], target K) (*PersistentPayloadTreapNode[K, P], bool)
	findPath = func(node TreapNodeInterface[K], target K) (*PersistentPayloadTreapNode[K, P], bool) {
		if node == nil || node.IsNil() {
			return nil, false
		}
		pNode, ok := node.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return nil, false
		}
		pNode.TouchAccessTime()
		path = append(path, pNode)

		nodeKey := pNode.GetKey().Value()
		if !t.PersistentTreap.Less(target, nodeKey) && !t.PersistentTreap.Less(nodeKey, target) {
			return pNode, true
		}

		if t.PersistentTreap.Less(target, nodeKey) {
			if found, ok := findPath(pNode.GetLeft(), target); ok {
				return found, true
			}
		} else {
			if found, ok := findPath(pNode.GetRight(), target); ok {
				return found, true
			}
		}

		path = path[:len(path)-1]
		return nil, false
	}

	payloadNode, found := findPath(t.root, key.Value())
	if !found || payloadNode == nil {
		return nil
	}

	// Update payload and re-persist the node to get a fresh ObjectId
	payloadNode.SetPayload(newPayload)
	if err := payloadNode.persist(); err != nil {
		return err
	}

	// Re-persist ancestors so their cached child ObjectIds are updated
	for i := len(path) - 2; i >= 0; i-- {
		if err := path[i].persist(); err != nil {
			return err
		}
	}
	return nil
}

// Compare compares this persistent payload treap with another persistent payload treap and invokes callbacks for keys that are:
// - Only in this treap (onlyInA)
// - In both treaps (inBoth)
// - Only in the other treap (onlyInB)
//
// This is a thread-safe wrapper that locks both treaps in a consistent data-driven order
// based on their root key values to prevent deadlocks.
//
// Note: The callbacks receive TreapNodeInterface[K] which can be type-asserted to
// PersistentPayloadNodeInterface[K, P] to access payloads:
//
//	treapA.Compare(treapB,
//	    func(node TreapNodeInterface[K]) error {
//	        payloadNode := node.(PersistentPayloadNodeInterface[K, P])
//	        fmt.Printf("Only in A: key=%v, payload=%v\n", node.GetKey().Value(), payloadNode.GetPayload())
//	        return nil
//	    },
//	    func(nodeA, nodeB TreapNodeInterface[K]) error {
//	        payloadA := nodeA.(PersistentPayloadNodeInterface[K, P]).GetPayload()
//	        payloadB := nodeB.(PersistentPayloadNodeInterface[K, P]).GetPayload()
//	        fmt.Printf("In both: key=%v\n", nodeA.GetKey().Value())
//	        return nil
//	    },
//	    func(node TreapNodeInterface[K]) error {
//	        fmt.Printf("Only in B: %v\n", node.GetKey().Value())
//	        return nil
//	    },
//	)
func (t *PersistentPayloadTreap[K, P]) Compare(
	other *PersistentPayloadTreap[K, P],
	onlyInA func(TreapNodeInterface[K]) error,
	inBoth func(nodeA, nodeB TreapNodeInterface[K]) error,
	onlyInB func(TreapNodeInterface[K]) error,
) error {
	// Lock both treaps for reading in a consistent order to avoid deadlocks.
	// Order is determined by the underlying PersistentTreap's data-driven comparison.
	if t.PersistentTreap.shouldLockFirst(&other.PersistentTreap) {
		t.PersistentTreap.mu.RLock()
		defer t.PersistentTreap.mu.RUnlock()
		other.PersistentTreap.mu.RLock()
		defer other.PersistentTreap.mu.RUnlock()
	} else {
		other.PersistentTreap.mu.RLock()
		defer other.PersistentTreap.mu.RUnlock()
		t.PersistentTreap.mu.RLock()
		defer t.PersistentTreap.mu.RUnlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nextA, cancelA := seq2Next(t.PersistentTreap.iterInOrder(ctx))
	nextB, cancelB := seq2Next(other.PersistentTreap.iterInOrder(ctx))
	defer cancelA()
	defer cancelB()

	return mergeOrdered(nextA, nextB, t.Less, onlyInA, inBoth, onlyInB)
}

// persistLockedTree persists the entire treap to the store.
// Assumes the caller already holds the write lock (t.mu.Lock()).
// getPayloadChildNodeTransient loads a child node from the cached pointer or from disk transiently.
// This is used during post-order traversal to load nodes without permanently caching them.
func getPayloadChildNodeTransient[K any, P types.PersistentPayload[P]](node *PersistentPayloadTreapNode[K, P], isLeft bool) (*PersistentPayloadTreapNode[K, P], error) {
	if node == nil || node.IsNil() {
		return nil, nil
	}

	if isLeft {
		if node.TreapNode.left != nil && !node.TreapNode.left.IsNil() {
			leftNode, ok := node.TreapNode.left.(*PersistentPayloadTreapNode[K, P])
			if !ok {
				return nil, fmt.Errorf("left child is not a PersistentPayloadTreapNode")
			}
			return leftNode, nil
		}
		if store.IsValidObjectId(node.leftObjectId) {
			return NewPayloadFromObjectId[K, P](node.leftObjectId, node.parent, node.Store)
		}
		return nil, nil
	}

	if node.TreapNode.right != nil && !node.TreapNode.right.IsNil() {
		rightNode, ok := node.TreapNode.right.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return nil, fmt.Errorf("right child is not a PersistentPayloadTreapNode")
		}
		return rightNode, nil
	}
	if store.IsValidObjectId(node.rightObjectId) {
		return NewPayloadFromObjectId[K, P](node.rightObjectId, node.parent, node.Store)
	}
	return nil, nil
}

// persistLockedTreePayload persists the entire payload treap to the store.
// Assumes the caller already holds the write lock.
// Uses the polymorphic post-order traversal to ensure children are persisted before parents.
func (t *PersistentPayloadTreap[K, P]) persistLockedTree() error {
	if t.root == nil {
		return nil
	}
	rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("root is not a PersistentPayloadTreapNode")
	}

	// Use polymorphic post-order traversal that properly dispatches
	_, err := rangeOverPostOrder[K](rootNode, func(node *PersistentPayloadTreapNode[K, P]) error {
		// Sync left child ObjectId (if pointer exists)
		if node.TreapNode.left != nil {
			leftNode, ok := node.TreapNode.left.(*PersistentPayloadTreapNode[K, P])
			if !ok {
				return fmt.Errorf("left child is not a PersistentPayloadTreapNode")
			}
			leftObjId := leftNode.objectId
			if store.IsValidObjectId(leftObjId) && leftObjId != node.leftObjectId {
				node.leftObjectId = leftObjId
				node.objectId = bobbob.ObjNotAllocated
			}
		}
		// Sync right child ObjectId (if pointer exists)
		if node.TreapNode.right != nil {
			rightNode, ok := node.TreapNode.right.(*PersistentPayloadTreapNode[K, P])
			if !ok {
				return fmt.Errorf("right child is not a PersistentPayloadTreapNode")
			}
			rightObjId := rightNode.objectId
			if store.IsValidObjectId(rightObjId) && rightObjId != node.rightObjectId {
				node.rightObjectId = rightObjId
				node.objectId = bobbob.ObjNotAllocated
			}
		}
		// Call the payload-specific persist() method
		return node.persist()
	})
	if err != nil {
		return err
	}
	// Safe to delete queued objects now that the tree is fully persisted.
	t.PersistentTreap.flushPendingDeletes()
	return nil
}

// Persist persists the entire payload treap to the store.
// Acquires the write lock to ensure atomic persistence.
func (t *PersistentPayloadTreap[K, P]) Persist() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.persistLockedTree()
}

// CompactSuboptimalAllocations deletes nodes that reside in sub-optimal block
// allocators (smaller blockCount than the current pool) and marks them for
// reallocation on the next persist. Nodes remain in the treap; only their
// stored ObjectIds are cleared and the backing objects are deleted.
func (t *PersistentPayloadTreap[K, P]) CompactSuboptimalAllocations() (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// TODO: BlockAllocatorCompactor interface not yet ported to new allocator package
	// This feature requires the new allocator to support detailed BlockAllocator inspection
	// For now, compaction is disabled until the interface is re-implemented
	return 0, nil

	// CODE BELOW TEMPORARILY DISABLED - BlockAllocatorCompactor not yet ported
	/*
		provider, ok := t.Store.(store.AllocatorProvider)
		if !ok {
			return 0, nil
		}
		compactor, ok := provider.Allocator().(allocator.BlockAllocatorCompactor)
		if !ok {
			return 0, nil
		}

		var zero PersistentPayloadTreapNode[K, P]
		nodeSize := zero.sizeInBytes()
		blockSize, allocatorIndex, _, found := compactor.FindSmallestBlockAllocatorForSize(nodeSize)
		if !found {
			return 0, nil
		}

		objectIds := compactor.GetObjectIdsInAllocator(blockSize, allocatorIndex)
		if len(objectIds) == 0 {
			return 0, nil
		}

		idSet := make(map[store.ObjectId]struct{}, len(objectIds))
		for _, id := range objectIds {
			if store.IsValidObjectId(id) {
				idSet[id] = struct{}{}
			}
		}

		deleted := 0
		var walk func(TreapNodeInterface[K])
		walk = func(node TreapNodeInterface[K]) {
			if node == nil || node.IsNil() {
				return
			}
			pnode, ok := node.(*PersistentPayloadTreapNode[K, P])
			if !ok {
				return
			}

			walk(pnode.GetLeft())
			if store.IsValidObjectId(pnode.objectId) {
				if _, hit := idSet[pnode.objectId]; hit {
					_ = t.Store.DeleteObj(pnode.objectId) // best effort cleanup
					pnode.SetObjectId(bobbob.ObjNotAllocated)
					deleted++
				}
			}
			walk(pnode.GetRight())
		}

		walk(t.root)
		return deleted, nil
	*/
}

// RangeOverTreapPayloadPostOrder performs a post-order traversal of the treap.
// It automatically tracks nodes that become dirty during traversal.
//
// Returns the list of dirty nodes (including ancestors) and any error.
func (t *PersistentPayloadTreap[K, P]) RangeOverTreapPayloadPostOrder(callback func(node *PersistentPayloadTreapNode[K, P]) error) ([]*PersistentPayloadTreapNode[K, P], error) {
	if t.root == nil {
		return nil, nil
	}
	rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return nil, fmt.Errorf("root is not a PersistentPayloadTreapNode")
	}

	return rangeOverPostOrder[K](rootNode, callback)
}

// PayloadNodeInfo contains information about a payload node in memory, including its access timestamp.
type PayloadNodeInfo[K any, P types.PersistentPayload[P]] struct {
	Node           *PersistentPayloadTreapNode[K, P]
	LastAccessTime int64
	Key            types.PersistentKey[K]
}

// GetInMemoryNodes traverses the treap and collects all nodes currently in memory.
// This method does NOT load nodes from disk and does NOT update access timestamps.
// It only includes nodes that are already loaded in memory.
// Returns a slice of PayloadNodeInfo containing each node and its last access time.
func (t *PersistentPayloadTreap[K, P]) GetInMemoryNodes() []PayloadNodeInfo[K, P] {
	var nodes []PayloadNodeInfo[K, P]
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.collectInMemoryPayloadNodes(t.root, &nodes)
	return nodes
}

// GetInMemoryNodesLocked traverses the treap and collects all nodes currently in memory.
// This variant assumes the caller already holds the write lock (e.g., from FlushOldestPercentile).
// It performs the same operation as GetInMemoryNodes but without acquiring locks.
// Use this when calling from within locked contexts to avoid deadlock.
func (t *PersistentPayloadTreap[K, P]) GetInMemoryNodesLocked() []PayloadNodeInfo[K, P] {
	var nodes []PayloadNodeInfo[K, P]
	t.collectInMemoryPayloadNodes(t.root, &nodes)
	return nodes
}

// CountInMemoryNodes returns the count of nodes currently loaded in memory.
// This is more efficient than len(GetInMemoryNodes()) as it doesn't allocate the slice.
func (t *PersistentPayloadTreap[K, P]) CountInMemoryNodes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.countInMemoryPayloadNodes(t.root)
}

// CountInMemoryNodesLocked returns the count of nodes currently loaded in memory.
// This variant assumes the caller already holds the write lock (e.g., from InOrderMutate).
// It performs the same operation as CountInMemoryNodes but without acquiring locks.
func (t *PersistentPayloadTreap[K, P]) CountInMemoryNodesLocked() int {
	return t.countInMemoryPayloadNodes(t.root)
}

func isNilTreapNode[K any](node TreapNodeInterface[K]) bool {
	if node == nil {
		return true
	}
	v := reflect.ValueOf(node)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// countInMemoryPayloadNodes recursively counts in-memory nodes.
func (t *PersistentPayloadTreap[K, P]) countInMemoryPayloadNodes(node TreapNodeInterface[K]) int {
	if isNilTreapNode(node) || node.IsNil() {
		return 0
	}

	pNode, ok := node.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return 0
	}

	count := 1 // Count this node

	// Recursively count children only if they're in memory
	if !isNilTreapNode(pNode.left) && !pNode.left.IsNil() {
		count += t.countInMemoryPayloadNodes(pNode.left)
	}
	if !isNilTreapNode(pNode.right) && !pNode.right.IsNil() {
		count += t.countInMemoryPayloadNodes(pNode.right)
	}

	return count
}

// collectInMemoryPayloadNodes is a helper that recursively collects in-memory nodes.
// It only traverses nodes that are already loaded (does not trigger disk reads).
func (t *PersistentPayloadTreap[K, P]) collectInMemoryPayloadNodes(node TreapNodeInterface[K], nodes *[]PayloadNodeInfo[K, P]) {
	if isNilTreapNode(node) || node.IsNil() {
		return
	}

	// Convert to PersistentPayloadTreapNode to access in-memory state
	pNode, ok := node.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return
	}

	// Add this node to the list
	*nodes = append(*nodes, PayloadNodeInfo[K, P]{
		Node:           pNode,
		LastAccessTime: pNode.GetLastAccessTime(),
		Key:            pNode.GetKey().(types.PersistentKey[K]),
	})

	// Only traverse children that are already in memory
	// Check the left child without triggering a load
	if !isNilTreapNode(pNode.TreapNode.left) && !pNode.TreapNode.left.IsNil() {
		t.collectInMemoryPayloadNodes(pNode.TreapNode.left, nodes)
	}

	// Check the right child without triggering a load
	if !isNilTreapNode(pNode.TreapNode.right) && !pNode.TreapNode.right.IsNil() {
		t.collectInMemoryPayloadNodes(pNode.TreapNode.right, nodes)
	}
}

// FlushOlderThan flushes all nodes that haven't been accessed since the given timestamp.
// This method first persists any unpersisted nodes, then removes them from memory
// if their last access time is older than the specified cutoff timestamp.
// Nodes can be reloaded later from disk when needed.
// Returns the number of nodes flushed and any error encountered.
func (t *PersistentPayloadTreap[K, P]) FlushOlderThan(cutoffTimestamp int64) (int, error) {
	// CRITICAL: Hold the lock during ENTIRE persist + flush operation
	// to prevent concurrent insertions from invalidating childObjectIds
	t.mu.Lock()
	defer t.mu.Unlock()

	// First, persist the entire tree to ensure all nodes are saved
	err := t.persistLockedTree()
	if err != nil {
		return 0, err
	}

	// Get all in-memory nodes (caller holds lock)
	var nodes []PayloadNodeInfo[K, P]
	t.collectInMemoryPayloadNodes(t.root, &nodes)

	// Count how many we flush
	flushedCount := 0

	// Flush nodes older than the cutoff
	for _, nodeInfo := range nodes {
		if nodeInfo.LastAccessTime < cutoffTimestamp {
			// Attempt to flush this node
			err := nodeInfo.Node.Flush()
			if err != nil {
				// If we get errNotFullyPersisted, it means children weren't persisted
				// but we already called persistLockedTree() above, so this shouldn't happen
				// Continue anyway to flush what we can
				if !errors.Is(err, errNotFullyPersisted) {
					return flushedCount, err
				}
			} else {
				flushedCount++
			}
		}
	}

	return flushedCount, nil
}

// FlushOldestPercentile flushes the oldest percentage of nodes from memory.
// This method first persists any unpersisted nodes, then removes the oldest N% of nodes
// from memory based on their last access time. Nodes can be reloaded later from disk.
//
// Parameters:
//   - percentage: percentage (0-100) of oldest nodes to flush
//
// Returns the number of nodes flushed and any error encountered.
func (t *PersistentPayloadTreap[K, P]) FlushOldestPercentile(percentage int) (int, error) {
	if percentage <= 0 || percentage > 100 {
		return 0, fmt.Errorf("percentage must be between 1 and 100, got %d", percentage)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.flushOldestPercentileLocked(percentage)
}

// flushOldestPercentileLocked is the internal implementation that assumes the caller holds the lock.
// Use this from within InOrderMutate callbacks or other locked contexts.
func (t *PersistentPayloadTreap[K, P]) flushOldestPercentileLocked(percentage int) (int, error) {
	if percentage <= 0 || percentage > 100 {
		return 0, fmt.Errorf("percentage must be between 1 and 100, got %d", percentage)
	}

	// First, persist the entire tree to ensure all nodes are saved
	// We must do this while holding the lock to prevent concurrent modifications
	err := t.persistLockedTree()
	if err != nil {
		return 0, err
	}

	// Get all in-memory nodes
	var nodes []PayloadNodeInfo[K, P]
	t.collectInMemoryPayloadNodes(t.root, &nodes)
	// nodes := t.GetInMemoryNodesLocked()
	if len(nodes) == 0 {
		return 0, nil
	}

	// Sort nodes by access time (oldest first) using Go's introsort
	// This is O(n log n) instead of O(n²) insertion sort
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].LastAccessTime < nodes[j].LastAccessTime
	})

	// Calculate how many nodes to flush
	numToFlush := (len(nodes) * percentage) / 100
	if numToFlush == 0 && percentage > 0 {
		numToFlush = 1 // Flush at least one node if percentage > 0
	}

	// Flush the oldest nodes
	flushedCount := 0
	for i := 0; i < numToFlush && i < len(nodes); i++ {
		err := nodes[i].Node.Flush()
		if err != nil {
			if !errors.Is(err, errNotFullyPersisted) {
				return flushedCount, err
			}
			// Log unpersisted children to help debug
			leftValid := store.IsValidObjectId(nodes[i].Node.leftObjectId)
			rightValid := store.IsValidObjectId(nodes[i].Node.rightObjectId)
			if !leftValid || !rightValid {
				// Silent ignore - but children with invalid ObjectIds will cause data loss
				// when this node is flushed from disk later
			}
		} else {
			flushedCount++
		}
	}

	return flushedCount, nil
}

// GetRootObjectId returns the ObjectId of the root node of the treap.
// Returns ObjNotAllocated if the tree is empty or hasn't been persisted yet.
func (t *PersistentPayloadTreap[K, P]) GetRootObjectId() (store.ObjectId, error) {
	if t.root == nil {
		return bobbob.ObjNotAllocated, nil
	}
	rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return bobbob.ObjNotAllocated, fmt.Errorf("root is not a PersistentPayloadTreapNode")
	}
	return rootNode.ObjectId()
}

func (n *PersistentPayloadTreapNode[K, P]) LateMarshal(stre store.Storer) (store.ObjectId, bobbob.Finisher) {
	marshalled, err := n.Marshal()
	if err != nil {
		return 0, func() error { return err }
	}
	return store.LateWriteNewObjFromBytes(stre, marshalled)
}

// persist does the actual work of writing a single node to disk.
// This is called from within the post-order traversal by PersistentPayloadTreap.persistLockedTree().
func (n *PersistentPayloadTreapNode[K, P]) persist() error {
	buf, err := n.Marshal()
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	objId, err := n.ObjectId()
	if err != nil {
		return fmt.Errorf("get objectId failed: %w", err)
	}

	// ObjectId() allocates if needed; this branch is a defensive fallback
	// in case an invalid ID is still returned.
	if !store.IsValidObjectId(objId) {
		// The allocator may round up our size request to a block boundary.
		// We need to write exactly as many bytes as we request to allocate,
		// padding with zeros if necessary to avoid uninitialized data.
		// Request enough space for our data
		requestedSize := len(buf)

		// Use LateWriteNewObj to allocate and get a writer
		newObjId, writer, finisher, err := n.Store.LateWriteNewObj(requestedSize)
		if err != nil {
			return fmt.Errorf("failed to allocate new object (size=%d): %w", requestedSize, err)
		}
		defer func() {
			if finisher != nil {
				finisher()
			}
		}()

		// Write our data
		written, err := writer.Write(buf)
		if err != nil {
			return fmt.Errorf("failed to write to new object %d: %w", newObjId, err)
		}
		if written != len(buf) {
			return fmt.Errorf("incomplete write to object %d: wrote %d of %d bytes", newObjId, written, len(buf))
		}

		n.objectId = newObjId
		return nil
	}

	// Check if the new data fits in the existing allocation
	// If the allocator reports the object's allocated size, verify the new buffer fits
	if objInfoProvider, ok := n.Store.(interface {
		GetObjectInfo(store.ObjectId) (store.ObjectInfo, bool)
	}); ok {
		objInfo, found := objInfoProvider.GetObjectInfo(objId)
		if found {
			// If the new data exceeds the allocated size, we need to reallocate
			if len(buf) > objInfo.Size {
				// Allocate new object with enough space
				newObjId, writer, finisher, err := n.Store.LateWriteNewObj(len(buf))
				if err != nil {
					return fmt.Errorf("failed to reallocate object (size=%d): %w", len(buf), err)
				}
				defer func() {
					if finisher != nil {
						finisher()
					}
				}()

				// Write to new object
				written, err := writer.Write(buf)
				if err != nil {
					return fmt.Errorf("failed to write to reallocated object %d: %w", newObjId, err)
				}
				if written != len(buf) {
					return fmt.Errorf("incomplete write to reallocated object %d: wrote %d of %d bytes", newObjId, written, len(buf))
				}

				// Delete old object (best effort)
				_ = n.Store.DeleteObj(objId)

				// Update to new object ID
				n.objectId = newObjId
				return nil
			}
		}
	}

	// Otherwise, update the existing object (data fits in allocated space)
	if err := store.WriteBytesToObj(n.Store, buf, objId); err != nil {
		return fmt.Errorf("failed to write to existing object %d: %w", objId, err)
	}
	return nil
}

// Marshal should Return some byte slice representing the payload treap
func (t *PersistentPayloadTreap[K, P]) Marshal() ([]byte, error) {
	root, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return nil, fmt.Errorf("root is not a PersistentPayloadTreapNode")
	}
	return root.objectId.Marshal()
}

// SizeInBytes returns the size in bytes of the marshalled treap (just the root ObjectId)
func (t *PersistentPayloadTreap[K, P]) SizeInBytes() int {
	var objId store.ObjectId
	return objId.SizeInBytes()
}

// Unmarshal implements the types.UntypedPersistentPayload interface
func (t *PersistentPayloadTreap[K, P]) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	var rootId store.ObjectId
	err := rootId.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	root, err := NewPayloadFromObjectId[K, P](rootId, &t.PersistentTreap, t.Store)
	if err != nil {
		return nil, err
	}
	t.root = root
	return t, nil
}

// iterateOnDiskTransient loads payload nodes transiently via object IDs without caching.
// This overrides the base PersistentTreap method to ensure we load PersistentPayloadTreapNodes.
func (t *PersistentPayloadTreap[K, P]) iterateOnDiskTransient(rootObjId store.ObjectId, callback IterationCallback[K]) error {
	if !store.IsValidObjectId(rootObjId) {
		return nil
	}

	// Stack for in-order traversal: (objId, state)
	// state: 0=enter, 1=visiting node, 2=exit right
	type stackFrame struct {
		objId store.ObjectId
		state int
	}

	stack := []stackFrame{{objId: rootObjId, state: 0}}

	for len(stack) > 0 {
		frame := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if !store.IsValidObjectId(frame.objId) {
			continue
		}

		// Load payload node transiently
		node, err := NewPayloadFromObjectId[K, P](frame.objId, &t.PersistentTreap, t.Store)
		if err != nil {
			return fmt.Errorf("failed to load payload node from disk (objId=%d): %w", frame.objId, err)
		}

		switch frame.state {
		case 0: // Enter: push right, visit node, push left
			if store.IsValidObjectId(node.rightObjectId) {
				stack = append(stack, stackFrame{objId: node.rightObjectId, state: 0})
			}
			stack = append(stack, stackFrame{objId: frame.objId, state: 1})
			if store.IsValidObjectId(node.leftObjectId) {
				stack = append(stack, stackFrame{objId: node.leftObjectId, state: 0})
			}

		case 1: // Visit node
			if err := callback(node); err != nil {
				return err
			}
		}
	}

	return nil
}

// iterateOnDiskAndLoad loads payload nodes and retains them in the in-memory tree.
// This overrides the base PersistentTreap method to ensure we load PersistentPayloadTreapNodes.
func (t *PersistentPayloadTreap[K, P]) iterateOnDiskAndLoad(objId store.ObjectId, callback IterationCallback[K]) error {
	if !store.IsValidObjectId(objId) {
		return nil
	}

	node, err := NewPayloadFromObjectId[K, P](objId, &t.PersistentTreap, t.Store)
	if err != nil {
		return fmt.Errorf("failed to load payload node from disk (objId=%d): %w", objId, err)
	}

	// In-order: left, node, right
	if store.IsValidObjectId(node.leftObjectId) {
		if err := t.iterateOnDiskAndLoad(node.leftObjectId, callback); err != nil {
			return err
		}
	}

	if err := callback(node); err != nil {
		return err
	}

	if store.IsValidObjectId(node.rightObjectId) {
		if err := t.iterateOnDiskAndLoad(node.rightObjectId, callback); err != nil {
			return err
		}
	}

	return nil
}
