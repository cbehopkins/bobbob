package treap

import (
	"errors"
	"fmt"
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
func (n *PersistentPayloadTreapNode[K, P]) SetPayload(payload P) {
	n.payload = payload
	_ = n.Store.DeleteObj(n.objectId) // Invalidate the stored object ID (best effort)
	n.objectId = bobbob.ObjNotAllocated
}

// deleteDependents best-effort deletes the key object and lets the payload
// delete any child objects it owns before the node itself is freed.
func (n *PersistentPayloadTreapNode[K, P]) deleteDependents() {
	if keyObjId, err := n.keyObjectIdFromStore(); err == nil && store.IsValidObjectId(keyObjId) {
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
	if len(data) < n.objectId.SizeInBytes() {
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
	if left == nil {
		return n.PersistentTreapNode.SetLeft(nil)
	}
	tmp, ok := left.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("left child is not a PersistentPayloadTreapNode")
	}
	return n.PersistentTreapNode.SetLeft(tmp)
}

// SetRight sets the right child of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetRight(right TreapNodeInterface[K]) error {
	if right == nil {
		return n.PersistentTreapNode.SetRight(nil)
	}
	tmp, ok := right.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("right child is not a PersistentPayloadTreapNode")
	}
	return n.PersistentTreapNode.SetRight(tmp)
}

// GetLeft returns the left child of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetLeft() TreapNodeInterface[K] {
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

// Marshal overrides the Marshal method to include the payload.
func (n *PersistentPayloadTreapNode[K, P]) Marshal() ([]byte, error) {
	baseData, err := n.PersistentTreapNode.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal base treap node: %w", err)
	}

	payloadData, err := n.payload.Marshal()
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
	val, err := n.payload.Unmarshal(data[payloadOffset:])
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

// insert overrides the base insert method to handle payload updates for duplicate keys.
// If a key already exists, it updates the payload instead of creating a duplicate node.
func (t *PersistentPayloadTreap[K, P]) insert(node TreapNodeInterface[K], newNode TreapNodeInterface[K]) TreapNodeInterface[K] {
	if node == nil || node.IsNil() {
		return newNode
	}

	newKey := newNode.GetKey().Value()
	nodeKey := node.GetKey().Value()

	// Check for exact key match - update payload instead of inserting duplicate
	if !t.Less(newKey, nodeKey) && !t.Less(nodeKey, newKey) {
		// Keys are equal - update the payload
		if payloadNode, ok := node.(*PersistentPayloadTreapNode[K, P]); ok {
			if newPayloadNode, ok := newNode.(*PersistentPayloadTreapNode[K, P]); ok {
				payloadNode.SetPayload(newPayloadNode.GetPayload())
				// Return the unused new node to the pool
				t.releasePayloadNode(newPayloadNode)
				return node
			}
		}
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

// InsertComplex inserts a new node with the given key, priority, and payload into the persistent payload treap.
// Use this method when you need to specify a custom priority value.
// If a key already exists, this will update its payload instead of creating a duplicate.
func (t *PersistentPayloadTreap[K, P]) InsertComplex(key types.PersistentKey[K], priority Priority, payload P) {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t.Store, t)
	var tmp TreapNodeInterface[K]
	tmp = t.insert(t.root, newNode)
	t.root = tmp
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
	var tmp TreapNodeInterface[K]
	tmp = t.insert(t.root, newNode)
	t.root = tmp
}

// Delete removes the node with the given key and frees any dependent objects
// (key object and payload-owned objects) even when the subtree root becomes nil.
func (t *PersistentPayloadTreap[K, P]) Delete(key types.PersistentKey[K]) {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()

	var target *PersistentPayloadTreapNode[K, P]
	if found, _ := t.searchComplex(t.root, key.Value(), nil); found != nil {
		if pNode, ok := found.(*PersistentPayloadTreapNode[K, P]); ok {
			target = pNode
		}
	}

	t.root = t.delete(t.root, key.Value())

	if target != nil {
		target.deleteDependents()
		if objId, err := target.ObjectId(); err == nil && store.IsValidObjectId(objId) {
			_ = t.Store.DeleteObj(objId)
		}
		target.SetObjectId(bobbob.ObjNotAllocated)
		// Return node to pool after cleanup
		t.releasePayloadNode(target)
	}
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
	return tmp, nil
}

func (t *PersistentPayloadTreap[K, P]) Load(objId store.ObjectId) error {
	var err error
	t.root, err = NewPayloadFromObjectId[K, P](objId, &t.PersistentTreap, t.Store)
	return err
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

	node, err := t.searchComplex(t.root, key.Value(), wrappedCallback)
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

	node, _ := t.searchComplex(t.root, key.Value(), wrappedCallback)
	if node == nil {
		return nil
	}
	n, _ := node.(*PersistentPayloadTreapNode[K, P])
	return n
} // UpdatePayload updates the payload of the node with the given key.
func (t *PersistentPayloadTreap[K, P]) UpdatePayload(key types.PersistentKey[K], newPayload P) error {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	// Create a wrapper callback that updates the access time
	wrappedCallback := func(node TreapNodeInterface[K]) error {
		// Update the access time if this is a persistent node
		if pNode, ok := node.(*PersistentPayloadTreapNode[K, P]); ok {
			pNode.TouchAccessTime()
		}
		return nil
	}

	node, _ := t.searchComplex(t.root, key.Value(), wrappedCallback)
	if node != nil && !node.IsNil() {
		payloadNode, ok := node.(*PersistentPayloadTreapNode[K, P])
		if ok {
			// Update the payload in memory and invalidate the stored object id
			payloadNode.SetPayload(newPayload)
			return payloadNode.persist(false, nil)
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

	return t.PersistentTreap.Treap.Compare(&other.PersistentTreap.Treap, onlyInA, inBoth, onlyInB)
}

func (t *PersistentPayloadTreap[K, P]) Persist() error {
	// FIXME can this use workers like PersistentTreap.Persist()?
	if t.root == nil {
		return nil
	}
	rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("root is not a PersistentPayloadTreapNode")
	}
	pwp := newPersistWorkerPool(4)
	if pwp == nil {
		return errors.New("failed to create persist worker pool")
	}
	err := rootNode.persist(true, pwp)
	if err != nil {
		_ = pwp.Close()
		return err
	}

	return pwp.Close()
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

// CountInMemoryNodes returns the count of nodes currently loaded in memory.
// This is more efficient than len(GetInMemoryNodes()) as it doesn't allocate the slice.
func (t *PersistentPayloadTreap[K, P]) CountInMemoryNodes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.countInMemoryPayloadNodes(t.root)
}

// countInMemoryPayloadNodes recursively counts in-memory nodes.
func (t *PersistentPayloadTreap[K, P]) countInMemoryPayloadNodes(node TreapNodeInterface[K]) int {
	if node == nil || node.IsNil() {
		return 0
	}

	pNode, ok := node.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return 0
	}

	count := 1 // Count this node

	// Recursively count children only if they're in memory
	if pNode.left != nil && !pNode.left.IsNil() {
		count += t.countInMemoryPayloadNodes(pNode.left)
	}
	if pNode.right != nil && !pNode.right.IsNil() {
		count += t.countInMemoryPayloadNodes(pNode.right)
	}

	return count
}

// collectInMemoryPayloadNodes is a helper that recursively collects in-memory nodes.
// It only traverses nodes that are already loaded (does not trigger disk reads).
func (t *PersistentPayloadTreap[K, P]) collectInMemoryPayloadNodes(node TreapNodeInterface[K], nodes *[]PayloadNodeInfo[K, P]) {
	if node == nil || node.IsNil() {
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
	if pNode.TreapNode.left != nil {
		t.collectInMemoryPayloadNodes(pNode.TreapNode.left, nodes)
	}

	// Check the right child without triggering a load
	if pNode.TreapNode.right != nil {
		t.collectInMemoryPayloadNodes(pNode.TreapNode.right, nodes)
	}
}

// FlushOlderThan flushes all nodes that haven't been accessed since the given timestamp.
// This method first persists any unpersisted nodes, then removes them from memory
// if their last access time is older than the specified cutoff timestamp.
// Nodes can be reloaded later from disk when needed.
// Returns the number of nodes flushed and any error encountered.
func (t *PersistentPayloadTreap[K, P]) FlushOlderThan(cutoffTimestamp int64) (int, error) {
	// First, persist the entire tree to ensure all nodes are saved
	err := t.Persist()
	if err != nil {
		return 0, err
	}

	// Get all in-memory nodes
	nodes := t.GetInMemoryNodes()

	// Count how many we flush
	flushedCount := 0

	// Flush nodes older than the cutoff
	for _, nodeInfo := range nodes {
		if nodeInfo.LastAccessTime < cutoffTimestamp {
			// Attempt to flush this node
			err := nodeInfo.Node.Flush()
			if err != nil {
				// If we get errNotFullyPersisted, it means children weren't persisted
				// but we already called Persist() above, so this shouldn't happen
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

	// First, persist the entire tree to ensure all nodes are saved
	// FIXME can we use BatchPersist here to be more efficient?
	err := t.Persist()
	if err != nil {
		return 0, err
	}

	// Get all in-memory nodes
	nodes := t.GetInMemoryNodes()
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

func (n *PersistentPayloadTreapNode[K, P]) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	marshalled, err := n.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, marshalled)
}
func (n *PersistentPayloadTreapNode[K, P]) LateMarshalToObjectId(stre store.Storer) (store.ObjectId, func() error) {
	marshalled, err := n.Marshal()
	if err != nil {
		return 0, func() error { return err }
	}
	return store.LateWriteNewObjFromBytes(stre, marshalled)
}
func (k *PersistentPayloadTreapNode[K, P]) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}

// persist walks and writes the subtree rooted at this node using the provided worker pool.
// If lockTree is true, it acquires the parent treap mutex; callers that
// already hold the treap lock must pass lockTree=false to avoid deadlock.
// If pwp is nil, work is executed synchronously on the calling goroutine.
func (n *PersistentPayloadTreapNode[K, P]) persist(lockTree bool, pwp *persistWorkerPool) error {
	if lockTree && n.parent != nil {
		n.parent.mu.Lock()
		defer n.parent.mu.Unlock()
	}

	// Iterative post-order traversal to persist children before parent,
	// avoiding deep recursion that can cause stack overflows.
	type nodePtr = *PersistentPayloadTreapNode[K, P]
	stack := make([]nodePtr, 0, 64)
	var lastVisited nodePtr
	curr := n

	for curr != nil || len(stack) > 0 {
		if curr != nil {
			stack = append(stack, curr)
			// descend left
			if left := curr.GetLeft(); left != nil {
				leftNode, ok := left.(*PersistentPayloadTreapNode[K, P])
				if !ok {
					return fmt.Errorf("left child is not a PersistentPayloadTreapNode")
				}
				curr = leftNode
				continue
			}
			curr = nil
		} else {
			peek := stack[len(stack)-1]
			// if right child exists and hasn't been visited, traverse it
			if right := peek.GetRight(); right != nil {
				rightNode, ok := right.(*PersistentPayloadTreapNode[K, P])
				if !ok {
					return fmt.Errorf("right child is not a PersistentPayloadTreapNode")
				}
				if rightNode != lastVisited {
					curr = rightNode
					continue
				}
			}

			// Persist the peek node (children already handled)
			objId, errFunc := peek.LateMarshalToObjectId(peek.Store)
			if pwp == nil {
				err := errFunc()
				if err != nil {
					return err
				}
			} else {
				if err := pwp.Submit(errFunc); err != nil {
					return err
				}
			}
			peek.objectId = objId
			lastVisited = peek
			stack = stack[:len(stack)-1]
		}
	}

	return nil
}

// Persist persists the subtree, acquiring the treap lock internally.
func (n *PersistentPayloadTreapNode[K, P]) Persist() error {
	pwp := newPersistWorkerPool(4)
	if pwp == nil {
		return errors.New("failed to create persist worker pool")
	}
	defer pwp.Close()
	return n.persist(true, pwp)
}

func (n *PersistentPayloadTreapNode[K, P]) sizeInBytes() int {
	objectIdSize := n.objectId.SizeInBytes()
	keySize := objectIdSize
	prioritySize := n.priority.SizeInBytes()
	leftSize := objectIdSize
	rightSize := objectIdSize
	selfSize := objectIdSize
	payloadSize := n.payload.SizeInBytes()
	return keySize + prioritySize + leftSize + rightSize + selfSize + payloadSize
}

// collectPayloadNodesPostOrder ensures children are before parents for batching.
func collectPayloadNodesPostOrder[K any, P types.PersistentPayload[P]](node TreapNodeInterface[K], out *[]*PersistentPayloadTreapNode[K, P]) error {
	if node == nil || node.IsNil() {
		return nil
	}
	if err := collectPayloadNodesPostOrder[K, P](node.GetLeft(), out); err != nil {
		return err
	}
	if err := collectPayloadNodesPostOrder[K, P](node.GetRight(), out); err != nil {
		return err
	}
	pNode, ok := node.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("node is not *PersistentPayloadTreapNode")
	}
	*out = append(*out, pNode)
	return nil
}

var errBatchAllocationFailed = errors.New("batch allocation failed")

func isContiguous(offsets []store.FileOffset, size int) bool {
	for i := 1; i < len(offsets); i++ {
		expectedOffset := offsets[i-1] + store.FileOffset(size)
		if offsets[i] != expectedOffset {
			return false
		}
	}
	return true
}

// BatchPersist attempts to persist all nodes using contiguous runs when supported.
// Iteratively requests allocations from the RunAllocator, handling partial results
// (e.g., when BlockAllocator has 400 slots available but 10,000 requested).
// Falls back to per-node Persist when run allocation is unavailable or sizes differ.
func (t *PersistentPayloadTreap[K, P]) BatchPersist() error {
	if t.root == nil {
		return nil
	}

	nodes := make([]*PersistentPayloadTreapNode[K, P], 0)
	if err := collectPayloadNodesPostOrder[K, P](t.root, &nodes); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}

	size := nodes[0].sizeInBytes()
	for _, n := range nodes[1:] {
		if n.sizeInBytes() != size {
			return t.Persist()
		}
	}

	ra, ok := t.Store.(store.RunAllocator)
	if !ok {
		return t.Persist()
	}

	// Process nodes in batches based on what the allocator can provide
	err := t.batchAdder(ra, size, nodes)

	if errors.Is(err, errBatchAllocationFailed) {
		return t.Persist()
	}
	return err
}

func (t *PersistentPayloadTreap[K, P]) batchAdder(ra store.RunAllocator, size int, nodes []*PersistentPayloadTreapNode[K, P]) error {
	type workerUnit struct {
		nodes  []*PersistentPayloadTreapNode[K, P]
		objIds []store.ObjectId
		size   int
	}
	numWorkers := 4
	workerChan := make(chan workerUnit, numWorkers)
	var workerWg sync.WaitGroup
	workerWg.Add(numWorkers)
	errorChan := make(chan error, numWorkers)
	for range numWorkers {
		go func() {
			defer workerWg.Done()
			for unit := range workerChan {
				err := t.processBatch(unit.nodes, unit.objIds, unit.size)
				if err != nil {
					errorChan <- err
					return
				}
			}
		}()
	}
	for len(nodes) > 0 {
		objIds, offsets, err := ra.AllocateRun(size, len(nodes))
		if err != nil {
			// Allocator doesn't support this size or has no space
			close(workerChan)
			workerWg.Wait()
			return errBatchAllocationFailed
		}
		if len(objIds) == 0 {
			// No space available, fall back to regular persist
			close(workerChan)
			workerWg.Wait()
			return errBatchAllocationFailed
		}

		if !isContiguous(offsets, size) {
			// Free the allocated ObjectIds before falling back
			for _, objId := range objIds {
				_ = t.Store.DeleteObj(objId)
			}
			close(workerChan)
			workerWg.Wait()
			return errBatchAllocationFailed
		}

		// Enqueue this batch for workers
		unit := workerUnit{
			nodes:  nodes[:len(objIds)],
			objIds: objIds,
			size:   size,
		}
		workerChan <- unit

		// Move to next batch
		nodes = nodes[len(objIds):]
	}

	// All work enqueued, wait for workers to complete
	close(workerChan)
	workerWg.Wait()
	close(errorChan)

	// Check if any worker encountered an error
	for err := range errorChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *PersistentPayloadTreap[K, P]) processBatch(nodes []*PersistentPayloadTreapNode[K, P], objIds []store.ObjectId, size int) error {
	for i, n := range nodes {
		n.SetObjectId(objIds[i])
	}

	// Prepare batch data for writing
	objIdsForWrite := make([]store.ObjectId, len(nodes))
	sizes := make([]int, len(nodes))
	batchData := make([]byte, 0, size*len(nodes))

	for i, n := range nodes {
		// Synchronize child object IDs before marshaling
		if n.TreapNode.left != nil {
			if leftNode, ok := n.TreapNode.left.(PersistentTreapNodeInterface[K]); ok {
				childId, childErr := leftNode.ObjectId()
				if childErr != nil {
					return childErr
				}
				n.leftObjectId = childId
			}
		}
		if n.TreapNode.right != nil {
			if rightNode, ok := n.TreapNode.right.(PersistentTreapNodeInterface[K]); ok {
				childId, childErr := rightNode.ObjectId()
				if childErr != nil {
					return childErr
				}
				n.rightObjectId = childId
			}
		}

		data, marshalErr := n.Marshal()
		if marshalErr != nil {
			return marshalErr
		}
		objId, idErr := n.ObjectId()
		if idErr != nil {
			return idErr
		}
		objIdsForWrite[i] = objId
		sizes[i] = len(data)
		batchData = append(batchData, data...)
	}

	expectedSize := len(objIdsForWrite) * size
	if expectedSize != len(batchData) {
		return fmt.Errorf("batched buffer size mismatch: expected %d bytes, got %d", expectedSize, len(batchData))
	}
	// Write this batch
	return t.writeBatch(objIdsForWrite, batchData, sizes, size)
}

func (t *PersistentPayloadTreap[K, P]) writeBatch(objIdsForWrite []store.ObjectId, batchData []byte, sizes []int, size int) error {
	if err := t.Store.WriteBatchedObjs(objIdsForWrite, batchData, sizes); err != nil {
		// Fallback to per-node writes for this batch
		for i, objId := range objIdsForWrite {
			start := i * size
			end := start + sizes[i]
			if writeErr := store.WriteBytesToObj(t.Store, batchData[start:end], objId); writeErr != nil {
				return writeErr
			}
		}
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
