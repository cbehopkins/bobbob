package treap

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/cbehopkins/bobbob/store"
)

// UntypedPersistentPayload and PersistentPayload interfaces have been moved to interfaces.go

// PersistentPayloadTreapNode represents a node in the persistent payload treap.
type PersistentPayloadTreapNode[K any, P PersistentPayload[P]] struct {
	PersistentTreapNode[K]
	payload P
}

// GetPayload returns the payload of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetPayload() P {
	return n.payload
}

// SetPayload sets the payload of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetPayload(payload P) {
	n.payload = payload
	n.Store.DeleteObj(n.objectId) // Invalidate the stored object ID
	n.objectId = store.ObjNotAllocated
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
func (n *PersistentPayloadTreapNode[K, P]) unmarshal(data []byte, key PersistentKey[K]) error {
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
type PersistentPayloadTreap[K any, P PersistentPayload[P]] struct {
	PersistentTreap[K]
	mu sync.RWMutex // Protects concurrent access to the treap
}

// NewPersistentPayloadTreapNode creates a new PersistentPayloadTreapNode with the given key, priority, and payload.
func NewPersistentPayloadTreapNode[K any, P PersistentPayload[P]](key PersistentKey[K], priority Priority, payload P, stre store.Storer, parent *PersistentPayloadTreap[K, P]) *PersistentPayloadTreapNode[K, P] {
	return &PersistentPayloadTreapNode[K, P]{
		PersistentTreapNode: PersistentTreapNode[K]{
			TreapNode: TreapNode[K]{
				key:      key,
				priority: priority,
			},
			objectId: store.ObjNotAllocated,
			Store:    stre,
			parent:   &parent.PersistentTreap,
		},
		payload: payload,
	}
}

// NewPersistentPayloadTreap creates a new PersistentPayloadTreap with the given comparison function and store reference.
func NewPersistentPayloadTreap[K any, P PersistentPayload[P]](lessFunc func(a, b K) bool, keyTemplate PersistentKey[K], store store.Storer) *PersistentPayloadTreap[K, P] {
	return &PersistentPayloadTreap[K, P]{
		PersistentTreap: PersistentTreap[K]{
			Treap: Treap[K]{
				root: nil,
				Less: lessFunc,
			},
			keyTemplate: keyTemplate,
			Store:       store,
		},
	}
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
func (t *PersistentPayloadTreap[K, P]) InsertComplex(key PersistentKey[K], priority Priority, payload P) {
	t.mu.Lock()
	defer t.mu.Unlock()
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t.Store, t)
	var tmp TreapNodeInterface[K]
	tmp = t.insert(t.root, newNode)
	t.root = tmp
}

// Insert inserts a new node with the given key and payload into the persistent payload treap.
// If the key implements PriorityProvider, its Priority() method is used;
// otherwise, a random priority is generated.
// If a key already exists, this will update its payload instead of creating a duplicate.
// This is the preferred method for most use cases.
func (t *PersistentPayloadTreap[K, P]) Insert(key PersistentKey[K], payload P) {
	var priority Priority
	if pp, ok := any(key).(PriorityProvider); ok {
		priority = pp.Priority()
	} else {
		priority = randomPriority()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t.Store, t)
	var tmp TreapNodeInterface[K]
	tmp = t.insert(t.root, newNode)
	t.root = tmp
}

// NewPayloadFromObjectId creates a PersistentPayloadTreapNode from the given object ID.
// Reading it in fron the store if it exuists.
func NewPayloadFromObjectId[T any, P PersistentPayload[P]](objId store.ObjectId, parent *PersistentTreap[T], stre store.Storer) (*PersistentPayloadTreapNode[T, P], error) {
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
func (t *PersistentPayloadTreap[K, P]) SearchComplex(key PersistentKey[K], callback func(TreapNodeInterface[K]) error) (PersistentPayloadNodeInterface[K, P], error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
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
func (t *PersistentPayloadTreap[K, P]) Search(key PersistentKey[K]) PersistentPayloadNodeInterface[K, P] {
	t.mu.RLock()
	defer t.mu.RUnlock()
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
func (t *PersistentPayloadTreap[K, P]) UpdatePayload(key PersistentKey[K], newPayload P) error {
	t.mu.Lock()
	defer t.mu.Unlock()
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
			payloadNode.SetPayload(newPayload)
			return payloadNode.Persist()
		}
	}
	return nil
}

// Compare compares this persistent payload treap with another persistent payload treap and invokes callbacks for keys that are:
// - Only in this treap (onlyInA)
// - In both treaps (inBoth)
// - Only in the other treap (onlyInB)
//
// This is a thread-safe wrapper around the base Treap.Compare method.
// Both treaps are locked for reading during the comparison.
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
	// Lock both treaps for reading
	// Always lock in a consistent order to avoid deadlocks
	// Use pointer addresses to determine order
	if uintptr(unsafe.Pointer(t)) < uintptr(unsafe.Pointer(other)) {
		t.mu.RLock()
		defer t.mu.RUnlock()
		other.mu.RLock()
		defer other.mu.RUnlock()
	} else {
		other.mu.RLock()
		defer other.mu.RUnlock()
		t.mu.RLock()
		defer t.mu.RUnlock()
	}

	return t.PersistentTreap.Treap.Compare(&other.PersistentTreap.Treap, onlyInA, inBoth, onlyInB)
}

func (t *PersistentPayloadTreap[K, P]) Persist() error {
	if t.root != nil {
		rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return fmt.Errorf("root is not a PersistentPayloadTreapNode")
		}
		return rootNode.Persist()
	}
	return nil
}

// PayloadNodeInfo contains information about a payload node in memory, including its access timestamp.
type PayloadNodeInfo[K any, P PersistentPayload[P]] struct {
	Node           *PersistentPayloadTreapNode[K, P]
	LastAccessTime int64
	Key            PersistentKey[K]
}

// GetInMemoryNodes traverses the treap and collects all nodes currently in memory.
// This method does NOT load nodes from disk and does NOT update access timestamps.
// It only includes nodes that are already loaded in memory.
// Returns a slice of PayloadNodeInfo containing each node and its last access time.
func (t *PersistentPayloadTreap[K, P]) GetInMemoryNodes() []PayloadNodeInfo[K, P] {
	var nodes []PayloadNodeInfo[K, P]
	t.collectInMemoryPayloadNodes(t.root, &nodes)
	return nodes
}

// CountInMemoryNodes returns the count of nodes currently loaded in memory.
// This is more efficient than len(GetInMemoryNodes()) as it doesn't allocate the slice.
func (t *PersistentPayloadTreap[K, P]) CountInMemoryNodes() int {
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
		Key:            pNode.GetKey().(PersistentKey[K]),
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
	err := t.Persist()
	if err != nil {
		return 0, err
	}

	// Get all in-memory nodes
	nodes := t.GetInMemoryNodes()
	if len(nodes) == 0 {
		return 0, nil
	}

	// Sort nodes by access time (oldest first)
	sortedNodes := make([]PayloadNodeInfo[K, P], len(nodes))
	copy(sortedNodes, nodes)

	// Simple insertion sort by LastAccessTime (ascending)
	for i := 1; i < len(sortedNodes); i++ {
		key := sortedNodes[i]
		j := i - 1
		for j >= 0 && sortedNodes[j].LastAccessTime > key.LastAccessTime {
			sortedNodes[j+1] = sortedNodes[j]
			j--
		}
		sortedNodes[j+1] = key
	}

	// Calculate how many nodes to flush
	numToFlush := (len(sortedNodes) * percentage) / 100
	if numToFlush == 0 && percentage > 0 {
		numToFlush = 1 // Flush at least one node if percentage > 0
	}

	// Flush the oldest nodes
	flushedCount := 0
	for i := 0; i < numToFlush && i < len(sortedNodes); i++ {
		err := sortedNodes[i].Node.Flush()
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
		return store.ObjNotAllocated, nil
	}
	rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return store.ObjNotAllocated, fmt.Errorf("root is not a PersistentPayloadTreapNode")
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

func (k *PersistentPayloadTreapNode[K, P]) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}

func (n *PersistentPayloadTreapNode[K, P]) Persist() error {
	// Persist children first so their object IDs are available when marshaling the parent
	if n.GetLeft() != nil {
		leftNode, ok := n.GetLeft().(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return fmt.Errorf("left child is not a PersistentPayloadTreapNode")
		}
		err := leftNode.Persist()
		if err != nil {
			return fmt.Errorf("failed to persist left child: %w", err)
		}
	}
	if n.GetRight() != nil {
		rightNode, ok := n.GetRight().(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return fmt.Errorf("right child is not a PersistentPayloadTreapNode")
		}
		err := rightNode.Persist()
		if err != nil {
			return fmt.Errorf("failed to persist right child: %w", err)
		}
	}

	// Now marshal and persist this node
	objId, err := n.MarshalToObjectId(n.Store)
	if err != nil {
		return fmt.Errorf("failed to marshal payload node to object ID: %w", err)
	}
	n.objectId = objId
	return nil
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

// Unmarshal implements the UntypedPersistentPayload interface
func (t *PersistentPayloadTreap[K, P]) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
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
