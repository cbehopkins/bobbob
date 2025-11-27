package yggdrasil

import (
	"errors"
	"fmt"

	"bobbob/internal/store"
)

type UntypedPersistentPayload interface {
	PersistentPayload[UntypedPersistentPayload]
	Unmarshal([]byte) (UntypedPersistentPayload, error)
}
type PersistentPayload[T any] interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) (UntypedPersistentPayload, error)
	SizeInBytes() int
}

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

type PersistentPayloadTreapInterface[T any, P any] interface {
	Insert(key PersistentKey[T], payload P)
	InsertComplex(key PersistentKey[T], priority Priority, payload P)
	Search(key PersistentKey[T]) PersistentPayloadNodeInterface[T, P]
	SearchComplex(key PersistentKey[T], callback func(TreapNodeInterface[T]) error) (PersistentPayloadNodeInterface[T, P], error)
	UpdatePriority(key PersistentKey[T], newPriority Priority)
	UpdatePayload(key PersistentKey[T], newPayload P) error
	Persist() error
	Load(objId store.ObjectId) error
	Marshal() ([]byte, error)
}
type PersistentPayloadNodeInterface[T any, P any] interface {
	PersistentTreapNodeInterface[T]
	GetPayload() P
	SetPayload(P)
}

// PersistentPayloadTreap represents a persistent treap with payloads.
type PersistentPayloadTreap[K any, P PersistentPayload[P]] struct {
	PersistentTreap[K]
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

// InsertComplex inserts a new node with the given key, priority, and payload into the persistent payload treap.
// Use this method when you need to specify a custom priority value.
func (t *PersistentPayloadTreap[K, P]) InsertComplex(key PersistentKey[K], priority Priority, payload P) {
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t.Store, t)
	var tmp TreapNodeInterface[K]
	tmp = t.insert(t.root, newNode)
	t.root = tmp
}

// Insert inserts a new node with the given key and payload into the persistent payload treap with a random priority.
// This is the preferred method for most use cases.
func (t *PersistentPayloadTreap[K, P]) Insert(key PersistentKey[K], payload P) {
	t.InsertComplex(key, randomPriority(), payload)
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
	result, _ := t.SearchComplex(key, nil)
	return result
} // UpdatePayload updates the payload of the node with the given key.
func (t *PersistentPayloadTreap[K, P]) UpdatePayload(key PersistentKey[K], newPayload P) error {
	node := t.Search(key)
	if node != nil && !node.IsNil() {
		payloadNode, ok := node.(*PersistentPayloadTreapNode[K, P])
		if ok {
			payloadNode.SetPayload(newPayload)
			return payloadNode.Persist()
		}
	}
	return nil
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
