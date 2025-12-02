package treap

import (
	"errors"
	"fmt"
	"time"

	"bobbob/internal/store"
)

var errNotFullyPersisted = errors.New("node not fully persisted")

// currentUnixTime returns the current Unix timestamp in seconds.
// This is used for tracking node access times for age-based memory management.
func currentUnixTime() int64 {
	return time.Now().Unix()
}

type PersistentObjectId store.ObjectId

func (id PersistentObjectId) New() PersistentKey[PersistentObjectId] {
	v := PersistentObjectId(store.ObjectId(store.ObjNotAllocated))
	return &v
}

func (id PersistentObjectId) Equals(other PersistentObjectId) bool {
	return id == other
}

func (id PersistentObjectId) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	return store.ObjectId(id), nil
}

func (id *PersistentObjectId) UnmarshalFromObjectId(id_src store.ObjectId, stre store.Storer) error {
	*id = PersistentObjectId(id_src)
	return nil
}

func (id PersistentObjectId) SizeInBytes() int {
	return store.ObjectId(0).SizeInBytes()
}

func (id PersistentObjectId) Value() PersistentObjectId {
	return id
}

// PersistentTreapNodeInterface has been moved to interfaces.go

// PersistentTreapNode represents a node in the persistent treap.
type PersistentTreapNode[T any] struct {
	TreapNode[T]   // Embed TreapNode to reuse its logic
	objectId       store.ObjectId
	leftObjectId   store.ObjectId
	rightObjectId  store.ObjectId
	Store          store.Storer
	parent         *PersistentTreap[T]
	lastAccessTime int64 // Unix timestamp of last access (in-memory only, not persisted)
}

func (n *PersistentTreapNode[T]) newFromObjectId(objId store.ObjectId) (*PersistentTreapNode[T], error) {
	tmp := NewPersistentTreapNode(n.parent.keyTemplate.New(), 0, n.Store, n.parent)
	err := store.ReadGeneric(n.Store, tmp, objId)
	if err != nil {
		return nil, fmt.Errorf("failed to read node from store (objectId=%d): %w", objId, err)
	}
	return tmp, nil
}

// NewFromObjectId creates a PersistentTreapNode by loading it from the store.
// It reads the node data from the given ObjectId and deserializes it.
func NewFromObjectId[T any](objId store.ObjectId, parent *PersistentTreap[T], stre store.Storer) (*PersistentTreapNode[T], error) {
	tmp := NewPersistentTreapNode[T](parent.keyTemplate.New(), 0, stre, parent)
	err := store.ReadGeneric(stre, tmp, objId)
	if err != nil {
		return nil, fmt.Errorf("failed to read node from store (objectId=%d): %w", objId, err)
	}
	return tmp, nil
}

// GetKey returns the key of the node.
func (n *PersistentTreapNode[T]) GetKey() Key[T] {
	return n.TreapNode.key // Explicitly access the key field from the embedded TreapNode
}

// GetPriority returns the priority of the node.
func (n *PersistentTreapNode[T]) GetPriority() Priority {
	return n.TreapNode.priority
}

// SetPriority sets the priority of the node.
func (n *PersistentTreapNode[T]) SetPriority(p Priority) {
	n.objectId = store.ObjNotAllocated
	n.TreapNode.priority = p
}

// GetLeft returns the left child of the node.
func (n *PersistentTreapNode[T]) GetLeft() TreapNodeInterface[T] {
	if n.TreapNode.left == nil && store.IsValidObjectId(n.leftObjectId) {
		tmp, err := n.newFromObjectId(n.leftObjectId)
		if err != nil {
			return nil
		}
		n.TreapNode.left = tmp
	}
	return n.TreapNode.left
}

// GetRight returns the right child of the node.
func (n *PersistentTreapNode[T]) GetRight() TreapNodeInterface[T] {
	if n.TreapNode.right == nil && store.IsValidObjectId(n.rightObjectId) {
		tmp, err := n.newFromObjectId(n.rightObjectId)
		if err != nil {
			return nil
		}
		n.TreapNode.right = tmp
	}
	return n.TreapNode.right
}

// SetLeft sets the left child of the node.
func (n *PersistentTreapNode[T]) SetLeft(left TreapNodeInterface[T]) error {
	n.TreapNode.left = left
	n.objectId = store.ObjNotAllocated
	return nil
}

// SetRight sets the right child of the node.
func (n *PersistentTreapNode[T]) SetRight(right TreapNodeInterface[T]) error {
	n.TreapNode.right = right
	n.objectId = store.ObjNotAllocated
	return nil
}

// IsNil checks if the node is nil.
func (n *PersistentTreapNode[T]) IsNil() bool {
	return n == nil
}

// GetLastAccessTime returns the Unix timestamp of the last access to this node.
// This is an in-memory field only and is not persisted to disk.
func (n *PersistentTreapNode[T]) GetLastAccessTime() int64 {
	if n == nil {
		return 0
	}
	return n.lastAccessTime
}

// SetLastAccessTime sets the Unix timestamp of the last access to this node.
// This is an in-memory field only and is not persisted to disk.
func (n *PersistentTreapNode[T]) SetLastAccessTime(timestamp int64) {
	if n == nil {
		return
	}
	n.lastAccessTime = timestamp
}

// TouchAccessTime updates the last access time to the current time.
func (n *PersistentTreapNode[T]) TouchAccessTime() {
	if n == nil {
		return
	}
	n.lastAccessTime = currentUnixTime()
}

func (n *PersistentTreapNode[T]) sizeInBytes() int {
	objectIdSize := n.objectId.SizeInBytes()
	keySize := objectIdSize
	prioritySize := n.priority.SizeInBytes()
	leftSize := objectIdSize
	rightSize := objectIdSize
	selfSize := objectIdSize
	return keySize + prioritySize + leftSize + rightSize + selfSize
}

// PersistentTreapObjectSizes returns the standard object sizes used by persistent treap nodes.
// This is used by allocators to pre-allocate blocks of the right sizes for efficient storage.
func PersistentTreapObjectSizes() []int {
	n := &PersistentTreapNode[any]{}
	return []int{
		n.objectId.SizeInBytes(),
		n.sizeInBytes(),
	}
}

// ObjectId returns the ObjectId of this node in the store.
// If the node hasn't been persisted yet, it allocates a new object.
func (n *PersistentTreapNode[T]) ObjectId() (store.ObjectId, error) {
	if n == nil {
		return store.ObjNotAllocated, nil
	}
	if n.objectId < 0 {
		objId, err := n.Store.NewObj(n.sizeInBytes())
		if err != nil {
			return store.ObjNotAllocated, err
		}
		n.objectId = objId
	}
	return n.objectId, nil
}

// SetObjectId sets the ObjectId for this node.
// This is typically used when loading a node from the store.
func (n *PersistentTreapNode[T]) SetObjectId(id store.ObjectId) {
	n.objectId = id
}

// Persist saves this node and its children to the store.
// It recursively persists child nodes that haven't been saved yet.
func (n *PersistentTreapNode[T]) Persist() error {
	if n == nil {
		return nil
	}
	if n.TreapNode.left != nil && !store.IsValidObjectId(n.leftObjectId) {
		leftNode, ok := n.TreapNode.left.(PersistentTreapNodeInterface[T])
		if !ok {
			return fmt.Errorf("left child is not a PersistentTreapNodeInterface")
		}
		if leftNode != nil {
			err := leftNode.Persist()
			if err != nil {
				return fmt.Errorf("failed to persist left child: %w", err)
			}
		}
		leftObjId, err := leftNode.ObjectId()
		if err != nil {
			return fmt.Errorf("failed to get left child object ID: %w", err)
		}
		n.leftObjectId = leftObjId
	}
	if n.TreapNode.right != nil && !store.IsValidObjectId(n.rightObjectId) {
		rightNode, ok := n.TreapNode.right.(PersistentTreapNodeInterface[T])
		if !ok {
			return fmt.Errorf("right child is not a PersistentTreapNodeInterface")
		}
		if rightNode != nil {
			err := rightNode.Persist()
			if err != nil {
				return fmt.Errorf("failed to persist right child: %w", err)
			}
		}
		rightObjId, err := rightNode.ObjectId()
		if err != nil {
			return fmt.Errorf("failed to get right child object ID: %w", err)
		}
		n.rightObjectId = rightObjId
	}
	return n.persist()
}

// flushChild flushes the given child node if it exists and is persisted, then clears its objectId and pointer.
func (n *PersistentTreapNode[T]) flushChild(child *TreapNodeInterface[T], childObjectId *store.ObjectId) error {
	if *child == nil {
		return nil
	}
	if !store.IsValidObjectId(*childObjectId) {
		return errNotFullyPersisted
	}
	childNode, ok := (*child).(PersistentTreapNodeInterface[T])
	if !ok {
		return fmt.Errorf("child is not a PersistentTreapNodeInterface")
	}
	err := childNode.Flush()
	if err != nil {
		return err
	}
	// Simply clear the pointer to the child
	// We still have the object ID if we want to re-load it
	// In the future, use a sync.Pool to avoid allocations
	*child = nil
	return nil
}

// Flush saves this node and its children to the store, then removes them from memory.
// This is used to reduce memory usage while keeping the tree accessible via the store.
// The node can be reloaded later using its ObjectId.
func (n *PersistentTreapNode[T]) Flush() error {
	if n == nil {
		return nil
	}
	// This is needed as we still want to try to flush the children where we can
	// even if one node fails
	allChildrenPersisted := true
	if err := n.flushChild(&n.TreapNode.left, &n.leftObjectId); err != nil {
		if errors.Is(err, errNotFullyPersisted) {
			allChildrenPersisted = false
		} else {
			return err
		}
	}
	if err := n.flushChild(&n.TreapNode.right, &n.rightObjectId); err != nil {
		if errors.Is(err, errNotFullyPersisted) {
			allChildrenPersisted = false
		} else {
			return err
		}
	}
	if allChildrenPersisted {
		return nil
	}
	return errNotFullyPersisted
}

func (n *PersistentTreapNode[T]) persist() error {
	buf, err := n.Marshal()
	if err != nil {
		return err
	}
	objId, err := n.ObjectId()
	if err != nil {
		return err
	}
	return store.WriteBytesToObj(n.Store, buf, objId)
}

func (n *PersistentTreapNode[T]) Marshal() ([]byte, error) {
	buf := make([]byte, n.sizeInBytes())
	offset := 0
	persistentKey, ok := n.key.(PersistentKey[T])
	if !ok {
		return nil, fmt.Errorf("key is not a PersistentKey")
	}
	keyAsObjectId, err := persistentKey.MarshalToObjectId(n.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key to object ID: %w", err)
	}

	if store.IsValidObjectId(n.leftObjectId) {
		if n.TreapNode.left != nil {
			leftNode, ok := n.TreapNode.left.(PersistentTreapNodeInterface[T])
			if !ok {
				return nil, fmt.Errorf("left child is not a PersistentTreapNodeInterface")
			}
			newLeftObjectId, err := leftNode.ObjectId()
			if err != nil {
				return nil, fmt.Errorf("failed to get left child object ID during marshal: %w", err)
			}
			if newLeftObjectId != n.leftObjectId {
				n.leftObjectId = newLeftObjectId
				n.objectId = store.ObjNotAllocated
			}
		}
	} else {
		if n.TreapNode.left != nil {
			leftNode, ok := n.TreapNode.left.(PersistentTreapNodeInterface[T])
			if !ok {
				return nil, fmt.Errorf("left child is not a PersistentTreapNodeInterface")
			}
			leftObjId, err := leftNode.ObjectId()
			if err != nil {
				return nil, fmt.Errorf("failed to allocate left child object ID during marshal: %w", err)
			}
			n.leftObjectId = leftObjId
			n.objectId = store.ObjNotAllocated
		}
	}
	if store.IsValidObjectId(n.rightObjectId) {
		if n.TreapNode.right != nil {
			rightNode, ok := n.TreapNode.right.(PersistentTreapNodeInterface[T])
			if !ok {
				return nil, fmt.Errorf("right child is not a PersistentTreapNodeInterface")
			}
			newRightObjectId, err := rightNode.ObjectId()
			if err != nil {
				return nil, fmt.Errorf("failed to get right child object ID during marshal: %w", err)
			}
			if newRightObjectId != n.rightObjectId {
				n.rightObjectId = newRightObjectId
				n.objectId = store.ObjNotAllocated
			}
		}
	} else {
		if n.TreapNode.right != nil {
			rightNode, ok := n.TreapNode.right.(PersistentTreapNodeInterface[T])
			if !ok {
				return nil, fmt.Errorf("right child is not a PersistentTreapNodeInterface")
			}
			rightObjId, err := rightNode.ObjectId()
			if err != nil {
				return nil, fmt.Errorf("failed to allocate right child object ID during marshal: %w", err)
			}
			n.rightObjectId = rightObjId
			n.objectId = store.ObjNotAllocated
		}
	}
	selfObjId, err := n.ObjectId()
	if err != nil {
		return nil, fmt.Errorf("failed to get self object ID during marshal: %w", err)
	}
	marshalables := []interface {
		Marshal() ([]byte, error)
	}{
		keyAsObjectId,
		n.priority,
		n.leftObjectId,
		n.rightObjectId,
		selfObjId,
	}

	for i, m := range marshalables {
		data, err := m.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal field %d: %w", i, err)
		}
		copy(buf[offset:], data)
		offset += len(data)
	}

	return buf, nil
}

func (n *PersistentTreapNode[T]) unmarshal(data []byte, key PersistentKey[T]) error {
	// Validate minimum data length (5 ObjectIds: key, priority placeholder, left, right, self)
	// Priority is 4 bytes, ObjectIds are 8 bytes each
	minSize := 4*8 + 4 // 4 ObjectIds + 1 Priority
	if len(data) < minSize {
		return fmt.Errorf("data too short for PersistentTreapNode: got %d bytes, need at least %d", len(data), minSize)
	}

	offset := 0

	keyAsObjectId := store.ObjectId(0)
	err := keyAsObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal key object ID: %w", err)
	}
	offset += keyAsObjectId.SizeInBytes()
	tmpKey := key.New()
	err = tmpKey.UnmarshalFromObjectId(keyAsObjectId, n.Store)
	if err != nil {
		return fmt.Errorf("failed to unmarshal key from object ID: %w", err)
	}
	convertedKey, ok := tmpKey.(Key[T])
	if !ok {
		return fmt.Errorf("unmarshalled key is not of expected type Key[T]")
	}
	n.key = convertedKey

	err = n.priority.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal priority: %w", err)
	}
	offset += n.priority.SizeInBytes()

	leftObjectId := store.ObjectId(store.ObjNotAllocated)
	err = leftObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal left object ID: %w", err)
	}
	offset += leftObjectId.SizeInBytes()
	n.leftObjectId = leftObjectId

	rightObjectId := store.ObjectId(store.ObjNotAllocated)
	err = rightObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal right object ID: %w", err)
	}
	offset += rightObjectId.SizeInBytes()
	n.rightObjectId = rightObjectId

	selfObjectId := store.ObjectId(store.ObjNotAllocated)
	err = selfObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal self object ID: %w", err)
	}
	offset += selfObjectId.SizeInBytes()
	n.objectId = selfObjectId

	return nil
}

// Unmarshal deserializes this node from bytes.
// It populates the key, priority, and child ObjectIds.
func (n *PersistentTreapNode[T]) Unmarshal(data []byte) error {
	return n.unmarshal(data, n.parent.keyTemplate)
}

// PersistentTreap is a treap that stores its nodes in a persistent store.
// It extends the in-memory Treap with the ability to save and load nodes from disk.
type PersistentTreap[T any] struct {
	Treap[T]
	keyTemplate PersistentKey[T]
	Store       store.Storer
}

// NewPersistentTreapNode creates a new PersistentTreapNode with the given key, priority, and store reference.
func NewPersistentTreapNode[T any](key PersistentKey[T], priority Priority, stre store.Storer, parent *PersistentTreap[T]) *PersistentTreapNode[T] {
	return &PersistentTreapNode[T]{
		TreapNode: TreapNode[T]{
			key:      key,
			priority: priority,
			left:     nil,
			right:    nil,
		},
		objectId: store.ObjNotAllocated,
		Store:    stre,
		parent:   parent,
	}
}

// NewPersistentTreap creates a new PersistentTreap with the given comparison function and store reference.
func NewPersistentTreap[T any](lessFunc func(a, b T) bool, keyTemplate PersistentKey[T], store store.Storer) *PersistentTreap[T] {
	return &PersistentTreap[T]{
		Treap: Treap[T]{
			root: nil,
			Less: func(a, b T) bool {
				return lessFunc(a, b)
			},
		},
		keyTemplate: keyTemplate,
		Store:       store,
	}
}

// insert is a helper function that inserts a new node into the persistent treap.
func (t *PersistentTreap[T]) insert(node TreapNodeInterface[T], newNode TreapNodeInterface[T]) TreapNodeInterface[T] {
	// Call the insert method of the embedded Treap
	result := t.Treap.insert(node, newNode)

	nodeCast, ok := result.(PersistentTreapNodeInterface[T])
	if !ok {
		return result // If type assertion fails, just return the result as-is
	}
	objId, err := nodeCast.ObjectId()
	if err == nil && objId > store.ObjNotAllocated {
		// We are modifying an existing node, so delete the old object
		t.Store.DeleteObj(store.ObjectId(objId))
	}
	nodeCast.SetObjectId(store.ObjNotAllocated)

	return result
}

// delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap[T]) delete(node TreapNodeInterface[T], key T) TreapNodeInterface[T] {
	// Call the delete method of the embedded Treap
	result := t.Treap.delete(node, key)

	if result != nil && !result.IsNil() {
		nodeCast, ok := result.(PersistentTreapNodeInterface[T])
		if ok {
			objId, err := nodeCast.ObjectId()
			if err == nil && objId > store.ObjNotAllocated {
				t.Store.DeleteObj(objId)
			}
			nodeCast.SetObjectId(store.ObjNotAllocated)
		}
	}

	return result
}

// InsertComplex inserts a new node with the given key and priority into the persistent treap.
// Use this method when you need to specify a custom priority value.
func (t *PersistentTreap[T]) InsertComplex(key PersistentKey[T], priority Priority) {
	newNode := NewPersistentTreapNode(key, priority, t.Store, t)
	t.root = t.insert(t.root, newNode)
}

// Insert inserts a new node with the given key into the persistent treap with a random priority.
// This is the preferred method for most use cases.
func (t *PersistentTreap[T]) Insert(key PersistentKey[T]) {
	t.InsertComplex(key, randomPriority())
}

// Delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap[T]) Delete(key PersistentKey[T]) {
	t.root = t.delete(t.root, key.Value())
}

// SearchComplex searches for the node with the given key in the persistent treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback receives the node that was accessed, allowing for custom operations
// such as updating access times for LRU caching or flushing stale nodes.
// This method automatically updates the lastAccessTime on each accessed node.
// The callback can return an error to abort the search.
func (t *PersistentTreap[T]) SearchComplex(key PersistentKey[T], callback func(TreapNodeInterface[T]) error) (TreapNodeInterface[T], error) {
	// Create a wrapper callback that updates the access time
	wrappedCallback := func(node TreapNodeInterface[T]) error {
		// Update the access time if this is a persistent node
		if pNode, ok := node.(*PersistentTreapNode[T]); ok {
			pNode.TouchAccessTime()
		}
		// Call the user's callback if provided
		if callback != nil {
			return callback(node)
		}
		return nil
	}
	return t.searchComplex(t.root, key.Value(), wrappedCallback)
}

// Search searches for the node with the given key in the persistent treap.
// It calls SearchComplex with a nil callback.
func (t *PersistentTreap[T]) Search(key PersistentKey[T]) TreapNodeInterface[T] {
	result, _ := t.SearchComplex(key, nil)
	return result
}

// UpdatePriority updates the priority of the node with the given key.
func (t *PersistentTreap[T]) UpdatePriority(key PersistentKey[T], newPriority Priority) {
	node := t.Search(key)
	if node != nil && !node.IsNil() {
		node.SetPriority(newPriority)
		// Delete and re-add as the priority change may violate treap properties
		t.Delete(key)
		t.InsertComplex(key, newPriority)
	}
}

// Persist persists the entire treap to the store.
func (t *PersistentTreap[T]) Persist() error {
	if t.root != nil {
		rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
		if !ok {
			return fmt.Errorf("root is not a PersistentTreapNodeInterface")
		}
		return rootNode.Persist()
	}
	return nil
}

// Load loads the treap from the store using the given root ObjectId.
func (t *PersistentTreap[T]) Load(objId store.ObjectId) error {
	var err error
	t.root, err = NewFromObjectId(objId, t, t.Store)
	return err
}

// NodeInfo contains information about a node in memory, including its access timestamp.
type NodeInfo[T any] struct {
	Node           *PersistentTreapNode[T]
	LastAccessTime int64
	Key            PersistentKey[T]
}

// GetInMemoryNodes traverses the treap and collects all nodes currently in memory.
// This method does NOT load nodes from disk and does NOT update access timestamps.
// It only includes nodes that are already loaded in memory.
// Returns a slice of NodeInfo containing each node and its last access time.
func (t *PersistentTreap[T]) GetInMemoryNodes() []NodeInfo[T] {
	var nodes []NodeInfo[T]
	t.collectInMemoryNodes(t.root, &nodes)
	return nodes
}

// CountInMemoryNodes returns the count of nodes currently loaded in memory.
// This is more efficient than len(GetInMemoryNodes()) as it doesn't allocate the slice.
func (t *PersistentTreap[T]) CountInMemoryNodes() int {
	return t.countInMemoryNodes(t.root)
}

// countInMemoryNodes recursively counts in-memory nodes.
func (t *PersistentTreap[T]) countInMemoryNodes(node TreapNodeInterface[T]) int {
	if node == nil || node.IsNil() {
		return 0
	}

	pNode, ok := node.(*PersistentTreapNode[T])
	if !ok {
		return 0
	}

	count := 1 // Count this node

	// Only traverse children that are already in memory
	if pNode.TreapNode.left != nil {
		count += t.countInMemoryNodes(pNode.TreapNode.left)
	}
	if pNode.TreapNode.right != nil {
		count += t.countInMemoryNodes(pNode.TreapNode.right)
	}

	return count
}

// collectInMemoryNodes is a helper that recursively collects in-memory nodes.
// It only traverses nodes that are already loaded (does not trigger disk reads).
func (t *PersistentTreap[T]) collectInMemoryNodes(node TreapNodeInterface[T], nodes *[]NodeInfo[T]) {
	if node == nil || node.IsNil() {
		return
	}

	// Convert to PersistentTreapNode to access in-memory state
	pNode, ok := node.(*PersistentTreapNode[T])
	if !ok {
		return
	}

	// Add this node to the list
	*nodes = append(*nodes, NodeInfo[T]{
		Node:           pNode,
		LastAccessTime: pNode.GetLastAccessTime(),
		Key:            pNode.GetKey().(PersistentKey[T]),
	})

	// Only traverse children that are already in memory
	// Check the left child without triggering a load
	if pNode.TreapNode.left != nil {
		t.collectInMemoryNodes(pNode.TreapNode.left, nodes)
	}

	// Check the right child without triggering a load
	if pNode.TreapNode.right != nil {
		t.collectInMemoryNodes(pNode.TreapNode.right, nodes)
	}
}

// FlushOlderThan flushes all nodes that haven't been accessed since the given timestamp.
// This method first persists any unpersisted nodes, then removes them from memory
// if their last access time is older than the specified cutoff timestamp.
// Nodes can be reloaded later from disk when needed.
// Returns the number of nodes flushed and any error encountered.
func (t *PersistentTreap[T]) FlushOlderThan(cutoffTimestamp int64) (int, error) {
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
func (t *PersistentTreap[T]) FlushOldestPercentile(percentage int) (int, error) {
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
	sortedNodes := make([]NodeInfo[T], len(nodes))
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
func (t *PersistentTreap[T]) GetRootObjectId() (store.ObjectId, error) {
	if t.root == nil {
		return store.ObjNotAllocated, nil
	}
	rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
	if !ok {
		return store.ObjNotAllocated, fmt.Errorf("root is not a PersistentTreapNodeInterface")
	}
	return rootNode.ObjectId()
}
