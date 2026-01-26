package treap

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cbehopkins/bobbob/internal"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

var errNotFullyPersisted = errors.New("node not fully persisted")

// IterationMode defines how the tree iterator should traverse nodes.
type IterationMode int

const (
	// IterateInMemoryOnly traverses only nodes currently loaded in memory.
	// No disk access is performed.
	IterateInMemoryOnly IterationMode = iota

	// IterateOnDiskTransient loads nodes from disk transiently during traversal
	// but does not cache them in the in-memory tree. Nodes are read, processed,
	// and child object IDs are followed without modifying tree pointers.
	IterateOnDiskTransient

	// IterateOnDiskAndLoad loads nodes from disk and retains them in memory
	// after visitation, populating the in-memory tree structure.
	IterateOnDiskAndLoad
)

// IterationCallback is invoked for each node visited during tree iteration.
// Return nil to continue; return an error to halt iteration immediately.
type IterationCallback[T any] func(node TreapNodeInterface[T]) error

// currentUnixTime returns the current Unix timestamp in seconds.
// This is used for tracking node access times for age-based memory management.
func currentUnixTime() int64 {
	return time.Now().Unix()
}

type PersistentObjectId store.ObjectId

func (id PersistentObjectId) New() types.PersistentKey[PersistentObjectId] {
	v := PersistentObjectId(store.ObjectId(internal.ObjNotAllocated))
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

// releaseToPool allows generic treap helpers to return persistent nodes to their treap's pool.
func (n *PersistentTreapNode[T]) releaseToPool() {
	if n == nil || n.parent == nil {
		return
	}
	// Reset in-memory fields to avoid holding references
	n.TreapNode.left = nil
	n.TreapNode.right = nil
	n.TreapNode.key = nil
	n.TreapNode.priority = 0
	n.objectId = internal.ObjNotAllocated
	n.leftObjectId = internal.ObjNotAllocated
	n.rightObjectId = internal.ObjNotAllocated
	n.lastAccessTime = 0
	n.Store = nil
	n.parent.nodePool.Put(n)
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
func (n *PersistentTreapNode[T]) GetKey() types.Key[T] {
	return n.TreapNode.key // Explicitly access the key field from the embedded TreapNode
}

// GetPriority returns the priority of the node.
func (n *PersistentTreapNode[T]) GetPriority() Priority {
	return n.TreapNode.priority
}

// SetPriority sets the priority of the node.
func (n *PersistentTreapNode[T]) SetPriority(p Priority) {
	_ = n.Store.DeleteObj(n.objectId) // Invalidate the stored object ID (best effort)
	n.objectId = internal.ObjNotAllocated
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
	_ = n.Store.DeleteObj(n.objectId) // Invalidate the stored object ID (best effort)
	n.objectId = internal.ObjNotAllocated
	return nil
}

// SetRight sets the right child of the node.
func (n *PersistentTreapNode[T]) SetRight(right TreapNodeInterface[T]) error {
	n.TreapNode.right = right
	_ = n.Store.DeleteObj(n.objectId) // Invalidate the stored object ID (best effort)
	n.objectId = internal.ObjNotAllocated
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
// Uses atomic operations for thread-safe concurrent access.
func (n *PersistentTreapNode[T]) TouchAccessTime() {
	if n == nil {
		return
	}
	atomic.StoreInt64(&n.lastAccessTime, currentUnixTime())
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
		return internal.ObjNotAllocated, nil
	}
	if n.objectId < 0 {
		objId, err := n.Store.NewObj(n.sizeInBytes())
		if err != nil {
			return internal.ObjNotAllocated, err
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

// IsObjectIdInvalid returns true if the node's ObjectId has been invalidated (is negative).
func (n *PersistentTreapNode[T]) IsObjectIdInvalid() bool {
	if n == nil {
		return true
	}
	return n.objectId < 0
}

// Persist saves this node and its children to the store.
// It recursively persists child nodes that haven't been saved yet.
func (n *PersistentTreapNode[T]) Persist() error {
	if n == nil {
		return nil
	}

	// Process left child
	if n.TreapNode.left != nil {
		leftNode, ok := n.TreapNode.left.(PersistentTreapNodeInterface[T])
		if !ok {
			return fmt.Errorf("left child is not a PersistentTreapNodeInterface")
		}

		// Check if child needs persisting: either cached ID is invalid OR child node itself is invalid
		needsPersist := !store.IsValidObjectId(n.leftObjectId) || leftNode.IsObjectIdInvalid()

		if needsPersist {
			err := leftNode.Persist()
			if err != nil {
				return fmt.Errorf("failed to persist left child: %w", err)
			}
			leftObjId, err := leftNode.ObjectId()
			if err != nil {
				return fmt.Errorf("failed to get left child object ID: %w", err)
			}
			// If the ObjectId changed, invalidate ourselves
			if leftObjId != n.leftObjectId && store.IsValidObjectId(n.leftObjectId) {
				_ = n.Store.DeleteObj(n.objectId) // Invalidate (best effort)
				n.objectId = internal.ObjNotAllocated
			}
			n.leftObjectId = leftObjId
		}
	}

	// Process right child
	if n.TreapNode.right != nil {
		rightNode, ok := n.TreapNode.right.(PersistentTreapNodeInterface[T])
		if !ok {
			return fmt.Errorf("right child is not a PersistentTreapNodeInterface")
		}

		// Check if child needs persisting: either cached ID is invalid OR child node itself is invalid
		needsPersist := !store.IsValidObjectId(n.rightObjectId) || rightNode.IsObjectIdInvalid()

		if needsPersist {
			err := rightNode.Persist()
			if err != nil {
				return fmt.Errorf("failed to persist right child: %w", err)
			}
			rightObjId, err := rightNode.ObjectId()
			if err != nil {
				return fmt.Errorf("failed to get right child object ID: %w", err)
			}
			// If the ObjectId changed, invalidate ourselves
			if rightObjId != n.rightObjectId && store.IsValidObjectId(n.rightObjectId) {
				_ = n.Store.DeleteObj(n.objectId) // Invalidate (best effort)
				n.objectId = internal.ObjNotAllocated
			}
			n.rightObjectId = rightObjId
		}
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

// Flush saves this node's children to the store and removes them from memory.
// This is used to reduce memory usage while keeping the tree accessible via the store.
// The children can be reloaded later using their ObjectIds.
// Note: This flushes the node's CHILDREN, not the node itself. To flush a node,
// its parent must call flushChild on it.
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

// syncChildObjectId ensures the cached object ID for a child matches the child's actual persisted ID.
// If the child exists:
// - When the cached ID is valid, it fetches the child's ID and if it differs, updates the cache and invalidates this node's objectId.
// - When the cached ID is invalid, it fetches the child's ID, sets the cache, and invalidates this node's objectId.
// If the child doesn't exist, it does nothing.
func (n *PersistentTreapNode[T]) syncChildObjectId(child TreapNodeInterface[T], cached *store.ObjectId, side string) error {
	if child == nil {
		return nil
	}
	pChild, ok := child.(PersistentTreapNodeInterface[T])
	if !ok {
		return fmt.Errorf("%s child is not a PersistentTreapNodeInterface", side)
	}

	childObjId, err := pChild.ObjectId()
	if err != nil {
		if store.IsValidObjectId(*cached) {
			return fmt.Errorf("failed to get %s child object ID during marshal: %w", side, err)
		}
		return fmt.Errorf("failed to allocate %s child object ID during marshal: %w", side, err)
	}

	// If cached is valid and differs, or cached is invalid and child exists, update and invalidate self
	if !store.IsValidObjectId(*cached) || childObjId != *cached {
		*cached = childObjId
		_ = n.Store.DeleteObj(n.objectId) // Invalidate (best effort)
		n.objectId = internal.ObjNotAllocated
	}
	return nil
}

func (n *PersistentTreapNode[T]) Marshal() ([]byte, error) {
	buf := make([]byte, n.sizeInBytes())
	offset := 0
	persistentKey, ok := n.key.(types.PersistentKey[T])
	if !ok {
		return nil, fmt.Errorf("key is not a types.PersistentKey")
	}
	keyAsObjectId, err := persistentKey.MarshalToObjectId(n.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key to object ID: %w", err)
	}

	// Sync left and right children object IDs and invalidate self if they changed
	if err := n.syncChildObjectId(n.TreapNode.left, &n.leftObjectId, "left"); err != nil {
		return nil, err
	}
	if err := n.syncChildObjectId(n.TreapNode.right, &n.rightObjectId, "right"); err != nil {
		return nil, err
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

func (n *PersistentTreapNode[T]) unmarshal(data []byte, key types.PersistentKey[T]) error {
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
	convertedKey, ok := tmpKey.(types.Key[T])
	if !ok {
		return fmt.Errorf("unmarshalled key is not of expected type types.Key[T]")
	}
	n.key = convertedKey

	err = n.priority.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal priority: %w", err)
	}
	offset += n.priority.SizeInBytes()

	leftObjectId := store.ObjectId(internal.ObjNotAllocated)
	err = leftObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal left object ID: %w", err)
	}
	offset += leftObjectId.SizeInBytes()
	n.leftObjectId = leftObjectId

	rightObjectId := store.ObjectId(internal.ObjNotAllocated)
	err = rightObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal right object ID: %w", err)
	}
	offset += rightObjectId.SizeInBytes()
	n.rightObjectId = rightObjectId

	selfObjectId := store.ObjectId(internal.ObjNotAllocated)
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
	keyTemplate types.PersistentKey[T]
	Store       store.Storer
	mu          sync.RWMutex // Protects concurrent access to the treap
	nodePool    sync.Pool    // Pool for *PersistentTreapNode[T]
}

// Root returns the current root node (may be nil). Exposed for external tests.
func (t *PersistentTreap[T]) Root() TreapNodeInterface[T] {
	return t.root
}

// NewPersistentTreapNode creates a new PersistentTreapNode with the given key, priority, and store reference.
func NewPersistentTreapNode[T any](key types.PersistentKey[T], priority Priority, stre store.Storer, parent *PersistentTreap[T]) *PersistentTreapNode[T] {
	v := parent.nodePool.Get()
	n, _ := v.(*PersistentTreapNode[T])
	if n == nil {
		n = &PersistentTreapNode[T]{}
	}
	n.TreapNode.key = key
	n.TreapNode.priority = priority
	n.TreapNode.left = nil
	n.TreapNode.right = nil
	n.objectId = internal.ObjNotAllocated
	n.leftObjectId = internal.ObjNotAllocated
	n.rightObjectId = internal.ObjNotAllocated
	n.Store = stre
	n.parent = parent
	n.lastAccessTime = 0
	return n
}

// NewPersistentTreap creates a new PersistentTreap with the given comparison function and store reference.
func NewPersistentTreap[T any](lessFunc func(a, b T) bool, keyTemplate types.PersistentKey[T], store store.Storer) *PersistentTreap[T] {
	t := &PersistentTreap[T]{
		Treap: Treap[T]{
			root: nil,
			Less: func(a, b T) bool {
				return lessFunc(a, b)
			},
		},
		keyTemplate: keyTemplate,
		Store:       store,
	}
	t.nodePool = sync.Pool{New: func() any { return new(PersistentTreapNode[T]) }}
	return t
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
	if err == nil && objId > internal.ObjNotAllocated {
		// We are modifying an existing node, so delete the old object
		_ = t.Store.DeleteObj(store.ObjectId(objId)) // Best effort cleanup
	}
	nodeCast.SetObjectId(internal.ObjNotAllocated)

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
			if err == nil && objId > internal.ObjNotAllocated {
				// Best-effort cleanup of associated objects (key/payload) before freeing node.
				if depProvider, ok := nodeCast.(interface{ DependentObjectIds() []store.ObjectId }); ok {
					for _, dep := range depProvider.DependentObjectIds() {
						if store.IsValidObjectId(dep) {
							_ = t.Store.DeleteObj(dep)
						}
					}
				}
				_ = t.Store.DeleteObj(objId) // Best effort cleanup of the node itself
			}
			nodeCast.SetObjectId(internal.ObjNotAllocated)
		}
	}

	return result
}

// InsertComplex inserts a new node with the given key and priority into the persistent treap.
// Use this method when you need to specify a custom priority value.
func (t *PersistentTreap[T]) InsertComplex(key types.PersistentKey[T], priority Priority) {
	t.mu.Lock()
	defer t.mu.Unlock()
	newNode := NewPersistentTreapNode(key, priority, t.Store, t)
	t.root = t.insert(t.root, newNode)
}

// Insert inserts a new node with the given key into the persistent treap.
// If the key implements types.PriorityProvider, its Priority() method is used;
// otherwise, a random priority is generated.
// This is the preferred method for most use cases.
func (t *PersistentTreap[T]) Insert(key types.PersistentKey[T]) {
	var priority Priority
	if pp, ok := any(key).(types.PriorityProvider); ok {
		priority = Priority(pp.Priority())
	} else {
		priority = randomPriority()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	newNode := NewPersistentTreapNode(key, priority, t.Store, t)
	t.root = t.insert(t.root, newNode)
}

// Delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap[T]) Delete(key types.PersistentKey[T]) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.root = t.delete(t.root, key.Value())
}

// SearchComplex searches for the node with the given key in the persistent treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback receives the node that was accessed, allowing for custom operations
// such as updating access times for LRU caching or flushing stale nodes.
// This method automatically updates the lastAccessTime on each accessed node.
// The callback can return an error to abort the search.
func (t *PersistentTreap[T]) SearchComplex(key types.PersistentKey[T], callback func(TreapNodeInterface[T]) error) (TreapNodeInterface[T], error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
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
func (t *PersistentTreap[T]) Search(key types.PersistentKey[T]) TreapNodeInterface[T] {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Create a wrapper callback that updates the access time
	wrappedCallback := func(node TreapNodeInterface[T]) error {
		// Update the access time if this is a persistent node
		if pNode, ok := node.(*PersistentTreapNode[T]); ok {
			pNode.TouchAccessTime()
		}
		return nil
	}
	result, _ := t.searchComplex(t.root, key.Value(), wrappedCallback)
	return result
}

// UpdatePriority updates the priority of the node with the given key.
func (t *PersistentTreap[T]) UpdatePriority(key types.PersistentKey[T], newPriority Priority) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.search(t.root, key.Value())
	if node != nil && !node.IsNil() {
		node.SetPriority(newPriority)
		// Delete and re-add as the priority change may violate treap properties
		t.root = t.delete(t.root, key.Value())
		newNode := NewPersistentTreapNode(key, newPriority, t.Store, t)
		t.root = t.insert(t.root, newNode)
	}
}

// shouldLockFirst determines if this treap should be locked before the other treap
// for deadlock prevention. It uses data-driven comparison of root keys rather than
// unsafe pointer comparisons.
//
// Lock order is determined by:
// 1. Comparing root keys if both trees have roots (using the treap's Less function)
// 2. If roots have equal keys, using pointer order as a tiebreaker
// 3. If one tree is empty, locking the non-empty tree first
// 4. If both are empty, using pointer order as tiebreaker
func (t *PersistentTreap[T]) shouldLockFirst(other *PersistentTreap[T]) bool {
	// Check if roots exist
	tHasRoot := t.root != nil && !t.root.IsNil()
	otherHasRoot := other.root != nil && !other.root.IsNil()

	// If both have roots, compare their keys
	if tHasRoot && otherHasRoot {
		tKey := t.root.GetKey().Value()
		otherKey := other.root.GetKey().Value()

		// If keys are equal, use pointer comparison as tiebreaker
		if !t.Less(tKey, otherKey) && !t.Less(otherKey, tKey) {
			return uintptr(unsafe.Pointer(t)) < uintptr(unsafe.Pointer(other))
		}

		// Use the treap's Less function for ordering
		return t.Less(tKey, otherKey)
	}

	// If only this tree has a root, lock it first
	if tHasRoot {
		return true
	}

	// If only other has a root, lock it first
	if otherHasRoot {
		return false
	}

	// Both empty: use pointer comparison as tiebreaker
	return uintptr(unsafe.Pointer(t)) < uintptr(unsafe.Pointer(other))
}

// Compare compares this persistent treap with another persistent treap and invokes callbacks for keys that are:
// - Only in this treap (onlyInA)
// - In both treaps (inBoth)
// - Only in the other treap (onlyInB)
//
// This method traverses both trees in sorted order, comparing keys.
// If nodes are loaded in memory, it uses them directly.
// If nodes need to be loaded from disk, they are loaded transiently without populating the in-memory tree.
// Both treaps are locked for reading during the comparison to ensure consistency.
// Lock ordering is determined by data-driven comparison of root keys, not pointer addresses.
func (t *PersistentTreap[T]) Compare(
	other *PersistentTreap[T],
	onlyInA func(TreapNodeInterface[T]) error,
	inBoth func(nodeA, nodeB TreapNodeInterface[T]) error,
	onlyInB func(TreapNodeInterface[T]) error,
) error {
	// Lock both treaps in a consistent order to avoid deadlocks
	// Order is determined by comparing root keys (data-driven), not pointer addresses
	if t.shouldLockFirst(other) {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nextA, cancelA := seq2Next(t.Iter(ctx))
	nextB, cancelB := seq2Next(other.Iter(ctx))
	defer cancelA()
	defer cancelB()

	return mergeOrdered(nextA, nextB, t.Less, onlyInA, inBoth, onlyInB)
}

// Iter returns an in-order iterator over the persistent treap using the Go iterator protocol.
// It respects context cancellation and surfaces iteration errors via Seq2.
func (t *PersistentTreap[T]) Iter(ctx context.Context) iter.Seq2[TreapNodeInterface[T], error] {
	return func(yield func(TreapNodeInterface[T], error) bool) {
		mode := t.getIterationMode()

		callback := func(node TreapNodeInterface[T]) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if !yield(node, nil) {
				return errWalkCanceled
			}
			return nil
		}

		var err error
		switch mode {
		case IterateInMemoryOnly:
			err = t.iterateInMemory(t.root, callback)
		case IterateOnDiskTransient:
			rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
			if !ok {
				err = fmt.Errorf("root is not a PersistentTreapNode")
				break
			}
			objId, objErr := rootNode.ObjectId()
			if objErr != nil {
				err = fmt.Errorf("failed to get root object ID: %w", objErr)
				break
			}
			if !store.IsValidObjectId(objId) {
				err = t.iterateInMemory(t.root, callback)
				break
			}
			err = t.iterateOnDiskTransient(objId, callback)
		case IterateOnDiskAndLoad:
			rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
			if !ok {
				err = fmt.Errorf("root is not a PersistentTreapNode")
				break
			}
			objId, objErr := rootNode.ObjectId()
			if objErr != nil {
				err = fmt.Errorf("failed to get root object ID: %w", objErr)
				break
			}
			if !store.IsValidObjectId(objId) {
				err = t.iterateInMemory(t.root, callback)
				break
			}
			err = t.iterateOnDiskAndLoad(objId, callback)
		default:
			err = fmt.Errorf("unknown iteration mode: %d", mode)
		}

		if err == errWalkCanceled {
			err = nil
		}

		if err != nil {
			_ = yield(nil, err)
		}
	}
}

// getIterationMode determines which iteration mode to use for this tree.
// If nodes are cached in memory, use in-memory iteration.
// If the tree has been persisted, use transient disk iteration.
// Otherwise fall back to in-memory iteration for unpersisted trees.
func (t *PersistentTreap[T]) getIterationMode() IterationMode {
	if t.root == nil {
		return IterateInMemoryOnly
	}

	// Check if we have in-memory nodes
	inMemoryCount := t.countInMemoryNodes(t.root)
	if inMemoryCount > 0 {
		return IterateInMemoryOnly
	}

	// No in-memory nodes, but check if the root has been persisted
	rootNode, ok := t.root.(*PersistentTreapNode[T])
	if !ok {
		return IterateInMemoryOnly
	}
	objId, err := rootNode.ObjectId()
	if err != nil || !store.IsValidObjectId(objId) {
		return IterateInMemoryOnly
	}

	// Use transient disk iteration for persisted trees without in-memory nodes
	return IterateOnDiskTransient
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

// CompactSuboptimalAllocations deletes nodes that reside in sub-optimal block
// allocators (smaller blockCount than the current pool) and marks them for
// reallocation on the next persist. Nodes remain in the treap; only their
// stored ObjectIds are cleared and the backing objects are deleted.
func (t *PersistentTreap[T]) CompactSuboptimalAllocations() (int, error) {
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

	var zero PersistentTreapNode[T]
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
	var walk func(TreapNodeInterface[T])
	walk = func(node TreapNodeInterface[T]) {
		if node == nil || node.IsNil() {
			return
		}
		pnode, ok := node.(*PersistentTreapNode[T])
		if !ok {
			return
		}

		walk(pnode.GetLeft())
		if store.IsValidObjectId(pnode.objectId) {
			if _, hit := idSet[pnode.objectId]; hit {
				_ = t.Store.DeleteObj(pnode.objectId) // best effort cleanup
				pnode.SetObjectId(internal.ObjNotAllocated)
				deleted++
			}
		}
		walk(pnode.GetRight())
	}

	walk(t.root)
	return deleted, nil
	*/
}

// collectPersistentNodesPostOrder collects persistent nodes in post-order to ensure
// children appear before parents. This is used by BatchPersist for contiguous runs.
func collectPersistentNodesPostOrder[T any](node TreapNodeInterface[T], out *[]*PersistentTreapNode[T]) error {
	if node == nil || node.IsNil() {
		return nil
	}
	if err := collectPersistentNodesPostOrder(node.GetLeft(), out); err != nil {
		return err
	}
	if err := collectPersistentNodesPostOrder(node.GetRight(), out); err != nil {
		return err
	}
	pNode, ok := node.(*PersistentTreapNode[T])
	if !ok {
		return fmt.Errorf("node is not *PersistentTreapNode")
	}
	*out = append(*out, pNode)
	return nil
}

// BatchPersist attempts to persist all nodes using a single contiguous run when the
// underlying store supports run allocation. It falls back to the standard Persist
// when run allocation is unsupported or unavailable.
func (t *PersistentTreap[T]) BatchPersist() error {
	if t.root == nil {
		return nil
	}

	nodes := make([]*PersistentTreapNode[T], 0)
	if err := collectPersistentNodesPostOrder(t.root, &nodes); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}

	size := nodes[0].sizeInBytes()

	ra, ok := t.Store.(store.RunAllocator)
	if !ok {
		return t.Persist()
	}

	objIds, offsets, err := ra.AllocateRun(size, len(nodes))
	if err != nil {
		return t.Persist()
	}

	// Verify offsets are actually contiguous before attempting batched write
	contiguous := true
	for i := 1; i < len(offsets); i++ {
		expectedOffset := offsets[i-1] + store.FileOffset(size)
		if offsets[i] != expectedOffset {
			contiguous = false
			break
		}
	}

	for i, n := range nodes {
		n.SetObjectId(objIds[i])
	}

	// If not contiguous, fall back to regular persist
	if !contiguous {
		return t.Persist()
	}

	objIdsForWrite := make([]store.ObjectId, len(nodes))
	sizes := make([]int, len(nodes))
	allData := make([]byte, 0, size*len(nodes))

	for i, n := range nodes {
		// Ensure child object IDs are synchronized before marshal
		if n.TreapNode.left != nil {
			if leftNode, ok := n.TreapNode.left.(PersistentTreapNodeInterface[T]); ok {
				childId, childErr := leftNode.ObjectId()
				if childErr != nil {
					return childErr
				}
				n.leftObjectId = childId
			}
		}
		if n.TreapNode.right != nil {
			if rightNode, ok := n.TreapNode.right.(PersistentTreapNodeInterface[T]); ok {
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
		allData = append(allData, data...)
	}

	if err := t.Store.WriteBatchedObjs(objIdsForWrite, allData, sizes); err != nil {
		// Fallback to per-node writes for robustness
		for i, objId := range objIdsForWrite {
			start := i * size
			end := start + sizes[i]
			if end > len(allData) {
				return fmt.Errorf("batched buffer bounds error")
			}
			if writeErr := store.WriteBytesToObj(t.Store, allData[start:end], objId); writeErr != nil {
				return writeErr
			}
		}
	}

	return nil
}

// Load loads the treap from the store using the given root ObjectId.
func (t *PersistentTreap[T]) Load(objId store.ObjectId) error {
	var err error
	t.root, err = NewFromObjectId(objId, t, t.Store)
	return err
}

// Iterate traverses the tree according to the specified mode, invoking the callback
// on each node visited. The callback can return an error to halt iteration.
//
// Behavior depends on mode:
//   - IterateInMemoryOnly: traverses only cached nodes (no disk access)
//   - IterateOnDiskTransient: loads nodes from disk transiently without caching
//     If the tree hasn't been persisted (no root object ID), falls back to in-memory iteration
//   - IterateOnDiskAndLoad: loads nodes from disk and retains them in memory
//     If the tree hasn't been persisted (no root object ID), falls back to in-memory iteration
//
// The iteration is in-order (left, node, right).
func (t *PersistentTreap[T]) Iterate(mode IterationMode, callback IterationCallback[T]) error {
	if t.root == nil {
		return nil
	}

	switch mode {
	case IterateInMemoryOnly:
		return t.iterateInMemory(t.root, callback)
	case IterateOnDiskTransient:
		rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
		if !ok {
			return fmt.Errorf("root is not a PersistentTreapNode")
		}
		objId, err := rootNode.ObjectId()
		if err != nil {
			return fmt.Errorf("failed to get root object ID: %w", err)
		}
		// If root hasn't been persisted, fall back to in-memory iteration
		if !store.IsValidObjectId(objId) {
			return t.iterateInMemory(t.root, callback)
		}
		return t.iterateOnDiskTransient(objId, callback)
	case IterateOnDiskAndLoad:
		rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
		if !ok {
			return fmt.Errorf("root is not a PersistentTreapNode")
		}
		objId, err := rootNode.ObjectId()
		if err != nil {
			return fmt.Errorf("failed to get root object ID: %w", err)
		}
		// If root hasn't been persisted, fall back to in-memory iteration
		if !store.IsValidObjectId(objId) {
			return t.iterateInMemory(t.root, callback)
		}
		return t.iterateOnDiskAndLoad(objId, callback)
	default:
		return fmt.Errorf("unknown iteration mode: %d", mode)
	}
}

// iterateInMemory traverses only cached nodes without disk access.
// Unlike the shared inOrderWalk, this directly accesses the cached pointers
// to avoid triggering lazy loading of nodes from disk.
func (t *PersistentTreap[T]) iterateInMemory(node TreapNodeInterface[T], callback IterationCallback[T]) error {
	if node == nil || node.IsNil() {
		return nil
	}

	pNode, ok := node.(PersistentTreapNodeInterface[T])
	if !ok {
		return nil // Skip non-persistent nodes
	}

	// In-order: left, node, right
	// Note: We need to access the underlying TreapNode pointers directly to avoid
	// triggering GetLeft()/GetRight() which would trigger lazy loading of children from disk.
	// For nodes that are PersistentTreapNode[T] directly, we access TreapNode.left/right.
	// For nodes that embed PersistentTreapNode (like PersistentPayloadTreapNode), we get the
	// embedded TreapNode from the interface.

	// Get left child from the underlying TreapNode
	var left TreapNodeInterface[T]
	if pNodeConcrete, ok := pNode.(*PersistentTreapNode[T]); ok {
		left = pNodeConcrete.TreapNode.left
	} else {
		// For other types that embed TreapNode, we can't directly access the embedded field
		// through the interface, so we use GetLeft() which may trigger disk loads in some cases
		left = pNode.GetLeft()
	}

	if left != nil && !left.IsNil() {
		if err := t.iterateInMemory(left, callback); err != nil {
			return err
		}
	}

	if err := callback(pNode); err != nil {
		return err
	}

	// Get right child from the underlying TreapNode
	var right TreapNodeInterface[T]
	if pNodeConcrete, ok := pNode.(*PersistentTreapNode[T]); ok {
		right = pNodeConcrete.TreapNode.right
	} else {
		// For other types that embed TreapNode, we can't directly access the embedded field
		// through the interface, so we use GetRight() which may trigger disk loads in some cases
		right = pNode.GetRight()
	}

	if right != nil && !right.IsNil() {
		if err := t.iterateInMemory(right, callback); err != nil {
			return err
		}
	}

	return nil
}

// iterateOnDiskTransient loads nodes transiently via object IDs without caching.
// Uses an explicit stack to avoid excessive recursion depth for large trees.
func (t *PersistentTreap[T]) iterateOnDiskTransient(rootObjId store.ObjectId, callback IterationCallback[T]) error {
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

		// Load node transiently
		node, err := NewFromObjectId(frame.objId, t, t.Store)
		if err != nil {
			return fmt.Errorf("failed to load node from disk (objId=%d): %w", frame.objId, err)
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

// iterateOnDiskAndLoad loads nodes and retains them in the in-memory tree.
func (t *PersistentTreap[T]) iterateOnDiskAndLoad(objId store.ObjectId, callback IterationCallback[T]) error {
	if !store.IsValidObjectId(objId) {
		return nil
	}

	node, err := NewFromObjectId(objId, t, t.Store)
	if err != nil {
		return fmt.Errorf("failed to load node from disk (objId=%d): %w", objId, err)
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

// NodeInfo contains information about a node in memory, including its access timestamp.
type NodeInfo[T any] struct {
	Node           *PersistentTreapNode[T]
	LastAccessTime int64
	Key            types.PersistentKey[T]
}

// GetInMemoryNodes traverses the treap and collects all nodes currently in memory.
// This method does NOT load nodes from disk and does NOT update access timestamps.
// It only includes nodes that are already loaded in memory.
// Returns a slice of NodeInfo containing each node and its last access time.
func (t *PersistentTreap[T]) GetInMemoryNodes() []NodeInfo[T] {
	var nodes []NodeInfo[T]
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.collectInMemoryNodes(t.root, &nodes)
	return nodes
}

// CountInMemoryNodes returns the count of nodes currently loaded in memory.
// This is more efficient than len(GetInMemoryNodes()) as it doesn't allocate the slice.
func (t *PersistentTreap[T]) CountInMemoryNodes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.countInMemoryNodes(t.root)
}

// countInMemoryNodes recursively counts in-memory nodes.
func (t *PersistentTreap[T]) countInMemoryNodes(node TreapNodeInterface[T]) int {
	if node == nil || node.IsNil() {
		return 0
	}

	pNode, ok := node.(PersistentTreapNodeInterface[T])
	if !ok {
		return 0
	}

	count := 1 // Count this node

	// Only traverse children that are already in memory
	left := pNode.GetLeft()
	if left != nil && !left.IsNil() {
		count += t.countInMemoryNodes(left)
	}
	right := pNode.GetRight()
	if right != nil && !right.IsNil() {
		count += t.countInMemoryNodes(right)
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
		Key:            pNode.GetKey().(types.PersistentKey[T]),
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
	if err := t.Persist(); err != nil {
		return 0, err
	}

	flushedCount := 0
	err := t.Iterate(IterateInMemoryOnly, func(node TreapNodeInterface[T]) error {
		pNode, ok := node.(*PersistentTreapNode[T])
		if !ok {
			return nil
		}
		if pNode.GetLastAccessTime() < cutoffTimestamp {
			flushErr := pNode.Flush()
			if flushErr != nil {
				// If errNotFullyPersisted, continue flushing others
				if !errors.Is(flushErr, errNotFullyPersisted) {
					return flushErr
				}
			} else {
				flushedCount++
			}
		}
		return nil
	})

	return flushedCount, err
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
	if err := t.Persist(); err != nil {
		return 0, err
	}

	// Collect all in-memory nodes with their access times
	var nodes []*PersistentTreapNode[T]
	if err := t.Iterate(IterateInMemoryOnly, func(node TreapNodeInterface[T]) error {
		pNode, ok := node.(*PersistentTreapNode[T])
		if !ok {
			return nil
		}
		nodes = append(nodes, pNode)
		return nil
	}); err != nil {
		return 0, err
	}

	if len(nodes) == 0 {
		return 0, nil
	}

	// Sort nodes by access time (oldest first) using slice sort
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetLastAccessTime() < nodes[j].GetLastAccessTime()
	})

	// Calculate how many nodes to flush
	numToFlush := (len(nodes) * percentage) / 100
	if numToFlush == 0 && percentage > 0 {
		numToFlush = 1 // Flush at least one node if percentage > 0
	}

	// Flush the oldest nodes
	flushedCount := 0
	for i := 0; i < numToFlush && i < len(nodes); i++ {
		err := nodes[i].Flush()
		if err != nil {
			if !errors.Is(err, errNotFullyPersisted) {
				return flushedCount, err
			}
		} else {
			flushedCount++
		}
	}

	// Release the nodes slice to help GC
	for i := range nodes {
		nodes[i] = nil
	}
	nodes = nil

	return flushedCount, nil
}

// GetRootObjectId returns the ObjectId of the root node of the treap.
// Returns ObjNotAllocated if the tree is empty or hasn't been persisted yet.
func (t *PersistentTreap[T]) GetRootObjectId() (store.ObjectId, error) {
	if t.root == nil {
		return internal.ObjNotAllocated, nil
	}
	rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
	if !ok {
		return internal.ObjNotAllocated, fmt.Errorf("root is not a PersistentTreapNodeInterface")
	}
	return rootNode.ObjectId()
}
