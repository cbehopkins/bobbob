package treap

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

var errNotFullyPersisted = errors.New("node not fully persisted")

// IterationMode defines how the tree iterator should traverse nodes.

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
	v := PersistentObjectId(store.ObjectId(bobbob.ObjNotAllocated))
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
	Store          store.Storer // FIXME: we should use parent.Store
	parent         *PersistentTreap[T]
	lastAccessTime int64 // Unix timestamp of last access (in-memory only, not persisted)
}

func (n *PersistentTreapNode[T]) String() string {
	return fmt.Sprintf("PersistentTreapNode{key=%v, priority=%d, objectId=%d, leftObjectId=%d, rightObjectId=%d}",
		n.GetKey(), n.GetPriority(), n.objectId, n.leftObjectId, n.rightObjectId)
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
	n.objectId = bobbob.ObjNotAllocated
	n.leftObjectId = bobbob.ObjNotAllocated
	n.rightObjectId = bobbob.ObjNotAllocated
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

// GetStore returns the backing store for this node.
func (n *PersistentTreapNode[T]) GetStore() store.Storer {
	return n.Store
}

// SetPriority sets the priority of the node.
func (n *PersistentTreapNode[T]) SetPriority(p Priority) {
	// Mark objectId as invalid so node will be re-persisted
	// CRITICAL: Do NOT call DeleteObj here! Parent nodes may still reference
	// this objectId in their leftObjectId/rightObjectId fields or on disk.
	// Deleting the object would orphan the node and cause data loss.
	n.objectId = bobbob.ObjNotAllocated
	n.TreapNode.priority = p
}

// GetLeft returns the left child of the node.
// loading the child from disk if necessary.
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
// loading the child from disk if necessary.
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

	// Always sync leftObjectId to match what the pointer points to
	// This keeps pointer and childObjectId in sync
	// IMPORTANT: Don't call ObjectId() here as it allocates storage prematurely!
	// Just read the existing objectId field without allocating.
	if left != nil {
		if pChild, ok := left.(*PersistentTreapNode[T]); ok {
			// Read the objectId field directly without allocating
			n.leftObjectId = pChild.objectId // May be -1 (invalid), persist() will allocate/write later
		} else {
			n.leftObjectId = bobbob.ObjNotAllocated
		}
	} else {
		// Setting to nil - invalidate
		n.leftObjectId = bobbob.ObjNotAllocated
	}

	// Mark as dirty so it gets re-persisted, but DON'T delete the old ObjectId
	// Other nodes (particularly the parent) may still reference it in their persisted data.
	// Deleting it would cause rehydration failures.
	// _ = n.Store.DeleteObj(n.objectId)  // <-- THIS WAS CAUSING CORRUPTION
	if store.IsValidObjectId(n.objectId) {
		n.parent.queueDelete(n.objectId)
	}
	n.objectId = bobbob.ObjNotAllocated
	return nil
}

// SetRight sets the right child of the node.
func (n *PersistentTreapNode[T]) SetRight(right TreapNodeInterface[T]) error {
	n.TreapNode.right = right

	// Always sync rightObjectId to match what the pointer points to
	// This keeps pointer and childObjectId in sync
	// IMPORTANT: Don't call ObjectId() here as it allocates storage prematurely!
	// Just read the existing objectId field without allocating.
	if right != nil {
		if pChild, ok := right.(*PersistentTreapNode[T]); ok {
			// Read the objectId field directly without allocating
			n.rightObjectId = pChild.objectId // May be -1 (invalid), persist() will allocate/write later
		} else {
			// FIXME This shouldn't be possible - I think we should panic here
			n.rightObjectId = bobbob.ObjNotAllocated
		}
	} else {
		// Setting to nil - invalidate
		n.rightObjectId = bobbob.ObjNotAllocated
	}

	// Mark as dirty so it gets re-persisted, but DON'T delete the old ObjectId
	// Other nodes (particularly the parent) may still reference it in their persisted data.
	// Deleting it would cause rehydration failures.
	// _ = n.Store.DeleteObj(n.objectId)  // <-- THIS WAS CAUSING CORRUPTION
	if store.IsValidObjectId(n.objectId) {
		n.parent.queueDelete(n.objectId)
	}
	n.objectId = bobbob.ObjNotAllocated
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
		return bobbob.ObjNotAllocated, nil
	}
	if n.objectId < 0 {
		objId, err := n.Store.NewObj(n.sizeInBytes())
		if err != nil {
			return bobbob.ObjNotAllocated, err
		}
		n.objectId = objId
	}
	return n.objectId, nil
}

// GetObjectIdNoAlloc returns the objectId without allocating a new one if invalid.
// This is useful for reading the objectId without side effects.
func (n *PersistentTreapNode[T]) GetObjectIdNoAlloc() store.ObjectId {
	if n == nil {
		return bobbob.ObjNotAllocated
	}
	return n.objectId
}

// getLeftObjectId returns the cached left ObjectId (may be invalid).
func (n *PersistentTreapNode[T]) getLeftObjectId() store.ObjectId {
	if n == nil {
		return bobbob.ObjNotAllocated
	}
	return n.leftObjectId
}

// setLeftObjectId sets the cached left ObjectId.
func (n *PersistentTreapNode[T]) setLeftObjectId(id store.ObjectId) {
	if n == nil {
		return
	}
	n.leftObjectId = id
}

// getRightObjectId returns the cached right ObjectId (may be invalid).
func (n *PersistentTreapNode[T]) getRightObjectId() store.ObjectId {
	if n == nil {
		return bobbob.ObjNotAllocated
	}
	return n.rightObjectId
}

// setRightObjectId sets the cached right ObjectId.
func (n *PersistentTreapNode[T]) setRightObjectId(id store.ObjectId) {
	if n == nil {
		return
	}
	n.rightObjectId = id
}

// GetLeftChild returns the left child pointer (may be nil if flushed).
// Does not load from disk; use GetTransientLeftChild for that.
func (n *PersistentTreapNode[T]) GetLeftChild() TreapNodeInterface[T] {
	if n == nil {
		return nil
	}
	return n.TreapNode.left
}

// GetRightChild returns the right child pointer (may be nil if flushed).
// Does not load from disk; use GetTransientRightChild for that.
func (n *PersistentTreapNode[T]) GetRightChild() TreapNodeInterface[T] {
	if n == nil {
		return nil
	}
	return n.TreapNode.right
}

// GetTransientLeftChild loads the left child from disk if needed, without caching.
// Returns the child node and error (may be nil if no child exists).
func (n *PersistentTreapNode[T]) GetTransientLeftChild() (PersistentNodeWalker[T], error) {
	return getChildNodeTransient(n, true)
}

// GetTransientRightChild loads the right child from disk if needed, without caching.
// Returns the child node and error (may be nil if no child exists).
func (n *PersistentTreapNode[T]) GetTransientRightChild() (PersistentNodeWalker[T], error) {
	return getChildNodeTransient(n, false)
}

// DependentObjectIds returns all ObjectIds owned by this node's key (if the key implements DependentObjectIds).
// These are objects that must be deleted when the node is removed.
func (n *PersistentTreapNode[T]) DependentObjectIds() []store.ObjectId {
	if n == nil || n.key == nil {
		return nil
	}
	// Check if the key implements the DependentObjectIds interface
	if depProvider, ok := n.key.(interface{ DependentObjectIds() []store.ObjectId }); ok {
		return depProvider.DependentObjectIds()
	}
	// If key doesn't implement DependentObjectIds, it's responsible for its own cleanup
	return nil
}

// SetObjectId sets the ObjectId for this node.
// This is typically used when loading a node from the store.
func (n *PersistentTreapNode[T]) SetObjectId(id store.ObjectId) {
	n.objectId = id
}

// persistSelf persists this node (used by the shared persist helper).
func (n *PersistentTreapNode[T]) persistSelf() error {
	if n == nil {
		return nil
	}
	return n.persist()
}

// IsObjectIdInvalid returns true if the node's ObjectId has been invalidated (is negative).
func (n *PersistentTreapNode[T]) IsObjectIdInvalid() bool {
	if n == nil {
		return true
	}
	return n.objectId < 0
}

// IsPersisted reports whether this node has been written to disk.
func (n *PersistentTreapNode[T]) IsPersisted() bool {
	if n == nil {
		return false
	}
	return store.IsValidObjectId(n.objectId)
}

// Persist saves this node and its subtree to the store.
// Traversal is handled by a dedicated post-order walker that loads nodes from disk as needed.
// The walker automatically tracks which nodes become dirty during persistence.
// Persist saves this node and its subtree to the store.
// Uses the polymorphic post-order walker (rangeOverTreapPostOrder) which automatically:
// - Tracks which nodes become dirty (valid→invalid objectId transition)
// - Propagates dirty state to ancestor nodes on the traversal stack
// - Loads nodes from disk transiently without mutating cached pointers
// The callback syncs child objectIds and persists each node during traversal.
func (n *PersistentTreapNode[T]) Persist() error {
	if n == nil {
		return nil
	}
	return persistLockedTreeCommon[T](
		n,
		rangeOverPostOrder[T],
	)
}

func getChildNodeTransient[T any](node *PersistentTreapNode[T], isLeft bool) (*PersistentTreapNode[T], error) {
	if node == nil || node.IsNil() {
		return nil, nil
	}

	if isLeft {
		if node.TreapNode.left != nil && !node.TreapNode.left.IsNil() {
			leftNode, ok := node.TreapNode.left.(*PersistentTreapNode[T])
			if !ok {
				return nil, fmt.Errorf("left child is not a PersistentTreapNode")
			}
			return leftNode, nil
		}
		if store.IsValidObjectId(node.leftObjectId) {
			return NewFromObjectId(node.leftObjectId, node.parent, node.Store)
		}
		return nil, nil
	}

	if node.TreapNode.right != nil && !node.TreapNode.right.IsNil() {
		rightNode, ok := node.TreapNode.right.(*PersistentTreapNode[T])
		if !ok {
			return nil, fmt.Errorf("right child is not a PersistentTreapNode")
		}
		return rightNode, nil
	}
	if store.IsValidObjectId(node.rightObjectId) {
		return NewFromObjectId(node.rightObjectId, node.parent, node.Store)
	}
	return nil, nil
}

// flushChild flushes the given child node if it exists and is persisted, then clears its objectId and pointer.
func (n *PersistentTreapNode[T]) flushChild(child *TreapNodeInterface[T], childObjectId *store.ObjectId) error {
	if *child == nil {
		return nil
	}
	if (*child).IsNil() {
		return nil
	}
	if !store.IsValidObjectId(*childObjectId) {
		return errNotFullyPersisted
	}
	childNode, ok := (*child).(PersistentTreapNodeInterface[T])
	if !ok {
		return errNotFullyPersisted
	}
	if childNode == nil || childNode.IsNil() {
		return nil
	}
	if childNode.IsObjectIdInvalid() {
		return errNotFullyPersisted
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

// FlushAll recursively flushes this node's entire subtree from memory.
// This clears child pointers throughout the subtree while keeping objectIds intact.
// Note: The node itself remains in memory; its parent must clear its pointer to evict it.
func (n *PersistentTreapNode[T]) FlushAll() error {
	if n == nil {
		return nil
	}

	allChildrenPersisted := true

	if n.TreapNode.left != nil {
		leftNode, ok := n.TreapNode.left.(*PersistentTreapNode[T])
		if !ok {
			return errNotFullyPersisted
		}
		if err := leftNode.FlushAll(); err != nil {
			if errors.Is(err, errNotFullyPersisted) {
				allChildrenPersisted = false
			} else {
				return err
			}
		}
	}

	if n.TreapNode.right != nil {
		rightNode, ok := n.TreapNode.right.(*PersistentTreapNode[T])
		if !ok {
			return errNotFullyPersisted
		}
		if err := rightNode.FlushAll(); err != nil {
			if errors.Is(err, errNotFullyPersisted) {
				allChildrenPersisted = false
			} else {
				return err
			}
		}
	}

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

// Persists this node.
func (n *PersistentTreapNode[T]) persist() error {
	buf, err := n.Marshal()
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	objId, err := n.ObjectId()
	if err != nil {
		return fmt.Errorf("get objectId failed: %w", err)
	}

	// n.parent.persistentWorkerPool.Submit(func() error {
	// 	// Otherwise, update the existing object (data fits in allocated space)
	// 	return store.WriteBytesToObj(n.Store, buf, objId)
	// })
	return store.WriteBytesToObj(n.Store, buf, objId)

	// return nil
}

// syncChildObjectId ensures the cached object ID for a child matches the child's actual persisted ID.
// If the child exists:
// - When the cached ID is valid, it fetches the child's ID and if it differs, updates the cache and invalidates this node's objectId.
// - When the cached ID is invalid, it fetches the child's ID, sets the cache, and invalidates this node's objectId.
// If the child doesn't exist, it does nothing.
func (n *PersistentTreapNode[T]) syncChildObjectId(child TreapNodeInterface[T], cached *store.ObjectId, side string) error {
	if child == nil {
		// Child pointer is nil - might be flushed
		// If cached objectId is valid, keep it (points to flushed child on disk)
		// If invalid, that's fine (no child)
		return nil
	}
	pChild, ok := child.(PersistentTreapNodeInterface[T])
	if !ok {
		return fmt.Errorf("%s child is not a PersistentTreapNodeInterface", side)
	}

	// Read objectId without allocation side effects
	childObjId := pChild.GetObjectIdNoAlloc()

	// Always sync childObjectId to match what pointer points to.
	// Only proceed if child has a valid objectId (has been persisted).
	if !store.IsValidObjectId(childObjId) {
		// Child not yet persisted, nothing to sync
		return nil
	}
	if childObjId != *cached {
		*cached = childObjId
		// Mark as dirty so it gets re-persisted, but DON'T delete the old ObjectId
		// Other nodes (particularly the parent) may still reference it in their persisted data.
		// Deleting it would cause rehydration failures.
		n.objectId = bobbob.ObjNotAllocated
	}
	return nil
}

func (n *PersistentTreapNode[T]) Marshal() ([]byte, error) {
	return marshalTreapNodeBase(n, n.ObjectId)
}

// marshalTreapNodeBase serializes the base treap node fields using the provided objectId allocator.
// This is shared by payload and non-payload nodes to avoid duplication while allocating the right size.
func marshalTreapNodeBase[T any](n *PersistentTreapNode[T], selfObjId func() (store.ObjectId, error)) ([]byte, error) {
	expectedSize := n.sizeInBytes()
	buf := make([]byte, expectedSize)
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
	selfId, err := selfObjId()
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
		selfId,
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

	err = n.TreapNode.priority.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal priority: %w", err)
	}
	offset += n.TreapNode.priority.SizeInBytes()

	leftObjectId := store.ObjectId(bobbob.ObjNotAllocated)
	err = leftObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal left object ID: %w", err)
	}
	offset += leftObjectId.SizeInBytes()
	n.leftObjectId = leftObjectId

	rightObjectId := store.ObjectId(bobbob.ObjNotAllocated)
	err = rightObjectId.Unmarshal(data[offset:])
	if err != nil {
		return fmt.Errorf("failed to unmarshal right object ID: %w", err)
	}
	offset += rightObjectId.SizeInBytes()
	n.rightObjectId = rightObjectId

	selfObjectId := store.ObjectId(bobbob.ObjNotAllocated)
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
	persistMu   sync.Mutex   // Ensures only one persist operation at a time
	nodePool    sync.Pool    // Pool for *PersistentTreapNode[T]
	// pendingDeletes holds ObjectIds scheduled for deletion after a successful persist.
	pendingDeletes       []store.ObjectId
	persistentWorkerPool *persistWorkerPool
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
	n.objectId = bobbob.ObjNotAllocated
	n.leftObjectId = bobbob.ObjNotAllocated
	n.rightObjectId = bobbob.ObjNotAllocated
	n.Store = stre
	n.parent = parent
	n.lastAccessTime = 0
	return n
}

// NewPersistentTreap creates a new PersistentTreap with the given comparison function and store reference.
func NewPersistentTreap[T any](lessFunc func(a, b T) bool, keyTemplate types.PersistentKey[T], stre store.Storer) *PersistentTreap[T] {
	t := &PersistentTreap[T]{
		Treap: Treap[T]{
			root: nil,
			Less: func(a, b T) bool {
				return lessFunc(a, b)
			},
		},
		keyTemplate:    keyTemplate,
		Store:          stre,
		pendingDeletes: make([]store.ObjectId, 0, 64),
	}
	t.nodePool = sync.Pool{New: func() any { return new(PersistentTreapNode[T]) }}
	t.persistentWorkerPool = nil
	return t
}

// queueDelete schedules an ObjectId for deletion after the next successful persist.
func (t *PersistentTreap[T]) queueDelete(objId store.ObjectId) {
	if store.IsValidObjectId(objId) {
		t.pendingDeletes = append(t.pendingDeletes, objId)
	}
}

// flushPendingDeletes deletes all queued ObjectIds and clears the queue.
// Call this only after a successful persist to avoid breaking on-disk references.
func (t *PersistentTreap[T]) flushPendingDeletes() {
	if len(t.pendingDeletes) == 0 {
		return
	}
	// Diagnostic: deduplicate pending deletes to avoid repeated DeleteObj calls
	uniq := make(map[store.ObjectId]struct{}, len(t.pendingDeletes))
	for _, id := range t.pendingDeletes {
		if store.IsValidObjectId(id) {
			uniq[id] = struct{}{}
		}
	}
	if len(uniq) == 0 {
		t.pendingDeletes = t.pendingDeletes[:0]
		return
	}
	// log.Printf("flushing %d unique pending deletes (from %d queued)", len(uniq), len(t.pendingDeletes))
	for id := range uniq {
		if err := t.Store.DeleteObj(id); err != nil {
			log.Printf("flushPendingDeletes: failed to delete obj %d: %v", id, err)
		}
	}
	t.pendingDeletes = t.pendingDeletes[:0]
}

// releaseNode returns a node to the pool if it's a persistent node.
func (t *PersistentTreap[T]) releaseNode(node TreapNodeInterface[T]) {
	if node == nil || node.IsNil() {
		return
	}
	if pNode, ok := node.(*PersistentTreapNode[T]); ok {
		pNode.releaseToPool()
	}
}

// trackDirty records a node as dirty for later invalidation.
func (t *PersistentTreap[T]) trackDirty(dirty *[]PersistentTreapNodeInterface[T], node TreapNodeInterface[T]) {
	if dirty == nil || node == nil || node.IsNil() {
		return
	}
	if pNode, ok := node.(PersistentTreapNodeInterface[T]); ok {
		*dirty = append(*dirty, pNode)
	}
}

// invalidateDirty marks all tracked nodes as needing re-persist.
func (t *PersistentTreap[T]) invalidateDirty(dirty []PersistentTreapNodeInterface[T]) {
	for _, node := range dirty {
		if node == nil {
			continue
		}

		// Queue old object ID for deletion
		if !node.IsObjectIdInvalid() {
			oldObjId := node.GetObjectIdNoAlloc()
			if store.IsValidObjectId(oldObjId) {
				t.queueDelete(oldObjId)
			}
			node.SetObjectId(bobbob.ObjNotAllocated)
		}
	}
}

// insertNodeTracked performs insertion using package-level logic with dirty tracking.
// This eliminates duplication with the base Treap.insert while adding persistence.
func (t *PersistentTreap[T]) insertNodeTracked(node TreapNodeInterface[T], newNode TreapNodeInterface[T], dirty *[]PersistentTreapNodeInterface[T]) (TreapNodeInterface[T], error) {
	// Create a duplicate key handler for PersistentTreap's replace-on-duplicate behavior
	duplicateHandler := func(existingNode, newNode TreapNodeInterface[T]) bool {
		if pNode, ok := existingNode.(*PersistentTreapNode[T]); ok {
			if newPNode, ok := newNode.(*PersistentTreapNode[T]); ok {
				if store.IsValidObjectId(pNode.objectId) {
					t.queueDelete(pNode.objectId)
				}
				pNode.key = newPNode.key
				pNode.priority = newPNode.priority
				pNode.objectId = bobbob.ObjNotAllocated
				return true // Handled
			}
		}
		return false // Not handled, use default
	}

	return InsertNodeTracked(
		node,
		newNode,
		t.Less,
		t.releaseNode,
		func(n TreapNodeInterface[T]) { t.trackDirty(dirty, n) },
		duplicateHandler,
	)
}

// deleteNodeTracked performs deletion using package-level logic with dirty tracking.
// This eliminates duplication with the base Treap.delete while adding persistence.
func (t *PersistentTreap[T]) deleteNodeTracked(node TreapNodeInterface[T], key T, dirty *[]PersistentTreapNodeInterface[T]) (TreapNodeInterface[T], error) {
	return DeleteNodeTracked(
		node,
		key,
		t.Less,
		t.releaseNode,
		func(n TreapNodeInterface[T]) { t.trackDirty(dirty, n) },
		func(n TreapNodeInterface[T], dt DirtyTracker[T]) { t.cleanupRemovedNode(n, dt) },
	)
}

// insertTracked inserts a new node and tracks all modified nodes for persistence.
// Now delegates to insertNodeTracked for the actual logic.
func (t *PersistentTreap[T]) insertTracked(node TreapNodeInterface[T], newNode TreapNodeInterface[T], dirty *[]PersistentTreapNodeInterface[T]) (TreapNodeInterface[T], error) {
	return t.insertNodeTracked(node, newNode, dirty)
}

// deleteTracked removes the node with the given key and tracks dirty nodes for invalidation.
// Now delegates to deleteNodeTracked for the actual logic.
func (t *PersistentTreap[T]) deleteTracked(node TreapNodeInterface[T], key T, dirty *[]PersistentTreapNodeInterface[T]) (TreapNodeInterface[T], error) {
	return t.deleteNodeTracked(node, key, dirty)
}

// cleanupRemovedNode performs best-effort cleanup for a removed node's persisted objects.
func (t *PersistentTreap[T]) cleanupRemovedNode(node TreapNodeInterface[T], trackDirty DirtyTracker[T]) {
	nodeCast, ok := node.(PersistentTreapNodeInterface[T])
	if !ok {
		return
	}
	objId := nodeCast.GetObjectIdNoAlloc()

	if objId > bobbob.ObjNotAllocated {
		// Best-effort cleanup of associated objects (key/payload) before freeing node.
		if depProvider, ok := nodeCast.(interface{ DependentObjectIds() []store.ObjectId }); ok {
			for _, dep := range depProvider.DependentObjectIds() {
				if store.IsValidObjectId(dep) {
					t.queueDelete(dep)
				}
			}
		}
		// Defer deletion of the node object until after a successful persist.
		t.queueDelete(objId)
	}

	nodeCast.SetObjectId(bobbob.ObjNotAllocated)
	if trackDirty != nil {
		trackDirty(node)
	}
}

// InsertComplex inserts a new node with the given key and priority into the persistent treap.
// Use this method when you need to specify a custom priority value.
func (t *PersistentTreap[T]) InsertComplex(key types.PersistentKey[T], priority Priority) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	newNode := NewPersistentTreapNode(key, priority, t.Store, t)
	dirty := make([]PersistentTreapNodeInterface[T], 0, 32)
	inserted, err := t.insertTracked(t.root, newNode, &dirty)
	if err != nil {
		return err
	}
	t.root = inserted
	t.invalidateDirty(dirty)
	t.flushPendingDeletes()
	return nil
}

// Insert inserts a new node with the given key into the persistent treap.
// If the key implements types.PriorityProvider, its Priority() method is used;
// otherwise, a random priority is generated.
// This is the preferred method for most use cases.
func (t *PersistentTreap[T]) Insert(key types.PersistentKey[T]) error {
	var priority Priority
	if pp, ok := any(key).(types.PriorityProvider); ok {
		priority = Priority(pp.Priority())
	} else {
		priority = randomPriority()
	}
	return t.InsertComplex(key, priority)
}

// Delete removes the node with the given key from the persistent treap.
func (t *PersistentTreap[T]) Delete(key types.PersistentKey[T]) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	dirty := make([]PersistentTreapNodeInterface[T], 0, 32)
	deleted, err := t.deleteTracked(t.root, key.Value(), &dirty)
	if err != nil {
		return err
	}
	t.root = deleted
	t.invalidateDirty(dirty)
	t.flushPendingDeletes()

	return nil
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
	return SearchNodeComplex(t.root, key.Value(), t.Less, wrappedCallback)
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
	result, _ := SearchNodeComplex(t.root, key.Value(), t.Less, wrappedCallback)
	return result
}

// UpdatePriority updates the priority of the node with the given key.
func (t *PersistentTreap[T]) UpdatePriority(key types.PersistentKey[T], newPriority Priority) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := SearchNode(t.root, key.Value(), t.Less)
	if node != nil && !node.IsNil() {
		// Delete and re-add as the priority change may violate treap properties
		dirty := make([]PersistentTreapNodeInterface[T], 0, 32)
		var err error
		t.root, err = t.deleteTracked(t.root, key.Value(), &dirty)
		if err != nil {
			return err
		}
		newNode := NewPersistentTreapNode(key, newPriority, t.Store, t)
		t.root, err = t.insertTracked(t.root, newNode, &dirty)
		if err != nil {
			return err
		}
		t.invalidateDirty(dirty)
	}
	return nil
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

		// If keys are equal, who cares?
		if !t.Less(tKey, otherKey) && !t.Less(otherKey, tKey) {
			return true
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

	// Both empty: who cares?
	return true
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

	nextA, cancelA := seq2Next(t.iterInOrder(ctx))
	nextB, cancelB := seq2Next(other.iterInOrder(ctx))
	defer cancelA()
	defer cancelB()

	return mergeOrdered(nextA, nextB, t.Less, onlyInA, inBoth, onlyInB)
}

// ValidateAgainstDisk walks the in-memory tree and validates each node's data against
// what's stored on disk. This is a diagnostic tool to detect corruption.
// Returns a slice of error messages for any inconsistencies found.
func (t *PersistentTreap[T]) ValidateAgainstDisk() []string {
	var errors []string

	if t.root == nil {
		return errors
	}

	// Walk all in-memory nodes
	nodes := t.GetInMemoryNodes()

	for _, info := range nodes {
		node := info.Node
		if node == nil {
			errors = append(errors, "node is nil")
			continue
		}

		// Read objectId without allocation side effects
		objId := node.GetObjectIdNoAlloc()

		if !store.IsValidObjectId(objId) {
			// Not persisted yet, skip validation
			continue
		}

		// Read the node from disk
		diskNode, err := NewFromObjectId(objId, t, t.Store)
		if err != nil {
			errors = append(errors, fmt.Sprintf("Failed to read objectId %d from disk: %v", objId, err))
			continue
		}

		// Validate key matches (compare the actual key values)
		if !node.key.Equals(diskNode.key.Value()) {
			errors = append(errors, fmt.Sprintf("ObjectId %d: in-memory key != disk key", objId))
		}

		// Validate priority matches
		if node.priority != diskNode.priority {
			errors = append(errors, fmt.Sprintf("ObjectId %d: in-memory priority %d != disk priority %d",
				objId, node.priority, diskNode.priority))
		}

		// Validate left child objectId
		memLeftObjId := node.leftObjectId
		diskLeftObjId := diskNode.leftObjectId
		if memLeftObjId != diskLeftObjId {
			errors = append(errors, fmt.Sprintf("ObjectId %d: in-memory leftObjectId %d != disk leftObjectId %d",
				objId, memLeftObjId, diskLeftObjId))
		}

		// Validate right child objectId
		memRightObjId := node.rightObjectId
		diskRightObjId := diskNode.rightObjectId
		if memRightObjId != diskRightObjId {
			errors = append(errors, fmt.Sprintf("ObjectId %d: in-memory rightObjectId %d != disk rightObjectId %d",
				objId, memRightObjId, diskRightObjId))
		}

		// If in-memory node has a left child pointer, validate it matches leftObjectId
		if node.TreapNode.left != nil {
			leftNode, ok := node.TreapNode.left.(PersistentTreapNodeInterface[T])
			if ok {
				// Read objectId without allocation side effects
				leftObjId := leftNode.GetObjectIdNoAlloc()
				if store.IsValidObjectId(leftObjId) {
					if leftObjId != memLeftObjId {
						errors = append(errors, fmt.Sprintf("ObjectId %d: left pointer's objectId %d != cached leftObjectId %d",
							objId, leftObjId, memLeftObjId))
					}
				}
			}
		}

		// If in-memory node has a right child pointer, validate it matches rightObjectId
		if node.TreapNode.right != nil {
			rightNode, ok := node.TreapNode.right.(PersistentTreapNodeInterface[T])
			if ok {
				// Read objectId without allocation side effects
				rightObjId := rightNode.GetObjectIdNoAlloc()
				if store.IsValidObjectId(rightObjId) {
					if rightObjId != memRightObjId {
						errors = append(errors, fmt.Sprintf("ObjectId %d: right pointer's objectId %d != cached rightObjectId %d",
							objId, rightObjId, memRightObjId))
					}
				}
			}
		}
	}

	return errors
}

// iterInOrder builds a Seq2 iterator for in-order traversal.
// It loads nodes from disk via GetLeft/GetRight when needed, ensuring full-tree traversal.
// This is called from Compare() which already holds R locks, so this must NOT acquire any locks.
func (t *PersistentTreap[T]) iterInOrder(ctx context.Context) iter.Seq2[TreapNodeInterface[T], error] {
	return func(yield func(TreapNodeInterface[T], error) bool) {
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

		// Since Compare() already holds R locks, we call hybridWalkInOrder directly
		// without acquiring any locks. This avoids deadlock.
		// We don't track mutations here (read-only traversal).
		_, err := hybridWalkInOrder(t.root, t.Store, callback, false)
		if err == errWalkCanceled {
			err = nil
		}
		if err != nil {
			_ = yield(nil, err)
		}
	}
}

// persistLockedTree persists the entire treap to the store.
// Assumes the caller already holds the write lock (t.mu.Lock()).
// This is the internal implementation that does not acquire locks itself,
// allowing multiple persist calls within a single locked operation.
func (t *PersistentTreap[T]) persistLockedTree() error {
	if t.root == nil {
		return nil
	}
	rootNode, ok := t.root.(*PersistentTreapNode[T])
	if !ok {
		return fmt.Errorf("root is not a PersistentTreapNode")
	}
	err := persistLockedTreeCommon[T](
		rootNode,
		rangeOverPostOrderInMemory[T],
	)
	if err != nil {
		return err
	}
	// Safe to delete queued objects now that the tree is fully persisted.
	t.flushPendingDeletes()
	return err
}

// RangeOverTreapPostOrder walks the treap in post-order (left, right, node),
// loading nodes from disk as needed. The callback receives each node.
// Returns the list of nodes that became dirty during traversal.
// NOTE: This method acquires the write lock because GetLeft()/GetRight()
// may load nodes from disk and mutate pointers.
func (t *PersistentTreap[T]) RangeOverTreapPostOrder(callback func(node *PersistentTreapNode[T]) error) ([]*PersistentTreapNode[T], error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rangeOverTreapPostOrderLocked(callback)
}

// rangeOverTreapPostOrderLocked is the internal implementation that assumes
// the caller already holds the write lock.
// Returns the list of dirty nodes that were modified during traversal.
func (t *PersistentTreap[T]) rangeOverTreapPostOrderLocked(callback func(node *PersistentTreapNode[T]) error) ([]*PersistentTreapNode[T], error) {
	if t.root == nil {
		return nil, nil
	}
	rootNode, ok := t.root.(*PersistentTreapNode[T])
	if !ok {
		return nil, fmt.Errorf("root is not a PersistentTreapNode")
	}
	return rangeOverPostOrder(rootNode, callback)
}

// Persist persists the entire treap to the store.
// Acquires the write lock to ensure atomic persistence.
func (t *PersistentTreap[T]) Persist() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.persistentWorkerPool != nil {
		return fmt.Errorf("cannot call Persist while another persistence operation is in progress")
	}
	t.persistentWorkerPool = newPersistWorkerPool(4)
	err := t.persistLockedTree()
	closeErr := t.persistentWorkerPool.Close()
	t.persistentWorkerPool = nil
	if closeErr != nil {
		return closeErr
	}
	return err
}

// FlushAll persists the tree and then flushes the entire subtree from memory.
// The root node remains in memory; all descendants are cleared from pointers.
func (t *PersistentTreap[T]) FlushAll() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.root == nil {
		return nil
	}

	if err := t.persistLockedTree(); err != nil {
		return err
	}

	rootNode, ok := t.root.(*PersistentTreapNode[T])
	if !ok {
		return fmt.Errorf("root is not a PersistentTreapNode")
	}
	return rootNode.FlushAll()
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
// NOTE: This method acquires a read lock. If called from within InOrderMutate callback,
// use GetInMemoryNodesLocked instead to avoid deadlock.
func (t *PersistentTreap[T]) GetInMemoryNodes() []NodeInfo[T] {
	var nodes []NodeInfo[T]
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.collectInMemoryNodesLocked(t.root, &nodes)
	return nodes
}

// GetInMemoryNodesLocked traverses the treap and collects all nodes currently in memory.
// This variant assumes the caller already holds the write lock (e.g., from InOrderMutate).
// It performs the same operation as GetInMemoryNodes but without acquiring locks.
// Use this when calling from within InOrderMutate callbacks to avoid deadlock.
func (t *PersistentTreap[T]) GetInMemoryNodesLocked() []NodeInfo[T] {
	var nodes []NodeInfo[T]
	t.collectInMemoryNodesLocked(t.root, &nodes)
	return nodes
}

// collectInMemoryNodesLocked is the internal helper that assumes the caller holds the lock.
func (t *PersistentTreap[T]) collectInMemoryNodesLocked(node TreapNodeInterface[T], nodes *[]NodeInfo[T]) {
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
		t.collectInMemoryNodesLocked(pNode.TreapNode.left, nodes)
	}

	// Check the right child without triggering a load
	if pNode.TreapNode.right != nil {
		t.collectInMemoryNodesLocked(pNode.TreapNode.right, nodes)
	}
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
	// IMPORTANT: Check the pointer directly, don't call GetLeft()/GetRight()
	// as those will reload flushed nodes from disk!
	if pNode, ok := pNode.(*PersistentTreapNode[T]); ok {
		if pNode.TreapNode.left != nil {
			count += t.countInMemoryNodes(pNode.TreapNode.left)
		}
		if pNode.TreapNode.right != nil {
			count += t.countInMemoryNodes(pNode.TreapNode.right)
		}
	}

	return count
}

// FlushOlderThan flushes all nodes that haven't been accessed since the given timestamp.
// This method first persists any unpersisted nodes, then removes them from memory
// if their last access time is older than the specified cutoff timestamp.
// Nodes can be reloaded later from disk when needed.
// Returns the number of nodes flushed and any error encountered.
func (t *PersistentTreap[T]) FlushOlderThan(cutoffTimestamp int64) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// First, persist the entire tree to ensure all nodes are saved
	if err := t.persistLockedTree(); err != nil {
		return 0, err
	}

	flushedCount := 0
	_, err := t.rangeOverTreapPostOrderLocked(func(node *PersistentTreapNode[T]) error {
		if node.GetLastAccessTime() < cutoffTimestamp {
			flushErr := node.Flush()
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

	t.mu.Lock()
	defer t.mu.Unlock()

	// First, persist the entire tree to ensure all nodes are saved
	if err := t.persistLockedTree(); err != nil {
		return 0, err
	}

	// Collect all in-memory nodes with their access times
	var nodes []*PersistentTreapNode[T]
	if _, err := t.rangeOverTreapPostOrderLocked(func(node *PersistentTreapNode[T]) error {
		nodes = append(nodes, node)
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
		return bobbob.ObjNotAllocated, nil
	}
	rootNode, ok := t.root.(PersistentTreapNodeInterface[T])
	if !ok {
		return bobbob.ObjNotAllocated, fmt.Errorf("root is not a PersistentTreapNodeInterface")
	}
	return rootNode.ObjectId()
}
