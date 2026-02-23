package treap

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

var tracePayload = os.Getenv("BOBBOB_TRACE_PAYLOAD") != ""
var debugPayload = os.Getenv("BOBBOB_DEBUG_PAYLOAD") != ""

// PersistentPayloadTreapFixedNodeSize returns the fixed node size for payload treaps
// when the payload is stored out-of-line (LateMarshaler). Returns false when the
// payload is inline and size depends on the payload value.
func PersistentPayloadTreapFixedNodeSize[P types.PersistentPayload[P]](payloadTemplate P) (int, bool) {
	if !payloadIsLateMarshaler(payloadTemplate) {
		return 0, false
	}
	var n PersistentPayloadTreapNode[any, P]
	n.payload = payloadTemplate
	return n.sizeInBytes(), true
}

func payloadIsLateMarshaler[P types.PersistentPayload[P]](payloadTemplate P) bool {
	if _, ok := any(payloadTemplate).(bobbob.LateMarshaler); ok {
		return true
	}
	if _, ok := any(&payloadTemplate).(bobbob.LateMarshaler); ok {
		return true
	}
	return false
}

// UntypedPersistentPayload and types.PersistentPayload interfaces have been moved to interfaces.go

// PersistentPayloadTreapNode represents a node in the persistent payload treap.
// It embeds the payload along with the persistent treap node functionality.
// That is when you persist this node, both the treap structure and the payload are persisted together.
type PersistentPayloadTreapNode[K any, P types.PersistentPayload[P]] struct {
	PersistentTreapNode[K]
	payload       P
	payloadLoaded bool
	// It is the responsibility of the toPayloadData/fromPayloadData methods to keep this in sync with the actual payload data on disk.
	// the deleteDependents method should also clean this up when the payload is updated or the node is deleted.
	payloadObjectId store.ObjectId // ObjectId of the payload if it was marshaled separately (e.g. via LateMarshaler), otherwise ObjNotAllocated
	payloadSize     uint32         // logical size of the payload when stored separately (0 if unknown/inline)
}

func (n PersistentPayloadTreapNode[K, P]) String() string {
	return fmt.Sprintf("PersistentPayloadTreapNode{key=%v, priority=%d, payload=%v, objectId=%d, leftObjectId=%d, rightObjectId=%d}",
		n.GetKey(), n.GetPriority(), n.payload, n.objectId, n.leftObjectId, n.rightObjectId)
}

// payloadDeleter is an optional interface payloads can implement to clean up
// any dependent ObjectIds they own before the treap frees the node itself.
// Implementors should delete any child objects they allocated.
type payloadDeleter interface {
	DeleteDependents(bobbob.Storer) error
}

// GetPayload returns the payload of the node.
func (n *PersistentPayloadTreapNode[K, P]) GetPayload() P {
	if n == nil {
		var zero P
		return zero
	}
	if !n.payloadLoaded {
		n.loadPayloadIfNeeded()
	}
	return n.payload
}

// setPayload sets the payload of the node.
// It first cleans up the old payload's dependent objects before setting the new payload.
func (n *PersistentPayloadTreapNode[K, P]) setPayload(payload P) {
	// Clean up old payload's dependent objects
	n.deletePayloadDependents()

	n.payload = payload
	n.payloadLoaded = true
	n.payloadSize = 0
	// Mark objectId as invalid so node will be re-persisted
	// CRITICAL: Do NOT call DeleteObj here! Parent nodes may still reference
	// this objectId in their leftObjectId/rightObjectId fields or on disk.
	// Deleting the object would orphan the node and cause data loss.
	if store.IsValidObjectId(n.objectId) {
		n.container.queueDelete(n.objectId)
	}
	n.objectId = bobbob.ObjNotAllocated
}

// deletePayloadDependents best-effort cleans up payload-owned objects.
// This is intentionally limited to payload-owned allocations (payloadObjectId
// and any children the payload itself owns). It does NOT attempt to free the
// key backing object or the node object; those are handled by node-level
// cleanup (DependentObjectIds + queueDelete) which preserves safe ordering
// and defers actual deletion until after persistence.
func (n *PersistentPayloadTreapNode[K, P]) deletePayloadDependents() {
	if store.IsValidObjectId(n.payloadObjectId) {
		if tracePayload {
			log.Printf("TRACE: deletePayloadDependents QUEUE nodeObj=%d oldPayloadObj=%d", n.objectId, n.payloadObjectId)
		}
		n.container.queueDelete(n.payloadObjectId)
		n.payloadObjectId = bobbob.ObjNotAllocated
	}

	if n.payloadLoaded {
		// Try value receiver first
		if deleter, ok := any(n.payload).(payloadDeleter); ok {
			if err := deleter.DeleteDependents(n.container.Store); err != nil {
				// Log but don't abort - best effort cleanup
				log.Printf("deletePayloadDependents: payload cleanup failed: %v", err)
			}
			return
		}
		// Try pointer receiver
		if deleter, ok := any(&n.payload).(payloadDeleter); ok {
			if err := deleter.DeleteDependents(n.container.Store); err != nil {
				// Log but don't abort - best effort cleanup
				log.Printf("deletePayloadDependents: payload cleanup failed: %v", err)
			}
		}
	}
}

func (n *PersistentPayloadTreapNode[K, P]) loadPayloadIfNeeded() {
	if n == nil || n.payloadLoaded {
		return
	}
	if !store.IsValidObjectId(n.payloadObjectId) {
		return
	}
	if tracePayload {
		log.Printf("TRACE: loadPayloadIfNeeded READ nodeObj=%d payloadObj=%d payloadSize=%d", n.objectId, n.payloadObjectId, n.payloadSize)
	}
	var finisher bobbob.Finisher
	if lateUnmarshalPayload, ok := any(&n.payload).(bobbob.LateUnmarshaler); ok {
		finisher = lateUnmarshalPayload.LateUnmarshal(n.payloadObjectId, int(n.payloadSize), n.container.Store)
	} else if lateUnmarshalPayload, ok := any(n.payload).(bobbob.LateUnmarshaler); ok {
		finisher = lateUnmarshalPayload.LateUnmarshal(n.payloadObjectId, int(n.payloadSize), n.container.Store)
	}
	if finisher == nil {
		return
	}
	if err := finisher(); err != nil {
		log.Printf("TRACE: loadPayloadIfNeeded FAILED nodeObj=%d payloadObj=%d err=%v", n.objectId, n.payloadObjectId, err)
		log.Printf("payload: late load failed nodeObj=%d payloadObj=%d err=%v", n.objectId, n.payloadObjectId, err)
		return
	}
	if tracePayload {
		log.Printf("TRACE: loadPayloadIfNeeded SUCCESS nodeObj=%d payloadObj=%d", n.objectId, n.payloadObjectId)
	}
	n.payloadLoaded = true
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
		n.container.queueDelete(n.objectId)
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
		n.container.queueDelete(n.objectId)
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
		tmp, err := NewPayloadFromObjectId[K, P](n.leftObjectId, n.container, n.container.Store)
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
		tmp, err := NewPayloadFromObjectId[K, P](n.rightObjectId, n.container, n.container.Store)
		if err != nil {
			return nil
		}
		n.TreapNode.right = tmp
	}
	return n.TreapNode.right
}

func (n *PersistentPayloadTreapNode[K, P]) toPayloadData() ([]byte, error) {
	if store.IsValidObjectId(n.payloadObjectId) {
		// Fast exact match (compare raw stored bytes to in-memory marshal)
		if debugPayload && n.payloadLoaded {
			if n.payloadSize <= 0 {
				return nil, fmt.Errorf("Equal passed without payloadSize set")
			}
			// When we're in a debug kind of place then check that things work as expected
			pd, err := store.ReadBytesFromObj(n.container.Store, n.payloadObjectId)
			if err != nil {
				return nil, fmt.Errorf("failed to read existing payload data from store for objectId %d: %w", n.payloadObjectId, err)
			}
			pd = pd[:n.payloadSize] // Trim to logical payload size for comparison

			md, err := n.payload.Marshal()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal current payload for comparison: %w", err)
			}
			if !bytes.Equal(pd, md) {
				return nil, fmt.Errorf("payload bytes should be equal")
			}
		}
		sizeBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(sizeBuf, n.payloadSize)

		objBytes, err := n.payloadObjectId.Marshal()
		if err != nil {
			return nil, err
		}
		return append(sizeBuf, objBytes...), nil
	}
	mp := func(lateMarshalPayload bobbob.LateMarshaler) ([]byte, error) {
		objId, size, finisher := lateMarshalPayload.LateMarshal(n.container.Store)
		// Record payload metadata before executing the finisher to avoid
		// re-entrancy or ordering races where the finisher triggers store
		// activity that expects these fields to be set.
		n.payloadObjectId = objId
		n.payloadSize = uint32(size)
		if tracePayload {
			log.Printf("TRACE: LateMarshal ASSIGN nodeObj=%d newPayloadObj=%d oldPayloadObj=(invalid) payloadSize=%d payloadType=%T", n.objectId, objId, size, n.payload)
		}
		if tracePayload {
			log.Printf("payload: LateMarshal nodeObj=%d payloadObj=%d payloadSize=%d payloadType=%T", n.objectId, objId, size, n.payload)
		}

		if err := n.SubmitWork(finisher); err != nil {
			return nil, fmt.Errorf("failed to submit late marshal finisher to worker pool: %w", err)
		}
		objBytes, err := objId.Marshal()
		if err != nil {
			return nil, err
		}
		// Prefix with 4-byte little-endian payloadSize so node always stores size
		sizeBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(sizeBuf, n.payloadSize)
		return append(sizeBuf, objBytes...), nil
	}
	if lateMarshalPayload, ok := any(n.payload).(bobbob.LateMarshaler); ok {
		return mp(lateMarshalPayload)
	}
	if lateMarshalPayload, ok := any(&n.payload).(bobbob.LateMarshaler); ok {
		return mp(lateMarshalPayload)
	}
	data, err := n.payload.Marshal()
	if err == nil && tracePayload {
		log.Printf("payload: inlineMarshal nodeObj=%d payloadLen=%d payloadType=%T", n.objectId, len(data), n.payload)
	}
	if err != nil {
		return nil, err
	}
	n.payloadSize = uint32(n.payload.SizeInBytes())
	n.payloadLoaded = true
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, n.payloadSize)
	return append(sizeBuf, data...), nil
}

func (n *PersistentPayloadTreapNode[K, P]) sizeInBytes() int {
	baseNodeSize := n.PersistentTreapNode.sizeInBytes() // Base node size
	if _, ok := any(n.payload).(bobbob.LateMarshaler); ok {
		baseNodeSize += 4                               // payloadSize uint32
		baseNodeSize += store.ObjectId(0).SizeInBytes() // Payload is an ObjectId reference
	} else if _, ok := any(&n.payload).(bobbob.LateMarshaler); ok {
		baseNodeSize += 4                               // payloadSize uint32
		baseNodeSize += store.ObjectId(0).SizeInBytes() // Payload is an ObjectId reference
	} else {
		baseNodeSize += 4
		baseNodeSize += n.payload.SizeInBytes()
	}
	return baseNodeSize
}

// ObjectId returns the ObjectId of this node in the store.
// If the node hasn't been persisted yet, it allocates a new object.
// This is needed to make sure we call the correct sizeInBytes() method for payload nodes, which may be larger than the base node size.
// NOT thread-safe: caller must hold container lock if concurrent access is possible.
func (n *PersistentPayloadTreapNode[K, P]) ObjectId() (store.ObjectId, error) {
	if n == nil {
		return bobbob.ObjNotAllocated, nil
	}
	if n.objectId < 0 {
		objId, err := n.container.Store.NewObj(n.sizeInBytes())
		if err != nil {
			return bobbob.ObjNotAllocated, err
		}
		n.objectId = objId
	}
	return n.objectId, nil
}

// Marshal overrides the Marshal method to include the payload.
func (n *PersistentPayloadTreapNode[K, P]) Marshal() ([]byte, error) {
	baseData, err := marshalTreapNodeBase(&n.PersistentTreapNode, n.ObjectId)
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

// A node may have either an inline marshaled payload or a separate payload object (if it implements LateMarshaler).
// The data therefore may be a marshalled ObjectId that we need to read from the store, or it may be the payload data itself.
// Once we know what we're dealing with, set payloadObjectId accordingly
// LateUnmarshal would ideally run in the background.
// But we don't have a good way to synchronise that it has finished loading...
func (n *PersistentPayloadTreapNode[K, P]) fromPayloadData(data []byte) error {
	if tracePayload {
		limit := min(len(data), 32)
		if limit > 0 {
			log.Printf("payload: fromPayloadData nodeObj=%d dataLen=%d prefix=%x", n.objectId, len(data), data[:limit])
		} else {
			log.Printf("payload: fromPayloadData nodeObj=%d dataLen=%d", n.objectId, len(data))
		}
	}
	parseObjId := func(payloadData []byte) (store.ObjectId, error) {
		objIdSize := store.ObjectId(0).SizeInBytes()
		if len(payloadData) < objIdSize {
			return bobbob.ObjNotAllocated, fmt.Errorf("payload region too short for objectId: got %d bytes", len(payloadData))
		}
		var objId store.ObjectId
		if err := objId.Unmarshal(payloadData[:objIdSize]); err != nil {
			return bobbob.ObjNotAllocated, fmt.Errorf("failed to unmarshal payload objectId: %w", err)
		}
		return objId, nil
	}

	// Payload region is always: [size][payload bytes] or [size][objectId] for late-marshaled payloads.
	if len(data) < 4 {
		return fmt.Errorf("payload region too short: got %d bytes", len(data))
	}

	n.payloadSize = binary.LittleEndian.Uint32(data[:4])
	payloadData := data[4:]
	setLateRef := func() error {
		objId, err := parseObjId(payloadData)
		if err != nil {
			return err
		}
		if tracePayload {
			log.Printf("payload: lateRef nodeObj=%d payloadObj=%d payloadSize=%d", n.objectId, objId, n.payloadSize)
		}
		if n.payloadObjectId == objId && n.payloadLoaded {
			return nil
		}
		n.payloadObjectId = objId
		n.payloadLoaded = false
		return nil
	}

	if _, ok := any(&n.payload).(bobbob.LateUnmarshaler); ok {
		return setLateRef()
	}
	if _, ok := any(n.payload).(bobbob.LateUnmarshaler); ok {
		return setLateRef()
	}

	if int(n.payloadSize) > len(payloadData) {
		return fmt.Errorf("payload size %d exceeds available data %d", n.payloadSize, len(payloadData))
	}
	data = payloadData[:n.payloadSize]

	// Clear any stale payloadObjectId cached in-memory (best-effort)
	if store.IsValidObjectId(n.payloadObjectId) {
		n.container.queueDelete(n.payloadObjectId)
		n.payloadObjectId = bobbob.ObjNotAllocated
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
	n.payloadLoaded = true
	return nil
}

func (n *PersistentPayloadTreapNode[K, P]) Unmarshal(data []byte) error {
	return n.unmarshal(data, n.PersistentTreapNode.container.keyTemplate)
}

// SetObjectId sets the object ID of the node.
func (n *PersistentPayloadTreapNode[K, P]) SetObjectId(id store.ObjectId) {
	n.objectId = id
}

// DependentObjectIds returns ObjectIds owned by this node that should be
// deleted when the node itself is removed. This includes any separately
// marshalled payload object and the backing object for the key (if present
// on disk). This allows the generic cleanup path in PersistentTreap to
// queue deletions without requiring special-case immediate deletes.
func (n *PersistentPayloadTreapNode[K, P]) DependentObjectIds() []store.ObjectId {
	var deps []store.ObjectId
	if n == nil {
		return deps
	}

	// If the payload was marshalled to its own object, include it.
	if store.IsValidObjectId(n.payloadObjectId) {
		deps = append(deps, n.payloadObjectId)
	}

	// If the key provides dependent ObjectIds, include them and skip fallback parsing.
	if n.key != nil {
		if depProvider, ok := n.key.(interface{ DependentObjectIds() []bobbob.ObjectId }); ok {
			for _, dep := range depProvider.DependentObjectIds() {
				if store.IsValidObjectId(dep) {
					deps = append(deps, dep)
				}
			}
			return deps
		}
	}

	// Fallback: attempt to read the node bytes to extract the key backing ObjectId.
	// This is required for key types like StringKey that store their value out-of-line.
	if store.IsValidObjectId(n.objectId) && n.container != nil && n.container.Store != nil {
		if data, err := store.ReadBytesFromObj(n.container.Store, n.objectId); err == nil {
			var keyObj store.ObjectId
			if err := keyObj.Unmarshal(data); err == nil {
				if store.IsValidObjectId(keyObj) {
					deps = append(deps, keyObj)
				}
			}
		}
	}
	return deps
}

// IsObjectIdInvalid returns true if the node's ObjectId has been invalidated (is negative).
func (n *PersistentPayloadTreapNode[K, P]) IsObjectIdInvalid() bool {
	if n == nil {
		return true
	}
	return n.objectId < 0
}

// Persist persists the node and its subtree to disk.
// persistWithLockHeld overrides base node's method to handle payload node types correctly.
// INTERNAL METHOD: Caller MUST already hold the PersistentPayloadTreap's write lock (mu.Lock()).
// Do NOT call this directly from external code!
// Use PersistentPayloadTreap.Persist() instead (it acquires the lock and is safe).
func (n *PersistentPayloadTreapNode[K, P]) persistWithLockHeld() error {
	if n == nil {
		return nil
	}
	return persistLockedTreeCommon[K](
		n,
		rangeOverPostOrder[K],
	)
}

// Persist is a convenience method that calls persistWithLockHeld.
// DEPRECATED: This is a public method on the node, which can be unsafe if used incorrectly.
// Prefer calling PersistentPayloadTreap.Persist() (on the tree) instead, which is safe.
// This exists only for test compatibility; it assumes the caller has already acquired the write lock.
func (n *PersistentPayloadTreapNode[K, P]) Persist() error {
	return n.persistWithLockHeld()
}

// persistSelf persists this node only (used by the shared persist helper).
func (n *PersistentPayloadTreapNode[K, P]) persistSelf() error {
	if n == nil {
		return nil
	}
	return n.persist()
}

// PersistentPayloadTreapInterface and PersistentPayloadNodeInterface have been moved to interfaces.go

// PersistentPayloadTreap represents a persistent treap with payloads.
type PersistentPayloadTreap[K any, P types.PersistentPayload[P]] struct {
	PersistentTreap[K]
	payloadPool sync.Pool // Pool for *PersistentPayloadTreapNode[K,P]
}

// NewPersistentPayloadTreapNode creates a new PersistentPayloadTreapNode with the given key, priority, and payload.
// NewPersistentPayloadTreapNode creates a new PersistentPayloadTreapNode with the given key, priority, payload, and parent treap.
// The store reference is obtained from the parent treap.
func NewPersistentPayloadTreapNode[K any, P types.PersistentPayload[P]](key types.PersistentKey[K], priority Priority, payload P, parent *PersistentPayloadTreap[K, P]) *PersistentPayloadTreapNode[K, P] {
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
	n.container = &parent.PersistentTreap
	// var zero P
	// n.payload = zero
	n.payload = payload
	n.payloadLoaded = true
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
	n.payloadLoaded = false
	n.payloadObjectId = bobbob.ObjNotAllocated
	n.payloadSize = 0
	n.container = nil
	t.payloadPool.Put(n)
}

// NewPersistentPayloadTreap creates a new PersistentPayloadTreap with the given comparison function and store reference.
func NewPersistentPayloadTreap[K any, P types.PersistentPayload[P]](lessFunc func(a, b K) bool, keyTemplate types.PersistentKey[K], store bobbob.Storer) *PersistentPayloadTreap[K, P] {
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

// findNodeWithAncestors finds a node by key and returns both the node and the path of ancestors.
// The path includes all ancestors from root to the target node (but NOT the target node itself).
// Returns (foundNode, ancestorPath, error).
func (t *PersistentPayloadTreap[K, P]) findNodeWithAncestors(targetKey K) (*PersistentPayloadTreapNode[K, P], []*PersistentPayloadTreapNode[K, P], error) {
	return t.findNodeWithAncestorsComplex(targetKey, nil)
}

func (t *PersistentPayloadTreap[K, P]) findNodeWithAncestorsComplex(targetKey K, callback func(TreapNodeInterface[K]) error) (*PersistentPayloadTreapNode[K, P], []*PersistentPayloadTreapNode[K, P], error) {
	path := make([]*PersistentPayloadTreapNode[K, P], 0, 32)

	var walk func(node TreapNodeInterface[K]) (*PersistentPayloadTreapNode[K, P], error)
	walk = func(node TreapNodeInterface[K]) (*PersistentPayloadTreapNode[K, P], error) {
		if node == nil || node.IsNil() {
			return nil, nil
		}

		pNode, ok := node.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return nil, fmt.Errorf("node is not a PersistentPayloadTreapNode")
		}

		// Update access time
		pNode.TouchAccessTime()

		if callback != nil {
			if err := callback(pNode); err != nil {
				return nil, err
			}
		}

		// Check if this is the target node
		if pNode.GetKey().Equals(targetKey) {
			// Found the node - current path contains ancestors (don't add this node)
			return pNode, nil
		}

		nodeKey := pNode.GetKey().Value()

		// Add this node to path and recurse
		path = append(path, pNode)

		var result *PersistentPayloadTreapNode[K, P]
		var err error

		// Recurse left or right
		if t.Less(targetKey, nodeKey) {
			result, err = walk(pNode.GetLeft())
		} else {
			result, err = walk(pNode.GetRight())
		}

		// If not found in subtree, remove this node from path (backtracking)
		if result == nil {
			path = path[:len(path)-1]
		}

		return result, err
	}

	found, err := walk(t.root)
	return found, path, err
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
		incomingPayloadNode, ok := incomingNode.(*PersistentPayloadTreapNode[K, P])
		if !ok {
			return false
		}

		payloadNode.setPayload(incomingPayloadNode.GetPayload())
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
func (t *PersistentPayloadTreap[K, P]) insertComplex(key types.PersistentKey[K], priority Priority, payload P) error {
	newNode := NewPersistentPayloadTreapNode(key, priority, payload, t)
	dirty := make([]PersistentTreapNodeInterface[K], 0, 32)
	inserted, err := t.insertTracked(t.root, newNode, &dirty)
	if err != nil {
		return err
	}
	t.root = inserted
	t.PersistentTreap.invalidateDirty(dirty)
	return nil
}

// InsertComplex inserts a new node with the given key, priority, and payload into the persistent payload treap.
// Use this method when you need to specify a custom priority value.
// If a key already exists, this will update its payload instead of creating a duplicate.
// Returns an error if insertion fails.
func (t *PersistentPayloadTreap[K, P]) InsertComplex(key types.PersistentKey[K], priority Priority, payload P) error {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	createdPool := t.PersistentTreap.beginOperationWorkerPool()
	defer t.PersistentTreap.endOperationWorkerPool(createdPool)
	return t.insertComplex(key, priority, payload)
}

// Insert inserts a new node with the given key and payload into the persistent payload treap.
// If the key implements types.PriorityProvider, its Priority() method is used;
// otherwise, a random priority is generated.
// If a key already exists, this will update its payload instead of creating a duplicate.
// This is the preferred method for most use cases.
// Returns an error if insertion fails.
func (t *PersistentPayloadTreap[K, P]) Insert(key types.PersistentKey[K], payload P) error {
	var priority Priority
	if pp, ok := any(key).(types.PriorityProvider); ok {
		priority = Priority(pp.Priority())
	} else {
		priority = randomPriority()
	}
	return t.InsertComplex(key, priority, payload)
}

// Delete removes the node with the given key and frees any dependent objects
// (key object and payload-owned objects) even when the subtree root becomes nil.
// Returns an error if deletion fails.
func (t *PersistentPayloadTreap[K, P]) Delete(key types.PersistentKey[K]) error {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	createdPool := t.PersistentTreap.beginOperationWorkerPool()
	defer t.PersistentTreap.endOperationWorkerPool(createdPool)

	var target *PersistentPayloadTreapNode[K, P]
	var targetObjectId store.ObjectId = bobbob.ObjNotAllocated
	found, _, err := t.findNodeWithAncestors(key.Value())
	if err != nil {
		return err
	}
	if found != nil {
		target = found
		// Save the objectId BEFORE deleteTracked, which may invalidate it during rotations
		targetObjectId = found.objectId
	}

	dirty := make([]PersistentTreapNodeInterface[K], 0, 32)
	deleted, err := t.deleteTracked(t.root, key.Value(), &dirty)
	if err != nil {
		return err
	}
	t.root = deleted

	// After removing from tree, delete all dependent objects if the node was actually removed
	// CRITICAL: Do this BEFORE invalidateDirty, which will set objectId to -1
	if target != nil {
		// We found this key in the first pass under the same write lock, and
		// deleteTracked completed successfully, so this node was removed.
		// Dirty nodes (including mutated ancestors/rotated nodes) are handled via
		// deleteTracked + invalidateDirty(dirty) below.

		// Temporarily restore objectId so we can discover dependent ObjectIds
		// (key backing object and payload object) and queue them for deletion.
		savedObjectId := target.objectId
		target.objectId = targetObjectId

		// Load payload if needed so DeleteDependents can be called
		target.loadPayloadIfNeeded()

		// First, perform payload-owned cleanup (may delete child objects immediately)
		target.deletePayloadDependents()

		// Queue any dependent ObjectIds discovered from the node (best-effort)
		if depIds := target.DependentObjectIds(); len(depIds) > 0 {
			for _, dep := range depIds {
				if store.IsValidObjectId(dep) {
					t.queueDelete(dep)
				}
			}
		}

		// Restore the invalidated objectId on the target node
		target.objectId = savedObjectId

		// Queue the node's own objectId for deletion (deferred until flush)
		if store.IsValidObjectId(targetObjectId) {
			t.queueDelete(targetObjectId)
		}

		// Return node to pool after cleanup
		t.releasePayloadNode(target)
	}

	// Invalidate dirty nodes from the deletion operation
	// These are nodes that were rotated during delete - their stale ObjectIds
	// will be queued and deleted after the next persist, NOT immediately.
	t.PersistentTreap.invalidateDirty(dirty)

	// Now flush any queued deletes (align with PersistentTreap.Delete semantics)
	t.PersistentTreap.flushPendingDeletes()

	return nil
}

// NewPayloadFromObjectId creates a PersistentPayloadTreapNode from the given object ID.
// Reading it in fron the store if it exuists.
func NewPayloadFromObjectId[T any, P types.PersistentPayload[P]](objId store.ObjectId, parent *PersistentTreap[T], stre bobbob.Storer) (*PersistentPayloadTreapNode[T, P], error) {
	tmp := &PersistentPayloadTreapNode[T, P]{
		PersistentTreapNode: PersistentTreapNode[T]{
			container: parent,
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

func (t *PersistentPayloadTreap[K, P]) searchLocked(key K, callback func(TreapNodeInterface[K]) error) (*PersistentPayloadTreapNode[K, P], error) {
	node, _, err := t.findNodeWithAncestorsComplex(key, callback)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// SearchComplex searches for the node with the given key in the persistent treap.
// It accepts a callback that is called when a node is accessed during the search.
// The callback receives the node that was accessed, allowing for custom operations
// such as updating access times for LRU caching or flushing stale nodes.
// This method automatically updates the lastAccessTime on each accessed node.
// The callback can return an error to abort the search.
func (t *PersistentPayloadTreap[K, P]) SearchComplex(key types.PersistentKey[K], callback func(TreapNodeInterface[K]) error) (PersistentPayloadNodeInterface[K, P], error) {
	// SearchNodeComplex can mutate the treap by re-hydrating nodes from disk, so we need a write lock here
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	node, err := t.searchLocked(key.Value(), callback)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}
	return node, nil
}

// Search searches for the node with the given key in the persistent treap.
// Uses Lock because the traversal calls GetLeft/GetRight which mutate cached node pointers.
func (t *PersistentPayloadTreap[K, P]) Search(key types.PersistentKey[K]) PersistentPayloadNodeInterface[K, P] {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	node, err := t.searchLocked(key.Value(), nil)
	if err != nil {
		return nil
	}
	if node == nil {
		return nil
	}
	return node
}

// UpdatePayload updates the payload of the node with the given key.
func (t *PersistentPayloadTreap[K, P]) UpdatePayload(key types.PersistentKey[K], newPayload P) error {
	t.PersistentTreap.mu.Lock()
	defer t.PersistentTreap.mu.Unlock()
	createdPool := t.PersistentTreap.beginOperationWorkerPool()
	defer t.PersistentTreap.endOperationWorkerPool(createdPool)

	// Find the target node and its ancestors in one traversal
	payloadNode, ancestors, err := t.findNodeWithAncestors(key.Value())
	if err != nil {
		return err
	}
	if payloadNode == nil {
		return nil // Key not found
	}

	// Update payload and mark objectIds as invalid; queue deletions for any
	// dependent objects. We DO NOT re-persist here — persistence should be
	// performed by the caller via `Persist()` when appropriate.
	payloadNode.setPayload(newPayload)

	// Invalidate ancestors' objectIds and queue any dependent objects for deletion.
	// Start at parent of the modified node and walk up to root.
	for i := len(ancestors) - 1; i >= 0; i-- {
		anc := ancestors[i]
		if anc == nil {
			continue
		}
		// If ancestor has a valid objectId, discover its dependents (best-effort)
		// and queue them for deletion before invalidating the ancestor itself.
		if store.IsValidObjectId(anc.objectId) {
			// Queue dependent object ids (includes payload object and key backing object)
			if deps := anc.DependentObjectIds(); len(deps) > 0 {
				for _, dep := range deps {
					if store.IsValidObjectId(dep) {
						t.queueDelete(dep)
					}
				}
			}
			// Queue the ancestor's own object for deletion and mark it invalid in-memory.
			t.queueDelete(anc.objectId)
			anc.objectId = bobbob.ObjNotAllocated
		}
	}
	t.PersistentTreap.flushPendingDeletes()
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
			return NewPayloadFromObjectId[K, P](node.leftObjectId, node.container, node.container.Store)
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
		return NewPayloadFromObjectId[K, P](node.rightObjectId, node.container, node.container.Store)
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
	err := persistLockedTreeCommon[K](
		rootNode,
		rangeOverPostOrderInMemory[K],
	)
	if err != nil {
		return err
	}
	// Safe to delete queued objects now that the tree is fully persisted.
	t.PersistentTreap.flushPendingDeletes()
	return nil
}

// persistBatchedLockedTree persists the entire payload treap using batched allocation and writes.
// Assumes the caller already holds the write lock.
// Groups nodes by size, allocates in batches, and writes in batches for efficiency.
// This is more efficient than persistLockedTree when persisting many nodes.
func (t *PersistentPayloadTreap[K, P]) persistBatchedLockedTree() error {
	if t.root == nil {
		return nil
	}
	rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("root is not a PersistentPayloadTreapNode")
	}

	// Use batched persist with polymorphic post-order traversal
	orphanedIds, err := persistBatchedCommon[K](
		rootNode,
	)
	if err != nil {
		return err
	}
	// Queue orphaned ObjectIds for deletion
	for _, id := range orphanedIds {
		t.PersistentTreap.queueDelete(id)
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
	if t.persistentWorkerPool != nil {
		return fmt.Errorf("cannot call Persist while another persistence operation is in progress")
	}
	t.persistentWorkerPool = newPersistWorkerPool(4)
	defer func() {
		t.persistentWorkerPool.Close()
		t.persistentWorkerPool = nil
	}()

	// Walk all nodes and persist both treap and payload
	if t.root == nil {
		return nil
	}
	rootNode, ok := t.root.(*PersistentPayloadTreapNode[K, P])
	if !ok {
		return fmt.Errorf("root is not a PersistentPayloadTreapNode")
	}

	// Use batched persist for payload nodes
	orphanedIds, err := persistBatchedCommon[K, *PersistentPayloadTreapNode[K, P]](
		rootNode,
	)
	if err != nil {
		return err
	}
	for _, id := range orphanedIds {
		t.PersistentTreap.queueDelete(id)
	}
	t.PersistentTreap.flushPendingDeletes()
	return nil
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
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rangeOverTreapPostOrderLocked(callback)
}

func (t *PersistentPayloadTreap[K, P]) rangeOverTreapPostOrderLocked(callback func(node *PersistentPayloadTreapNode[K, P]) error) ([]*PersistentPayloadTreapNode[K, P], error) {
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
	if pNode.TreapNode.left != nil && !pNode.TreapNode.left.IsNil() {
		t.collectInMemoryPayloadNodes(pNode.TreapNode.left, nodes)
	}

	// Check the right child without triggering a load
	if pNode.TreapNode.right != nil && !pNode.TreapNode.right.IsNil() {
		t.collectInMemoryPayloadNodes(pNode.TreapNode.right, nodes)
	}
}

// FlushOlderThan flushes all nodes that haven't been accessed since the given timestamp.
// This method first persists any unpersisted nodes, then removes them from memory
// if their last access time is older than the specified cutoff timestamp.
// Nodes can be reloaded later from disk when needed.
// Returns the number of nodes flushed and any error encountered.
func (t *PersistentPayloadTreap[K, P]) FlushOlderThan(cutoffTimestamp int64) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushOlderThanLocked(cutoffTimestamp)
}

// flushOlderThanLocked assumes the caller already holds t.mu.
func (t *PersistentPayloadTreap[K, P]) flushOlderThanLocked(cutoffTimestamp int64) (int, error) {
	createdPool := t.beginOperationWorkerPool()
	defer t.endOperationWorkerPool(createdPool)
	// First, persist the entire tree to ensure all nodes are saved
	if err := t.persistBatchedLockedTree(); err != nil {
		return 0, err
	}

	// In-memory only: do not load additional nodes from disk while selecting candidates.
	var nodes []PayloadNodeInfo[K, P]
	t.collectInMemoryPayloadNodes(t.root, &nodes)
	flushedCount := 0
	for _, nodeInfo := range nodes {
		if nodeInfo.Node == nil {
			continue
		}
		if nodeInfo.LastAccessTime < cutoffTimestamp {
			flushErr := nodeInfo.Node.Flush()
			if flushErr != nil {
				if !errors.Is(flushErr, errNotFullyPersisted) {
					return flushedCount, flushErr
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
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.flushOldestPercentileLocked(percentage)
}

// flushOldestPercentileLocked assumes the caller already holds t.mu.
func (t *PersistentPayloadTreap[K, P]) flushOldestPercentileLocked(percentage int) (int, error) {
	if percentage <= 0 || percentage > 100 {
		return 0, fmt.Errorf("percentage must be between 1 and 100, got %d", percentage)
	}
	createdPool := t.beginOperationWorkerPool()
	defer t.endOperationWorkerPool(createdPool)
	// First, persist the entire tree to ensure all nodes are saved
	err := t.persistBatchedLockedTree()
	if err != nil {
		return 0, err
	}

	return flushOldestPercentileCommon(
		percentage,
		func() ([]*PersistentPayloadTreapNode[K, P], error) {
			var info []PayloadNodeInfo[K, P]
			t.collectInMemoryPayloadNodes(t.root, &info)
			nodes := make([]*PersistentPayloadTreapNode[K, P], 0, len(info))
			for _, item := range info {
				if item.Node != nil {
					nodes = append(nodes, item.Node)
				}
			}
			return nodes, nil
		},
		func(node *PersistentPayloadTreapNode[K, P]) int64 {
			return node.GetLastAccessTime()
		},
		func(node *PersistentPayloadTreapNode[K, P]) error {
			return node.Flush()
		},
	)
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

func (n *PersistentPayloadTreapNode[K, P]) LateMarshal(stre bobbob.Storer) (store.ObjectId, int, bobbob.Finisher) {
	marshalled, err := n.Marshal()
	if err != nil {
		return 0, 0, func() error { return err }
	}
	size := len(marshalled)
	objId, fin := store.LateWriteNewObjFromBytes(stre, marshalled)
	return objId, size, fin
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

	finisher := func() error {
		return store.WriteBytesToObj(n.container.Store, buf, objId)
	}

	return n.SubmitWork(finisher)
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
