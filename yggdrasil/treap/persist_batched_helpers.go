package treap

import (
	"errors"
	"fmt"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

// persistBatchedNode extends persistSyncNode with marshaling capability for batched persist.
type persistBatchedNode[T any] interface {
	persistSyncNode[T]
	Marshal() ([]byte, error)
	sizeInBytes() int
	ObjectId() (bobbob.ObjectId, error)
	GetObjectIdNoAlloc() bobbob.ObjectId
	SetObjectId(bobbob.ObjectId)
	SubmitWork(func() error) error
	AllocateRun(size, count int) ([]bobbob.ObjectId, []store.FileOffset, error)
	GetObjectInfo(bobbob.ObjectId) (store.ObjectInfo, bool)
}

// persistBatchedCommon attempts batched persistence for efficiency.
//
// Strategy:
//  1. Collect all nodes that need persisting (objectId < 0).
//  2. If all nodes are the same size, attempt AllocateRun for consecutive allocation.
//  3. If AllocateRun succeeds, use batched write path (WriteBatchedObjs).
//  4. If batching is not possible (different sizes or AllocateRun fails),
//     fallback to persistLockedTreeCommon which handles individual persistence.
//
// Returns: list of ObjectIds that were invalidated (queued for deletion).
func persistBatchedCommon[T any, N persistBatchedNode[T]](
	root N,
) ([]bobbob.ObjectId, error) {
	if any(root) == nil || root.IsNil() {
		return nil, nil
	}

	// Get store reference early
	storeInst := root.GetStore()
	if storeInst == nil {
		return nil, fmt.Errorf("store is nil")
	}

	// Phase 1: Collect nodes in post-order and track invalidations.
	var allNodes []N
	var orphanedIds []bobbob.ObjectId
	queuedDeletes := make(map[bobbob.ObjectId]struct{})

	invalidateNode := func(node N) {
		oldObjId := node.GetObjectIdNoAlloc()
		if store.IsValidObjectId(oldObjId) {
			if _, seen := queuedDeletes[oldObjId]; !seen {
				queuedDeletes[oldObjId] = struct{}{}
				orphanedIds = append(orphanedIds, oldObjId)
			}
			node.SetObjectId(bobbob.ObjNotAllocated)
		}
	}

	invalidateAncestors := func(path []N) {
		if len(path) <= 1 {
			return
		}
		for i := len(path) - 2; i >= 0; i-- {
			ancestor := path[i]
			invalidateNode(ancestor)
		}
	}

	// Custom post-order traversal that keeps an ancestor path (in-memory only).
	type frame struct {
		node    N
		visited bool
	}
	stack := []frame{{node: root, visited: false}}
	var path []N

	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if any(f.node) == nil || f.node.IsNil() {
			continue
		}

		if f.visited {
			// Post-order visit
			allNodes = append(allNodes, f.node)

			// Defensive mismatch checks (should not happen in correct mutation logic).
			leftChild := f.node.GetLeftChild()
			if leftChild != nil && !leftChild.IsNil() {
				leftNode, ok := leftChild.(persistBatchedNode[T])
				if !ok {
					return nil, fmt.Errorf("left child is not a persistent batched node")
				}
				leftObjId := leftNode.GetObjectIdNoAlloc()
				if store.IsValidObjectId(leftObjId) && leftObjId != f.node.getLeftObjectId() {
					f.node.setLeftObjectId(leftObjId)
					invalidateNode(f.node)
					invalidateAncestors(path)
				}
			}

			rightChild := f.node.GetRightChild()
			if rightChild != nil && !rightChild.IsNil() {
				rightNode, ok := rightChild.(persistBatchedNode[T])
				if !ok {
					return nil, fmt.Errorf("right child is not a persistent batched node")
				}
				rightObjId := rightNode.GetObjectIdNoAlloc()
				if store.IsValidObjectId(rightObjId) && rightObjId != f.node.getRightObjectId() {
					f.node.setRightObjectId(rightObjId)
					invalidateNode(f.node)
					invalidateAncestors(path)
				}
			}

			// If this node is already invalid, cascade invalidation upward.
			if !store.IsValidObjectId(f.node.GetObjectIdNoAlloc()) {
				invalidateAncestors(path)
			}

			if len(path) > 0 {
				path = path[:len(path)-1]
			}
			continue
		}

		// Pre-visit: push children then node for post-order.
		path = append(path, f.node)
		stack = append(stack, frame{node: f.node, visited: true})

		rightChild := f.node.GetRightChild()
		if rightChild != nil && !rightChild.IsNil() {
			rightNode, ok := rightChild.(N)
			if !ok {
				return nil, fmt.Errorf("right child is not expected node type")
			}
			stack = append(stack, frame{node: rightNode, visited: false})
		}

		leftChild := f.node.GetLeftChild()
		if leftChild != nil && !leftChild.IsNil() {
			leftNode, ok := leftChild.(N)
			if !ok {
				return nil, fmt.Errorf("left child is not expected node type")
			}
			stack = append(stack, frame{node: leftNode, visited: false})
		}
	}

	if len(allNodes) == 0 {
		return nil, nil
	}

	// Phase 2: Identify unpersisted nodes
	var nodesToPersist []N
	for _, node := range allNodes {
		if !store.IsValidObjectId(node.GetObjectIdNoAlloc()) {
			nodesToPersist = append(nodesToPersist, node)
		}
	}

	if len(nodesToPersist) == 0 {
		return orphanedIds, nil
	}

	// Phase 3: Decide batching strategy
	// Check if all nodes are the same size (required for AllocateRun)
	firstSize := nodesToPersist[0].sizeInBytes()
	allSameSize := true
	for _, node := range nodesToPersist {
		if node.sizeInBytes() != firstSize {
			allSameSize = false
			break
		}
	}

	// If nodes are different sizes, we can't use AllocateRun (requires uniform size).
	// Fallback to individual persist path via persistLockedTreeCommon.
	if !allSameSize {
		err := persistLockedTreeCommon[T, N](
			root,
			rangeOverPostOrder[T, N],
		)
		return orphanedIds, err
	}

	// Phase 4: Batched path - AllocateRun and WriteBatchedObjs
	// Process in chunks to avoid excessive memory usage
	const maxBatchSize = 1024
	remaining := nodesToPersist

	for len(remaining) > 0 {
		batchSize := min(len(remaining), maxBatchSize)
		requestBatch := remaining[:batchSize]
		remaining = remaining[batchSize:]

		var err error
		remaining, err = persistBatchedProcessBatch(
			root,
			storeInst,
			firstSize,
			requestBatch,
			remaining,
		)
		if err != nil {
			return nil, err
		}
	}

	return orphanedIds, nil
}

func persistBatchedProcessBatch[T any, N persistBatchedNode[T]](
	root N,
	storeInst bobbob.Storer,
	objSize int,
	requestBatch []N,
	remaining []N,
) ([]N, error) {
	// Attempt consecutive allocation using AllocateRun
	// Like io.Writer.Write(), AllocateRun may return fewer objects than requested.
	// This is normal behavior, not an error - process what we got and continue.
	objIds, _, allocErr := root.AllocateRun(objSize, len(requestBatch))
	var batch []N

	if allocErr != nil {
		return nil, fmt.Errorf("AllocateRun failed: %w", allocErr)
	}

	if len(objIds) == 0 {
		return nil, errors.New("AllocateRun returned zero ObjectIds")
	}
	// AllocateRun succeeded (full or partial) - process what we got
	batch = requestBatch[:len(objIds)]
	for i := range batch {
		batch[i].SetObjectId(objIds[i])
	}

	// Put unallocated nodes back for next iteration (like continuing after partial write)
	if len(objIds) < len(requestBatch) {
		remaining = append(requestBatch[len(objIds):], remaining...)
	}

	// Sync cached child ObjectIds
	for _, node := range batch {
		leftChild := node.GetLeftChild()
		if leftChild != nil && !leftChild.IsNil() {
			leftNode, ok := leftChild.(persistBatchedNode[T])
			if !ok {
				return nil, fmt.Errorf("left child is not a persistent batched node")
			}
			leftObjId := leftNode.GetObjectIdNoAlloc()
			if store.IsValidObjectId(leftObjId) && leftObjId != node.getLeftObjectId() {
				node.setLeftObjectId(leftObjId)
			}

		}

		rightChild := node.GetRightChild()
		if rightChild != nil && !rightChild.IsNil() {
			rightNode, ok := rightChild.(persistBatchedNode[T])
			if !ok {
				return nil, fmt.Errorf("right child is not a persistent batched node")
			}
			rightObjId := rightNode.GetObjectIdNoAlloc()
			if store.IsValidObjectId(rightObjId) && rightObjId != node.getRightObjectId() {
				node.setRightObjectId(rightObjId)
			}

		}
	}

	// Marshal batch
	marshaledData := make([][]byte, len(batch))
	batchObjIds := make([]bobbob.ObjectId, len(batch))
	for i, node := range batch {
		data, mErr := node.Marshal()
		if mErr != nil {
			return nil, fmt.Errorf("failed to marshal node %d: %w", i, mErr)
		}
		marshaledData[i] = data
		batchObjIds[i] = node.GetObjectIdNoAlloc()
		if !store.IsValidObjectId(batchObjIds[i]) {
			return nil, fmt.Errorf("node %d has invalid ObjectId after allocation", i)
		}
	}

	allocSizes := make([]int, len(batchObjIds))
	allocTotal := 0
	for i, objId := range batchObjIds {
		info, found := root.GetObjectInfo(objId)
		if !found || info.Size <= 0 {
			return nil, errors.New("invalid object info or size")
		}
		if info.Size < len(marshaledData[i]) {
			return nil, fmt.Errorf("object %d allocated size %d smaller than marshaled size %d",
				objId, info.Size, len(marshaledData[i]))
		}
		allocSizes[i] = info.Size
		allocTotal += info.Size
	}

	// Build combined buffer with padding
	combinedData := make([]byte, allocTotal)
	offset := 0
	for i, data := range marshaledData {
		copy(combinedData[offset:], data)
		offset += allocSizes[i]
	}

	// Submit batched write asynchronously
	if wErr := root.SubmitWork(func() error {
		return storeInst.WriteBatchedObjs(batchObjIds, combinedData, allocSizes)
	}); wErr != nil {
		return nil, wErr
	}

	return remaining, nil
}
