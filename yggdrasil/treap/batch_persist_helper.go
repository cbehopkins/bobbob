package treap

import (
	"fmt"

	"github.com/cbehopkins/bobbob"
)

// batchPersistNodes persists a slice of PersistentTreapNode[T] using batched allocation and writes.
// Only the provided nodes are persisted; the rest of the tree is not traversed.
// Returns a slice of orphaned ObjectIds (to be deleted) and error if any.
func batchPersistNodes[T any](nodes []*PersistentTreapNode[T], storeInst bobbob.Storer) ([]bobbob.ObjectId, error) {
    if len(nodes) == 0 {
        return nil, nil
    }
    // Defensive: check all nodes use the same store
    for _, n := range nodes {
        if n == nil || n.GetStore() != storeInst {
            return nil, fmt.Errorf("all nodes must use the provided store")
        }
    }
    // If only one node, just persist it directly
    if len(nodes) == 1 {
        if err := nodes[0].persist(); err != nil {
            return nil, err
        }
        return nil, nil
    }
    // Group nodes by size for batching
    sizeGroups := make(map[int][]*PersistentTreapNode[T])
    for _, n := range nodes {
        sz := n.sizeInBytes()
        sizeGroups[sz] = append(sizeGroups[sz], n)
    }
    var allOrphaned []bobbob.ObjectId
    for sz, group := range sizeGroups {
        if len(group) == 0 {
            continue
        }
        // Use persistBatchedCommon logic, but with a fake root that exposes only this group
        // We need a wrapper type to satisfy persistBatchedNode interface for the group
        // We'll use the first node as the root for persistBatchedCommon, but override traversal
        // Instead, just persist all nodes in the group as a flat batch
        // For now, just allocate and write in batch
        objIds, _, allocErr := group[0].AllocateRun(sz, len(group))
        if allocErr != nil {
            return allOrphaned, fmt.Errorf("AllocateRun failed: %w", allocErr)
        }
        if len(objIds) != len(group) {
            return allOrphaned, fmt.Errorf("AllocateRun returned %d ids for %d nodes", len(objIds), len(group))
        }
        // Assign objectIds
        for i, n := range group {
            n.SetObjectId(objIds[i])
        }
        // Marshal all nodes
        marshaled := make([][]byte, len(group))
        for i, n := range group {
            data, err := n.Marshal()
            if err != nil {
                return allOrphaned, fmt.Errorf("marshal failed: %w", err)
            }
            marshaled[i] = data
        }
        // Write all nodes in batch
        // Use WriteBatchedObjs if available, else fallback to WriteToObj
        for i := range group {
            writer, finisher, err := storeInst.WriteToObj(objIds[i])
            if err != nil {
                return allOrphaned, fmt.Errorf("WriteToObj failed: %w", err)
            }
            _, err = writer.Write(marshaled[i])
            if finisher != nil {
                finisher()
            }
            if err != nil {
                return allOrphaned, fmt.Errorf("write failed: %w", err)
            }
        }
        // No orphaned ids in this simple batch path
    }
    return allOrphaned, nil
}
