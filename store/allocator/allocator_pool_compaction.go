package allocator

// BlockAllocatorCompactor exposes inspection helpers used for application-level
// compaction. It identifies sub-optimal block allocators for a given object
// size and lists all ObjectIds allocated in a specific allocator instance.
type BlockAllocatorCompactor interface {
	// FindSmallestBlockAllocatorForSize returns the blockSize, allocator index
	// within the pool (available + full), the allocator's blockCount, and a
	// found flag. Only allocators smaller than the current pool configuration
	// are returned.
	FindSmallestBlockAllocatorForSize(size int) (blockSize int, allocatorIndex int, blockCount int, found bool)

	// GetObjectIdsInAllocator returns all ObjectIds allocated in the specified
	// allocator (index is scoped to the pool for that blockSize).
	GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []ObjectId
}

// GetObjectIdsInAllocator returns all ObjectIds allocated in a specific allocator.
// This is useful for identifying which objects are using an allocator so they can
// be migrated to a more efficient allocator if needed.
// The allocatorIndex should be 0-indexed across both available and full lists.
// blockSize must match this pool's blockSize; mismatches return an empty list.
func (p *allocatorPool) GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []ObjectId {
	if p == nil || blockSize != p.blockSize {
		return nil
	}
	// Combine available and full lists for indexing
	var objectIds []ObjectId

	// Check if index is in available list
	if allocatorIndex < len(p.available) {
		ref := p.available[allocatorIndex]
		if ref != nil && ref.allocator != nil {
			for i, allocated := range ref.allocator.allocatedList {
				if allocated {
					objId := ref.allocator.startingObjectId + ObjectId(i)
					objectIds = append(objectIds, objId)
				}
			}
		}
		return objectIds
	}

	// Otherwise check full list
	fullIndex := allocatorIndex - len(p.available)
	if fullIndex < len(p.full) {
		ref := p.full[fullIndex]
		if ref != nil && ref.allocator != nil {
			for i, allocated := range ref.allocator.allocatedList {
				if allocated {
					objId := ref.allocator.startingObjectId + ObjectId(i)
					objectIds = append(objectIds, objId)
				}
			}
		}
		return objectIds
	}

	return objectIds
}

// FindSmallestBlockAllocatorForSize searches for the smallest (least efficient)
// block allocator that can handle the given size, excluding the current pool's
// block allocators if they match the pool configuration.
// Returns the pool's blockSize, allocator index within the combined pool
// (available + full), the blockCount of that allocator, and whether one was
// found. This identifies candidates for compaction/migration when pool config
// changes.
//
// Example usage in PersistentTreap:
//
//	if blockSize, idx, _, found := pool.FindSmallestBlockAllocatorForSize(nodeSize); found {
//	    objectIds := pool.GetObjectIdsInAllocator(blockSize, idx)
//	    // Delete these nodes from the treap
//	    for _, objId := range objectIds {
//	        treap.DeleteNodeWithObjectId(objId)
//	    }
//	    // Next persist will reallocate using the more efficient allocator
//	}
func (p *allocatorPool) FindSmallestBlockAllocatorForSize(size int) (int, int, int, bool) {
	// Only look for allocators smaller than the current pool's blockCount
	minBlockCount := p.blockCount
	var smallestIndex int
	var smallestBlockCount int
	found := false

	// Check available allocators
	for i, ref := range p.available {
		if ref == nil || ref.allocator == nil {
			continue
		}
		alloc := ref.allocator
		// Must be same blockSize and have sub-optimal blockCount
		if alloc.blockSize == p.blockSize && alloc.blockCount < minBlockCount && alloc.blockCount > 0 {
			if !found || alloc.blockCount < smallestBlockCount {
				smallestIndex = i
				smallestBlockCount = alloc.blockCount
				found = true
				minBlockCount = alloc.blockCount // Keep searching for even smaller ones
			}
		}
	}

	// Check full allocators
	for i, ref := range p.full {
		if ref == nil || ref.allocator == nil {
			continue
		}
		alloc := ref.allocator
		fullIndex := i + len(p.available)
		// Must be same blockSize and have sub-optimal blockCount
		if alloc.blockSize == p.blockSize && alloc.blockCount < minBlockCount && alloc.blockCount > 0 {
			if !found || alloc.blockCount < smallestBlockCount {
				smallestIndex = fullIndex
				smallestBlockCount = alloc.blockCount
				found = true
				minBlockCount = alloc.blockCount // Keep searching for even smaller ones
			}
		}
	}

	if found {
		return p.blockSize, smallestIndex, smallestBlockCount, true
	}
	return 0, 0, 0, false
}
