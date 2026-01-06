package allocator

import (
	"errors"
	"sort"
)

// AllocatorRange represents a contiguous range of ObjectIds managed by a single allocator.
// This is used for caching which allocator is responsible for which ObjectId range.
type AllocatorRange struct {
	StartObjectId int64 // inclusive
	EndObjectId   int64 // exclusive
	BlockSize     int
	Allocator     interface{} // *blockAllocator (avoid circular import)
}

// ObjectIdLookupCache is a runtime cache for fast ObjectId -> Allocator lookups.
// It maintains a sorted list of AllocatorRanges and performs binary search for lookups.
// This cache does not need to be persisted - it can be rebuilt from allocator state.
type ObjectIdLookupCache struct {
	ranges []*AllocatorRange
}

// NewObjectIdLookupCache creates an empty lookup cache.
func NewObjectIdLookupCache() *ObjectIdLookupCache {
	return &ObjectIdLookupCache{
		ranges: make([]*AllocatorRange, 0),
	}
}

// AddRange adds a new allocator range to the cache.
// This should be called when a new allocator is created.
// Panics if ranges would overlap (defensive check for bugs).
func (c *ObjectIdLookupCache) AddRange(startObjectId int64, blockSize int, allocator interface{}) error {
	endObjectId := startObjectId // Will be updated when we know the allocator's block count
	
	// Check for overlaps
	for _, r := range c.ranges {
		if !(endObjectId <= r.StartObjectId || startObjectId >= r.EndObjectId) {
			return errors.New("overlapping allocator ranges")
		}
	}

	newRange := &AllocatorRange{
		StartObjectId: startObjectId,
		EndObjectId:   endObjectId,
		BlockSize:     blockSize,
		Allocator:     allocator,
	}

	c.ranges = append(c.ranges, newRange)
	c.sortRanges()
	return nil
}

// UpdateRangeEnd updates the EndObjectId for an existing range.
// This is called when we determine the full extent of an allocator's ObjectId range.
func (c *ObjectIdLookupCache) UpdateRangeEnd(startObjectId int64, endObjectId int64) error {
	for _, r := range c.ranges {
		if r.StartObjectId == startObjectId {
			r.EndObjectId = endObjectId
			// Check for overlaps after update
			for _, other := range c.ranges {
				if other.StartObjectId == r.StartObjectId {
					continue // Skip self-comparison
				}
				// Check if they overlap (not just adjacent)
				if r.StartObjectId < other.EndObjectId && other.StartObjectId < r.EndObjectId {
					return errors.New("overlapping allocator ranges after update")
				}
			}
			return nil
		}
	}
	return errors.New("range with startObjectId not found")
}

// Lookup performs a binary search to find the allocator responsible for the given ObjectId.
// Returns the allocator (cast to *blockAllocator) and blockSize, or an error if not found.
func (c *ObjectIdLookupCache) Lookup(objectId int64) (interface{}, int, error) {
	// Binary search for the range containing this ObjectId
	idx := sort.Search(len(c.ranges), func(i int) bool {
		return c.ranges[i].StartObjectId > objectId
	})

	// Check the range immediately before the insertion point
	if idx > 0 {
		idx--
		r := c.ranges[idx]
		if objectId >= r.StartObjectId && objectId < r.EndObjectId {
			return r.Allocator, r.BlockSize, nil
		}
	}

	return nil, 0, errors.New("ObjectId not found in cache")
}

// Clear removes all ranges from the cache.
func (c *ObjectIdLookupCache) Clear() {
	c.ranges = c.ranges[:0]
}

// Len returns the number of allocator ranges in the cache.
func (c *ObjectIdLookupCache) Len() int {
	return len(c.ranges)
}

// sortRanges sorts the ranges by StartObjectId.
func (c *ObjectIdLookupCache) sortRanges() {
	sort.Slice(c.ranges, func(i, j int) bool {
		return c.ranges[i].StartObjectId < c.ranges[j].StartObjectId
	})
}
