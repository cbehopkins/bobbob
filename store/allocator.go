package store

import (
	"container/heap"
	"errors"
	"io"
	"os"
	"sync"
)

// Allocator manages the allocation and deallocation of file space.
// It tracks which regions of a file are in use and which are free for reuse.
type Allocator interface {
	// Allocate requests a new block of the given size.
	// Returns the ObjectId, FileOffset where the block starts, and any error.
	Allocate(size int) (ObjectId, FileOffset, error)
	// Free marks the given region as available for reuse.
	Free(fileOffset FileOffset, size int) error
}

// Gap represents an unused region of file space between Start and End offsets.
type Gap struct {
	Start int64
	End   int64
}

// GapHeap is a min-heap of Gaps
type GapHeap []Gap

// Len returns the number of gaps in the heap.
func (h GapHeap) Len() int { return len(h) }

// Less reports whether the gap at index i is smaller than the gap at index j.
func (h GapHeap) Less(i, j int) bool { return h[i].End-h[i].Start < h[j].End-h[j].Start }

// Swap exchanges the gaps at indices i and j.
func (h GapHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds a gap to the heap.
func (h *GapHeap) Push(x any) {
	*h = append(*h, x.(Gap))
}

// Pop removes and returns the smallest gap from the heap.
func (h *GapHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// AllocatedRegion tracks metadata for an allocated region.
type AllocatedRegion struct {
	FileOffset FileOffset
	Size       int
}

// BasicAllocator is a struct to manage the allocation of space in the file
// It tracks both free gaps and allocated regions to support reverse lookups.
type BasicAllocator struct {
	mu           sync.Mutex
	freeList     GapHeap
	end          int64
	allocations  map[ObjectId]AllocatedRegion // Tracks allocated regions for reverse lookup
}

// NewBasicAllocator creates a new BasicAllocator for the given file.
// It initializes the allocator by seeking to the end of the file to determine
// the current file size.
func NewBasicAllocator(file *os.File) (*BasicAllocator, error) {
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return &BasicAllocator{
		end:         offset,
		freeList:    make(GapHeap, 0),
		allocations: make(map[ObjectId]AllocatedRegion),
	}, nil
}

// NewEmptyBasicAllocator creates an uninitialized BasicAllocator.
// This is used when loading from a serialized state.
func NewEmptyBasicAllocator() *BasicAllocator {
	return &BasicAllocator{
		freeList:    make(GapHeap, 0),
		allocations: make(map[ObjectId]AllocatedRegion),
	}
}

// RefreshFreeList to refresh the free list
// This is an io intensive function
func (a *BasicAllocator) RefreshFreeList(s *baseStore) error {
	return nil
}

// Allocate to request a new space
// Thread-safe for concurrent access
func (a *BasicAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var gap Gap
	if len(a.freeList) > 0 {
		gap = heap.Pop(&a.freeList).(Gap)
		if gap.End-gap.Start >= int64(size) {
			objId := ObjectId(gap.Start)
			fileOffset := FileOffset(gap.Start)
			if gap.End-gap.Start > int64(size) {
				heap.Push(&a.freeList, Gap{gap.Start + int64(size), gap.End})
			}
			// Record the allocation
			a.allocations[objId] = AllocatedRegion{FileOffset: fileOffset, Size: size}
			return objId, fileOffset, nil
		}
	}
	objId := ObjectId(a.end)
	fileOffset := FileOffset(a.end)
	a.end += int64(size)
	// Record the allocation
	a.allocations[objId] = AllocatedRegion{FileOffset: fileOffset, Size: size}
	return objId, fileOffset, nil
}

// Free to mark the space as free
func (a *BasicAllocator) Free(fileOffset FileOffset, size int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Remove from allocations tracking
	objId := ObjectId(fileOffset)
	delete(a.allocations, objId)
	
	heap.Push(&a.freeList, Gap{int64(fileOffset), int64(fileOffset) + int64(size)})
	return nil
}

// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
// Returns an error if the ObjectId is not allocated or invalid.
func (a *BasicAllocator) GetObjectInfo(objId ObjectId) (FileOffset, int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	region, found := a.allocations[objId]
	if !found {
		return 0, 0, errors.New("object not allocated")
	}
	return region.FileOffset, region.Size, nil
}

// Marshal serializes the BasicAllocator's state to a byte slice.
// Format: [end:8][freeListCount:8][gaps...][allocCount:8][allocations...]
func (a *BasicAllocator) Marshal() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Calculate size: 8 (end) + 8 (freeList count) + 16*gaps + 8 (alloc count) + 24*allocs
	// Each allocation: 8 (ObjectId) + 8 (FileOffset) + 4 (Size) = 20 bytes, but align to 24
	allocSize := len(a.allocations) * 24 // 8 bytes ObjectId + 8 bytes FileOffset + 4 bytes Size + 4 bytes padding
	size := 16 + len(a.freeList)*16 + 8 + allocSize
	data := make([]byte, size)

	offset := 0

	// Marshal end offset
	data[offset] = byte(a.end >> 56)
	data[offset+1] = byte(a.end >> 48)
	data[offset+2] = byte(a.end >> 40)
	data[offset+3] = byte(a.end >> 32)
	data[offset+4] = byte(a.end >> 24)
	data[offset+5] = byte(a.end >> 16)
	data[offset+6] = byte(a.end >> 8)
	data[offset+7] = byte(a.end)
	offset += 8

	// Marshal freeList count
	freeListCount := int64(len(a.freeList))
	data[offset] = byte(freeListCount >> 56)
	data[offset+1] = byte(freeListCount >> 48)
	data[offset+2] = byte(freeListCount >> 40)
	data[offset+3] = byte(freeListCount >> 32)
	data[offset+4] = byte(freeListCount >> 24)
	data[offset+5] = byte(freeListCount >> 16)
	data[offset+6] = byte(freeListCount >> 8)
	data[offset+7] = byte(freeListCount)
	offset += 8

	// Marshal each gap
	for _, gap := range a.freeList {
		data[offset] = byte(gap.Start >> 56)
		data[offset+1] = byte(gap.Start >> 48)
		data[offset+2] = byte(gap.Start >> 40)
		data[offset+3] = byte(gap.Start >> 32)
		data[offset+4] = byte(gap.Start >> 24)
		data[offset+5] = byte(gap.Start >> 16)
		data[offset+6] = byte(gap.Start >> 8)
		data[offset+7] = byte(gap.Start)
		offset += 8

		data[offset] = byte(gap.End >> 56)
		data[offset+1] = byte(gap.End >> 48)
		data[offset+2] = byte(gap.End >> 40)
		data[offset+3] = byte(gap.End >> 32)
		data[offset+4] = byte(gap.End >> 24)
		data[offset+5] = byte(gap.End >> 16)
		data[offset+6] = byte(gap.End >> 8)
		data[offset+7] = byte(gap.End)
		offset += 8
	}

	// Marshal allocation count
	allocCount := int64(len(a.allocations))
	data[offset] = byte(allocCount >> 56)
	data[offset+1] = byte(allocCount >> 48)
	data[offset+2] = byte(allocCount >> 40)
	data[offset+3] = byte(allocCount >> 32)
	data[offset+4] = byte(allocCount >> 24)
	data[offset+5] = byte(allocCount >> 16)
	data[offset+6] = byte(allocCount >> 8)
	data[offset+7] = byte(allocCount)
	offset += 8

	// Marshal each allocation (in arbitrary order, but deterministic is better)
	// Create a sorted slice for deterministic ordering
	ids := make([]ObjectId, 0, len(a.allocations))
	for id := range a.allocations {
		ids = append(ids, id)
	}
	// Simple insertion sort for small maps
	for i := 1; i < len(ids); i++ {
		key := ids[i]
		j := i - 1
		for j >= 0 && ids[j] > key {
			ids[j+1] = ids[j]
			j--
		}
		ids[j+1] = key
	}

	for _, id := range ids {
		region := a.allocations[id]
		// ObjectId
		data[offset] = byte(id >> 56)
		data[offset+1] = byte(id >> 48)
		data[offset+2] = byte(id >> 40)
		data[offset+3] = byte(id >> 32)
		data[offset+4] = byte(id >> 24)
		data[offset+5] = byte(id >> 16)
		data[offset+6] = byte(id >> 8)
		data[offset+7] = byte(id)
		offset += 8

		// FileOffset
		data[offset] = byte(region.FileOffset >> 56)
		data[offset+1] = byte(region.FileOffset >> 48)
		data[offset+2] = byte(region.FileOffset >> 40)
		data[offset+3] = byte(region.FileOffset >> 32)
		data[offset+4] = byte(region.FileOffset >> 24)
		data[offset+5] = byte(region.FileOffset >> 16)
		data[offset+6] = byte(region.FileOffset >> 8)
		data[offset+7] = byte(region.FileOffset)
		offset += 8

		// Size
		data[offset] = byte(region.Size >> 24)
		data[offset+1] = byte(region.Size >> 16)
		data[offset+2] = byte(region.Size >> 8)
		data[offset+3] = byte(region.Size)
		offset += 4

		// Padding (4 bytes)
		offset += 4
	}

	return data, nil
}

// Unmarshal deserializes the BasicAllocator's state from a byte slice.
func (a *BasicAllocator) Unmarshal(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(data) < 16 {
		return errors.New("invalid data length")
	}

	offset := 0

	// Unmarshal end offset
	a.end = int64(data[offset])<<56 | int64(data[offset+1])<<48 |
		int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
		int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
		int64(data[offset+6])<<8 | int64(data[offset+7])
	offset += 8

	// Unmarshal freeList count
	freeListCount := int(int64(data[offset])<<56 | int64(data[offset+1])<<48 |
		int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
		int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
		int64(data[offset+6])<<8 | int64(data[offset+7]))
	offset += 8

	// Verify we have enough data for gaps
	minLen := 16 + freeListCount*16 + 8 // end + gap count + gaps + alloc count
	if len(data) < minLen {
		// Initialize allocations map for backward compatibility with old data
		if a.allocations == nil {
			a.allocations = make(map[ObjectId]AllocatedRegion)
		}
		return errors.New("invalid data length for freeList")
	}

	// Unmarshal gaps
	a.freeList = make(GapHeap, freeListCount)
	for i := 0; i < freeListCount; i++ {
		start := int64(data[offset])<<56 | int64(data[offset+1])<<48 |
			int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
			int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
			int64(data[offset+6])<<8 | int64(data[offset+7])
		offset += 8

		end := int64(data[offset])<<56 | int64(data[offset+1])<<48 |
			int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
			int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
			int64(data[offset+6])<<8 | int64(data[offset+7])
		offset += 8

		a.freeList[i] = Gap{Start: start, End: end}
	}

	// Rebuild the heap
	heap.Init(&a.freeList)

	// Unmarshal allocation count
	allocCount := int(int64(data[offset])<<56 | int64(data[offset+1])<<48 |
		int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
		int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
		int64(data[offset+6])<<8 | int64(data[offset+7]))
	offset += 8

	// Verify we have enough data for allocations
	expectedLen := 16 + freeListCount*16 + 8 + allocCount*24
	if len(data) != expectedLen {
		// Initialize allocations map for backward compatibility with old data
		if a.allocations == nil {
			a.allocations = make(map[ObjectId]AllocatedRegion)
		}
		return errors.New("invalid data length for allocations")
	}

	// Unmarshal allocations
	a.allocations = make(map[ObjectId]AllocatedRegion, allocCount)
	for i := 0; i < allocCount; i++ {
		// ObjectId
		objId := ObjectId(int64(data[offset])<<56 | int64(data[offset+1])<<48 |
			int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
			int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
			int64(data[offset+6])<<8 | int64(data[offset+7]))
		offset += 8

		// FileOffset
		fileOffset := FileOffset(int64(data[offset])<<56 | int64(data[offset+1])<<48 |
			int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
			int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
			int64(data[offset+6])<<8 | int64(data[offset+7]))
		offset += 8

		// Size
		size := int(int(data[offset])<<24 | int(data[offset+1])<<16 |
			int(data[offset+2])<<8 | int(data[offset+3]))
		offset += 4

		// Skip padding
		offset += 4

		a.allocations[objId] = AllocatedRegion{FileOffset: fileOffset, Size: size}
	}

	return nil
}
