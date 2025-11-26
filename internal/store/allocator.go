package store

import (
	"container/heap"
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

// BasicAllocator is a struct to manage the allocation of space in the file
type BasicAllocator struct {
	mu       sync.Mutex
	freeList GapHeap
	end      int64
}

// NewBasicAllocator creates a new BasicAllocator for the given file.
// It initializes the allocator by seeking to the end of the file to determine
// the current file size.
func NewBasicAllocator(file *os.File) (*BasicAllocator, error) {
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return &BasicAllocator{end: offset, freeList: make(GapHeap, 0)}, nil
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
			return objId, fileOffset, nil
		}
	}
	objId := ObjectId(a.end)
	fileOffset := FileOffset(a.end)
	a.end += int64(size)
	return objId, fileOffset, nil
}

// Free to mark the space as free
func (a *BasicAllocator) Free(fileOffset FileOffset, size int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	heap.Push(&a.freeList, Gap{int64(fileOffset), int64(fileOffset) + int64(size)})
	return nil
}
