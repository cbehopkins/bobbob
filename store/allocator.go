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

// NewEmptyBasicAllocator creates an uninitialized BasicAllocator.
// This is used when loading from a serialized state.
func NewEmptyBasicAllocator() *BasicAllocator {
	return &BasicAllocator{freeList: make(GapHeap, 0)}
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

// Marshal serializes the BasicAllocator's state to a byte slice.
// Format: [end:8 bytes][freeListCount:8 bytes][gap1.Start:8][gap1.End:8]...
func (a *BasicAllocator) Marshal() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Calculate size: 8 bytes for end, 8 bytes for count, 16 bytes per gap
	size := 16 + len(a.freeList)*16
	data := make([]byte, size)

	// Marshal end offset
	offset := 0
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

	// Verify we have enough data
	expectedLen := 16 + freeListCount*16
	if len(data) != expectedLen {
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

	return nil
}
