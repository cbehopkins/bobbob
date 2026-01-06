package allocator

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// ObjectId is an identifier unique within the store for an object.
type ObjectId int64

// FileOffset represents a byte offset within a file.
type FileOffset int64

// SizeInBytes returns the number of bytes required to marshal this ObjectId.
// It must satisfy the PersistentKey interface.
func (id ObjectId) SizeInBytes() int {
	return 8
}

// Equals reports whether this ObjectId equals another.
func (id ObjectId) Equals(other ObjectId) bool {
	return id == other
}

// Marshal converts the ObjectId into a fixed length bytes encoding
func (id ObjectId) Marshal() ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(id))
	return buf, nil
}

// Unmarshal converts a fixed length bytes encoding into an ObjectId
func (id *ObjectId) Unmarshal(data []byte) error {
	if len(data) < 8 {
		return errors.New("invalid data length for ObjectId")
	}
	*id = ObjectId(binary.LittleEndian.Uint64(data[:8]))
	return nil
}

// PreMarshal returns the sizes of sub-objects needed to store the ObjectId.
// For ObjectId, this is a single 8-byte value.
func (id ObjectId) PreMarshal() []int {
	return []int{8}
}

// Allocator manages the allocation and deallocation of file space.
// It tracks which regions of a file are in use and which are free for reuse.
type Allocator interface {
	// Allocate requests a new block of the given size.
	// Returns the ObjectId, FileOffset where the block starts, and any error.
	Allocate(size int) (ObjectId, FileOffset, error)
	// Free marks the given region as available for reuse.
	Free(fileOffset FileOffset, size int) error
}

// RunAllocator extends Allocator with the ability to reserve a contiguous run
// of objects of the same size. Implementations should return ErrNoContiguousRange
// when contiguous space cannot be found and ErrRunUnsupported if the allocator
// cannot guarantee contiguity.
type RunAllocator interface {
	AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error)
}

// Gap represents an unused region of file space between Start and End offsets.
type Gap struct {
	Start int64
	End   int64
}

// gapHeap is a min-heap of Gaps (internal use only)
type gapHeap []Gap

// Len returns the number of gaps in the heap.
func (h gapHeap) Len() int { return len(h) }

// Less reports whether the gap at index i is smaller than the gap at index j.
func (h gapHeap) Less(i, j int) bool { return h[i].End-h[i].Start < h[j].End-h[j].Start }

// Swap exchanges the gaps at indices i and j.
func (h gapHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds a gap to the heap.
func (h *gapHeap) Push(x any) {
	*h = append(*h, x.(Gap))
}

// Pop removes and returns the smallest gap from the heap.
func (h *gapHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// allocatedRegion tracks metadata for an allocated region (internal use only)
type allocatedRegion struct {
	fileOffset FileOffset
	size       int
}

// BasicAllocator is a struct to manage the allocation of space in the file
// It tracks both free gaps and allocated regions to support reverse lookups.
type BasicAllocator struct {
	mu          sync.Mutex
	FreeList    gapHeap
	End         int64
	Allocations map[ObjectId]allocatedRegion // Tracks allocated regions for reverse lookup
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
		End:         offset,
		FreeList:    make(gapHeap, 0),
		Allocations: make(map[ObjectId]allocatedRegion),
	}, nil
}

// NewEmptyBasicAllocator creates an uninitialized BasicAllocator.
// This is used when loading from a serialized state.
func NewEmptyBasicAllocator() *BasicAllocator {
	return &BasicAllocator{
		FreeList:    make(gapHeap, 0),
		Allocations: make(map[ObjectId]allocatedRegion),
	}
}

// RefreshFreeListFromGaps populates the free list from a channel of gaps.
// This is typically called when loading a store to rebuild the free list.
func (a *BasicAllocator) RefreshFreeListFromGaps(gaps <-chan Gap) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.FreeList = make(gapHeap, 0)
	for gap := range gaps {
		a.FreeList = append(a.FreeList, gap)
	}
	heap.Init(&a.FreeList)
	return nil
}

// Allocate to request a new space
// Thread-safe for concurrent access
func (a *BasicAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var gap Gap
	if len(a.FreeList) > 0 {
		gap = heap.Pop(&a.FreeList).(Gap)
		if gap.End-gap.Start >= int64(size) {
			objId := ObjectId(gap.Start)
			fileOffset := FileOffset(gap.Start)
			if gap.End-gap.Start > int64(size) {
				heap.Push(&a.FreeList, Gap{gap.Start + int64(size), gap.End})
			}
			// Record the allocation
			a.Allocations[objId] = allocatedRegion{fileOffset: fileOffset, size: size}
			return objId, fileOffset, nil
		}
	}
	objId := ObjectId(a.End)
	fileOffset := FileOffset(a.End)
	a.End += int64(size)
	// Record the allocation
	a.Allocations[objId] = allocatedRegion{fileOffset: fileOffset, size: size}
	return objId, fileOffset, nil
}

// Free to mark the space as free
func (a *BasicAllocator) Free(fileOffset FileOffset, size int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Remove from allocations tracking
	objId := ObjectId(fileOffset)
	delete(a.Allocations, objId)

	heap.Push(&a.FreeList, Gap{int64(fileOffset), int64(fileOffset) + int64(size)})
	return nil
}

// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
// Returns an error if the ObjectId is not allocated or invalid.
func (a *BasicAllocator) GetObjectInfo(objId ObjectId) (FileOffset, int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	region, found := a.Allocations[objId]
	if !found {
		return 0, 0, errors.New("object not allocated")
	}
	return region.fileOffset, region.size, nil
}

// Marshal serializes the BasicAllocator's state to a byte slice.
// Format: [end:8][freeListCount:8][gaps...][allocCount:8][allocations...]
func (a *BasicAllocator) Marshal() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Calculate size
	allocSize := len(a.Allocations) * 24 // 8 bytes ObjectId + 8 bytes FileOffset + 4 bytes Size + 4 bytes padding
	size := 16 + len(a.FreeList)*16 + 8 + allocSize
	data := make([]byte, size)

	offset := 0

	// Marshal end offset
	data[offset] = byte(a.End >> 56)
	data[offset+1] = byte(a.End >> 48)
	data[offset+2] = byte(a.End >> 40)
	data[offset+3] = byte(a.End >> 32)
	data[offset+4] = byte(a.End >> 24)
	data[offset+5] = byte(a.End >> 16)
	data[offset+6] = byte(a.End >> 8)
	data[offset+7] = byte(a.End)
	offset += 8

	// Marshal freeList count
	freeListCount := int64(len(a.FreeList))
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
	for _, gap := range a.FreeList {
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
	allocCount := int64(len(a.Allocations))
	data[offset] = byte(allocCount >> 56)
	data[offset+1] = byte(allocCount >> 48)
	data[offset+2] = byte(allocCount >> 40)
	data[offset+3] = byte(allocCount >> 32)
	data[offset+4] = byte(allocCount >> 24)
	data[offset+5] = byte(allocCount >> 16)
	data[offset+6] = byte(allocCount >> 8)
	data[offset+7] = byte(allocCount)
	offset += 8

	// Create a sorted slice for deterministic ordering
	ids := make([]ObjectId, 0, len(a.Allocations))
	for id := range a.Allocations {
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
		region := a.Allocations[id]
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
		data[offset] = byte(region.fileOffset >> 56)
		data[offset+1] = byte(region.fileOffset >> 48)
		data[offset+2] = byte(region.fileOffset >> 40)
		data[offset+3] = byte(region.fileOffset >> 32)
		data[offset+4] = byte(region.fileOffset >> 24)
		data[offset+5] = byte(region.fileOffset >> 16)
		data[offset+6] = byte(region.fileOffset >> 8)
		data[offset+7] = byte(region.fileOffset)
		offset += 8

		// Size
		data[offset] = byte(region.size >> 24)
		data[offset+1] = byte(region.size >> 16)
		data[offset+2] = byte(region.size >> 8)
		data[offset+3] = byte(region.size)
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
	a.End = int64(data[offset])<<56 | int64(data[offset+1])<<48 |
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
		if a.Allocations == nil {
			a.Allocations = make(map[ObjectId]allocatedRegion)
		}
		return errors.New("invalid data length for freeList")
	}

	// Unmarshal gaps
	a.FreeList = make(gapHeap, freeListCount)
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

		a.FreeList[i] = Gap{Start: start, End: end}
	}

	// Rebuild the heap
	heap.Init(&a.FreeList)

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
		if a.Allocations == nil {
			a.Allocations = make(map[ObjectId]allocatedRegion)
		}
		return errors.New("invalid data length for allocations")
	}

	// Unmarshal allocations
	a.Allocations = make(map[ObjectId]allocatedRegion, allocCount)
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

		a.Allocations[objId] = allocatedRegion{fileOffset: fileOffset, size: size}
	}

	return nil
}

var AllAllocated = errors.New("no free blocks available")
// ErrNoContiguousRange indicates the allocator has free space but cannot satisfy
// the requested contiguous run length.
var ErrNoContiguousRange = errors.New("no contiguous range available")

// ErrRunUnsupported indicates the allocator cannot guarantee contiguity for run allocations.
var ErrRunUnsupported = errors.New("run allocation unsupported")
