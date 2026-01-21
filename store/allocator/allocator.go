package allocator

import (
	"container/heap"
	"errors"
	"io"
	"maps"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cbehopkins/bobbob/internal"
)

// ObjectId and FileOffset are aliases to internal definitions to avoid import cycles.
type ObjectId = internal.ObjectId
type FileOffset = internal.FileOffset

// Gap represents a free (unused) region in the file.
type Gap struct {
	Start int64
	End   int64
}

// Marshal interface type aliases to internal definitions.
type MarshalComplex = internal.MarshalComplex
type UnmarshalComplex = internal.UnmarshalComplex
type ObjectAndByteFunc = internal.ObjectAndByteFunc

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
	mu       sync.Mutex
	FreeList gapHeap
	End      int64
	// Allocations acts as a cache of recent mutations for reverse lookup.
	Allocations map[ObjectId]allocatedRegion
	// Persistent backing store for the full allocation table.
	backing AllocationStore
	// File handle for direct I/O operations
	file *os.File
	// Unified allocator index (objects + ranges); BasicAllocator uses objects segment.
	idx *AllocatorIndex
	// Pending deletions to flush to backing store.
	pendingDel map[ObjectId]struct{}
	// Background flush controls.
	flushTicker *time.Ticker
	stopFlush   chan struct{}
	flushDone   chan struct{}
	// OnAllocate is called after a successful allocation (for testing/monitoring).
	OnAllocate func(objId ObjectId, offset FileOffset, size int)
}

// NewBasicAllocator creates a new BasicAllocator for the given file.
// It initializes the allocator by seeking to the end of the file to determine
// the current file size.
func NewBasicAllocator(file *os.File) (*BasicAllocator, error) {
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	// Derive default paths for disk-backed metadata (alongside the data file).
	// Use the full file path to ensure uniqueness across different test directories.
	dataFilePath := file.Name()
	allocPath := dataFilePath + ".allocs.json"
	indexPath := dataFilePath + ".allocs.idx"
	return &BasicAllocator{
		End:         offset,
		FreeList:    make(gapHeap, 0),
		Allocations: make(map[ObjectId]allocatedRegion),
		backing:     NewFileAllocationStore(allocPath),
		idx:         NewAllocatorIndex(indexPath),
		file:        file,
		pendingDel:  make(map[ObjectId]struct{}),
		stopFlush:   make(chan struct{}),
	}, nil
}

// NewEmptyBasicAllocator creates an uninitialized BasicAllocator.
// This is used when loading from a serialized state.
func NewEmptyBasicAllocator() *BasicAllocator {
	return &BasicAllocator{
		FreeList:    make(gapHeap, 0),
		Allocations: make(map[ObjectId]allocatedRegion),
		backing:     NewInMemoryAllocationStore(),
		pendingDel:  make(map[ObjectId]struct{}),
		stopFlush:   make(chan struct{}),
	}
}

// SetFile sets the file handle for the allocator.
// This is used when loading from a serialized state.
func (a *BasicAllocator) SetFile(file *os.File) {
	a.file = file
}

// StartBackgroundFlush starts periodic flushing of cache entries to the backing store.
// Call StopBackgroundFlush to terminate.
func (a *BasicAllocator) StartBackgroundFlush(interval time.Duration) {
	a.mu.Lock()
	if a.flushTicker != nil {
		a.mu.Unlock()
		return
	}
	a.flushTicker = time.NewTicker(interval)
	a.flushDone = make(chan struct{})
	go a.runFlushLoop(a.flushTicker)
	a.mu.Unlock()
}

// StopBackgroundFlush stops the background flush loop.
func (a *BasicAllocator) StopBackgroundFlush() {
	a.mu.Lock()
	if a.flushTicker != nil {
		a.flushTicker.Stop()
		a.flushTicker = nil
	}
	close(a.stopFlush)
	done := a.flushDone
	a.mu.Unlock()
	// Wait for flush loop to exit to avoid racing with file deletion
	if done != nil {
		<-done
	}
	a.mu.Lock()
	a.stopFlush = make(chan struct{})
	a.flushDone = nil
	a.mu.Unlock()
}

func (a *BasicAllocator) runFlushLoop(t *time.Ticker) {
	for {
		select {
		case <-t.C:
			a.FlushCacheToBacking(0) // default: flush all
		case <-a.stopFlush:
			if a.flushDone != nil {
				close(a.flushDone)
			}
			return
		}
	}
}

// FlushCacheToBacking persists a percentage of cache entries to the backing store.
// percent == 0 flushes all; otherwise flushes approximately percent% of entries.
func (a *BasicAllocator) FlushCacheToBacking(percent int) {
	a.mu.Lock()
	// Snapshot keys to avoid holding lock during I/O
	total := len(a.Allocations)
	keys := make([]ObjectId, 0, total)
	for id := range a.Allocations {
		keys = append(keys, id)
	}
	delKeys := make([]ObjectId, 0, len(a.pendingDel))
	for id := range a.pendingDel {
		delKeys = append(delKeys, id)
	}
	a.mu.Unlock()

	// Determine how many to flush
	flushCount := total
	if percent > 0 && percent < 100 {
		flushCount = (total * percent) / 100
	}

	// Batch allocations for efficient persistence
	batch := make(map[ObjectId]allocatedRegion, flushCount)
	for i := 0; i < flushCount; i++ {
		id := keys[i]
		a.mu.Lock()
		r := a.Allocations[id]
		a.mu.Unlock()
		batch[id] = r
	}

	// Prefer unified index for persistence when available
	if a.idx != nil {
		// Batch add allocations
		a.idx.BatchAddObjects(batch)
		// Apply deletions
		for _, id := range delKeys {
			a.idx.DeleteObject(id)
		}
		// Persist to disk
		_ = a.idx.Flush()
	} else {
		// Fallback to backing store behavior
		if batchStore, ok := a.backing.(interface {
			BatchAdd(map[ObjectId]allocatedRegion) error
		}); ok {
			_ = batchStore.BatchAdd(batch)
		} else {
			for id, r := range batch {
				_ = a.backing.Add(id, r.fileOffset, r.size)
			}
		}
		for _, id := range delKeys {
			_ = a.backing.Delete(id)
		}
		if flusher, ok := a.backing.(interface{ Flush() error }); ok {
			_ = flusher.Flush()
		}
	}

	// Clear flushed entries from cache if percent==0 (flush all)
	if percent == 0 {
		a.mu.Lock()
		a.Allocations = make(map[ObjectId]allocatedRegion)
		a.pendingDel = make(map[ObjectId]struct{})
		a.mu.Unlock()
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

// SetOnAllocate registers a callback to be invoked after each successful allocation.
// Useful for testing and monitoring allocator usage patterns.
func (a *BasicAllocator) SetOnAllocate(cb func(objId ObjectId, offset FileOffset, size int)) {
	a.mu.Lock()
	a.OnAllocate = cb
	a.mu.Unlock()
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
			// Cache the allocation; background flush will persist
			a.Allocations[objId] = allocatedRegion{fileOffset: fileOffset, size: size}
			if a.OnAllocate != nil {
				a.OnAllocate(objId, fileOffset, size)
			}
			return objId, fileOffset, nil
		}
	}
	objId := ObjectId(a.End)
	fileOffset := FileOffset(a.End)
	a.End += int64(size)
	// Cache the allocation; background flush will persist
	a.Allocations[objId] = allocatedRegion{fileOffset: fileOffset, size: size}
	if a.OnAllocate != nil {
		a.OnAllocate(objId, fileOffset, size)
	}
	return objId, fileOffset, nil
}

// Free to mark the space as free
func (a *BasicAllocator) Free(fileOffset FileOffset, size int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Mark deletion in cache; background flush will persist removal
	objId := ObjectId(fileOffset)
	delete(a.Allocations, objId)
	a.pendingDel[objId] = struct{}{}

	heap.Push(&a.FreeList, Gap{int64(fileOffset), int64(fileOffset) + int64(size)})
	return nil
}

// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
// Returns an error if the ObjectId is not allocated or invalid.
func (a *BasicAllocator) GetObjectInfo(objId ObjectId) (FileOffset, int, error) {
	a.mu.Lock()
	// Check if pending deletion
	if _, deleted := a.pendingDel[objId]; deleted {
		a.mu.Unlock()
		return 0, 0, errors.New("object has been deleted")
	}
	// Check cache
	region, found := a.Allocations[objId]
	a.mu.Unlock()
	if found {
		return region.fileOffset, region.size, nil
	}
	// Check unified index first
	if a.idx != nil {
		a.mu.Lock()
		// Double-check pendingDel before returning index result
		if _, deleted := a.pendingDel[objId]; deleted {
			a.mu.Unlock()
			return 0, 0, errors.New("object has been deleted")
		}
		a.mu.Unlock()
		off, sz, err := a.idx.Get(objId)
		if err == nil {
			return off, sz, nil
		}
	}
	// Fallback to backing store
	off, sz, ok, err := a.backing.Get(objId)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, errors.New("object not allocated")
	}
	return off, sz, nil
}

// LateReadObj returns an io.Reader positioned at the object's data in the file.
// Returns a reader, a finisher function (no-op for BasicAllocator), and any error.
// This method enables reading object data without loading it entirely into memory.
func (a *BasicAllocator) LateReadObj(objId ObjectId) (io.Reader, func() error, error) {
	offset, size, err := a.GetObjectInfo(objId)
	if err != nil {
		return nil, nil, err
	}
	reader := io.NewSectionReader(a.file, int64(offset), int64(size))
	return reader, func() error { return nil }, nil
}

// Marshal serializes the BasicAllocator's state to a byte slice.
// Format: [end:8][freeListCount:8][gaps...][allocCount:8][allocations...]
func (a *BasicAllocator) Marshal() ([]byte, error) {
	a.mu.Lock()
	freeListCopy := make(gapHeap, len(a.FreeList))
	copy(freeListCopy, a.FreeList)
	endCopy := a.End
	// Snapshot cache
	cacheCopy := make(map[ObjectId]allocatedRegion, len(a.Allocations))
	maps.Copy(cacheCopy, a.Allocations)
	a.mu.Unlock()

	// Collect full allocation set = backing store merged with cache.
	full := make(map[ObjectId]allocatedRegion)
	_ = a.backing.IterateAll(func(id ObjectId, offset FileOffset, size int) bool {
		full[id] = allocatedRegion{fileOffset: offset, size: size}
		return true
	})
	maps.Copy(full, cacheCopy)

	// Calculate size
	allocSize := len(full) * 24 // 8 bytes ObjectId + 8 bytes FileOffset + 4 bytes Size + 4 bytes padding
	size := 16 + len(freeListCopy)*16 + 8 + allocSize
	data := make([]byte, size)

	offset := 0

	// Marshal end offset
	data[offset] = byte(endCopy >> 56)
	data[offset+1] = byte(endCopy >> 48)
	data[offset+2] = byte(endCopy >> 40)
	data[offset+3] = byte(endCopy >> 32)
	data[offset+4] = byte(endCopy >> 24)
	data[offset+5] = byte(endCopy >> 16)
	data[offset+6] = byte(endCopy >> 8)
	data[offset+7] = byte(endCopy)
	offset += 8

	// Marshal freeList count
	freeListCount := int64(len(freeListCopy))
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
	for _, gap := range freeListCopy {
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
	allocCount := int64(len(full))
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
	ids := make([]ObjectId, 0, len(full))
	for id := range full {
		ids = append(ids, id)
	}
	// Use standard library sort for O(n log n) performance
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	for _, id := range ids {
		region := full[id]
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
	for i := range freeListCount {
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

	// Unmarshal allocations (populate cache and backing store)
	a.Allocations = make(map[ObjectId]allocatedRegion, allocCount)
	records := make([]AllocationRecord, 0, allocCount)
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

		rec := allocatedRegion{fileOffset: fileOffset, size: size}
		a.Allocations[objId] = rec
		records = append(records, AllocationRecord{ID: objId, Offset: fileOffset, Size: size})
	}
	// Replace backing store with the unmarshaled content
	_ = a.backing.ReplaceAll(records)
	return nil
}

var AllAllocated = errors.New("no free blocks available")

// ErrNoContiguousRange indicates the allocator has free space but cannot satisfy
// the requested contiguous run length.
var ErrNoContiguousRange = errors.New("no contiguous range available")

// ErrRunUnsupported indicates the allocator cannot guarantee contiguity for run allocations.
var ErrRunUnsupported = errors.New("run allocation unsupported")
