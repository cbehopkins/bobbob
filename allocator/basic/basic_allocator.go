package basic

import (
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"sync"

	"github.com/cbehopkins/bobbob/allocator/types"
)

var (
	ErrInvalidObjectId  = errors.New("invalid ObjectId")
	ErrInsufficientData = errors.New("insufficient data for unmarshal")
)

const (
	minGapSize = 4 // Minimum gap size to track (bytes)
)

// Gap represents a free region in the file.
type Gap struct {
	FileOffset types.FileOffset
	Size       types.FileSize
}

// FreeList manages gaps sorted by FileOffset for efficient coalescing.
type FreeList struct {
	gaps []Gap // sorted by FileOffset
}

// NewFreeList creates a new empty FreeList.
func NewFreeList() *FreeList {
	return &FreeList{
		gaps: make([]Gap, 0),
	}
}

// Insert adds a gap to the list and coalesces with adjacent gaps.
func (fl *FreeList) Insert(gap Gap) {
	if gap.Size < minGapSize {
		// Too small to track
		return
	}

	// Binary search to find insertion point
	idx := sort.Search(len(fl.gaps), func(i int) bool {
		return fl.gaps[i].FileOffset > gap.FileOffset
	})

	// Insert gap at idx
	fl.gaps = append(fl.gaps, Gap{})
	copy(fl.gaps[idx+1:], fl.gaps[idx:])
	fl.gaps[idx] = gap

	// Coalesce with adjacent gaps
	fl.coalesce(idx)
}

// coalesce merges gap at idx with adjacent gaps if they're contiguous.
func (fl *FreeList) coalesce(idx int) {
	// Coalesce with next gap
	if idx+1 < len(fl.gaps) {
		current := &fl.gaps[idx]
		next := fl.gaps[idx+1]

		if current.FileOffset+types.FileOffset(current.Size) == next.FileOffset {
			// Merge with next
			current.Size += next.Size
			fl.gaps = append(fl.gaps[:idx+1], fl.gaps[idx+2:]...)
		}
	}

	// Coalesce with previous gap
	if idx > 0 {
		prev := &fl.gaps[idx-1]
		current := fl.gaps[idx]

		if prev.FileOffset+types.FileOffset(prev.Size) == current.FileOffset {
			// Merge with previous
			prev.Size += current.Size
			fl.gaps = append(fl.gaps[:idx], fl.gaps[idx+1:]...)
		}
	}
}

// FindGap searches for a gap that can fit the requested size.
// Uses FirstFit strategy. Returns gap index and true if found.
func (fl *FreeList) FindGap(size types.FileSize) (int, bool) {
	for i, gap := range fl.gaps {
		if gap.Size >= size {
			return i, true
		}
	}
	return -1, false
}

// Remove removes a gap at the given index.
func (fl *FreeList) Remove(idx int) Gap {
	gap := fl.gaps[idx]
	fl.gaps = append(fl.gaps[:idx], fl.gaps[idx+1:]...)
	return gap
}

// BasicAllocator is the lowest-level allocator.
// ObjectId always equals FileOffset (1:1 mapping).
type BasicAllocator struct {
	mu         sync.RWMutex
	file       *os.File

	// objectTracking: tracks object locations and sizes
	// Uses objectTracker for abstraction layer that allows future optimization
	objectTracking *objectTracker

	// FreeList: not persisted, reconstructed on load
	freeList *FreeList

	// Current end of file
	fileLength types.FileOffset

	// Optional callback fired on each allocation
	onAllocate func(types.ObjectId, types.FileOffset, int)
}

// ReservePrefix sets the initial file length/ObjectId cursor. This is used at
// allocator bootstrap time to skip over reserved regions (e.g. PrimeTable).
// It only moves the cursor forward; it never shrinks it.
func (ba *BasicAllocator) ReservePrefix(offset types.FileOffset) {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	if offset > ba.fileLength {
		ba.fileLength = offset
	}
}

// RemoveGapsBefore removes all gaps that start before the given offset.
// This is used after loading to prevent allocations from using reserved regions.
func (ba *BasicAllocator) RemoveGapsBefore(offset types.FileOffset) {
	filtered := make([]Gap, 0, len(ba.freeList.gaps))
	for _, gap := range ba.freeList.gaps {
		if gap.FileOffset >= offset {
			filtered = append(filtered, gap)
		}
	}
	ba.freeList.gaps = filtered
}

// New creates a new BasicAllocator for a file.
func New(file *os.File) (*BasicAllocator, error) {
	if file == nil {
		return nil, errors.New("file cannot be nil")
	}

	// Get initial file size
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &BasicAllocator{
		file:           file,
		objectTracking: newObjectTracker(),
		freeList:       NewFreeList(),
		fileLength:     types.FileOffset(stat.Size()),
	}, nil
}

// Allocate allocates a new object of the given size.
// Returns ObjectId (which equals FileOffset) and FileOffset.
func (ba *BasicAllocator) Allocate(size int) (types.ObjectId, types.FileOffset, error) {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	fileSize := types.FileSize(size)

	// Try to find a gap in FreeList
	if idx, found := ba.freeList.FindGap(fileSize); found {
		gap := ba.freeList.Remove(idx)

		// Use this gap
		objId := types.ObjectId(gap.FileOffset)
		offset := gap.FileOffset

		// If gap is larger, return remainder to FreeList
		if gap.Size > fileSize {
			remainder := Gap{
				FileOffset: offset + types.FileOffset(fileSize),
				Size:       gap.Size - fileSize,
			}
			ba.freeList.Insert(remainder)
		}

		// Record in object tracking
		ba.objectTracking.Set(objId, fileSize)

		// Fire callback
		if ba.onAllocate != nil {
			ba.onAllocate(objId, offset, size)
		}

		return objId, offset, nil
	}

	// No gap found - append to end of file
	objId := types.ObjectId(ba.fileLength)
	offset := ba.fileLength

	ba.objectTracking.Set(objId, fileSize)
	ba.fileLength += types.FileOffset(fileSize)

	// Fire callback
	if ba.onAllocate != nil {
		ba.onAllocate(objId, offset, size)
	}

	return objId, offset, nil
}

// AllocateRun is not supported by BasicAllocator.
// BasicAllocator manages variable-size allocations and cannot efficiently allocate contiguous runs.
// Use PoolAllocator (via OmniAllocator) for fixed-size run allocations.
func (ba *BasicAllocator) AllocateRun(size int, count int) ([]types.ObjectId, []types.FileOffset, error) {
	return nil, nil, types.ErrUnsupported
}

// DeleteObj frees an object by ObjectId.
func (ba *BasicAllocator) DeleteObj(objId types.ObjectId) error {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	size, exists := ba.objectTracking.Get(objId)
	if !exists {
		return ErrInvalidObjectId
	}

	offset := types.FileOffset(objId) // ObjectId == FileOffset

	// Remove from object tracking
	ba.objectTracking.Delete(objId)

	// Special case: if this is the last object in the file, truncate
	if offset+types.FileOffset(size) == ba.fileLength {
		// Find new file length by checking for objects at end
		newLength := types.FileOffset(0)

		// Scan to find highest allocated object
		ba.objectTracking.ForEach(func(allocObjId types.ObjectId, allocSize types.FileSize) {
			allocOffset := types.FileOffset(allocObjId)
			allocEnd := allocOffset + types.FileOffset(allocSize)
			if allocEnd > newLength {
				newLength = allocEnd
			}
		})

		ba.fileLength = newLength

		// Remove any gaps beyond new file length
		ba.removeGapsBeyond(newLength)

		// Truncate the actual file
		if err := ba.file.Truncate(int64(ba.fileLength)); err != nil {
			return err
		}

		return nil
	}

	// Add gap to FreeList
	gap := Gap{
		FileOffset: offset,
		Size:       size,
	}
	ba.freeList.Insert(gap)

	return nil
}

// removeGapsBeyond removes all gaps at or beyond the specified offset.
func (ba *BasicAllocator) removeGapsBeyond(offset types.FileOffset) {
	filtered := make([]Gap, 0, len(ba.freeList.gaps))
	for _, gap := range ba.freeList.gaps {
		if gap.FileOffset < offset {
			filtered = append(filtered, gap)
		}
	}
	ba.freeList.gaps = filtered
}

// GetObjectInfo returns the FileOffset and Size for an ObjectId.
func (ba *BasicAllocator) GetObjectInfo(objId types.ObjectId) (types.FileOffset, types.FileSize, error) {
	ba.mu.RLock()
	defer ba.mu.RUnlock()

	size, exists := ba.objectTracking.Get(objId)
	if !exists {
		return 0, 0, ErrInvalidObjectId
	}

	offset := types.FileOffset(objId) // ObjectId == FileOffset
	return offset, size, nil
}

// GetFile returns the file handle.
func (ba *BasicAllocator) GetFile() *os.File {
	return ba.file
}

// FileLength returns the current end-of-file position.
// Used by Top.Save() to know where to append BasicAllocator's own marshaled data.
func (ba *BasicAllocator) FileLength() types.FileOffset {
	return ba.fileLength
}

// ContainsObjectId returns true if this allocator owns the ObjectId.
func (ba *BasicAllocator) ContainsObjectId(objId types.ObjectId) bool {
	ba.mu.RLock()
	defer ba.mu.RUnlock()

	return ba.objectTracking.Contains(objId)
}

// GetObjectCount returns the number of currently tracked objects.
// Used primarily for testing.
func (ba *BasicAllocator) GetObjectCount() int {
	ba.mu.RLock()
	defer ba.mu.RUnlock()

	return ba.objectTracking.Len()
}

// SetOnAllocate registers a callback invoked after each allocation.
func (ba *BasicAllocator) SetOnAllocate(callback func(types.ObjectId, types.FileOffset, int)) {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	ba.onAllocate = callback
}

// Marshal serializes the allocator state to bytes.
// Format: fileLength (8) | objectTracker marshaled data
// Note: FreeList is NOT persisted - it's reconstructed on load.
func (ba *BasicAllocator) Marshal() ([]byte, error) {
	ba.mu.RLock()
	defer ba.mu.RUnlock()

	// Get marshaled object tracker data
	trackerData, err := ba.objectTracking.Marshal()
	if err != nil {
		return nil, err
	}

	// Create buffer: fileLength (8) + tracker data
	totalSize := 8 + len(trackerData)
	data := make([]byte, totalSize)

	// Write file length
	binary.LittleEndian.PutUint64(data[0:8], uint64(ba.fileLength))

	// Write tracker data
	copy(data[8:], trackerData)

	return data, nil
}

// Unmarshal restores allocator state from bytes and reconstructs FreeList.
func (ba *BasicAllocator) Unmarshal(data []byte) error {
	if len(data) < 8 {
		return ErrInsufficientData
	}

	// Read file length
	ba.fileLength = types.FileOffset(binary.LittleEndian.Uint64(data[0:8]))

	// Clear freeList
	ba.freeList = NewFreeList()

	// Restore object tracking from remaining data
	err := ba.objectTracking.Unmarshal(data[8:])
	if err != nil {
		return err
	}

	// Reconstruct FreeList from gaps
	ba.reconstructFreeList()

	return nil
}

// reconstructFreeList builds FreeList by finding gaps between allocations.
func (ba *BasicAllocator) reconstructFreeList() {
	if ba.objectTracking.Len() == 0 {
		// No allocations - entire file is one gap (but we don't track it)
		return
	}

	// Collect all allocations sorted by offset
	type allocation struct {
		offset types.FileOffset
		size   types.FileSize
	}

	allocs := make([]allocation, 0, ba.objectTracking.Len())
	ba.objectTracking.ForEach(func(objId types.ObjectId, size types.FileSize) {
		allocs = append(allocs, allocation{
			offset: types.FileOffset(objId), // ObjectId == FileOffset
			size:   size,
		})
	})

	sort.Slice(allocs, func(i, j int) bool {
		return allocs[i].offset < allocs[j].offset
	})

	// Find gaps between allocations
	currentOffset := types.FileOffset(0)

	for _, alloc := range allocs {
		if alloc.offset > currentOffset {
			// Gap found
			gap := Gap{
				FileOffset: currentOffset,
				Size:       types.FileSize(alloc.offset - currentOffset),
			}
			ba.freeList.Insert(gap)
		}
		currentOffset = alloc.offset + types.FileOffset(alloc.size)
	}

	// No gap at end (fileLength is set correctly)
}

// Stats returns allocation statistics.
func (ba *BasicAllocator) Stats() (allocated, free, gaps int) {
	allocated = ba.objectTracking.Len()
	gaps = len(ba.freeList.gaps)

	var freeBytes types.FileSize
	for _, gap := range ba.freeList.gaps {
		freeBytes += gap.Size
	}

	free = int(freeBytes)
	return
}
