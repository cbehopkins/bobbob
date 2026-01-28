package block

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/cbehopkins/bobbob/allocator/types"
)

var (
	ErrSizeMismatch = errors.New("size must match block size")
	ErrAllAllocated = errors.New("all blocks are allocated")
	ErrInvalidSlot  = errors.New("invalid slot index")
	ErrSlotNotFound = errors.New("slot not allocated")
	ErrOutOfRange   = errors.New("ObjectId not in range")
	ErrInvalidCount = errors.New("count must be positive")
)

// BlockAllocator manages fixed-size block allocations using a bitmap.
// Memory overhead: ~1 byte per block (bitmap) + 2 bytes per block (requested size tracking).
type BlockAllocator struct {
	blockSize          int
	blockCount         int
	startingFileOffset types.FileOffset
	startingObjectId   types.ObjectId
	file               *os.File

	// Bitmap: one bit per block (true = allocated, false = free)
	allocatedBitmap []bool

	// Optimization: track if all blocks are allocated
	allAllocated bool

	// Track remaining free blocks to avoid rescanning the bitmap on every allocation check
	freeCount int

	// Hint cursor: start search from this position to improve locality
	// When allocating, we scan forward from this position, wrapping around if needed
	lastAllocatedSlot int

	// Optional callback fired on each allocation
	onAllocate func(types.ObjectId, types.FileOffset, int)
}

// New creates a new block allocator for fixed-size blocks.
// All allocations from this allocator will be exactly blockSize bytes.
func New(blockSize, blockCount int, startingFileOffset types.FileOffset, startingObjectId types.ObjectId, file *os.File) *BlockAllocator {
	return &BlockAllocator{
		blockSize:          blockSize,
		blockCount:         blockCount,
		startingFileOffset: startingFileOffset,
		startingObjectId:   startingObjectId,
		file:               file,
		allocatedBitmap:    make([]bool, blockCount),
		allAllocated:       false,
		freeCount:          blockCount,
		lastAllocatedSlot:  0,
	}
}

// Allocate allocates a single block.
// Size must match the blockSize configured for this allocator.
func (a *BlockAllocator) Allocate(size int) (types.ObjectId, types.FileOffset, error) {
	if size != a.blockSize {
		return types.ObjectId(0), 0, ErrSizeMismatch
	}

	if a.allAllocated {
		return types.ObjectId(0), 0, ErrAllAllocated
	}

	// Find first free slot starting from hint cursor
	for i := 0; i < a.blockCount; i++ {
		slot := (a.lastAllocatedSlot + i) % a.blockCount
		if !a.allocatedBitmap[slot] {
			a.allocatedBitmap[slot] = true
			a.freeCount--
			a.lastAllocatedSlot = (slot + 1) % a.blockCount

			objId := a.startingObjectId + types.ObjectId(slot)
			offset := a.startingFileOffset + types.FileOffset(slot*a.blockSize)

			// Fire callback if registered
			if a.onAllocate != nil {
				a.onAllocate(objId, offset, size)
			}

			// If we just allocated the last slot, mark as full
			if a.freeCount == 0 {
				a.allAllocated = true
			}

			return objId, offset, nil
		}
	}

	// All slots exhausted
	a.allAllocated = true
	return types.ObjectId(0), 0, ErrAllAllocated
}

// AllocateRun allocates a contiguous run of blocks.
// Returns as many contiguous blocks as available (up to count), following io.Writer semantics.
// Returns empty slices if no contiguous blocks are available.
func (a *BlockAllocator) AllocateRun(size int, count int) ([]types.ObjectId, []types.FileOffset, error) {
	if size != a.blockSize {
		return nil, nil, ErrSizeMismatch
	}
	if count <= 0 {
		return nil, nil, ErrInvalidCount
	}

	if a.allAllocated {
		return []types.ObjectId{}, []types.FileOffset{}, nil
	}

	// Find longest contiguous run up to min(count, blockCount)
	maxAllocate := count
	if maxAllocate > a.blockCount {
		maxAllocate = a.blockCount
	}

	bestStart := -1
	bestLength := 0
	currentStart := -1
	currentLength := 0

	for i, allocated := range a.allocatedBitmap {
		if !allocated {
			if currentStart == -1 {
				currentStart = i
				currentLength = 1
			} else {
				currentLength++
			}

			// Found enough for full request?
			if currentLength == maxAllocate {
				bestStart = currentStart
				bestLength = currentLength
				break
			}

			// Track best run so far
			if currentLength > bestLength {
				bestStart = currentStart
				bestLength = currentLength
			}
		} else {
			// Reset current run
			currentStart = -1
			currentLength = 0
		}
	}

	// No contiguous blocks available
	if bestLength == 0 {
		return []types.ObjectId{}, []types.FileOffset{}, nil
	}

	// Allocate the best run found
	objIds := make([]types.ObjectId, bestLength)
	offsets := make([]types.FileOffset, bestLength)

	for j := 0; j < bestLength; j++ {
		idx := bestStart + j
		a.allocatedBitmap[idx] = true
		a.freeCount--

		objIds[j] = a.startingObjectId + types.ObjectId(idx)
		offsets[j] = a.startingFileOffset + types.FileOffset(idx*a.blockSize)

		// Fire callback if registered
		if a.onAllocate != nil {
			a.onAllocate(objIds[j], offsets[j], size)
		}
	}

	// If no free blocks remain, mark allocator as full
	if a.freeCount == 0 {
		a.allAllocated = true
	}

	return objIds, offsets, nil
}

// DeleteObj frees a block by ObjectId.
func (a *BlockAllocator) DeleteObj(objId types.ObjectId) error {
	if !a.ContainsObjectId(objId) {
		return ErrOutOfRange
	}

	slot := int(objId - a.startingObjectId)
	if slot < 0 || slot >= a.blockCount {
		return ErrInvalidSlot
	}

	if !a.allocatedBitmap[slot] {
		return ErrSlotNotFound
	}

	a.allocatedBitmap[slot] = false
	a.freeCount++
	a.allAllocated = false

	// Update hint cursor to point to the freed slot only if it's before the current cursor.
	// This ensures we'll revisit freed slots when the cursor wraps around, promoting reuse.
	if slot < a.lastAllocatedSlot {
		a.lastAllocatedSlot = slot
	}

	return nil
}

// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
func (a *BlockAllocator) GetObjectInfo(objId types.ObjectId) (types.FileOffset, types.FileSize, error) {
	if !a.ContainsObjectId(objId) {
		return 0, 0, ErrOutOfRange
	}

	slot := int(objId - a.startingObjectId)
	if slot < 0 || slot >= a.blockCount {
		return 0, 0, ErrInvalidSlot
	}

	if !a.allocatedBitmap[slot] {
		return 0, 0, ErrSlotNotFound
	}

	offset := a.startingFileOffset + types.FileOffset(slot*a.blockSize)
	return offset, types.FileSize(a.blockSize), nil
}

// GetFile returns the file handle associated with this allocator.
func (a *BlockAllocator) GetFile() *os.File {
	return a.file
}

// ContainsObjectId returns true if this allocator owns the given ObjectId.
func (a *BlockAllocator) ContainsObjectId(objId types.ObjectId) bool {
	return objId >= a.startingObjectId && objId < a.startingObjectId+types.ObjectId(a.blockCount)
}

// SetOnAllocate registers a callback invoked after each allocation.
func (a *BlockAllocator) SetOnAllocate(callback func(types.ObjectId, types.FileOffset, int)) {
	a.onAllocate = callback
}

// Marshal serializes the allocator state to bytes.
// Format: startingFileOffset (8) | bitmap (ceil(blockCount/8))
func (a *BlockAllocator) Marshal() ([]byte, error) {
	bitmapBytes := (a.blockCount + 7) / 8
	totalSize := 8 + bitmapBytes
	data := make([]byte, totalSize)

	// Write startingFileOffset
	binary.LittleEndian.PutUint64(data[0:8], uint64(a.startingFileOffset))

	// Write allocation bitmap (packed)
	for i, allocated := range a.allocatedBitmap {
		if allocated {
			data[8+i/8] |= 1 << (i % 8)
		}
	}

	return data, nil
}

// Unmarshal restores allocator state from bytes.
func (a *BlockAllocator) Unmarshal(data []byte) error {
	bitmapBytes := (a.blockCount + 7) / 8
	expectedSize := 8 + bitmapBytes

	// Handle short data by padding with zeros
	if len(data) < expectedSize {
		padded := make([]byte, expectedSize)
		copy(padded, data)
		data = padded
	} else if len(data) > expectedSize {
		// Truncate excess data
		data = data[:expectedSize]
	}

	// Read startingFileOffset
	a.startingFileOffset = types.FileOffset(binary.LittleEndian.Uint64(data[0:8]))

	// Derive startingObjectId from fileOffset (BlockAllocator convention)
	a.startingObjectId = types.ObjectId(a.startingFileOffset)

	// Read allocation bitmap
	for i := range a.allocatedBitmap {
		a.allocatedBitmap[i] = (data[8+i/8] & (1 << (i % 8))) != 0
	}

	// Recompute freeCount and allAllocated flag
	free := 0
	for _, allocated := range a.allocatedBitmap {
		if !allocated {
			free++
		}
	}
	a.freeCount = free
	a.allAllocated = (free == 0)

	// Reset hint cursor to start of allocator
	a.lastAllocatedSlot = 0

	return nil
}

// GetObjectIdsInAllocator returns all allocated ObjectIds.
// Used for introspection, testing, and compaction.
func (a *BlockAllocator) GetObjectIdsInAllocator() []types.ObjectId {
	var objIds []types.ObjectId
	for i, allocated := range a.allocatedBitmap {
		if allocated {
			objIds = append(objIds, a.startingObjectId+types.ObjectId(i))
		}
	}
	return objIds
}

// BlockSize returns the fixed block size managed by this allocator.
func (a *BlockAllocator) BlockSize() int {
	return a.blockSize
}

// BlockCount returns the number of blocks managed by this allocator.
func (a *BlockAllocator) BlockCount() int {
	return a.blockCount
}

// BaseObjectId returns the first ObjectId managed by this allocator.
func (a *BlockAllocator) BaseObjectId() types.ObjectId {
	return a.startingObjectId
}

// BaseFileOffset returns the starting file offset managed by this allocator.
func (a *BlockAllocator) BaseFileOffset() types.FileOffset {
	return a.startingFileOffset
}

// Stats returns allocation statistics for monitoring.
func (a *BlockAllocator) Stats() (allocated, free, total int) {
	free = a.freeCount
	allocated = a.blockCount - free
	total = a.blockCount
	return
}

// SizeInBytes returns the number of bytes needed to store the marshalled structure.
// This is the size that Marshal() will return.
func (a *BlockAllocator) SizeInBytes() int {
	bitmapBytes := (a.blockCount + 7) / 8
	return 8 + bitmapBytes
}

// RangeSizeInBytes returns the total size of the backing array in bytes.
// This is the size that needs to be allocated from the parent allocator
// to hold all blocks: blockSize * blockCount.
func (a *BlockAllocator) RangeSizeInBytes() int {
	return a.blockSize * a.blockCount
}
