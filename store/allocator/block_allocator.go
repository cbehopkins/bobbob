package allocator

import (
	"encoding/binary"
	"errors"
	"os"
)

type blockAllocator struct {
	blockSize          int
	blockCount         int
	allocatedList      []bool
	requestedSizes     []int
	startingFileOffset FileOffset
	startingObjectId   ObjectId
	allAllocated       bool
	// File handle for direct I/O operations
	file *os.File
}

// NewBlockAllocator creates a new block allocator for fixed-size blocks.
// This allocator is useful when you know the size of the blocks you want to allocate.
// It uses a simple boolean array instead of an allocation map, making it more space-efficient.
// blockSize is the size of each block in bytes.
// blockCount is the number of blocks in the allocator.
// startingFileOffset is the file offset where the first block begins.
// startingObjectId is the ObjectId for the first block.
// file is the file handle for direct I/O operations (can be nil if not needed).
func NewBlockAllocator(blockSize, blockCount int, startingFileOffset FileOffset, startingObjectId ObjectId, file *os.File) *blockAllocator {
	allocatedList := make([]bool, blockCount)
	requestedSizes := make([]int, blockCount)
	return &blockAllocator{
		blockSize:          blockSize,
		blockCount:         blockCount,
		allocatedList:      allocatedList,
		requestedSizes:     requestedSizes,
		startingFileOffset: startingFileOffset,
		startingObjectId:   startingObjectId,
		file:               file,
	}
}

// Allocate allocates a block of the specified size.
// The size must match the blockSize configured for this allocator.
// Returns an error if the size doesn't match or if all blocks are allocated.
func (a *blockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	if size != a.blockSize {
		return 0, 0, errors.New("size must match block size")
	}

	// TBD we currently use a list of booleans
	// This is far from the most efficient structure
	// And we could make use of a bitset, but KISS
	if a.allAllocated {
		return 0, 0, AllAllocated
	}
	for i, allocated := range a.allocatedList {
		if !allocated {
			a.allocatedList[i] = true
			fileOffset := a.startingFileOffset + FileOffset(i*a.blockSize)
			objectId := a.startingObjectId + ObjectId(i)
			return objectId, fileOffset, nil
		}
	}
	a.allAllocated = true
	return 0, 0, AllAllocated
}

// AllocateRun reserves a contiguous run of free slots within this block allocator.
// Returns as many contiguous slots as available (up to count), mimicking io.Write semantics.
// Returns empty slices if no contiguous slots are available.
func (a *blockAllocator) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	if size != a.blockSize {
		return nil, nil, errors.New("size must match block size")
	}
	if count <= 0 {
		return nil, nil, errors.New("count must be positive")
	}

	// Find the longest contiguous run, up to min(count, blockCount)
	maxAllocate := count
	if maxAllocate > a.blockCount {
		maxAllocate = a.blockCount
	}

	bestStart := -1
	bestLength := 0
	currentStart := -1
	currentLength := 0

	for i, allocated := range a.allocatedList {
		if !allocated {
			if currentStart == -1 {
				currentStart = i
				currentLength = 1
			} else {
				currentLength++
			}

			// If we've found enough for the full request, use it immediately
			if currentLength == maxAllocate {
				bestStart = currentStart
				bestLength = currentLength
				break
			}

			// Track the best run so far
			if currentLength > bestLength {
				bestStart = currentStart
				bestLength = currentLength
			}
		} else {
			currentStart = -1
			currentLength = 0
		}
	}

	// Return the best contiguous run found (may be 0)
	if bestLength == 0 {
		return []ObjectId{}, []FileOffset{}, nil
	}

	objIds := make([]ObjectId, bestLength)
	offsets := make([]FileOffset, bestLength)
	for j := 0; j < bestLength; j++ {
		idx := bestStart + j
		a.allocatedList[idx] = true
		objIds[j] = a.startingObjectId + ObjectId(idx)
		offsets[j] = a.startingFileOffset + FileOffset(idx*a.blockSize)
		a.requestedSizes[idx] = size
	}
	return objIds, offsets, nil
}

func (a *blockAllocator) Free(fileOffset FileOffset, size int) error {
	a.allAllocated = false
	if size != a.blockSize {
		return errors.New("invalid block size")
	}
	blockIndex := (fileOffset - a.startingFileOffset) / FileOffset(a.blockSize)
	if blockIndex < 0 || blockIndex >= FileOffset(len(a.allocatedList)) {
		return errors.New("invalid file offset")
	}
	a.allocatedList[blockIndex] = false
	a.requestedSizes[blockIndex] = 0
	return nil
}

// setRequestedSize records the caller-requested size for an allocated slot.
func (a *blockAllocator) setRequestedSize(objId ObjectId, size int) {
	if !a.ContainsObjectId(objId) {
		return
	}
	slot := objId - a.startingObjectId
	if slot < 0 || int(slot) >= len(a.requestedSizes) {
		return
	}
	a.requestedSizes[slot] = size
}

// requestedSize returns the caller-requested size for an allocated slot if available.
func (a *blockAllocator) requestedSize(objId ObjectId) (int, bool) {
	if !a.ContainsObjectId(objId) {
		return 0, false
	}
	slot := objId - a.startingObjectId
	if slot < 0 || int(slot) >= len(a.requestedSizes) {
		return 0, false
	}
	size := a.requestedSizes[slot]
	if size == 0 {
		return 0, false
	}
	return size, true
}

// objectIdForOffset derives the objectId from a file offset if it belongs to this allocator.
func (a *blockAllocator) objectIdForOffset(fileOffset FileOffset) (ObjectId, bool) {
	if fileOffset < a.startingFileOffset {
		return 0, false
	}
	maxOffset := a.startingFileOffset + FileOffset(a.blockCount*a.blockSize)
	if fileOffset >= maxOffset {
		return 0, false
	}
	slot := (fileOffset - a.startingFileOffset) / FileOffset(a.blockSize)
	if slot < 0 || int(slot) >= len(a.allocatedList) {
		return 0, false
	}
	return a.startingObjectId + ObjectId(slot), true
}

// ContainsObjectId checks if an ObjectId belongs to this block allocator's range.
func (a *blockAllocator) ContainsObjectId(objId ObjectId) bool {
	return objId >= a.startingObjectId && objId < a.startingObjectId+ObjectId(a.blockCount)
}

// GetFileOffset returns the FileOffset for an ObjectId managed by this allocator.
// Returns an error if the ObjectId is not in range or not allocated.
func (a *blockAllocator) GetFileOffset(objId ObjectId) (FileOffset, error) {
	if !a.ContainsObjectId(objId) {
		return 0, errors.New("ObjectId not in range")
	}

	slotIndex := int(objId - a.startingObjectId)
	if slotIndex < 0 || slotIndex >= len(a.allocatedList) {
		return 0, errors.New("invalid slot index")
	}

	if !a.allocatedList[slotIndex] {
		return 0, errors.New("slot not allocated")
	}

	return a.startingFileOffset + FileOffset(slotIndex*a.blockSize), nil
}

func (a *blockAllocator) SizeInBytes() int {
	bitCount := (a.blockCount + 7) / 8
	return 8 + bitCount + 2*a.blockCount
}

func (a *blockAllocator) Marshal() ([]byte, error) {
	bitCount := (a.blockCount + 7) / 8
	data := make([]byte, 8+bitCount+2*a.blockCount)
	// startingFileOffset
	binary.LittleEndian.PutUint64(data[0:8], uint64(a.startingFileOffset))
	// allocation bitmap
	for i, allocated := range a.allocatedList {
		if allocated {
			data[8+i/8] |= 1 << (i % 8)
		}
	}
	offset := 8 + bitCount
	for i, size := range a.requestedSizes {
		binary.LittleEndian.PutUint16(data[offset+2*i:], uint16(size))
	}
	return data, nil
}

func (a *blockAllocator) Unmarshal(data []byte) error {
	bitCount := (a.blockCount + 7) / 8
	expected := 8 + bitCount + 2*a.blockCount
	if len(data) < expected {
		// Pad with zeros to expected length
		padded := make([]byte, expected)
		copy(padded, data)
		data = padded
	}
	if len(data) > expected {
		data = data[:expected]
	}
	a.startingFileOffset = FileOffset(binary.LittleEndian.Uint64(data[0:8]))
	a.startingObjectId = ObjectId(a.startingFileOffset)
	for i := range a.allocatedList {
		a.allocatedList[i] = (data[8+i/8] & (1 << (i % 8))) != 0
	}
	offset := 8 + bitCount
	for i := range a.requestedSizes {
		a.requestedSizes[i] = int(binary.LittleEndian.Uint16(data[offset+2*i:]))
	}
	return nil
}
