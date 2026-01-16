package allocator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type blockAllocator struct {
	blockSize          int
	blockCount         int
	allocatedList      []bool
	requestedSizes     []int
	startingFileOffset FileOffset
	startingObjectId   ObjectId
	allAllocated       bool
}

// NewBlockAllocator creates a new block allocator for fixed-size blocks.
// This allocator is useful when you know the size of the blocks you want to allocate.
// It uses a simple boolean array instead of an allocation map, making it more space-efficient.
// blockSize is the size of each block in bytes.
// blockCount is the number of blocks in the allocator.
// startingFileOffset is the file offset where the first block begins.
// startingObjectId is the ObjectId for the first block.
func NewBlockAllocator(blockSize, blockCount int, startingFileOffset FileOffset, startingObjectId ObjectId) *blockAllocator {
	allocatedList := make([]bool, blockCount)
	requestedSizes := make([]int, blockCount)
	return &blockAllocator{
		blockSize:          blockSize,
		blockCount:         blockCount,
		allocatedList:      allocatedList,
		requestedSizes:     requestedSizes,
		startingFileOffset: startingFileOffset,
		startingObjectId:   startingObjectId,
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
	if len(data) != expected {
		return errors.New("invalid data length")
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

type multiBlockAllocator struct {
	blockSize          int
	blockCount         int
	parent             Allocator
	allocators         []*blockAllocator
	startOffsets       []FileOffset
	preParentAllocate  func(size int) error
	postParentAllocate func(ObjectId, FileOffset) error
}

// NewMultiBlockAllocator creates a new multi-block allocator that manages multiple block allocators.
// When all existing block allocators are full, it automatically allocates a new one from the parent.
// blockSize is the size of each block in bytes.
// blockCount is the number of blocks in each sub-allocator.
// parent is the parent allocator used to allocate new block regions.
func NewMultiBlockAllocator(blockSize, blockCount int, parent Allocator) *multiBlockAllocator {
	return &multiBlockAllocator{
		blockSize:  blockSize,
		blockCount: blockCount,
		parent:     parent,
		allocators: []*blockAllocator{},
	}
}

func (m *multiBlockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	// Verify the requested size matches our block size
	if size != m.blockSize {
		return 0, 0, errors.New("size must match block size")
	}

	for _, allocator := range m.allocators {
		// TBD add a bitfield/map/whatever to track which allocators are full
		// Then we can save quering them
		ObjectId, FileOffset, err := allocator.Allocate(m.blockSize)
		if err == nil {
			return ObjectId, FileOffset, nil
		}
		if !errors.Is(err, AllAllocated) {
			return 0, 0, err
		}
	}

	// All allocators are full, create a new one
	if m.preParentAllocate != nil {
		if err := m.preParentAllocate(size); err != nil {
			return 0, 0, err
		}
	}
	parentObjectId, parentFileOffset, err := m.parent.Allocate(m.blockSize * m.blockCount)
	if err != nil {
		return 0, 0, err
	}
	if m.postParentAllocate != nil {
		if err := m.postParentAllocate(parentObjectId, parentFileOffset); err != nil {
			return 0, 0, err
		}
	}

	newAllocator := NewBlockAllocator(m.blockSize, m.blockCount, parentFileOffset, parentObjectId)
	m.allocators = append(m.allocators, newAllocator)
	m.startOffsets = append(m.startOffsets, parentFileOffset)

	return newAllocator.Allocate(m.blockSize)
}

// AllocateRun reserves a contiguous run of blocks within a single sub-allocator.
// Returns as many contiguous blocks as available (up to count), trying existing
// allocators first, then creating a new one if needed. Returns empty slices if
// no contiguous blocks are available and new allocator creation fails.
func (m *multiBlockAllocator) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	if size != m.blockSize {
		return nil, nil, ErrRunUnsupported
	}
	if count <= 0 {
		return nil, nil, errors.New("count must be positive")
	}

	// Clamp count to blockCount since runs can't span allocators
	requestCount := count
	if requestCount > m.blockCount {
		requestCount = m.blockCount
	}

	// Try existing allocators - they now return partial results
	for _, allocator := range m.allocators {
		objIds, offsets, err := allocator.AllocateRun(size, requestCount)
		if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
			return nil, nil, err
		}
		// Accept any non-zero allocation
		if len(objIds) > 0 {
			return objIds, offsets, nil
		}
	}

	// Need a new allocator segment
	if m.preParentAllocate != nil {
		if err := m.preParentAllocate(size); err != nil {
			return []ObjectId{}, []FileOffset{}, nil
		}
	}
	parentObjectId, parentFileOffset, err := m.parent.Allocate(m.blockSize * m.blockCount)
	if err != nil {
		return []ObjectId{}, []FileOffset{}, nil
	}
	if m.postParentAllocate != nil {
		if err := m.postParentAllocate(parentObjectId, parentFileOffset); err != nil {
			return []ObjectId{}, []FileOffset{}, nil
		}
	}

	newAllocator := NewBlockAllocator(m.blockSize, m.blockCount, parentFileOffset, parentObjectId)
	m.allocators = append(m.allocators, newAllocator)
	m.startOffsets = append(m.startOffsets, parentFileOffset)

	return newAllocator.AllocateRun(size, requestCount)
}

func (m *multiBlockAllocator) Free(fileOffset FileOffset, size int) error {
	if size != m.blockSize {
		return errors.New("invalid block size")
	}
	for i, startOffset := range m.startOffsets {
		if fileOffset >= startOffset && fileOffset < startOffset+FileOffset(m.blockSize*m.blockCount) {
			return m.allocators[i].Free(fileOffset, size)
		}
	}
	return errors.New("invalid file offset")
}

func (m *multiBlockAllocator) Marshal() ([]byte, error) {
	// Serialize the state of the multiBlockAllocator
	data := make([]byte, 0)

	// Serialize blockSize and blockCount
	data = append(data, byte(m.blockSize>>8), byte(m.blockSize))
	data = append(data, byte(m.blockCount>>8), byte(m.blockCount))

	// Serialize the number of allocators
	data = append(data, byte(len(m.allocators)>>8), byte(len(m.allocators)))

	// Serialize each blockAllocator
	for _, allocator := range m.allocators {
		allocatorData, err := allocator.Marshal()
		if err != nil {
			return nil, err
		}
		data = append(data, allocatorData...)
	}

	// Serialize startOffsets
	for _, offset := range m.startOffsets {
		data = append(data, byte(offset>>56), byte(offset>>48), byte(offset>>40), byte(offset>>32),
			byte(offset>>24), byte(offset>>16), byte(offset>>8), byte(offset))
	}

	return data, nil
}

func (m *multiBlockAllocator) Unmarshal(data []byte) error {
	// Deserialize blockSize and blockCount
	if len(data) < 4 {
		return errors.New("invalid data length")
	}
	m.blockSize = int(data[0])<<8 | int(data[1])
	m.blockCount = int(data[2])<<8 | int(data[3])
	data = data[4:]

	// Deserialize the number of allocators
	if len(data) < 2 {
		return errors.New("invalid data length")
	}
	numAllocators := int(data[0])<<8 | int(data[1])
	data = data[2:]

	// Deserialize each blockAllocator
	m.allocators = make([]*blockAllocator, numAllocators)
	for i := range numAllocators {
		allocator := &blockAllocator{
			blockSize:          m.blockSize,
			blockCount:         m.blockCount,
			allocatedList:      make([]bool, m.blockCount), // Properly initialize allocatedList
			requestedSizes:     make([]int, m.blockCount),  // Track requested sizes per slot
			startingFileOffset: 0,                          // Placeholder, will be set later
			startingObjectId:   0,                          // Placeholder, will be set later
		}
		bitCount := (m.blockCount + 7) / 8
		allocatorDataSize := 8 + bitCount + 2*m.blockCount
		if len(data) < allocatorDataSize {
			return errors.New("invalid data length")
		}
		if err := allocator.Unmarshal(data[:allocatorDataSize]); err != nil {
			return err
		}
		m.allocators[i] = allocator
		data = data[allocatorDataSize:]
	}

	// Deserialize startOffsets
	m.startOffsets = make([]FileOffset, numAllocators)
	for i := range numAllocators {
		if len(data) < 8 {
			return errors.New("invalid data length")
		}
		offset := FileOffset(data[0])<<56 | FileOffset(data[1])<<48 | FileOffset(data[2])<<40 | FileOffset(data[3])<<32 |
			FileOffset(data[4])<<24 | FileOffset(data[5])<<16 | FileOffset(data[6])<<8 | FileOffset(data[7])
		m.startOffsets[i] = offset
		m.allocators[i].startingFileOffset = offset         // Set startingFileOffset for each allocator
		m.allocators[i].startingObjectId = ObjectId(offset) // Align object IDs to parent allocator offsets
		data = data[8:]
	}

	return nil
}

// defaultBlockSizes returns the default block sizes used by omniBlockAllocator.
// These provide sensible defaults for small to medium objects (64 bytes to 4KB).
// Larger objects are handled by the parent BasicAllocator.
func defaultBlockSizes() []int {
	return []int{
		64,   // 64 bytes
		128,  // 128 bytes
		256,  // 256 bytes
		512,  // 512 bytes
		1024, // 1 KB
		2048, // 2 KB
		4096, // 4 KB
	}
}

type omniBlockAllocator struct {
	// blockMap holds per-size pools split into available vs full allocators. This keeps
	// the hot allocation path focused on non-full allocators while retaining filled ones
	// to reuse after frees.
	blockMap     map[int]*allocatorPool
	sortedSizes  []int // sorted block sizes for best-fit allocation
	blockCount   int
	parent       Allocator
	preAllocate  func(size int) error
	postAllocate func(id ObjectId, offset FileOffset) error
	preFree      func(offset FileOffset, size int) error
	postFree     func(offset FileOffset, size int) error
	idx          *AllocatorIndex
	maxBlockSize int        // largest block size, objects larger than this use parent
	mu           sync.Mutex // guards pool state for concurrent callers
	// OnAllocate is called after a successful allocation (for testing/monitoring).
	OnAllocate func(objId ObjectId, offset FileOffset, size int)
}

type omniBlockAllocatorOptions struct {
	preallocate bool
}

// allocatorPool keeps available and full block allocators for a given size.
type allocatorPool struct {
	available []*blockAllocator
	full      []*blockAllocator
}

// OmniBlockAllocatorOption configures optional behaviors for NewOmniBlockAllocator.
type OmniBlockAllocatorOption func(*omniBlockAllocatorOptions)

// WithoutPreallocation builds the allocator metadata without reserving space from the parent.
// Useful when immediately calling Unmarshal on persisted data to avoid mutating the parent state.
func WithoutPreallocation() OmniBlockAllocatorOption {
	return func(o *omniBlockAllocatorOptions) { o.preallocate = false }
}

// NewOmniBlockAllocator creates an allocator that manages multiple block sizes.
// It combines default block sizes (64B-4KB) with any additional provided sizes.
// The allocator uses a best-fit strategy: for allocation requests, it finds the
// smallest block size that fits. Objects larger than the maximum block size are
// delegated to the parent allocator.
//
// blockSize is a slice of additional block sizes to support beyond the defaults.
// blockCount is the number of blocks in each sub-allocator.
// parent is the fallback allocator for sizes not in the block allocator range.
// opts can be used to disable upfront parent allocations (e.g., when unmarshaling persisted data).
func NewOmniBlockAllocator(blockSize []int, blockCount int, parent Allocator, opts ...OmniBlockAllocatorOption) (*omniBlockAllocator, error) {
	config := omniBlockAllocatorOptions{preallocate: true}
	for _, opt := range opts {
		opt(&config)
	}

	// Combine default sizes with provided sizes
	allSizes := append(defaultBlockSizes(), blockSize...)

	// Deduplicate and sort sizes
	seen := make(map[int]bool)
	var uniqueSizes []int
	for _, size := range allSizes {
		if !seen[size] {
			seen[size] = true
			uniqueSizes = append(uniqueSizes, size)
		}
	}

	// Sort sizes for binary search during allocation
	sort.Ints(uniqueSizes)

	blockMap := make(map[int]*allocatorPool)
	idx := NewAllocatorIndex("")
	var maxBlockSize int
	if len(uniqueSizes) > 0 {
		maxBlockSize = uniqueSizes[len(uniqueSizes)-1]
	}

	if config.preallocate {
		// Assign non-overlapping ObjectId ranges to each block allocator using the parent allocator.
		for _, size := range uniqueSizes {
			totalSize := blockCount * size
			baseObjId, fileOffset, err := parent.Allocate(totalSize)
			if err != nil {
				return nil, fmt.Errorf("prealloc block size %d: %w", size, err)
			}

			ba := NewBlockAllocator(size, blockCount, fileOffset, baseObjId)
			pool, ok := blockMap[size]
			if !ok {
				pool = &allocatorPool{}
				blockMap[size] = pool
			}
			pool.available = append(pool.available, ba)

			// Register the range with the unified index
			_ = idx.RegisterRange(int64(baseObjId), size, fileOffset)
			_ = idx.UpdateRangeEnd(int64(baseObjId), int64(baseObjId)+int64(blockCount))
		}
	}

	return &omniBlockAllocator{
		blockMap:     blockMap,
		sortedSizes:  uniqueSizes,
		blockCount:   blockCount,
		parent:       parent,
		idx:          idx,
		maxBlockSize: maxBlockSize,
	}, nil
}

// SetOnAllocate registers a callback to be invoked after each successful allocation.
// Useful for testing and monitoring allocator usage patterns.
func (o *omniBlockAllocator) SetOnAllocate(cb func(objId ObjectId, offset FileOffset, size int)) {
	o.OnAllocate = cb
}

// Parent returns the parent allocator, allowing external code to configure it.
// For example, to set allocation callbacks on a BasicAllocator parent:
//
//	if ba, ok := omni.Parent().(*BasicAllocator); ok {
//	    ba.SetOnAllocate(callback)
//	}
func (o *omniBlockAllocator) Parent() Allocator {
	return o.parent
}

func (o *omniBlockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.preAllocate != nil {
		if err := o.preAllocate(size); err != nil {
			return 0, 0, err
		}
	}

	var id ObjectId
	var offset FileOffset
	var err error

	// Try best-fit: find the smallest block size that can hold this object
	if size <= o.maxBlockSize {
		// Binary search for the first block size >= requested size
		idx := sort.SearchInts(o.sortedSizes, size)

		if idx < len(o.sortedSizes) {
			blockSize := o.sortedSizes[idx]
			pool := o.blockMap[blockSize]
			if pool == nil {
				pool = &allocatorPool{}
				o.blockMap[blockSize] = pool
			}

			// Try available allocators first
			for len(pool.available) > 0 {
				allocator := pool.available[0]
				id, offset, err = allocator.Allocate(blockSize)
				if errors.Is(err, AllAllocated) {
					// move to full list
					pool.full = append(pool.full, allocator)
					pool.available = pool.available[1:]
					continue
				}
				if err == nil {
					allocator.setRequestedSize(id, size)
					if o.postAllocate != nil {
						if postErr := o.postAllocate(id, offset); postErr != nil {
							return 0, 0, postErr
						}
					}
					if o.OnAllocate != nil {
						o.OnAllocate(id, offset, size)
					}
					return id, offset, nil
				}
				return 0, 0, err
			}

			// No available allocators or all were full: provision a new allocator from parent
			baseObjId, fileOffset, allocErr := o.parent.Allocate(blockSize * o.blockCount)
			if allocErr != nil {
				return 0, 0, fmt.Errorf("parent allocate for block size %d failed: %w", blockSize, allocErr)
			}
			newAllocator := NewBlockAllocator(blockSize, o.blockCount, fileOffset, baseObjId)
			pool.available = append(pool.available, newAllocator)
			if o.idx != nil {
				_ = o.idx.RegisterRange(int64(baseObjId), blockSize, fileOffset)
				_ = o.idx.UpdateRangeEnd(int64(baseObjId), int64(baseObjId)+int64(o.blockCount))
			}
			id, offset, err = newAllocator.Allocate(blockSize)
			if err == nil {
				newAllocator.setRequestedSize(id, size)
				if o.postAllocate != nil {
					if postErr := o.postAllocate(id, offset); postErr != nil {
						return 0, 0, postErr
					}
				}
				if o.OnAllocate != nil {
					o.OnAllocate(id, offset, size)
				}
				return id, offset, nil
			}
		}
	}

	// Object too large or all suitable block allocators are full:
	// Defer to the parent allocator
	id, offset, err = o.parent.Allocate(size)

	if o.postAllocate != nil {
		if postErr := o.postAllocate(id, offset); postErr != nil {
			return 0, 0, postErr
		}
	}

	if o.OnAllocate != nil {
		o.OnAllocate(id, offset, size)
	}

	return id, offset, err
}

// AllocateRun attempts to reserve a contiguous run of blocks for the given size.
// Returns as many contiguous blocks as available (up to count). Only sizes that
// fit within the managed block sizes are supported; larger sizes defer to the
// parent if it supports RunAllocator.
func (o *omniBlockAllocator) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if count <= 0 {
		return nil, nil, errors.New("count must be positive")
	}

	// Try best-fit block size for requested size
	if size <= o.maxBlockSize {
		idx := sort.SearchInts(o.sortedSizes, size)
		if idx < len(o.sortedSizes) {
			blockSize := o.sortedSizes[idx]
			pool := o.blockMap[blockSize]
			if pool == nil {
				pool = &allocatorPool{}
				o.blockMap[blockSize] = pool
			}

			// Try available allocators - they now return partial results
			for i := 0; i < len(pool.available); {
				allocator := pool.available[i]
				objIds, offsets, err := allocator.AllocateRun(blockSize, count)
				if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
					return nil, nil, err
				}
				// If no slots available, move to full list
				if len(objIds) == 0 {
					pool.full = append(pool.full, allocator)
					pool.available = append(pool.available[:i], pool.available[i+1:]...)
					continue
				}
				// Accept partial allocation
				for _, objId := range objIds {
					allocator.setRequestedSize(objId, size)
				}
				return objIds, offsets, nil
			}

			// Need a new allocator segment
			baseObjId, fileOffset, allocErr := o.parent.Allocate(blockSize * o.blockCount)
			if allocErr != nil {
				return []ObjectId{}, []FileOffset{}, nil
			}
			newAllocator := NewBlockAllocator(blockSize, o.blockCount, fileOffset, baseObjId)
			pool.available = append(pool.available, newAllocator)
			if o.idx != nil {
				_ = o.idx.RegisterRange(int64(baseObjId), blockSize, fileOffset)
				_ = o.idx.UpdateRangeEnd(int64(baseObjId), int64(baseObjId)+int64(o.blockCount))
			}

			objIds, offsets, err := newAllocator.AllocateRun(blockSize, count)
			if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
				return nil, nil, err
			}
			for _, objId := range objIds {
				newAllocator.setRequestedSize(objId, size)
			}
			return objIds, offsets, nil
		}
	}

	// Fall back to parent if it supports run allocation
	if ra, ok := o.parent.(RunAllocator); ok {
		return ra.AllocateRun(size, count)
	}

	return nil, nil, ErrRunUnsupported
}

func (o *omniBlockAllocator) Free(fileOffset FileOffset, size int) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.preFree != nil {
		if err := o.preFree(fileOffset, size); err != nil {
			return err
		}
	}

	var err error

	// Try best-fit: find the smallest block size that would have held this object
	if size <= o.maxBlockSize {
		idx := sort.SearchInts(o.sortedSizes, size)

		if idx < len(o.sortedSizes) {
			blockSize := o.sortedSizes[idx]
			pool := o.blockMap[blockSize]

			// Check available allocators first
			if pool != nil {
				for _, allocator := range pool.available {
					if allocator == nil {
						continue
					}
					if fileOffset < allocator.startingFileOffset || fileOffset >= allocator.startingFileOffset+FileOffset(allocator.blockCount*allocator.blockSize) {
						continue
					}
					if objId, ok := allocator.objectIdForOffset(fileOffset); ok {
						allocator.setRequestedSize(objId, 0)
					}
					err = allocator.Free(fileOffset, blockSize)
					if err == nil {
						if o.postFree != nil {
							if postErr := o.postFree(fileOffset, blockSize); postErr != nil {
								return postErr
							}
						}
						return nil
					}
				}

				// Then check allocators previously marked full; once a slot is freed, move it back to available
				for i, allocator := range pool.full {
					if allocator == nil {
						continue
					}
					if fileOffset < allocator.startingFileOffset || fileOffset >= allocator.startingFileOffset+FileOffset(allocator.blockCount*allocator.blockSize) {
						continue
					}
					if objId, ok := allocator.objectIdForOffset(fileOffset); ok {
						allocator.setRequestedSize(objId, 0)
					}
					err = allocator.Free(fileOffset, blockSize)
					if err == nil {
						pool.available = append(pool.available, allocator)
						pool.full = append(pool.full[:i], pool.full[i+1:]...)
						if o.postFree != nil {
							if postErr := o.postFree(fileOffset, blockSize); postErr != nil {
								return postErr
							}
						}
						return nil
					}
				}
			}
		}
	}

	// Object too large or not in block allocators: defer to parent
	err = o.parent.Free(fileOffset, size)

	if o.postFree != nil {
		if postErr := o.postFree(fileOffset, size); postErr != nil {
			return postErr
		}
	}

	return err
}

// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
// It uses the unified AllocatorIndex for O(log n) range lookup.
// Falls back to parent allocator if ObjectId not found in local allocators.
func (o *omniBlockAllocator) GetObjectInfo(objId ObjectId) (FileOffset, int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Try the index first for O(log n) lookup
	if o.idx != nil {
		_, blockSize, err := o.idx.Get(objId)
		if err == nil {
			pool := o.blockMap[blockSize]
			if pool != nil {
				for _, allocator := range pool.available {
					if allocator == nil {
						continue
					}
					if !allocator.ContainsObjectId(objId) {
						continue
					}
					realOff, allocErr := allocator.GetFileOffset(objId)
					if allocErr != nil {
						return 0, 0, allocErr
					}
					sz := blockSize
					if requested, ok := allocator.requestedSize(objId); ok {
						sz = requested
					}
					return realOff, sz, nil
				}
				for _, allocator := range pool.full {
					if allocator == nil {
						continue
					}
					if !allocator.ContainsObjectId(objId) {
						continue
					}
					realOff, allocErr := allocator.GetFileOffset(objId)
					if allocErr != nil {
						return 0, 0, allocErr
					}
					sz := blockSize
					if requested, ok := allocator.requestedSize(objId); ok {
						sz = requested
					}
					return realOff, sz, nil
				}
			}
		}
	}

	// Not in cache, fall back to parent allocator
	if parentHasGetObjectInfo, ok := o.parent.(interface {
		GetObjectInfo(ObjectId) (FileOffset, int, error)
	}); ok {
		return parentHasGetObjectInfo.GetObjectInfo(objId)
	}

	return 0, 0, errors.New("ObjectId not found and parent allocator does not support GetObjectInfo")
}

// Marshal serializes the omniBlockAllocator's state.
// Format: [blockCount:4][numSizes:4][size1:4][size2:4]...[allocator1 data][allocator2 data]...
func (o *omniBlockAllocator) Marshal() ([]byte, error) {
	data := make([]byte, 0)

	// Serialize blockCount
	data = append(data,
		byte(o.blockCount>>24), byte(o.blockCount>>16),
		byte(o.blockCount>>8), byte(o.blockCount))

	// Serialize the number of block sizes
	numSizes := len(o.blockMap)
	data = append(data,
		byte(numSizes>>24), byte(numSizes>>16),
		byte(numSizes>>8), byte(numSizes))

	// Serialize block sizes in deterministic order (sorted)
	sizes := make([]int, 0, numSizes)
	for size := range o.blockMap {
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)

	for _, size := range sizes {
		pool := o.blockMap[size]
		if pool == nil {
			continue
		}
		// size
		data = append(data,
			byte(size>>24), byte(size>>16),
			byte(size>>8), byte(size))
		// number of allocators for this size (available + full)
		count := len(pool.available) + len(pool.full)
		data = append(data,
			byte(count>>24), byte(count>>16),
			byte(count>>8), byte(count))

		for _, allocator := range pool.available {
			allocatorData, err := allocator.Marshal()
			if err != nil {
				return nil, err
			}
			data = append(data, allocatorData...)
		}
		for _, allocator := range pool.full {
			allocatorData, err := allocator.Marshal()
			if err != nil {
				return nil, err
			}
			data = append(data, allocatorData...)
		}
	}

	return data, nil
}

// Unmarshal deserializes the omniBlockAllocator's state.
func (o *omniBlockAllocator) Unmarshal(data []byte) error {
	if len(data) < 8 {
		return errors.New("invalid data length")
	}

	offset := 0

	// Deserialize blockCount
	o.blockCount = int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Deserialize number of sizes
	numSizes := int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Initialize blockMap and common metadata
	o.blockMap = make(map[int]*allocatorPool)
	o.idx = NewAllocatorIndex("")
	bitCount := (o.blockCount + 7) / 8
	allocatorDataSize := 8 + bitCount + 2*o.blockCount
	existingSizes := o.sortedSizes

	sizes := make([]int, 0, numSizes)

	for i := 0; i < numSizes; i++ {
		if len(data) < offset+8 {
			return errors.New("invalid data length for size header")
		}
		size := int(data[offset])<<24 | int(data[offset+1])<<16 |
			int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
		allocCount := int(data[offset])<<24 | int(data[offset+1])<<16 |
			int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		pool := &allocatorPool{}
		for j := 0; j < allocCount; j++ {
			if len(data) < offset+allocatorDataSize {
				return errors.New("invalid data length for allocator")
			}
			allocator := &blockAllocator{
				blockSize:          size,
				blockCount:         o.blockCount,
				allocatedList:      make([]bool, o.blockCount),
				requestedSizes:     make([]int, o.blockCount),
				startingFileOffset: 0,
				startingObjectId:   0,
			}
			if err := allocator.Unmarshal(data[offset : offset+allocatorDataSize]); err != nil {
				return err
			}
			offset += allocatorDataSize
			pool.available = append(pool.available, allocator)
			_ = o.idx.RegisterRange(int64(allocator.startingObjectId), allocator.blockSize, allocator.startingFileOffset)
			_ = o.idx.UpdateRangeEnd(int64(allocator.startingObjectId), int64(allocator.startingObjectId)+int64(o.blockCount))
		}

		o.blockMap[size] = pool
		sizes = append(sizes, size)
	}

	if numSizes == 0 {
		o.sortedSizes = make([]int, len(existingSizes))
		copy(o.sortedSizes, existingSizes)
	} else {
		o.sortedSizes = make([]int, len(sizes))
		copy(o.sortedSizes, sizes)
		sort.Ints(o.sortedSizes)
	}
	if len(o.sortedSizes) > 0 {
		o.maxBlockSize = o.sortedSizes[len(o.sortedSizes)-1]
	} else {
		o.maxBlockSize = 0
	}

	return nil
}
