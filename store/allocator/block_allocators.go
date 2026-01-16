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

// allocatorRef wraps a blockAllocator with its persistent ObjectId for storage management.
type allocatorRef struct {
	ObjectId  ObjectId
	allocator *blockAllocator
}

// Delegation methods for allocatorRef to blockAllocator methods
func (r *allocatorRef) Allocate(size int) (ObjectId, FileOffset, error) {
	return r.allocator.Allocate(size)
}

func (r *allocatorRef) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	return r.allocator.AllocateRun(size, count)
}

func (r *allocatorRef) Free(fileOffset FileOffset, size int) error {
	return r.allocator.Free(fileOffset, size)
}

func (r *allocatorRef) setRequestedSize(objId ObjectId, size int) {
	r.allocator.setRequestedSize(objId, size)
}

func (r *allocatorRef) ContainsObjectId(objId ObjectId) bool {
	return r.allocator.ContainsObjectId(objId)
}

func (r *allocatorRef) objectIdForOffset(fileOffset FileOffset) (ObjectId, bool) {
	return r.allocator.objectIdForOffset(fileOffset)
}

func (r *allocatorRef) GetFileOffset(objId ObjectId) (FileOffset, error) {
	return r.allocator.GetFileOffset(objId)
}

func (r *allocatorRef) requestedSize(objId ObjectId) (int, bool) {
	return r.allocator.requestedSize(objId)
}

func (r *allocatorRef) Marshal() ([]byte, error) {
	return r.allocator.Marshal()
}

// allocatorPool keeps available and full block allocators for a given size.
// The pool manages both the allocators and their persistence metadata (ObjectIds).
type allocatorPool struct {
	available []*allocatorRef
	full      []*allocatorRef
}

// Marshal serializes the allocator pool state.
// Returns the serialized data containing all available and full allocators.
func (p *allocatorPool) Marshal() ([]byte, error) {
	data := make([]byte, 0)

	// Serialize available allocators count
	availCount := len(p.available)
	data = append(data,
		byte(availCount>>24), byte(availCount>>16),
		byte(availCount>>8), byte(availCount))

	// Serialize each available allocator's ObjectId
	for _, ref := range p.available {
		data = append(data,
			byte(ref.ObjectId>>56), byte(ref.ObjectId>>48), byte(ref.ObjectId>>40), byte(ref.ObjectId>>32),
			byte(ref.ObjectId>>24), byte(ref.ObjectId>>16), byte(ref.ObjectId>>8), byte(ref.ObjectId))
	}

	// Serialize full allocators count
	fullCount := len(p.full)
	data = append(data,
		byte(fullCount>>24), byte(fullCount>>16),
		byte(fullCount>>8), byte(fullCount))

	// Serialize each full allocator's ObjectId
	for _, ref := range p.full {
		data = append(data,
			byte(ref.ObjectId>>56), byte(ref.ObjectId>>48), byte(ref.ObjectId>>40), byte(ref.ObjectId>>32),
			byte(ref.ObjectId>>24), byte(ref.ObjectId>>16), byte(ref.ObjectId>>8), byte(ref.ObjectId))
	}

	return data, nil
}

// Unmarshal deserializes the allocator pool state.
// Note: allocator pointers are NOT reconstructed from disk - they must be provided separately.
// This method only restores the ObjectId metadata.
func (p *allocatorPool) Unmarshal(data []byte, blockCount int) error {
	if len(data) < 4 {
		return errors.New("invalid pool data length")
	}

	offset := 0

	// Deserialize available allocators count
	availCount := int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Deserialize available allocators (create refs with nil allocators - will be populated later)
	p.available = make([]*allocatorRef, availCount)
	for i := 0; i < availCount; i++ {
		if len(data) < offset+8 {
			return errors.New("invalid available allocator data")
		}
		objId := ObjectId(data[offset])<<56 | ObjectId(data[offset+1])<<48 | ObjectId(data[offset+2])<<40 | ObjectId(data[offset+3])<<32 |
			ObjectId(data[offset+4])<<24 | ObjectId(data[offset+5])<<16 | ObjectId(data[offset+6])<<8 | ObjectId(data[offset+7])
		offset += 8

		p.available[i] = &allocatorRef{
			ObjectId:  objId,
			allocator: nil, // Will be set by caller
		}
	}

	// Deserialize full allocators count
	if len(data) < offset+4 {
		return errors.New("invalid pool data length for full count")
	}
	fullCount := int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Deserialize full allocators
	p.full = make([]*allocatorRef, fullCount)
	for i := 0; i < fullCount; i++ {
		if len(data) < offset+8 {
			return errors.New("invalid full allocator data")
		}
		objId := ObjectId(data[offset])<<56 | ObjectId(data[offset+1])<<48 | ObjectId(data[offset+2])<<40 | ObjectId(data[offset+3])<<32 |
			ObjectId(data[offset+4])<<24 | ObjectId(data[offset+5])<<16 | ObjectId(data[offset+6])<<8 | ObjectId(data[offset+7])
		offset += 8

		p.full[i] = &allocatorRef{
			ObjectId:  objId,
			allocator: nil, // Will be set by caller
		}
	}

	return nil
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
			ref := &allocatorRef{
				ObjectId:  baseObjId,
				allocator: ba,
			}
			pool.available = append(pool.available, ref)

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
				ref := pool.available[0]
				allocator := ref.allocator
				id, offset, err = allocator.Allocate(blockSize)
				if errors.Is(err, AllAllocated) {
					// move to full list
					pool.full = append(pool.full, ref)
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
			newRef := &allocatorRef{
				ObjectId:  baseObjId,
				allocator: newAllocator,
			}
			pool.available = append(pool.available, newRef)
			if o.idx != nil {
				_ = o.idx.RegisterRange(int64(baseObjId), blockSize, fileOffset)
				_ = o.idx.UpdateRangeEnd(int64(baseObjId), int64(baseObjId)+int64(o.blockCount))
			}
			id, offset, err = newRef.allocator.Allocate(blockSize)
			if err == nil {
				newRef.allocator.setRequestedSize(id, size)
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
				ref := pool.available[i]
				allocator := ref.allocator
				objIds, offsets, err := allocator.AllocateRun(blockSize, count)
				if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
					return nil, nil, err
				}
				// If no slots available, move to full list
				if len(objIds) == 0 {
					pool.full = append(pool.full, ref)
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
			newRef := &allocatorRef{
				ObjectId:  baseObjId,
				allocator: newAllocator,
			}
			pool.available = append(pool.available, newRef)
			if o.idx != nil {
				_ = o.idx.RegisterRange(int64(baseObjId), blockSize, fileOffset)
				_ = o.idx.UpdateRangeEnd(int64(baseObjId), int64(baseObjId)+int64(o.blockCount))
			}

			objIds, offsets, err := newRef.allocator.AllocateRun(blockSize, count)
			if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
				return nil, nil, err
			}
			for _, objId := range objIds {
				newRef.allocator.setRequestedSize(objId, size)
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
				for _, ref := range pool.available {
					if ref == nil || ref.allocator == nil {
						continue
					}
					allocator := ref.allocator
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
				for i, ref := range pool.full {
					if ref == nil || ref.allocator == nil {
						continue
					}
					allocator := ref.allocator
					if fileOffset < allocator.startingFileOffset || fileOffset >= allocator.startingFileOffset+FileOffset(allocator.blockCount*allocator.blockSize) {
						continue
					}
					if objId, ok := allocator.objectIdForOffset(fileOffset); ok {
						allocator.setRequestedSize(objId, 0)
					}
					err = allocator.Free(fileOffset, blockSize)
					if err == nil {
						pool.available = append(pool.available, ref)
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
				for _, ref := range pool.available {
					if ref == nil || ref.allocator == nil {
						continue
					}
					allocator := ref.allocator
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
				for _, ref := range pool.full {
					if ref == nil || ref.allocator == nil {
						continue
					}
					allocator := ref.allocator
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
			ref := &allocatorRef{
				ObjectId:  allocator.startingObjectId,
				allocator: allocator,
			}
			pool.available = append(pool.available, ref)
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
