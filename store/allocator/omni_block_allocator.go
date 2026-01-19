package allocator

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

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
	maxBlockSize int        // largest block size, objects larger than this use parent
	mu           sync.Mutex // guards pool state for concurrent callers
	// OnAllocate is called after a successful allocation (for testing/monitoring).
	OnAllocate func(objId ObjectId, offset FileOffset, size int)
	// File handle for direct I/O operations
	file *os.File
}

type omniBlockAllocatorOptions struct {
	preallocate bool
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

// OmniBlockAllocatorOption configures optional behaviors for NewOmniBlockAllocator.
type OmniBlockAllocatorOption func(*omniBlockAllocatorOptions)

// WithoutPreallocation builds the allocator metadata without reserving space from the parent.
// CRITICAL: Use this when loading persisted data via Unmarshal to prevent double allocation.
// When unmarshaling, BlockAllocators already have their file ranges stored in the serialized data.
// If we called parent.Allocate() again during construction, we would duplicate space reservations
// in the parent's internal maps, causing file offset conflicts and potential data corruption.
// Default behavior (preallocate=true) is correct for new allocators, which need upfront space.
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
// file is the file handle for direct I/O operations (can be nil if not needed).
// opts can be used to disable upfront parent allocations (e.g., when unmarshaling persisted data).
func NewOmniBlockAllocator(blockSize []int, blockCount int, parent Allocator, file *os.File, opts ...OmniBlockAllocatorOption) (*omniBlockAllocator, error) {
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
	var maxBlockSize int
	if len(uniqueSizes) > 0 {
		maxBlockSize = uniqueSizes[len(uniqueSizes)-1]
	}

	if config.preallocate {
		// Assign non-overlapping ObjectId ranges to each block allocator using the parent allocator.
		// NOTE: This is skipped when loading persisted data (WithoutPreallocation option) because
		// Unmarshal will restore BlockAllocators with their existing file ranges. Re-allocating here
		// would cause the parent to reserve the same space twice, leading to file corruption.
		for _, size := range uniqueSizes {
			totalSize := blockCount * size
			baseObjId, fileOffset, err := parent.Allocate(totalSize)
			if err != nil {
				return nil, fmt.Errorf("prealloc block size %d: %w", size, err)
			}

			ba := NewBlockAllocator(size, blockCount, fileOffset, baseObjId, file)
			pool, ok := blockMap[size]
			if !ok {
				pool = NewAllocatorPool(size, blockCount, parent, file)
				blockMap[size] = pool
			}
			ref := &allocatorRef{
				allocator: ba,
				file:      file,
			}
			pool.available = append(pool.available, ref)
		}
	}

	return &omniBlockAllocator{
		blockMap:     blockMap,
		sortedSizes:  uniqueSizes,
		blockCount:   blockCount,
		parent:       parent,
		maxBlockSize: maxBlockSize,
		file:         file,
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

	// Try best-fit: find the smallest block size that can hold this object
	if size <= o.maxBlockSize {
		// Binary search for the first block size >= requested size
		idx := sort.SearchInts(o.sortedSizes, size)

		if idx < len(o.sortedSizes) {
			blockSize := o.sortedSizes[idx]
			pool := o.blockMap[blockSize]
			if pool == nil {
				pool = NewAllocatorPool(blockSize, o.blockCount, o.parent, o.file)
				o.blockMap[blockSize] = pool
			}

			// Delegate to pool for allocation
			id, offset, _, err := pool.Allocate()
			if err != nil {
				return 0, 0, err
			}

			// Call postAllocate callback if set
			if o.postAllocate != nil {
				if postErr := o.postAllocate(id, offset); postErr != nil {
					return 0, 0, postErr
				}
			}

			// Set requested size on the allocator that owns this ID
			o.setRequestedSize(id, size, blockSize)
			if o.OnAllocate != nil {
				o.OnAllocate(id, offset, size)
			}
			return id, offset, nil

		}
	}

	// Object too large: defer to parent allocator
	id, offset, err := o.parent.Allocate(size)
	if err != nil {
		return 0, 0, err
	}

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

// setRequestedSize finds the pool for the given blockSize and delegates to it.
func (o *omniBlockAllocator) setRequestedSize(objId ObjectId, size, blockSize int) {
	pool := o.blockMap[blockSize]
	if pool == nil {
		return
	}
	pool.SetRequestedSize(objId, size)
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
				pool = NewAllocatorPool(blockSize, o.blockCount, o.parent, o.file)
				o.blockMap[blockSize] = pool
			}

			// Delegate to pool for run allocation
			objIds, offsets, _, err := pool.AllocateRun(count)
			if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
				return nil, nil, err
			}

			// Set requested size for all allocated objects
			for _, objId := range objIds {
				o.setRequestedSize(objId, size, blockSize)
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

	// Try best-fit: find the smallest block size that would have held this object
	if size <= o.maxBlockSize {
		idx := sort.SearchInts(o.sortedSizes, size)

		if idx < len(o.sortedSizes) {
			blockSize := o.sortedSizes[idx]
			pool := o.blockMap[blockSize]

			// Delegate to pool for freeing
			if pool != nil {
				err := pool.freeFromPool(fileOffset, blockSize, o.postFree)
				if err == nil {
					// Successfully freed from this pool
					return nil
				}
				// If not found in this pool, fall through to try parent allocator
			}
		}
	}

	// Object either too large for block allocators, or not found in the best-fit pool.
	err := o.parent.Free(fileOffset, size)

	if o.postFree != nil {
		if postErr := o.postFree(fileOffset, size); postErr != nil {
			return postErr
		}
	}

	return err
}

// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
// With rangeCache removed we now scan pools in sorted size order and only fall
// back to the parent if no pool claims ownership. This prevents parent
// allocators from reporting objects that belong to block pools (e.g., after a
// slot was freed). Falls back to parent allocator if ObjectId not found in
// local allocators.
func (o *omniBlockAllocator) GetObjectInfo(objId ObjectId) (FileOffset, int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Search pools in deterministic (sorted) block-size order; only consult parent
	// if no pool claims the ObjectId.
	for _, blockSize := range o.sortedSizes {
		pool := o.blockMap[blockSize]
		if pool == nil {
			continue
		}
		if pool.ContainsObjectId(objId) {
			return pool.GetObjectInfo(objId, blockSize)
		}
	}

	// Not found in pools, fall back to parent allocator
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
		// Serialize complete pool (metadata + allocator data)
		poolData, err := pool.Marshal()
		if err != nil {
			return nil, err
		}
		data = append(data, poolData...)
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
	existingSizes := o.sortedSizes

	sizes := make([]int, 0, numSizes)

	for i := 0; i < numSizes; i++ {
		if len(data) < offset+4 {
			return errors.New("invalid data length for size header")
		}
		size := int(data[offset])<<24 | int(data[offset+1])<<16 |
			int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		// Create pool with proper initialization
		pool := NewAllocatorPool(size, o.blockCount, o.parent, o.file)
		poolBytes, err := pool.Unmarshal(data[offset:], o.blockCount)
		if err != nil {
			return err
		}
		offset += poolBytes

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

// UnmarshalMultiple implements a simple UnmarshalComplex-style loader for a single object payload.
// It reads all bytes from objData and delegates to Unmarshal.
// The ObjReader is unused in this implementation because the omni allocator serializes
// to a single blob.
func (o *omniBlockAllocator) UnmarshalMultiple(objData io.Reader, _ any) error {
	bytes, err := io.ReadAll(objData)
	if err != nil {
		return err
	}
	return o.Unmarshal(bytes)
}

// PreMarshal implements the MarshalComplex interface by serializing the allocator
// and returning the resulting size as a single allocation request.
func (o *omniBlockAllocator) PreMarshal() ([]int, error) {
	data, err := o.Marshal()
	if err != nil {
		return nil, err
	}
	return []int{len(data)}, nil
}

// MarshalMultiple implements the MarshalComplex interface for a single-object payload.
// It writes the serialized allocator into the provided object ID and returns that ID
// as the identity function result.
func (o *omniBlockAllocator) MarshalMultiple(objectIds []ObjectId) (func() ObjectId, []ObjectAndByteFunc, error) {
	if len(objectIds) == 0 {
		return nil, nil, errors.New("no objectIds provided to MarshalMultiple")
	}

	data, err := o.Marshal()
	if err != nil {
		return nil, nil, err
	}

	objId := objectIds[0]
	identityFunc := func() ObjectId { return objId }
	objectAndByteFuncs := []ObjectAndByteFunc{
		{
			ObjectId: objId,
			ByteFunc: func() ([]byte, error) { return data, nil },
		},
	}

	return identityFunc, objectAndByteFuncs, nil
}

// Delete is a no-op for the allocator implementation.
func (o *omniBlockAllocator) Delete() error {
	return nil
}
