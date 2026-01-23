package allocator

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/cbehopkins/bobbob/internal"
)

// fileOffsetReader provides LateReadObj by reading directly from FileOffsets in the file.
// This is used during unmarshal when allocators are not yet functional.
type fileOffsetReader struct {
	file *os.File
}

// LateReadObj reads allocatorRef metadata from a FileOffset (passed as ObjectId for interface compatibility).
// Returns a SectionReader positioned at the blockAllocator's marshaled data.
func (r *fileOffsetReader) LateReadObj(id ObjectId) (io.Reader, func() error, error) {
	// The "ObjectId" here is actually a FileOffset from the LUT
	fileOff := FileOffset(id)

	// Read the blockAllocator metadata header to determine size
	// blockAllocator.Marshal format: [startingFileOffset:8][blockCount:4][blockSize:4][allocatedBitmap:blockCount bytes][requestedSizes:blockCount*4 bytes]
	headerSize := 16 // startingFileOffset + blockCount + blockSize
	header := make([]byte, headerSize)
	if _, err := r.file.ReadAt(header, int64(fileOff)); err != nil {
		return nil, nil, err
	}

	blockCount := int(header[8])<<24 | int(header[9])<<16 | int(header[10])<<8 | int(header[11])
	metadataSize := headerSize + blockCount + (blockCount * 4) // header + bitmap + requestedSizes

	reader := io.NewSectionReader(r.file, int64(fileOff), int64(metadataSize))
	return reader, func() error { return nil }, nil
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
	maxBlockSize int        // largest block size, objects larger than this use parent
	mu           sync.Mutex // guards pool state for concurrent callers
	stopPersist  chan struct{}
	persistWG    sync.WaitGroup
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
			id, offset, err := pool.Allocate()
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
			objIds, offsets, err := pool.AllocateRun(count)
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

// DeleteObj removes an object by ObjectId, resolving its location and freeing the space.
// This is the high-level API that wraps Free() with ObjectId-based lookup.
func (o *omniBlockAllocator) DeleteObj(objId ObjectId) error {
	// Get object info to find FileOffset and size
	fileOffset, size, err := o.GetObjectInfo(objId)
	if err != nil {
		return err
	}
	
	// Delegate to existing Free implementation
	return o.Free(fileOffset, size)
}

// FindSmallestBlockAllocatorForSize exposes the pool-level helper used during
// application compaction. It locates the smallest allocator (by blockCount)
// that can satisfy the given size, but only when that allocator is smaller
// than the current pool configuration.
func (o *omniBlockAllocator) FindSmallestBlockAllocatorForSize(size int) (int, int, int, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	idx := sort.SearchInts(o.sortedSizes, size)
	if idx >= len(o.sortedSizes) {
		return 0, 0, 0, false
	}

	blockSize := o.sortedSizes[idx]
	pool := o.blockMap[blockSize]
	if pool == nil {
		return 0, 0, 0, false
	}

	blockSize, allocatorIndex, blockCount, found := pool.FindSmallestBlockAllocatorForSize(size)
	if !found {
		return 0, 0, 0, false
	}

	return blockSize, allocatorIndex, blockCount, true
}

// GetObjectIdsInAllocator returns all ObjectIds allocated in the specified
// allocator (index scoped to the pool for blockSize). The slice is copied to
// avoid leaking internal slices to callers.
func (o *omniBlockAllocator) GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []ObjectId {
	o.mu.Lock()
	defer o.mu.Unlock()

	pool := o.blockMap[blockSize]
	if pool == nil {
		return nil
	}
	ids := pool.GetObjectIdsInAllocator(blockSize, allocatorIndex)
	if len(ids) == 0 {
		return nil
	}

	result := make([]ObjectId, len(ids))
	copy(result, ids)
	return result
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
// Format: [blockCount:4][numSizes:4][size1:4][poolObjId1:8][size2:4][poolObjId2:8]...
// Each pool is now stored using MarshalComplex and referenced by its identity ObjectId.
func (o *omniBlockAllocator) Marshal() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
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
		// Serialize block size
		data = append(data,
			byte(size>>24), byte(size>>16),
			byte(size>>8), byte(size))

		// Marshal pool using MarshalComplex with retry
		allocFunc := func(poolSizes []int) ([]ObjectId, error) {
			objIds := make([]ObjectId, len(poolSizes))
			for i, sz := range poolSizes {
				objId, _, err := o.parent.Allocate(sz)
				if err != nil {
					return nil, err
				}
				objIds[i] = objId
			}
			return objIds, nil
		}

		poolObjId, objectAndByteFuncs, err := internal.MarshalComplexWithRetry(pool, allocFunc, 10)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal pool for size %d: %w", size, err)
		}

		// Write pool data to parent allocator and get FileOffset for LUT
		var lutFileOffset FileOffset
		for _, obj := range objectAndByteFuncs {
			bytes, err := obj.ByteFunc()
			if err != nil {
				return nil, fmt.Errorf("failed to get bytes for pool object: %w", err)
			}
			offset, _, err := o.parent.(interface {
				GetObjectInfo(ObjectId) (FileOffset, int, error)
			}).GetObjectInfo(obj.ObjectId)
			if err != nil {
				return nil, fmt.Errorf("failed to get object info: %w", err)
			}
			if _, err := o.file.WriteAt(bytes, int64(offset)); err != nil {
				return nil, fmt.Errorf("failed to write pool data: %w", err)
			}
			// The first object is the LUT - save its FileOffset
			if obj.ObjectId == poolObjId {
				lutFileOffset = offset
			}
		}

		// Serialize the LUT's FileOffset (8 bytes) so unmarshal can read directly
		data = append(data,
			byte(lutFileOffset>>56), byte(lutFileOffset>>48),
			byte(lutFileOffset>>40), byte(lutFileOffset>>32),
			byte(lutFileOffset>>24), byte(lutFileOffset>>16),
			byte(lutFileOffset>>8), byte(lutFileOffset))
	}

	// Flush parent allocator cache to backing store so pool ObjectIds are persisted
	if flusher, ok := o.parent.(interface {
		FlushCacheToBacking(percent int)
	}); ok {
		flusher.FlushCacheToBacking(100)
	}

	return data, nil
}

// Unmarshal deserializes the omniBlockAllocator's state.
func (o *omniBlockAllocator) Unmarshal(data []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

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

	// Create a FileOffset-based reader for unmarshal (allocators not functional yet)
	reader := &fileOffsetReader{file: o.file}

	for i := 0; i < numSizes; i++ {
		if len(data) < offset+12 {
			return errors.New("invalid data length for size and FileOffset header")
		}
		size := int(data[offset])<<24 | int(data[offset+1])<<16 |
			int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		// Read LUT FileOffset (8 bytes) - this is where the LUT data is in the file
		lutFileOffset := FileOffset(int64(data[offset])<<56 | int64(data[offset+1])<<48 |
			int64(data[offset+2])<<40 | int64(data[offset+3])<<32 |
			int64(data[offset+4])<<24 | int64(data[offset+5])<<16 |
			int64(data[offset+6])<<8 | int64(data[offset+7]))
		offset += 8

		// Create pool with proper initialization
		pool := NewAllocatorPool(size, o.blockCount, o.parent, o.file)

		// Read LUT header to determine size
		lutHeader := make([]byte, 8)
		if _, err := o.file.ReadAt(lutHeader, int64(lutFileOffset)); err != nil {
			return fmt.Errorf("failed to read LUT header for size %d: %w", size, err)
		}

		availCount := int(lutHeader[0])<<24 | int(lutHeader[1])<<16 | int(lutHeader[2])<<8 | int(lutHeader[3])
		fullCount := int(lutHeader[4])<<24 | int(lutHeader[5])<<16 | int(lutHeader[6])<<8 | int(lutHeader[7])
		totalCount := availCount + fullCount

		// Calculate LUT size: header (8 bytes) + (totalCount * 8 bytes for each FileOffset)
		lutSize := 8 + (totalCount * 8)

		// Read full LUT data from file
		lutData := make([]byte, lutSize)
		if _, err := o.file.ReadAt(lutData, int64(lutFileOffset)); err != nil {
			return fmt.Errorf("failed to read pool LUT for size %d: %w", size, err)
		}

		// Unmarshal pool using UnmarshalMultiple
		if err := pool.UnmarshalMultiple(io.NopCloser(bytes.NewReader(lutData)), reader); err != nil {
			return fmt.Errorf("failed to unmarshal pool for size %d: %w", size, err)
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
