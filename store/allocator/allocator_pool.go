package allocator

import (
	"errors"
	"os"
)

// allocatorRef wraps a blockAllocator with its persistent storage management.
type allocatorRef struct {
	allocator *blockAllocator
	// File handle for direct I/O operations
	file *os.File
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

func (r *allocatorRef) ContainsObjectId(objId ObjectId) bool {
	if r == nil {
		return false
	}
	return r.allocator.ContainsObjectId(objId)
}

func (r *allocatorRef) GetFileOffset(objId ObjectId) (FileOffset, error) {
	return r.allocator.GetFileOffset(objId)
}

func (r *allocatorRef) Marshal() ([]byte, error) {
	return r.allocator.Marshal()
}

// GetObjectInfo checks if this allocator contains the ObjectId and returns its metadata.
// Returns FileOffset and size if found, or an error if not found or invalid.
func (r *allocatorRef) GetObjectInfo(objId ObjectId, blockSize int) (FileOffset, int, error) {
	if r == nil {
		return 0, 0, errors.New("allocatorRef is nil")
	}
	if !r.ContainsObjectId(objId) {
		return 0, 0, errors.New("ObjectId not in allocator range")
	}

	realOff, allocErr := r.allocator.GetFileOffset(objId)
	if allocErr != nil {
		return 0, 0, allocErr
	}

	sz := blockSize
	if requested, ok := r.allocator.requestedSize(objId); ok {
		sz = requested
	}

	return realOff, sz, nil
}

type allocatorSlice []*allocatorRef

// Marshal serializes the allocator slice.
// Returns: [count:4][allocatorData...]
func (s allocatorSlice) Marshal() ([]byte, error) {
	data := make([]byte, 0)

	// Serialize slice count
	count := len(s)
	data = append(data,
		byte(count>>24), byte(count>>16),
		byte(count>>8), byte(count))

	// Serialize allocator state data (includes startingFileOffset via blockAllocator.Marshal)
	for _, allocator := range s {
		allocatorData, err := allocator.Marshal()
		if err != nil {
			return nil, err
		}
		data = append(data, allocatorData...)
	}

	return data, nil
}

// GetObjectInfo searches for an ObjectId within this slice's allocators.
// Returns the FileOffset and size if found, or an error if not found.
func (s allocatorSlice) GetObjectInfo(objId ObjectId, blockSize int) (FileOffset, int, error) {
	for _, ref := range s {
		if !ref.ContainsObjectId(objId) {
			continue
		}
		realOff, sz, err := ref.GetObjectInfo(objId, blockSize)
		if err == nil {
			return realOff, sz, nil
		}
	}
	return 0, 0, errors.New("ObjectId not found in slice")
}

// Unmarshal deserializes the allocator slice.
// Reconstructs allocator state data (ObjectIds are derived from startingFileOffset in blockAllocator).
// Returns the number of bytes consumed, or an error.
func (s *allocatorSlice) Unmarshal(data []byte, blockCount int) (int, error) {
	if len(data) < 4 {
		return 0, errors.New("invalid slice data length")
	}

	offset := 0

	// Deserialize slice count
	count := int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Deserialize allocator state data (includes startingFileOffset via blockAllocator.Unmarshal)
	bitCount := (blockCount + 7) / 8
	allocatorDataSize := 8 + bitCount + 2*blockCount

	*s = make(allocatorSlice, count)
	for i := range count {
		if len(data) < offset+allocatorDataSize {
			return 0, errors.New("invalid allocator state data")
		}
		allocator := &blockAllocator{
			blockSize:      0, // Will be set by caller
			blockCount:     blockCount,
			allocatedList:  make([]bool, blockCount),
			requestedSizes: make([]int, blockCount),
		}
		if err := allocator.Unmarshal(data[offset : offset+allocatorDataSize]); err != nil {
			return 0, err
		}
		offset += allocatorDataSize
		(*s)[i] = &allocatorRef{
			allocator: allocator,
		}
	}

	return offset, nil
}

// allocatorPool keeps available and full block allocators for a given size.
// The pool manages both the allocators and their persistence metadata (ObjectIds).
type allocatorPool struct {
	available allocatorSlice
	full      allocatorSlice
	// File handle for direct I/O operations
	file *os.File
	// Parent allocator for provisioning new block allocators
	parent Allocator
	// Block size for this pool
	blockSize int
	// Number of blocks per allocator
	blockCount int
}

// ContainsObjectId checks if any allocator in this pool owns the given ObjectId.
func (p *allocatorPool) ContainsObjectId(objId ObjectId) bool {
	for _, ref := range p.available {
		if ref != nil && ref.allocator != nil && ref.allocator.ContainsObjectId(objId) {
			return true
		}
	}
	for _, ref := range p.full {
		if ref != nil && ref.allocator != nil && ref.allocator.ContainsObjectId(objId) {
			return true
		}
	}
	return false
}

// NewAllocatorPool creates a new allocator pool for a specific block size.
func NewAllocatorPool(blockSize, blockCount int, parent Allocator, file *os.File) *allocatorPool {
	return &allocatorPool{
		available:  make(allocatorSlice, 0),
		full:       make(allocatorSlice, 0),
		file:       file,
		parent:     parent,
		blockSize:  blockSize,
		blockCount: blockCount,
	}
}

// Marshal serializes the complete allocator pool state.
// Uses the allocatorSlice Marshal methods for each list.
func (p *allocatorPool) Marshal() ([]byte, error) {
	// Serialize available allocators
	availData, err := p.available.Marshal()
	if err != nil {
		return nil, err
	}

	// Serialize full allocators
	fullData, err := p.full.Marshal()
	if err != nil {
		return nil, err
	}

	// Combine both
	return append(availData, fullData...), nil
}

// Unmarshal deserializes the complete allocator pool state.
// Uses the allocatorSlice Unmarshal methods for each list.
// Returns the number of bytes consumed, or an error.
func (p *allocatorPool) Unmarshal(data []byte, blockCount int) (int, error) {
	offset := 0

	// Deserialize available allocators
	availBytes, err := p.available.Unmarshal(data[offset:], blockCount)
	if err != nil {
		return 0, err
	}
	offset += availBytes

	// Deserialize full allocators
	fullBytes, err := p.full.Unmarshal(data[offset:], blockCount)
	if err != nil {
		return 0, err
	}
	offset += fullBytes

	// Set blockSize on all unmarshaled allocators
	for _, ref := range p.available {
		if ref != nil && ref.allocator != nil {
			ref.allocator.blockSize = p.blockSize
		}
	}
	for _, ref := range p.full {
		if ref != nil && ref.allocator != nil {
			ref.allocator.blockSize = p.blockSize
		}
	}

	return offset, nil
}

// freeFromPool attempts to free a block from this pool's allocators.
// Checks both available and full allocators. If the block is in a full allocator,
// moves that allocator back to available after freeing.
// Returns nil if successfully freed, error if not found or if free fails.
func (p *allocatorPool) freeFromPool(fileOffset FileOffset, blockSize int, postFree func(FileOffset, int) error) error {
	// Check available allocators first
	for _, ref := range p.available {
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
		err := allocator.Free(fileOffset, blockSize)
		if err == nil {
			if postFree != nil {
				if postErr := postFree(fileOffset, blockSize); postErr != nil {
					return postErr
				}
			}
			return nil
		}
	}

	// Then check allocators previously marked full; once a slot is freed, move it back to available
	for i, ref := range p.full {
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
		err := allocator.Free(fileOffset, blockSize)
		if err == nil {
			p.available = append(p.available, ref)
			p.full = append(p.full[:i], p.full[i+1:]...)
			if postFree != nil {
				if postErr := postFree(fileOffset, blockSize); postErr != nil {
					return postErr
				}
			}
			return nil
		}
	}

	return errors.New("block not found in pool")
}

// GetObjectInfo searches for an ObjectId within this pool's allocators.
// Returns the FileOffset and size if found, or an error if not found.
// Checks both available and full allocators.
func (p *allocatorPool) GetObjectInfo(objId ObjectId, blockSize int) (FileOffset, int, error) {
	// Check available allocators first
	if realOff, sz, err := p.available.GetObjectInfo(objId, blockSize); err == nil {
		return realOff, sz, nil
	}

	// Check full allocators
	return p.full.GetObjectInfo(objId, blockSize)
}

// Allocate attempts to allocate a block from this pool.
// It tries available allocators first, moving them to full if they become exhausted.
// If no available allocators have space, it provisions a new allocator from the parent.
// Returns the ObjectId, FileOffset, whether a new allocator was created, and any error.
//
// CODE SMELL: Returning *allocatorRef couples the pool to caller's needs (omni uses this
// to register new allocators in rangeCache). Consider observer pattern or callback instead.
func (p *allocatorPool) Allocate() (ObjectId, FileOffset, *allocatorRef, error) {
	// Try available allocators first
	for len(p.available) > 0 {
		ref := p.available[0]
		allocator := ref.allocator
		id, offset, err := allocator.Allocate(p.blockSize)
		if errors.Is(err, AllAllocated) {
			// Move to full list
			p.full = append(p.full, ref)
			p.available = p.available[1:]
			continue
		}
		if err != nil {
			return 0, 0, nil, err
		}
		return id, offset, nil, nil
	}

	// No available allocators or all were full: provision a new one from parent
	baseObjId, fileOffset, err := p.parent.Allocate(p.blockSize * p.blockCount)
	if err != nil {
		return 0, 0, nil, err
	}

	newAllocator := NewBlockAllocator(p.blockSize, p.blockCount, fileOffset, baseObjId, p.file)
	newRef := &allocatorRef{
		allocator: newAllocator,
		file:      p.file,
	}
	p.available = append(p.available, newRef)

	// Allocate from the new allocator
	id, offset, err := newRef.allocator.Allocate(p.blockSize)
	if err != nil {
		return 0, 0, nil, err
	}
	return id, offset, newRef, nil
}

// SetRequestedSize finds the allocator containing objId and sets its requested size.
// Searches both available and full allocator lists.
func (p *allocatorPool) SetRequestedSize(objId ObjectId, size int) {
	// Check available allocators
	for _, ref := range p.available {
		if ref.allocator.ContainsObjectId(objId) {
			ref.allocator.setRequestedSize(objId, size)
			return
		}
	}

	// Check full allocators
	for _, ref := range p.full {
		if ref.allocator.ContainsObjectId(objId) {
			ref.allocator.setRequestedSize(objId, size)
			return
		}
	}
}

// AllocateRun attempts to allocate a contiguous run of blocks from this pool.
// It tries available allocators first, moving them to full if they become exhausted.
// If no available allocators have space, it provisions a new allocator from the parent.
// Returns partial results if fewer than requested blocks are available, along with the allocator ref if a new one was created.
//
// CODE SMELL: Returning *allocatorRef couples the pool to caller's needs (omni uses this
// to register new allocators in rangeCache). Consider observer pattern or callback instead.
func (p *allocatorPool) AllocateRun(requestCount int) ([]ObjectId, []FileOffset, *allocatorRef, error) {
	// Try available allocators - they now return partial results
	for i := 0; i < len(p.available); {
		ref := p.available[i]
		allocator := ref.allocator
		objIds, offsets, err := allocator.AllocateRun(p.blockSize, requestCount)
		if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
			return nil, nil, nil, err
		}
		// If no slots available, move to full list
		if len(objIds) == 0 {
			p.full = append(p.full, ref)
			p.available = append(p.available[:i], p.available[i+1:]...)
			continue
		}
		// Accept partial allocation
		return objIds, offsets, nil, nil
	}

	// Need a new allocator segment
	baseObjId, fileOffset, err := p.parent.Allocate(p.blockSize * p.blockCount)
	if err != nil {
		return []ObjectId{}, []FileOffset{}, nil, nil
	}

	newAllocator := NewBlockAllocator(p.blockSize, p.blockCount, fileOffset, baseObjId, p.file)
	newRef := &allocatorRef{
		allocator: newAllocator,
		file:      p.file,
	}
	p.available = append(p.available, newRef)

	objIds, offsets, err := newRef.allocator.AllocateRun(p.blockSize, requestCount)
	return objIds, offsets, newRef, err
}
