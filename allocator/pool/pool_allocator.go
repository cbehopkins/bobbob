package pool

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/cbehopkins/bobbob/allocator/block"
	"github.com/cbehopkins/bobbob/allocator/types"
)

var (
	ErrSizeMismatch      = errors.New("size must match pool block size")
	ErrNoParent          = errors.New("parent allocator required")
	ErrAllocatorNotFound = errors.New("allocator not found for ObjectId")
)

const (
	initialBlockCount = 1024
	maxBlockCount     = 32768
)

// ParentAllocator defines the interface needed from parent (typically BasicAllocator).
type ParentAllocator interface {
	Allocate(size int) (types.ObjectId, types.FileOffset, error)
	DeleteObj(objId types.ObjectId) error
	GetFile() *os.File
}

// PoolAllocator manages fixed-size allocations using multiple BlockAllocators.
// Maintains separate lists for available and full BlockAllocators.
type PoolAllocator struct {
	blockSize      int
	nextBlockCount int // Next BlockAllocator will have this many slots
	parent         ParentAllocator
	file           *os.File

	// Available BlockAllocators (have free slots)
	available []*block.BlockAllocator

	// Full BlockAllocators (all slots allocated)
	full []*block.BlockAllocator

	// Optional callback fired on each allocation
	onAllocate func(types.ObjectId, types.FileOffset, int)
}

// New creates a new PoolAllocator for fixed-size blocks.
func New(blockSize int, parent ParentAllocator, file *os.File) (*PoolAllocator, error) {
	if parent == nil {
		return nil, ErrNoParent
	}

	return &PoolAllocator{
		blockSize:      blockSize,
		nextBlockCount: initialBlockCount,
		parent:         parent,
		file:           file,
		available:      make([]*block.BlockAllocator, 0),
		full:           make([]*block.BlockAllocator, 0),
	}, nil
}

// Allocate allocates a single block from the pool.
// Size must match the pool's blockSize.
func (p *PoolAllocator) Allocate(size int) (types.ObjectId, types.FileOffset, error) {
	if size != p.blockSize {
		return 0, 0, ErrSizeMismatch
	}

	// Try available BlockAllocators
	for i, blkAlloc := range p.available {
		objId, offset, err := blkAlloc.Allocate(size)
		if err == nil {
			// Success - fire callback if registered
			if p.onAllocate != nil {
				p.onAllocate(objId, offset, size)
			}

			// Check if this allocator is now full
			_, free, _ := blkAlloc.Stats()
			if free == 0 {
				// Move to full list
				p.full = append(p.full, blkAlloc)
				p.available = append(p.available[:i], p.available[i+1:]...)
			}

			return objId, offset, nil
		}
	}

	// No available allocators with space - create new one
	newAlloc, err := p.createBlockAllocator()
	if err != nil {
		return 0, 0, err
	}

	p.available = append(p.available, newAlloc)

	// Allocate from new allocator
	objId, offset, err := newAlloc.Allocate(size)
	if err != nil {
		return 0, 0, err
	}

	if p.onAllocate != nil {
		p.onAllocate(objId, offset, size)
	}

	return objId, offset, nil
}

// NextBlockCount returns the planned block count for the next BlockAllocator created.
func (p *PoolAllocator) NextBlockCount() int {
	return p.nextBlockCount
}

// SetNextBlockCount restores the nextBlockCount cursor (used during unmarshal).
func (p *PoolAllocator) SetNextBlockCount(count int) {
	p.nextBlockCount = count
}

// AllocateRun allocates a contiguous run of blocks.
// Delegates to a single BlockAllocator for contiguity.
func (p *PoolAllocator) AllocateRun(size int, count int) ([]types.ObjectId, []types.FileOffset, error) {
	if size != p.blockSize {
		return nil, nil, ErrSizeMismatch
	}

	// Try available BlockAllocators
	for i, blkAlloc := range p.available {
		objIds, offsets, err := blkAlloc.AllocateRun(size, count)
		if err != nil {
			continue
		}

		if len(objIds) > 0 {
			// Fire callbacks
			if p.onAllocate != nil {
				for j := range objIds {
					p.onAllocate(objIds[j], offsets[j], size)
				}
			}

			// Check if allocator is now full
			_, free, _ := blkAlloc.Stats()
			if free == 0 {
				p.full = append(p.full, blkAlloc)
				p.available = append(p.available[:i], p.available[i+1:]...)
			}

			return objIds, offsets, nil
		}
	}

	// No allocator had enough contiguous space - create new one
	newAlloc, err := p.createBlockAllocator()
	if err != nil {
		return nil, nil, err
	}

	p.available = append(p.available, newAlloc)

	objIds, offsets, err := newAlloc.AllocateRun(size, count)
	if err != nil {
		return nil, nil, err
	}

	if p.onAllocate != nil {
		for j := range objIds {
			p.onAllocate(objIds[j], offsets[j], size)
		}
	}

	return objIds, offsets, nil
}

// DeleteObj frees a block by ObjectId.
func (p *PoolAllocator) DeleteObj(objId types.ObjectId) error {
	// Check available allocators
	for _, blkAlloc := range p.available {
		if blkAlloc.ContainsObjectId(objId) {
			return blkAlloc.DeleteObj(objId)
		}
	}

	// Check full allocators
	for i, blkAlloc := range p.full {
		if blkAlloc.ContainsObjectId(objId) {
			err := blkAlloc.DeleteObj(objId)
			if err != nil {
				return err
			}

			// Move from full to available
			p.available = append(p.available, blkAlloc)
			p.full = append(p.full[:i], p.full[i+1:]...)

			return nil
		}
	}

	return ErrAllocatorNotFound
}

// GetObjectInfo returns the FileOffset and Size for an ObjectId.
func (p *PoolAllocator) GetObjectInfo(objId types.ObjectId) (types.FileOffset, types.FileSize, error) {
	// Check available allocators
	for _, blkAlloc := range p.available {
		if blkAlloc.ContainsObjectId(objId) {
			return blkAlloc.GetObjectInfo(objId)
		}
	}

	// Check full allocators
	for _, blkAlloc := range p.full {
		if blkAlloc.ContainsObjectId(objId) {
			return blkAlloc.GetObjectInfo(objId)
		}
	}

	return 0, 0, ErrAllocatorNotFound
}

// GetFile returns the file handle.
func (p *PoolAllocator) GetFile() *os.File {
	return p.file
}

// ContainsObjectId returns true if this pool owns the ObjectId.
func (p *PoolAllocator) ContainsObjectId(objId types.ObjectId) bool {
	// Check available allocators
	// FIXME: we should be able to search the lists
	// if the lists are in order by ObjectId ranges
	for _, blkAlloc := range p.available {
		if blkAlloc.ContainsObjectId(objId) {
			return true
		}
	}

	// Check full allocators
	for _, blkAlloc := range p.full {
		if blkAlloc.ContainsObjectId(objId) {
			return true
		}
	}

	return false
}

// SetOnAllocate registers a callback invoked after each allocation.
func (p *PoolAllocator) SetOnAllocate(callback func(types.ObjectId, types.FileOffset, int)) {
	p.onAllocate = callback
}

// DrainFullAllocators removes all full allocators from the pool and returns them.
// Useful when delegating allocator ownership to the PoolCache.
func (p *PoolAllocator) DrainFullAllocators() []*block.BlockAllocator {
	allocs := p.full
	p.full = make([]*block.BlockAllocator, 0)
	return allocs
}

// AttachAllocator attaches an existing BlockAllocator back to the pool.
// If available is true it is placed in the available list, otherwise in the full list.
func (p *PoolAllocator) AttachAllocator(alloc *block.BlockAllocator, available bool) error {
	if alloc == nil {
		return ErrAllocatorNotFound
	}
	if alloc.BlockSize() != p.blockSize {
		return ErrSizeMismatch
	}

	if available {
		p.available = append(p.available, alloc)
	} else {
		p.full = append(p.full, alloc)
	}
	return nil
}

// Marshal serializes the pool state to bytes.
// Format: blockSize (4) | nextBlockCount (4) | numAvailable (4) | numFull (4) | [availableObjIds] | [fullObjIds]
func (p *PoolAllocator) Marshal() ([]byte, error) {
	// Persist all BlockAllocators first and collect their ObjectIds
	availableObjIds := make([]types.ObjectId, 0, len(p.available))
	for _, blkAlloc := range p.available {
		allocData, err := blkAlloc.Marshal()
		if err != nil {
			return nil, err
		}

		// Request space from parent
		objId, offset, err := p.parent.Allocate(len(allocData))
		if err != nil {
			return nil, err
		}

		// Write BlockAllocator data to file
		if _, err := p.file.WriteAt(allocData, int64(offset)); err != nil {
			return nil, err
		}

		availableObjIds = append(availableObjIds, objId)
	}

	fullObjIds := make([]types.ObjectId, 0, len(p.full))
	for _, blkAlloc := range p.full {
		allocData, err := blkAlloc.Marshal()
		if err != nil {
			return nil, err
		}

		objId, offset, err := p.parent.Allocate(len(allocData))
		if err != nil {
			return nil, err
		}

		if _, err := p.file.WriteAt(allocData, int64(offset)); err != nil {
			return nil, err
		}

		fullObjIds = append(fullObjIds, objId)
	}

	// Marshal pool metadata
	totalSize := 16 + (len(availableObjIds) * 8) + (len(fullObjIds) * 8)
	data := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(data[0:4], uint32(p.blockSize))
	binary.LittleEndian.PutUint32(data[4:8], uint32(p.nextBlockCount))
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(availableObjIds)))
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(fullObjIds)))

	offset := 16
	for _, objId := range availableObjIds {
		binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(objId))
		offset += 8
	}

	for _, objId := range fullObjIds {
		binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(objId))
		offset += 8
	}

	return data, nil
}

// Unmarshal restores pool state from bytes.
// Note: BlockAllocators are NOT loaded into memory - they remain as ObjectIds for lazy loading.
func (p *PoolAllocator) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return errors.New("insufficient data for pool unmarshal")
	}

	p.blockSize = int(binary.LittleEndian.Uint32(data[0:4]))
	p.nextBlockCount = int(binary.LittleEndian.Uint32(data[4:8]))
	numAvailable := int(binary.LittleEndian.Uint32(data[8:12]))
	numFull := int(binary.LittleEndian.Uint32(data[12:16]))

	expectedSize := 16 + ((numAvailable + numFull) * 8)
	if len(data) < expectedSize {
		return errors.New("insufficient data for pool entries")
	}

	// For now, we leave allocators unloaded
	// PoolCache will handle lazy loading and delegating to this pool
	// Clear existing allocators
	p.available = make([]*block.BlockAllocator, 0)
	p.full = make([]*block.BlockAllocator, 0)

	// In future: store ObjectIds for lazy loading via PoolCache
	// For now: empty pool that will create allocators on demand

	return nil
}

// createBlockAllocator creates a new BlockAllocator from parent.
func (p *PoolAllocator) createBlockAllocator() (*block.BlockAllocator, error) {
	// Calculate size needed for block allocator's range
	rangeSize := p.blockSize * p.nextBlockCount

	// Request space from parent
	parentObjId, parentOffset, err := p.parent.Allocate(rangeSize)
	if err != nil {
		return nil, err
	}

	// Create BlockAllocator
	blkAlloc := block.New(
		p.blockSize,
		p.nextBlockCount,
		parentOffset,
		parentObjId,
		p.file,
	)

	// Update nextBlockCount for next allocator (double, capped at max)
	if p.nextBlockCount < maxBlockCount {
		p.nextBlockCount *= 2
		if p.nextBlockCount > maxBlockCount {
			p.nextBlockCount = maxBlockCount
		}
	}

	return blkAlloc, nil
}

// Stats returns allocation statistics across all BlockAllocators.
func (p *PoolAllocator) Stats() (allocated, free, total int) {
	for _, blkAlloc := range p.available {
		a, f, t := blkAlloc.Stats()
		allocated += a
		free += f
		total += t
	}

	for _, blkAlloc := range p.full {
		a, f, t := blkAlloc.Stats()
		allocated += a
		free += f
		total += t
	}

	return
}

// AvailableAllocators returns a copy of the allocators that currently have free slots.
func (p *PoolAllocator) AvailableAllocators() []*block.BlockAllocator {
	allocs := make([]*block.BlockAllocator, len(p.available))
	copy(allocs, p.available)
	return allocs
}

// FullAllocators returns a copy of the allocators that are fully allocated.
func (p *PoolAllocator) FullAllocators() []*block.BlockAllocator {
	allocs := make([]*block.BlockAllocator, len(p.full))
	copy(allocs, p.full)
	return allocs
}

// RestoreAllocators replaces the available and full allocator lists.
func (p *PoolAllocator) RestoreAllocators(available []*block.BlockAllocator, full []*block.BlockAllocator) {
	p.available = available
	p.full = full
}
