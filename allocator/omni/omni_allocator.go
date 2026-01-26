package omni

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/cbehopkins/bobbob/allocator/block"
	"github.com/cbehopkins/bobbob/allocator/cache"
	"github.com/cbehopkins/bobbob/allocator/pool"
	"github.com/cbehopkins/bobbob/allocator/types"
)

var (
	ErrNoParentAllocator  = errors.New("parent allocator required")
	ErrNoMatchingPoolSize = errors.New("no pool supports requested size")
)

// OmniAllocator routes allocation requests to size-appropriate PoolAllocators
// and delegates oversized requests to the parent allocator. It can unload full
// pools into a PoolCache to keep memory usage low and rehydrate them on demand.
type OmniAllocator struct {
	blockSizes []int
	parent     types.Allocator
	file       *os.File
	cache      *cache.PoolCache
	pools      map[int]*pool.PoolAllocator
	onAllocate func(types.ObjectId, types.FileOffset, int)
}

func NewOmniAllocator(blockSizes []int, parent types.Allocator, file *os.File, pc *cache.PoolCache) (*OmniAllocator, error) {
	if parent == nil {
		return nil, ErrNoParentAllocator
	}
	normalized := normalizeSizes(blockSizes)
	if len(normalized) == 0 {
		normalized = []int{64, 256, 1024, 4096}
	}
	if pc == nil {
		var err error
		pc, err = cache.New()
		if err != nil {
			return nil, err
		}
	}
	oa := &OmniAllocator{
		blockSizes: normalized,
		parent:     parent,
		file:       file,
		cache:      pc,
		pools:      make(map[int]*pool.PoolAllocator),
	}
	return oa, nil
}

func (o *OmniAllocator) Allocate(size int) (types.ObjectId, types.FileOffset, error) {
	blockSize, ok := o.selectBlockSize(size)
	if ok {
		pl, err := o.ensurePool(blockSize)
		if err != nil {
			return 0, 0, err
		}
		o.pullAvailableFromCache(blockSize, pl)
		objId, offset, err := pl.Allocate(blockSize)
		if err == nil {
			o.fireOnAllocate(objId, offset, size)
		}
		return objId, offset, err
	}
	objId, offset, err := o.parent.Allocate(size)
	if err == nil {
		o.fireOnAllocate(objId, offset, size)
	}
	return objId, offset, err
}

func (o *OmniAllocator) AllocateRun(size int, count int) ([]types.ObjectId, []types.FileOffset, error) {
	blockSize, ok := o.selectBlockSize(size)
	if ok {
		pl, err := o.ensurePool(blockSize)
		if err != nil {
			return nil, nil, err
		}
		o.pullAvailableFromCache(blockSize, pl)
		objIds, offsets, err := pl.AllocateRun(blockSize, count)
		if err == nil {
			for i := range objIds {
				o.fireOnAllocate(objIds[i], offsets[i], size)
			}
		}
		return objIds, offsets, err
	}
	if parentRun, ok := o.parent.(types.ManyAllocatable); ok {
		objIds, offsets, err := parentRun.AllocateRun(size, count)
		if err == nil {
			for i := range objIds {
				o.fireOnAllocate(objIds[i], offsets[i], size)
			}
		}
		return objIds, offsets, err
	}
	return nil, nil, ErrNoMatchingPoolSize
}

func (o *OmniAllocator) DeleteObj(objId types.ObjectId) error {
	if pl := o.findPoolByOwnership(objId); pl != nil {
		return pl.DeleteObj(objId)
	}
	if pl := o.rehydrateForObject(objId); pl != nil {
		return pl.DeleteObj(objId)
	}
	return o.parent.DeleteObj(objId)
}

func (o *OmniAllocator) GetObjectInfo(objId types.ObjectId) (types.FileOffset, types.FileSize, error) {
	if pl := o.findPoolByOwnership(objId); pl != nil {
		return pl.GetObjectInfo(objId)
	}
	if pl := o.rehydrateForObject(objId); pl != nil {
		return pl.GetObjectInfo(objId)
	}
	return o.parent.GetObjectInfo(objId)
}

func (o *OmniAllocator) ContainsObjectId(objId types.ObjectId) bool {
	if pl := o.findPoolByOwnership(objId); pl != nil {
		return true
	}
	if o.cache != nil {
		if _, err := o.cache.Query(objId); err == nil {
			return true
		}
	}
	return o.parent.ContainsObjectId(objId)
}

func (o *OmniAllocator) GetFile() *os.File {
	if o.file != nil {
		return o.file
	}
	return o.parent.GetFile()
}

func (o *OmniAllocator) SetOnAllocate(callback func(types.ObjectId, types.FileOffset, int)) {
	o.onAllocate = callback
	for _, pl := range o.pools {
		pl.SetOnAllocate(callback)
	}
	if cbParent, ok := o.parent.(types.AllocateCallbackable); ok {
		cbParent.SetOnAllocate(callback)
	}
}

func (o *OmniAllocator) Marshal() ([]byte, error) {
	// Layout:
	// [poolCount:4]
	// repeat poolCount times:
	//   [blockSize:4][nextBlockCount:4][availableCount:4][fullCount:4]
	//   repeat availableCount times: [blkLen:4][blkData]
	//   repeat fullCount times: [blkLen:4][blkData]

	// Deterministic iteration by blockSize
	blockSizes := make([]int, 0, len(o.pools))
	for size := range o.pools {
		blockSizes = append(blockSizes, size)
	}
	sort.Ints(blockSizes)

	// If no pools yet, at least persist configured blockSizes and defaults
	if len(blockSizes) == 0 {
		blockSizes = append(blockSizes, o.blockSizes...)
	}

	buf := make([]byte, 0)
	poolCount := uint32(len(blockSizes))
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, poolCount)
	buf = append(buf, tmp...)

	for _, size := range blockSizes {
		pl := o.pools[size]
		if pl == nil {
			// Lazy-create empty pool to persist configuration
			var err error
			pl, err = pool.New(size, o.parent, o.file)
			if err != nil {
				return nil, err
			}
		}

		// Header for this pool
		poolHeader := make([]byte, 16)
		binary.BigEndian.PutUint32(poolHeader[0:4], uint32(size))
		binary.BigEndian.PutUint32(poolHeader[4:8], uint32(pl.NextBlockCount()))
		binary.BigEndian.PutUint32(poolHeader[8:12], uint32(len(pl.AvailableAllocators())))
		binary.BigEndian.PutUint32(poolHeader[12:16], uint32(len(pl.FullAllocators())))
		buf = append(buf, poolHeader...)

		serializeBlocks := func(blocks []*block.BlockAllocator) error {
			for _, blk := range blocks {
				blkData, err := blk.Marshal()
				if err != nil {
					return err
				}
				meta := make([]byte, 8)
				binary.BigEndian.PutUint32(meta[0:4], uint32(blk.BlockCount()))
				binary.BigEndian.PutUint32(meta[4:8], uint32(len(blkData)))
				buf = append(buf, meta...)
				buf = append(buf, blkData...)
			}
			return nil
		}

		if err := serializeBlocks(pl.AvailableAllocators()); err != nil {
			return nil, err
		}
		if err := serializeBlocks(pl.FullAllocators()); err != nil {
			return nil, err
		}
	}

	return buf, nil
}

func (o *OmniAllocator) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("insufficient data for unmarshal")
	}

	cursor := 0
	poolCount := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
	cursor += 4

	o.pools = make(map[int]*pool.PoolAllocator)
	o.blockSizes = make([]int, 0, poolCount)

	for i := 0; i < poolCount; i++ {
		if cursor+16 > len(data) {
			return fmt.Errorf("insufficient data for pool header %d", i)
		}
		blockSize := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		nextBlockCount := int(binary.BigEndian.Uint32(data[cursor+4 : cursor+8]))
		availCount := int(binary.BigEndian.Uint32(data[cursor+8 : cursor+12]))
		fullCount := int(binary.BigEndian.Uint32(data[cursor+12 : cursor+16]))
		cursor += 16

		pl, err := pool.New(blockSize, o.parent, o.file)
		if err != nil {
			return err
		}
		pl.SetNextBlockCount(nextBlockCount)
		if o.onAllocate != nil {
			pl.SetOnAllocate(o.onAllocate)
		}

		loadBlocks := func(count int) ([]*block.BlockAllocator, error) {
			blocks := make([]*block.BlockAllocator, 0, count)
			for j := 0; j < count; j++ {
				if cursor+8 > len(data) {
					return nil, fmt.Errorf("insufficient data for block meta (pool %d)", i)
				}
				blkCount := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
				blkLen := int(binary.BigEndian.Uint32(data[cursor+4 : cursor+8]))
				cursor += 8
				if cursor+blkLen > len(data) {
					return nil, fmt.Errorf("insufficient data for block body (pool %d)", i)
				}
				blkData := data[cursor : cursor+blkLen]
				cursor += blkLen

				blk := block.New(blockSize, blkCount, 0, 0, o.file)
				if err := blk.Unmarshal(blkData); err != nil {
					return nil, err
				}
				blocks = append(blocks, blk)
			}
			return blocks, nil
		}

		available, err := loadBlocks(availCount)
		if err != nil {
			return err
		}
		full, err := loadBlocks(fullCount)
		if err != nil {
			return err
		}

		pl.RestoreAllocators(available, full)
		o.pools[blockSize] = pl
		o.blockSizes = append(o.blockSizes, blockSize)
	}

	// Ensure sizes are normalized for Allocate routing
	o.blockSizes = normalizeSizes(o.blockSizes)
	return nil
}

func (o *OmniAllocator) Parent() types.Allocator {
	return o.parent
}

func (o *OmniAllocator) GetObjectIdsInAllocator(blockSize int, allocatorIndex int) []types.ObjectId {
	if pl, ok := o.pools[blockSize]; ok {
		ids := make([]types.ObjectId, 0)
		for _, blk := range plAvail(pl) {
			ids = append(ids, blk.GetObjectIdsInAllocator()...)
		}
		for _, blk := range plFull(pl) {
			ids = append(ids, blk.GetObjectIdsInAllocator()...)
		}
		return ids
	}
	if introspect, ok := o.parent.(interface {
		GetObjectIdsInAllocator(int, int) []types.ObjectId
	}); ok {
		return introspect.GetObjectIdsInAllocator(blockSize, allocatorIndex)
	}
	return nil
}

func (o *OmniAllocator) DelegateFullAllocatorsToCache() error {
	if o.cache == nil {
		return nil
	}
	for size, pl := range o.pools {
		full := pl.DrainFullAllocators()
		for _, blk := range full {
			entry := cache.UnloadedBlock{
				ObjId:          blk.BaseObjectId(),
				BaseObjId:      blk.BaseObjectId(),
				BaseFileOffset: blk.BaseFileOffset(),
				BlockSize:      types.FileSize(blk.BlockSize()),
				BlockCount:     blk.BlockCount(),
				Available:      false,
			}
			if err := o.cache.Insert(entry); err != nil {
				return err
			}
		}
		o.pools[size] = pl
	}
	return nil
}

func normalizeSizes(sizes []int) []int {
	seen := make(map[int]struct{})
	normalized := make([]int, 0, len(sizes))
	for _, sz := range sizes {
		if sz <= 0 {
			continue
		}
		if _, exists := seen[sz]; exists {
			continue
		}
		seen[sz] = struct{}{}
		normalized = append(normalized, sz)
	}
	sort.Ints(normalized)
	return normalized
}

func (o *OmniAllocator) selectBlockSize(size int) (int, bool) {
	idx := sort.SearchInts(o.blockSizes, size)
	if idx < len(o.blockSizes) {
		return o.blockSizes[idx], true
	}
	return 0, false
}

func (o *OmniAllocator) ensurePool(blockSize int) (*pool.PoolAllocator, error) {
	if pl, ok := o.pools[blockSize]; ok {
		return pl, nil
	}
	parent := o.parent
	pl, err := pool.New(blockSize, parent, o.GetFile())
	if err != nil {
		return nil, err
	}
	if o.onAllocate != nil {
		pl.SetOnAllocate(o.onAllocate)
	}
	o.pools[blockSize] = pl
	return pl, nil
}

func (o *OmniAllocator) findPoolByOwnership(objId types.ObjectId) *pool.PoolAllocator {
	for _, pl := range o.pools {
		if pl.ContainsObjectId(objId) {
			return pl
		}
	}
	return nil
}

func (o *OmniAllocator) pullAvailableFromCache(blockSize int, pl *pool.PoolAllocator) {
	if o.cache == nil {
		return
	}
	for _, entry := range o.cache.GetAll() {
		if int(entry.BlockSize) != blockSize || !entry.Available {
			continue
		}
	}
}

func (o *OmniAllocator) rehydrateForObject(objId types.ObjectId) *pool.PoolAllocator {
	if o.cache == nil {
		return nil
	}
	entry, err := o.cache.Query(objId)
	if err != nil {
		return nil
	}
	_ = entry
	return nil
}

func (o *OmniAllocator) fireOnAllocate(objId types.ObjectId, offset types.FileOffset, size int) {
	if o.onAllocate != nil {
		o.onAllocate(objId, offset, size)
	}
}

func binaryBigEndianPutUint32(buf []byte, v uint32) {
	buf[0] = byte(v >> 24)
	buf[1] = byte(v >> 16)
	buf[2] = byte(v >> 8)
	buf[3] = byte(v)
}

func binaryBigEndianUint32(buf []byte) uint32 {
	return uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
}

func plAvail(pl *pool.PoolAllocator) []*block.BlockAllocator {
	return pl.AvailableAllocators()
}

func plFull(pl *pool.PoolAllocator) []*block.BlockAllocator {
	return pl.FullAllocators()
}
