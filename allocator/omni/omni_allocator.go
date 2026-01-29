package omni

import (
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

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
// OmniAllocator is thread-safe and protects concurrent access to pools and cache.
type OmniAllocator struct {
	mu              sync.RWMutex
	blockSizes      []int
	parent          types.Allocator
	file            *os.File
	cache           *cache.PoolCache
	pools           map[int]*pool.PoolAllocator
	onAllocate      func(types.ObjectId, types.FileOffset, int)
	drainTicker     *time.Ticker
	drainDone       chan struct{}
	drainRunning    bool
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
	o.mu.Lock()
	defer o.mu.Unlock()

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
	o.mu.Lock()
	defer o.mu.Unlock()

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
	o.mu.Lock()
	defer o.mu.Unlock()

	if pl := o.findPoolByOwnership(objId); pl != nil {
		return pl.DeleteObj(objId)
	}
	if pl := o.rehydrateForObject(objId); pl != nil {
		return pl.DeleteObj(objId)
	}
	return o.parent.DeleteObj(objId)
}

func (o *OmniAllocator) GetObjectInfo(objId types.ObjectId) (types.FileOffset, types.FileSize, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if pl := o.findPoolByOwnership(objId); pl != nil {
		return pl.GetObjectInfo(objId)
	}
	if pl := o.rehydrateForObject(objId); pl != nil {
		return pl.GetObjectInfo(objId)
	}
	return o.parent.GetObjectInfo(objId)
}

func (o *OmniAllocator) ContainsObjectId(objId types.ObjectId) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()

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
	o.mu.Lock()
	defer o.mu.Unlock()

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
	// [blockSizesCount:4][blockSize:4]... (configuration)
	// [poolMetadataCount:4]
	// repeat poolMetadataCount times:
	//   [blockSize:4][nextBlockCount:4]
	// [cacheLen:4][cacheData...]

	// Step 1: Delegate ALL pool allocators (both available and full) to cache
	result, err := o.drainAllocators(true, true)
	if err != nil {
		return result, err
	}

	// Step 2: Persist configuration (blockSizes)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(o.blockSizes)))
	for _, size := range o.blockSizes {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(size))
		buf = append(buf, tmp...)
	}

	// Step 3: Persist pool metadata (nextBlockCount for each pool)
	// Sort pools by blockSize for deterministic output
	poolSizes := make([]int, 0, len(o.pools))
	for size := range o.pools {
		poolSizes = append(poolSizes, size)
	}
	sort.Ints(poolSizes)

	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(poolSizes)))
	buf = append(buf, tmp...)

	for _, size := range poolSizes {
		pl := o.pools[size]
		poolMeta := make([]byte, 8)
		binary.BigEndian.PutUint32(poolMeta[0:4], uint32(size))
		binary.BigEndian.PutUint32(poolMeta[4:8], uint32(pl.NextBlockCount()))
		buf = append(buf, poolMeta...)
	}

	// Step 4: Marshal the cache itself
	// o.cache is guaranteed to be non-nil from NewOmniAllocator
	var cacheData []byte
	cacheData, err = o.cache.Marshal()
	if err != nil {
		return nil, err
	}
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(cacheData)))
	buf = append(buf, tmp...)
	buf = append(buf, cacheData...)

	return buf, nil
}

// drainAllocators is a helper that drains allocators to cache with bitmap preservation.
// If drainFull is true, drains full allocators; if drainAvail is true, drains available allocators.
func (o *OmniAllocator) drainAllocators(drainFull, drainAvail bool) ([]byte, error) {
	var bitmapData []byte
	var err error

	for _, pl := range o.pools {
		if drainFull {
			// Drain full allocators to cache
			fullAllocators := pl.DrainFullAllocators()
			for _, blk := range fullAllocators {
				bitmapData, err = blk.Marshal()
				if err != nil {
					return nil, err
				}
				entry := cache.UnloadedBlock{
					ObjId:          blk.BaseObjectId(),
					BaseObjId:      blk.BaseObjectId(),
					BaseFileOffset: blk.BaseFileOffset(),
					BlockSize:      types.FileSize(blk.BlockSize()),
					BlockCount:     blk.BlockCount(),
					Available:      false,
					BitmapData:     bitmapData,
				}
				if err := o.cache.Insert(entry); err != nil {
					return nil, err
				}
			}
		}

		if drainAvail {
			// Get and drain available allocators to cache
			availAllocators := pl.AvailableAllocators()
			if len(availAllocators) > 0 {
				// Clear both available and full from pool (full ones were already drained above)
				pl.RestoreAllocators(make([]*block.BlockAllocator, 0), make([]*block.BlockAllocator, 0))

				for _, blk := range availAllocators {
					bitmapData, err = blk.Marshal()
					if err != nil {
						return nil, err
					}
					entry := cache.UnloadedBlock{
						ObjId:          blk.BaseObjectId(),
						BaseObjId:      blk.BaseObjectId(),
						BaseFileOffset: blk.BaseFileOffset(),
						BlockSize:      types.FileSize(blk.BlockSize()),
						BlockCount:     blk.BlockCount(),
						Available:      true,
						BitmapData:     bitmapData,
					}
					if err := o.cache.Insert(entry); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return nil, nil
}

func (o *OmniAllocator) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("insufficient data for unmarshal")
	}

	cursor := 0

	// Step 1: Restore blockSizes configuration
	blockSizesCount := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
	cursor += 4

	if cursor+blockSizesCount*4 > len(data) {
		return errors.New("insufficient data for blockSizes")
	}

	o.blockSizes = make([]int, blockSizesCount)
	for i := 0; i < blockSizesCount; i++ {
		o.blockSizes[i] = int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
	}

	// Step 2: Restore pool metadata (create empty pools with nextBlockCount)
	if cursor+4 > len(data) {
		return errors.New("insufficient data for pool metadata count")
	}

	poolMetaCount := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
	cursor += 4

	if cursor+poolMetaCount*8 > len(data) {
		return errors.New("insufficient data for pool metadata")
	}

	o.pools = make(map[int]*pool.PoolAllocator)
	for range poolMetaCount {
		blockSize := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		nextBlockCount := int(binary.BigEndian.Uint32(data[cursor+4 : cursor+8]))
		cursor += 8

		// Create empty pool (allocators will be rehydrated from cache on demand)
		pl, err := pool.New(blockSize, o.parent, o.file)
		if err != nil {
			return err
		}
		pl.SetNextBlockCount(nextBlockCount)
		if o.onAllocate != nil {
			pl.SetOnAllocate(o.onAllocate)
		}
		o.pools[blockSize] = pl
	}

	// Step 3: Unmarshal cache (contains all BlockAllocator metadata)
	if cursor+4 > len(data) {
		return errors.New("insufficient data for cache length")
	}

	cacheLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
	cursor += 4

	if cacheLen > 0 {
		if cursor+cacheLen > len(data) {
			return errors.New("insufficient data for cache")
		}

		cacheData := data[cursor : cursor+cacheLen]
		if err := o.cache.Unmarshal(cacheData); err != nil {
			return err
		}
		cursor += cacheLen
	}

	// Pools are now empty - BlockAllocators will be lazily rehydrated
	// from cache when objects are accessed via rehydrateForObject()

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
	_, err := o.drainAllocators(true, false)
	return err
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
	// First try to find an exact fit in configured sizes
	idx := sort.SearchInts(o.blockSizes, size)
	if idx < len(o.blockSizes) {
		return o.blockSizes[idx], true
	}

	// If no configured size is large enough, only delegate to parent if size
	// is larger than maxBlockSize. For sizes that fall through the cracks,
	// dynamically create a block allocator to avoid parent allocations for
	// small objects. Return true with the requested size as the block size.
	if len(o.blockSizes) > 0 && size <= o.blockSizes[len(o.blockSizes)-1]*2 {
		// Size is not more than 2x the largest configured block size,
		// so we'll create a dynamic block allocator for it
		return size, true
	}

	// Size is too large for block allocation, delegate to parent
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

	// Get all cache entries
	entries := o.cache.GetAll()
	entriesToDelete := make([]types.ObjectId, 0)

	for _, entry := range entries {
		// Find entries matching this blockSize that have available slots
		if int(entry.BlockSize) != blockSize || !entry.Available {
			continue
		}

		// Reconstruct the BlockAllocator from cache metadata and bitmap
		ba := block.New(
			int(entry.BlockSize),
			entry.BlockCount,
			entry.BaseFileOffset,
			entry.BaseObjId,
			o.file,
		)

		// Restore the bitmap state
		if len(entry.BitmapData) > 0 {
			if err := ba.Unmarshal(entry.BitmapData); err != nil {
				continue // Skip entries with bad bitmap data
			}
		}

		// Add to pool's available allocators
		_ = pl.AttachAllocator(ba, true)

		// Mark for deletion from cache (we've rehydrated it)
		entriesToDelete = append(entriesToDelete, entry.BaseObjId)
	}

	// Remove rehydrated entries from cache
	for _, baseObjId := range entriesToDelete {
		_ = o.cache.Delete(baseObjId)
	}
}

func (o *OmniAllocator) rehydrateForObject(objId types.ObjectId) *pool.PoolAllocator {
	if o.cache == nil {
		return nil
	}

	// Query cache for the block allocator containing this object
	entry, err := o.cache.Query(objId)
	if err != nil {
		return nil
	}

	// Get or create the pool for this block size
	pl, err := o.ensurePool(int(entry.BlockSize))
	if err != nil {
		return nil
	}

	// Reconstruct the BlockAllocator from cache metadata and bitmap
	ba := block.New(
		int(entry.BlockSize),
		entry.BlockCount,
		entry.BaseFileOffset,
		entry.BaseObjId,
		o.file,
	)

	// Restore the bitmap state (and startingFileOffset)
	if len(entry.BitmapData) > 0 {
		if err := ba.Unmarshal(entry.BitmapData); err != nil {
			return nil // Failed to restore bitmap
		}
		// After unmarshal, the startingObjectId will be derived from FileOffset
		// Ensure it matches what we expect from the cache entry
		if ba.BaseObjectId() != entry.BaseObjId {
			// The unmarshal may have derived a different ObjectId from FileOffset
			// This shouldn't happen if ObjectId == FileOffset by design
			// But if it does, we log it
			// For now, we trust the unmarshal to have done the right thing
		}
	}

	// Add to pool (available/full based on cache metadata)
	if entry.Available {
		_ = pl.AttachAllocator(ba, true)
	} else {
		_ = pl.AttachAllocator(ba, false)
	}

	// Remove from cache since we've rehydrated it
	_ = o.cache.Delete(entry.BaseObjId)

	return pl
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

// Close closes the OmniAllocator and cleans up all associated resources.
// This includes closing the internal PoolCache and any open pools.
func (o *OmniAllocator) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Close the cache
	if o.cache != nil {
		if err := o.cache.Close(); err != nil {
			return err
		}
	}

	// Stop drain worker if running
	o.stopDrainWorker()

	// Clear pools map (PoolAllocators don't need explicit closing)
	o.pools = nil

	return nil
}

// StartDrainWorker starts a background worker that periodically delegates full allocators to cache.
// interval specifies how often the draining occurs (defaults to 5 minutes if 0).
// The worker will run until Close() is called or StopDrainWorker() is called.
func (o *OmniAllocator) StartDrainWorker(interval time.Duration) error {
	o.mu.Lock()

	if o.drainRunning {
		o.mu.Unlock()
		return errors.New("drain worker already running")
	}

	if interval == 0 {
		interval = 5 * time.Minute
	}

	o.drainTicker = time.NewTicker(interval)
	o.drainDone = make(chan struct{})
	o.drainRunning = true

	// Capture references while holding the lock to pass to the goroutine.
	// This avoids race conditions where the goroutine accesses these fields
	// while StopDrainWorker() might be modifying them.
	ticker := o.drainTicker
	done := o.drainDone

	o.mu.Unlock()

	go o.drainWorker(ticker, done)

	return nil
}

// StopDrainWorker stops the background drain worker if it's running.
func (o *OmniAllocator) StopDrainWorker() {
	o.mu.Lock()
	o.stopDrainWorker()
	o.mu.Unlock()
}

// stopDrainWorker is the internal implementation (assumes lock is held).
func (o *OmniAllocator) stopDrainWorker() {
	if !o.drainRunning {
		return
	}
	o.drainRunning = false
	if o.drainTicker != nil {
		o.drainTicker.Stop()
	}
	if o.drainDone != nil {
		close(o.drainDone)
	}
}

// drainWorker runs in a goroutine and periodically drains full allocators.
// ticker and done are captured references to avoid race conditions.
func (o *OmniAllocator) drainWorker(ticker *time.Ticker, done chan struct{}) {
	for {
		select {
		case <-ticker.C:
			// Acquire lock and drain full allocators
			o.mu.Lock()
			_ = o.DelegateFullAllocatorsToCache()
			o.mu.Unlock()
		case <-done:
			// Stop the ticker when exiting
			ticker.Stop()
			return
		}
	}
}

func plAvail(pl *pool.PoolAllocator) []*block.BlockAllocator {
	return pl.AvailableAllocators()
}

func plFull(pl *pool.PoolAllocator) []*block.BlockAllocator {
	return pl.FullAllocators()
}
