package allocator

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator/basic"
	"github.com/cbehopkins/bobbob/allocator/omni"
	"github.com/cbehopkins/bobbob/allocator/types"
)

// Top is a concrete implementation of types.TopAllocator.
// It manages the full allocator hierarchy: BasicAllocator (parent), OmniAllocator (child),
// and coordinates their persistence through PrimeTable.
//
// Hierarchy:
//   - BasicAllocator: manages large/variable-size allocations
//   - OmniAllocator: manages fixed-size allocations through PoolAllocators (internally uses PoolCache)
//   - PrimeTable: bootstrap index for loading allocators from disk
type Top struct {
	mu             sync.RWMutex // protects allocator state for Save/Load
	file           *os.File
	basicAllocator *basic.BasicAllocator
	omniAllocator  *omni.OmniAllocator
	primeTable     *PrimeTable
	storeMeta      FileInfo
	blockSizes     []int // Configuration for OmniAllocator pools
	maxBlockCount  int   // Configuration for BlockAllocators
}

// NewTop creates a new Top allocator for a fresh file.
// It wires up BasicAllocator, OmniAllocator (with internal PoolCache), and PrimeTable.
func NewTop(file *os.File, blockSizes []int, maxBlockCount int) (*Top, error) {
	if file == nil {
		return nil, fmt.Errorf("file cannot be nil")
	}

	// Create BasicAllocator
	basicAlloc, err := basic.NewWithFileBasedTracking(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create BasicAllocator: %w", err)
	}

	// Create OmniAllocator with BasicAllocator as parent
	// (PoolCache is created internally by OmniAllocator)
	omniAlloc, err := omni.NewOmniAllocator(blockSizes, basicAlloc, file, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create OmniAllocator: %w", err)
	}

	// Create PrimeTable and register slots
	primeTable := NewPrimeTable()
	primeTable.Add() // Slot 0: BasicAllocator
	primeTable.Add() // Slot 1: OmniAllocator
	primeTable.Add() // Slot 2: Allocator state metadata (reserved by store layer)

	// Reserve the PrimeTable space so BasicAllocator starts after it.
	primeSize := primeTable.SizeInBytes()
	basicAlloc.ReservePrefix(types.FileOffset(primeSize))

	return &Top{
		file:           file,
		basicAllocator: basicAlloc,
		omniAllocator:  omniAlloc,
		primeTable:     primeTable,
		blockSizes:     blockSizes,
		maxBlockCount:  maxBlockCount,
	}, nil
}

// NewTopFromFile loads an existing Top allocator from disk.
// It reads PrimeTable from offset 0, then unmarshals BasicAllocator and OmniAllocator.
func NewTopFromFile(file *os.File, blockSizes []int, maxBlockCount int) (*Top, error) {
	if file == nil {
		return nil, fmt.Errorf("file cannot be nil")
	}

	// Read PrimeTable from offset 0 with exact sizing
	header := make([]byte, 4)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("failed to read PrimeTable header: %w", err)
	}

	entryCount := binary.LittleEndian.Uint32(header)
	primeSize := 4 + (int(entryCount) * 24)
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat allocator file: %w", err)
	}
	if int64(primeSize) > info.Size() {
		return nil, fmt.Errorf("prime table truncated: need %d bytes, file size %d", primeSize, info.Size())
	}
	primeData := make([]byte, primeSize)
	copy(primeData, header)
	if primeSize > 4 {
		if _, err := file.ReadAt(primeData[4:], 4); err != nil {
			return nil, fmt.Errorf("failed to read PrimeTable body: %w", err)
		}
	}

	// Unmarshal PrimeTable
	primeTable := NewPrimeTable()
	if err := primeTable.Unmarshal(primeData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PrimeTable: %w", err)
	}

	// Load BasicAllocator
	basicAlloc, err := basic.New(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create BasicAllocator for loading: %w", err)
	}

	basicInfo, err := primeTable.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to read BasicAllocator info from PrimeTable: %w", err)
	}

	basicData := make([]byte, basicInfo.Sz)
	if _, err := file.ReadAt(basicData, int64(basicInfo.Fo)); err != nil {
		return nil, fmt.Errorf("failed to read BasicAllocator data from file: %w", err)
	}

	if err := basicAlloc.Unmarshal(basicData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BasicAllocator: %w", err)
	}

	// After loading, prevent allocations from using the PrimeTable region
	basicAlloc.ReservePrefix(types.FileOffset(primeSize))
	basicAlloc.RemoveGapsBefore(types.FileOffset(primeSize))

	// Load OmniAllocator (with internally-created PoolCache)
	omniAlloc, err := omni.NewOmniAllocator(blockSizes, basicAlloc, file, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create OmniAllocator for loading: %w", err)
	}

	omniInfo, err := primeTable.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to read OmniAllocator info from PrimeTable: %w", err)
	}

	omniData := make([]byte, omniInfo.Sz)
	if _, err := file.ReadAt(omniData, int64(omniInfo.Fo)); err != nil {
		return nil, fmt.Errorf("failed to read OmniAllocator data from file (offset=%d, size=%d): %w", omniInfo.Fo, omniInfo.Sz, err)
	}

	if err := omniAlloc.Unmarshal(omniData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal OmniAllocator (data size=%d): %w", len(omniData), err)
	}

	// Load store metadata info
	storeInfo, err := primeTable.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to read store metadata from PrimeTable: %w", err)
	}

	return &Top{
		file:           file,
		basicAllocator: basicAlloc,
		omniAllocator:  omniAlloc,
		primeTable:     primeTable,
		storeMeta:      storeInfo,
		blockSizes:     blockSizes,
		maxBlockCount:  maxBlockCount,
	}, nil
}

// Allocate delegates to OmniAllocator (preferred for fixed-size allocations) or BasicAllocator.
func (t *Top) Allocate(size int) (bobbob.ObjectId, bobbob.FileOffset, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.Allocate(size)
}

// AllocateAtParent allocates directly from the parent BasicAllocator (no block pooling).
// Useful for metadata objects that must have exact sizes (e.g., prime object).
func (t *Top) AllocateAtParent(size int) (types.ObjectId, types.FileOffset, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.basicAllocator.Allocate(size)
}

// DeleteObj delegates to OmniAllocator (which routes to appropriate sub-allocator).
func (t *Top) DeleteObj(objId bobbob.ObjectId) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.DeleteObj(objId)
}

// GetObjectInfo delegates to OmniAllocator (which routes to appropriate sub-allocator).
func (t *Top) GetObjectInfo(objId bobbob.ObjectId) (bobbob.FileOffset, bobbob.FileSize, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.GetObjectInfo(objId)
}

// GetFile returns the file handle.
func (t *Top) GetFile() *os.File {
	return t.file
}

// ContainsObjectId delegates to OmniAllocator.
func (t *Top) ContainsObjectId(objId bobbob.ObjectId) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.ContainsObjectId(objId)
}

// AllocateRun delegates to OmniAllocator if supported.
func (t *Top) AllocateRun(size int, count int) ([]bobbob.ObjectId, []bobbob.FileOffset, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.AllocateRun(size, count)
}

// SetOnAllocate delegates to OmniAllocator if supported.
func (t *Top) SetOnAllocate(cb func(bobbob.ObjectId, bobbob.FileOffset, int)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.omniAllocator.SetOnAllocate(cb)
}

// Parent returns the BasicAllocator (parent of OmniAllocator).
// This allows external code to distinguish between allocations handled by
// OmniAllocator (small, fixed-size) vs BasicAllocator (large, variable-size).
func (t *Top) Parent() types.Allocator {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.basicAllocator
}

// Marshal delegates to OmniAllocator.
func (t *Top) Marshal() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.Marshal()
}

// Unmarshal delegates to OmniAllocator.
func (t *Top) Unmarshal(data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.omniAllocator.Unmarshal(data)
}

// Save persists the entire allocator hierarchy to disk.
// Sequence:
//  1. Marshal OmniAllocator, allocate from BasicAllocator, write to file
//  2. Update PrimeTable with OmniAllocator ObjectId
//  3. Marshal BasicAllocator, append at current file length (special behavior - see comment below)
//  4. Update PrimeTable with BasicAllocator ObjectId
//  5. Marshal and write PrimeTable at offset 0 (atomic commit)
//
// IMPORTANT: BasicAllocator is special - it does NOT allocate space for itself!
// Instead, it appends its marshaled data directly at the current file length.
// This works because:
//  - BasicAllocator is the last allocator to persist
//  - No more objects will be created after BasicAllocator's data is written
//  - PrimeTable is always written last (at offset 0), so it can overwrite offset 0..primeSize
//
// When loading in a new session, NewTopFromFile must call basicAlloc.ReservePrefix()
// to prevent gaps from being reconstructed at offset 0..primeSize, which would allow
// new allocations to overwrite the PrimeTable.
//
// Note: PoolCache unloading should be coordinated at a higher level (e.g., by the application)
// before calling Save(). This ensures flexibility in unload timing and strategy.
func (t *Top) Save() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Ensure store metadata was provided by the store layer.
	if t.storeMeta.Sz == 0 {
		return fmt.Errorf("store metadata not set before Save")
	}

	// Step 1: Marshal and persist OmniAllocator
	omniData, err := t.omniAllocator.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal OmniAllocator: %w", err)
	}

	omniObjId, omniOffset, err := t.basicAllocator.Allocate(len(omniData))
	if err != nil {
		return fmt.Errorf("failed to allocate space for OmniAllocator: %w", err)
	}

	_, err = t.file.WriteAt(omniData, int64(omniOffset))
	if err != nil {
		return fmt.Errorf("failed to write OmniAllocator data to file: %w", err)
	}

	// Step 2: Update PrimeTable with Store metadata (index 2)
	if err := t.primeTable.Store(t.storeMeta); err != nil {
		return fmt.Errorf("failed to store store metadata in PrimeTable: %w", err)
	}

	// Step 3: Update PrimeTable with OmniAllocator (reverse order: store last allocator first)
	if err := t.primeTable.Store(FileInfo{
		ObjId: omniObjId,
		Fo:    omniOffset,
		Sz:    types.FileSize(len(omniData)),
	}); err != nil {
		return fmt.Errorf("failed to store OmniAllocator info in PrimeTable: %w", err)
	}

	// Step 4: Marshal and persist BasicAllocator
	basicData, err := t.basicAllocator.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal BasicAllocator: %w", err)
	}

	// BasicAllocator is special: it doesn't allocate space for itself.
	// Instead, it appends directly at the current file length.
	// This works because BasicAllocator is the last allocator to persist,
	// so we know no more objects will be created after it.
	basicOffset := t.basicAllocator.FileLength()
	basicObjId := types.ObjectId(basicOffset)

	_, err = t.file.WriteAt(basicData, int64(basicOffset))
	if err != nil {
		return fmt.Errorf("failed to write BasicAllocator data to file: %w", err)
	}

	// Step 5: Update PrimeTable with BasicAllocator
	if err := t.primeTable.Store(FileInfo{
		ObjId: basicObjId,
		Fo:    basicOffset,
		Sz:    types.FileSize(len(basicData)),
	}); err != nil {
		return fmt.Errorf("failed to store BasicAllocator info in PrimeTable: %w", err)
	}

	// Step 6: Marshal and persist PrimeTable (atomic commit)
	primeData, err := t.primeTable.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal PrimeTable: %w", err)
	}

	_, err = t.file.WriteAt(primeData, 0)
	if err != nil {
		return fmt.Errorf("failed to write PrimeTable to file: %w", err)
	}

	return t.file.Sync()
}

// Load restores the allocator state from disk.
// This method is primarily for use by NewTopFromFile; it updates the current allocators
// by unmarshaling data that was previously read and stored.
func (t *Top) Load() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Read and unmarshal PrimeTable
	primeData := make([]byte, 1024)
	n, err := t.file.ReadAt(primeData, 0)
	if err != nil {
		return fmt.Errorf("failed to read PrimeTable: %w", err)
	}

	if err := t.primeTable.Unmarshal(primeData[:n]); err != nil {
		return fmt.Errorf("failed to unmarshal PrimeTable: %w", err)
	}

	// Load BasicAllocator
	basicInfo, err := t.primeTable.Load()
	if err != nil {
		return fmt.Errorf("failed to read BasicAllocator info from PrimeTable: %w", err)
	}

	basicData := make([]byte, basicInfo.Sz)
	n, err = t.file.ReadAt(basicData, int64(basicInfo.Fo))
	if err != nil {
		return fmt.Errorf("failed to read BasicAllocator data from file: %w", err)
	}

	if err := t.basicAllocator.Unmarshal(basicData[:n]); err != nil {
		return fmt.Errorf("failed to unmarshal BasicAllocator: %w", err)
	}

	// Load OmniAllocator
	omniInfo, err := t.primeTable.Load()
	if err != nil {
		return fmt.Errorf("failed to read OmniAllocator info from PrimeTable: %w", err)
	}

	omniData := make([]byte, omniInfo.Sz)
	n, err = t.file.ReadAt(omniData, int64(omniInfo.Fo))
	if err != nil {
		return fmt.Errorf("failed to read OmniAllocator data from file: %w", err)
	}

	if err := t.omniAllocator.Unmarshal(omniData[:n]); err != nil {
		return fmt.Errorf("failed to unmarshal OmniAllocator: %w", err)
	}

	// Load store metadata
	storeInfo, err := t.primeTable.Load()
	if err != nil {
		return fmt.Errorf("failed to read store metadata from PrimeTable: %w", err)
	}

	t.storeMeta = storeInfo

	return nil
}

// SetStoreMeta records allocator state metadata to be written into the PrimeTable during Save.
// This provides a slot for the store layer to persist its own metadata.
func (t *Top) SetStoreMeta(fi FileInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.storeMeta = fi
}

// GetStoreMeta returns the allocator state metadata captured during load.
func (t *Top) GetStoreMeta() FileInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.storeMeta
}

// Close releases resources associated with the Top allocator.
// This must be called to properly close any file-based trackers.
func (t *Top) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Close OmniAllocator first (it manages PoolCache)
	if t.omniAllocator != nil {
		if err := t.omniAllocator.Close(); err != nil {
			return err
		}
	}

	// Close BasicAllocator
	if t.basicAllocator != nil {
		return t.basicAllocator.Close()
	}
	return nil
}

// Ensure Top implements types.TopAllocator.
var _ types.TopAllocator = (*Top)(nil)
