package allocator

import (
	"fmt"
	"os"
	"sync"

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
	basicAlloc, err := basic.New(file)
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

	// Read PrimeTable from offset 0
	primeData := make([]byte, 1024) // Conservative initial size for PrimeTable
	n, err := file.ReadAt(primeData, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read PrimeTable: %w", err)
	}

	// Unmarshal PrimeTable
	primeTable := NewPrimeTable()
	if err := primeTable.Unmarshal(primeData[:n]); err != nil {
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
	n, err = file.ReadAt(basicData, int64(basicInfo.Fo))
	if err != nil {
		return nil, fmt.Errorf("failed to read BasicAllocator data from file: %w", err)
	}

	if err := basicAlloc.Unmarshal(basicData[:n]); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BasicAllocator: %w", err)
	}

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
	n, err = file.ReadAt(omniData, int64(omniInfo.Fo))
	if err != nil {
		return nil, fmt.Errorf("failed to read OmniAllocator data from file: %w", err)
	}

	if err := omniAlloc.Unmarshal(omniData[:n]); err != nil {
		return nil, fmt.Errorf("failed to unmarshal OmniAllocator: %w", err)
	}

	return &Top{
		file:           file,
		basicAllocator: basicAlloc,
		omniAllocator:  omniAlloc,
		primeTable:     primeTable,
		blockSizes:     blockSizes,
		maxBlockCount:  maxBlockCount,
	}, nil
}


// Allocate delegates to OmniAllocator (preferred for fixed-size allocations) or BasicAllocator.
func (t *Top) Allocate(size int) (types.ObjectId, types.FileOffset, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.Allocate(size)
}

// DeleteObj delegates to OmniAllocator (which routes to appropriate sub-allocator).
func (t *Top) DeleteObj(objId types.ObjectId) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.DeleteObj(objId)
}

// GetObjectInfo delegates to OmniAllocator (which routes to appropriate sub-allocator).
func (t *Top) GetObjectInfo(objId types.ObjectId) (types.FileOffset, types.FileSize, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.GetObjectInfo(objId)
}

// GetFile returns the file handle.
func (t *Top) GetFile() *os.File {
	return t.file
}

// ContainsObjectId delegates to OmniAllocator.
func (t *Top) ContainsObjectId(objId types.ObjectId) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.ContainsObjectId(objId)
}

// AllocateRun delegates to OmniAllocator if supported.
func (t *Top) AllocateRun(size int, count int) ([]types.ObjectId, []types.FileOffset, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.omniAllocator.AllocateRun(size, count)
}

// SetOnAllocate delegates to OmniAllocator if supported.
func (t *Top) SetOnAllocate(cb func(types.ObjectId, types.FileOffset, int)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.omniAllocator.SetOnAllocate(cb)
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
//   1. Marshal OmniAllocator, allocate from BasicAllocator, write to file
//   2. Update PrimeTable with OmniAllocator ObjectId
//   3. Marshal BasicAllocator (which stores fileLength), allocate from BasicAllocator, write to file
//   4. Update PrimeTable with BasicAllocator ObjectId
//   5. Marshal and write PrimeTable at offset 0 (atomic commit)
//
// Note: PoolCache unloading should be coordinated at a higher level (e.g., by the application)
// before calling Save(). This ensures flexibility in unload timing and strategy.
func (t *Top) Save() error {
	t.mu.Lock()
	defer t.mu.Unlock()

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

	// Step 2: Update PrimeTable with OmniAllocator (reverse order: store last allocator first)
	if err := t.primeTable.Store(FileInfo{
		ObjId: omniObjId,
		Fo:    omniOffset,
		Sz:    types.FileSize(len(omniData)),
	}); err != nil {
		return fmt.Errorf("failed to store OmniAllocator info in PrimeTable: %w", err)
	}

	// Step 3: Marshal and persist BasicAllocator
	basicData, err := t.basicAllocator.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal BasicAllocator: %w", err)
	}

	basicObjId, basicOffset, err := t.basicAllocator.Allocate(len(basicData))
	if err != nil {
		return fmt.Errorf("failed to allocate space for BasicAllocator: %w", err)
	}

	_, err = t.file.WriteAt(basicData, int64(basicOffset))
	if err != nil {
		return fmt.Errorf("failed to write BasicAllocator data to file: %w", err)
	}

	// Step 4: Update PrimeTable with BasicAllocator
	if err := t.primeTable.Store(FileInfo{
		ObjId: basicObjId,
		Fo:    basicOffset,
		Sz:    types.FileSize(len(basicData)),
	}); err != nil {
		return fmt.Errorf("failed to store BasicAllocator info in PrimeTable: %w", err)
	}

	// Step 5: Marshal and persist PrimeTable (atomic commit)
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

	return nil
}

// Ensure Top implements types.TopAllocator.
var _ types.TopAllocator = (*Top)(nil)
