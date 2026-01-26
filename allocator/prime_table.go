// Package allocator provides the core allocation primitives for bobbob.
//
// PrimeTable manages the bootstrap mechanism for allocator persistence.
// It is always written at FileOffset 0 in the file and provides the locations
// of all persisted allocators.
//
// Usage pattern:
//
//	CREATE PHASE (new store):
//	  1. Create PrimeTable
//	  2. Call Add() for each allocator (BasicAllocator, OmniAllocator, etc.)
//	  3. Calculate SizeInBytes() - this is where BasicAllocator starts
//
//	SAVE PHASE (persist to disk):
//	  1. Child allocators persist first (reverse order: Omni, then Basic)
//	  2. Each allocator: Marshal() -> Allocate(parent) -> Write to file
//	  3. Call Store(FileInfo) for each allocator (reverse order)
//	  4. Marshal() the PrimeTable and write to offset 0 (atomic commit)
//
//	LOAD PHASE (restore from disk):
//	  1. Read PrimeTable from offset 0 and Unmarshal()
//	  2. Load(0) -> read BasicAllocator bytes and Unmarshal
//	  3. Load(1) -> read OmniAllocator bytes and Unmarshal
//	  4. Other allocators load lazily via ObjectIds from parents
package allocator

import (
	"encoding/binary"
	"errors"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator/types"
)

var (
	ErrInvalidEntry     = errors.New("invalid entry index")
	ErrTableReadOnly    = errors.New("table is read-only after persistence")
	ErrInsufficientData = errors.New("insufficient data for unmarshaling")
)

// FileInfo represents the location and size of a persisted allocator.
type FileInfo struct {
	ObjId bobbob.ObjectId
	Fo    bobbob.FileOffset
	Sz    bobbob.FileSize
}

// Marshal serializes FileInfo to 24 bytes.
// Format: objId (8) | offset (8) | size (8)
func (fi FileInfo) Marshal() []byte {
	data := make([]byte, 24)
	binary.LittleEndian.PutUint64(data[0:8], uint64(fi.ObjId))
	binary.LittleEndian.PutUint64(data[8:16], uint64(fi.Fo))
	binary.LittleEndian.PutUint64(data[16:24], uint64(fi.Sz))
	return data
}

// Unmarshal deserializes FileInfo from 24 bytes.
func (fi *FileInfo) Unmarshal(data []byte) error {
	if len(data) < 24 {
		return ErrInsufficientData
	}
	fi.ObjId = types.ObjectId(binary.LittleEndian.Uint64(data[0:8]))
	fi.Fo = types.FileOffset(binary.LittleEndian.Uint64(data[8:16]))
	fi.Sz = types.FileSize(binary.LittleEndian.Uint64(data[16:24]))
	return nil
}

// PrimeTable manages the prime object table that tracks allocator persistence locations.
// The table is written at FileOffset 0 and provides the bootstrap mechanism for loading
// allocator hierarchy from disk.
type PrimeTable struct {
	entries     []FileInfo
	storeOffset int  // Index for Store operations (starts at end, works backward)
	loadOffset  int  // Index for Load operations (starts at 0, works forward)
	sealed      bool // True after first Store() - prevents Add()
}

// New creates a new empty PrimeTable.
func NewPrimeTable() *PrimeTable {
	return &PrimeTable{
		entries:     make([]FileInfo, 0),
		storeOffset: -1,
		loadOffset:  0,
		sealed:      false,
	}
}

// Add registers a new allocator slot in the table.
// Must be called before any Store operations.
// Order of Add() calls determines allocator indices.
func (p *PrimeTable) Add() {
	if p.sealed {
		panic("cannot Add after Store operations have begun")
	}

	p.entries = append(p.entries, FileInfo{})
}

// Store saves allocator persistence information to the table.
// Called during save sequence, starting from the last allocator.
// Automatically seals the table on first call (no more Add allowed).
func (p *PrimeTable) Store(fi FileInfo) error {
	if !p.sealed {
		// First Store call - initialize offset at end
		p.storeOffset = len(p.entries) - 1
		p.sealed = true
	}

	if p.storeOffset < 0 || p.storeOffset >= len(p.entries) {
		return ErrInvalidEntry
	}

	p.entries[p.storeOffset] = fi
	p.storeOffset--

	return nil
}

// Load retrieves the next allocator persistence information from the table.
// Called during load sequence, automatically advances to next entry.
// Calling Load() multiple times returns entries in order (0, 1, 2, ...).
func (p *PrimeTable) Load() (FileInfo, error) {
	if p.loadOffset < 0 || p.loadOffset >= len(p.entries) {
		return FileInfo{}, ErrInvalidEntry
	}

	fi := p.entries[p.loadOffset]
	p.loadOffset++

	return fi, nil
}

// Marshal serializes the table to bytes.
// Format: numEntries (4) | [objId (8) | offset (8) | size (8)]...
func (p *PrimeTable) Marshal() ([]byte, error) {
	numEntries := uint32(len(p.entries))
	totalSize := 4 + (numEntries * 24) // 4 for count + 24 bytes per entry
	data := make([]byte, totalSize)

	// Write entry count
	binary.LittleEndian.PutUint32(data[0:4], numEntries)

	// Write each entry
	offset := 4
	for _, entry := range p.entries {
		entryData := entry.Marshal()
		copy(data[offset:offset+24], entryData)
		offset += 24
	}

	return data, nil
}

// Unmarshal restores table state from bytes.
func (p *PrimeTable) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return ErrInsufficientData
	}

	numEntries := binary.LittleEndian.Uint32(data[0:4])
	expectedSize := 4 + (numEntries * 24)

	if len(data) < int(expectedSize) {
		return ErrInsufficientData
	}

	// Reconstruct entries
	p.entries = make([]FileInfo, numEntries)
	offset := 4

	for i := range numEntries {
		if err := p.entries[i].Unmarshal(data[offset : offset+24]); err != nil {
			return err
		}
		offset += 24
	}

	// Reset state after load
	p.storeOffset = -1
	p.loadOffset = 0
	p.sealed = false

	return nil
}

// SizeInBytes returns the number of bytes needed to marshal the table.
func (p *PrimeTable) SizeInBytes() int64 {
	return int64(4 + (len(p.entries) * 24))
}

// NumEntries returns the number of allocator slots in the table.
func (p *PrimeTable) NumEntries() int {
	return len(p.entries)
}

// Reset clears the table to initial state (useful for testing).
func (p *PrimeTable) Reset() {
	p.entries = make([]FileInfo, 0)
	p.storeOffset = -1
	p.loadOffset = 0
	p.sealed = false
}

// AddForTest registers a slot and returns its index (for testing only).
// Production code should use Add() which doesn't return the index.
func (p *PrimeTable) AddForTest() int {
	index := len(p.entries)
	p.Add()
	return index
}
