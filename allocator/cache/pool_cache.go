package cache

import (
	"encoding/binary"
	"errors"
	"os"
	"sort"

	"github.com/cbehopkins/bobbob/allocator/types"
)

var (
	ErrObjectIdNotFound = errors.New("ObjectId not found in cache")
	ErrCacheFull        = errors.New("cache is full")
	ErrInvalidEntry     = errors.New("invalid cache entry")
)

const (
	entrySize = 41 // objId(8) + baseObjId(8) + baseFileOffset(8) + blockSize(8) + blockCount(8) + available(1)
	headerSize = 8  // version(4) + count(4)
)

// UnloadedBlock represents metadata for a BlockAllocator stored on disk.
// This is the lightweight in-memory representation used for quick lookups.
type UnloadedBlock struct {
	ObjId           types.ObjectId   // ObjectId where this block allocator is stored
	BaseObjId       types.ObjectId   // Starting ObjectId this allocator manages
	BaseFileOffset  types.FileOffset // Starting FileOffset for this allocator's objects
	BlockSize       types.FileSize   // Size of each object in the allocator
	BlockCount      int              // Number of blocks (slots) this allocator manages
	Available       bool             // Does it have free slots?
}

// PoolCache manages unloaded BlockAllocators using a temporary file for persistence.
type PoolCache struct {
	// In-memory metadata (sorted by BaseObjId)
	entries []UnloadedBlock

	// Temporary file backing store
	tempFile *os.File

	// Index for fast lookup by BaseObjId range
	// This allows O(log n) search for ObjectId ranges
}

// New creates a new PoolCache with a temporary file.
func New() (*PoolCache, error) {
	tempFile, err := os.CreateTemp(os.TempDir(), "bobbob_pool_cache_*.tmp")
	if err != nil {
		return nil, err
	}

	pc := &PoolCache{
		entries:  make([]UnloadedBlock, 0),
		tempFile: tempFile,
	}

	// Write empty header
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], 0) // version
	binary.LittleEndian.PutUint32(header[4:8], 0) // count
	if _, err := tempFile.Write(header); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, err
	}

	return pc, nil
}

// Insert adds an unloaded block entry to the cache.
// Maintains sorted order by BaseObjId and appends to temp file.
func (pc *PoolCache) Insert(entry UnloadedBlock) error {
	if pc.tempFile == nil {
		return errors.New("cache is closed")
	}

	// Find insertion position (binary search)
	idx := sort.Search(len(pc.entries), func(i int) bool {
		return pc.entries[i].BaseObjId > entry.BaseObjId
	})

	// Check for duplicate
	if idx > 0 && pc.entries[idx-1].BaseObjId == entry.BaseObjId {
		return ErrInvalidEntry // Duplicate BaseObjId
	}

	// Insert into in-memory list
	pc.entries = append(pc.entries, UnloadedBlock{})
	copy(pc.entries[idx+1:], pc.entries[idx:])
	pc.entries[idx] = entry

	// Append to temp file
	data := marshalUnloadedBlock(entry)
	if _, err := pc.tempFile.Write(data); err != nil {
		// Rollback insertion on write failure
		pc.entries = append(pc.entries[:idx], pc.entries[idx+1:]...)
		return err
	}

	return nil
}

// Query searches for a cached block by ObjectId.
// Returns the UnloadedBlock metadata if found, without loading full BlockAllocator.
// Returns ErrObjectIdNotFound if ObjectId is not in any cached range.
func (pc *PoolCache) Query(objId types.ObjectId) (*UnloadedBlock, error) {
	if pc == nil || len(pc.entries) == 0 {
		return nil, ErrObjectIdNotFound
	}

	// Binary search for the block containing this ObjectId
	idx := sort.Search(len(pc.entries), func(i int) bool {
		return pc.entries[i].BaseObjId > objId
	})

	// Check previous entry
	if idx > 0 {
		entry := &pc.entries[idx-1]
		// Check if objId falls within this block's range
		// Range is [BaseObjId, BaseObjId + BlockCount)
		if entry.BaseObjId <= objId && objId < entry.BaseObjId+types.ObjectId(entry.BlockCount) {
			return entry, nil
		}
	}

	return nil, ErrObjectIdNotFound
}

// Delete removes an entry from the cache by BaseObjId.
func (pc *PoolCache) Delete(baseObjId types.ObjectId) error {
	// Binary search for entry with this BaseObjId
	idx := sort.Search(len(pc.entries), func(i int) bool {
		return pc.entries[i].BaseObjId >= baseObjId
	})

	if idx >= len(pc.entries) || pc.entries[idx].BaseObjId != baseObjId {
		return ErrObjectIdNotFound
	}

	// Remove from in-memory list
	pc.entries = append(pc.entries[:idx], pc.entries[idx+1:]...)

	// Note: Temp file is NOT updated immediately (marked for deletion on persist)
	return nil
}

// GetAll returns all entries in the cache (for iteration).
func (pc *PoolCache) GetAll() []UnloadedBlock {
	result := make([]UnloadedBlock, len(pc.entries))
	copy(result, pc.entries)
	return result
}

// SizeInBytes returns the size of the marshaled cache data.
// This is needed to request storage from parent allocator.
func (pc *PoolCache) SizeInBytes() int {
	return headerSize + (len(pc.entries) * entrySize)
}

// Marshal serializes the cache to bytes for persistence.
// Reads from temp file and wraps with header metadata.
// Note: Only includes entries currently in the in-memory list (deleted entries excluded).
func (pc *PoolCache) Marshal() ([]byte, error) {
	// Create buffer with exact size
	size := pc.SizeInBytes()
	data := make([]byte, size)

	// Write header
	binary.LittleEndian.PutUint32(data[0:4], 0) // version
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(pc.entries)))

	// Write entries (serialized in current order)
	offset := headerSize
	for _, entry := range pc.entries {
		entryData := marshalUnloadedBlock(entry)
		copy(data[offset:offset+entrySize], entryData)
		offset += entrySize
	}

	return data, nil
}

// Unmarshal restores the cache from serialized bytes.
func (pc *PoolCache) Unmarshal(data []byte) error {
	if len(data) < headerSize {
		return ErrInvalidEntry
	}

	// Read header
	_ = binary.LittleEndian.Uint32(data[0:4]) // version (ignored for now)
	count := binary.LittleEndian.Uint32(data[4:8])

	expectedSize := headerSize + (int(count) * entrySize)
	if len(data) != expectedSize {
		return ErrInvalidEntry
	}

	// Clear existing entries
	pc.entries = make([]UnloadedBlock, 0, count)

	// Unmarshal entries
	offset := headerSize
	for i := uint32(0); i < count; i++ {
		if offset+entrySize > len(data) {
			return ErrInvalidEntry
		}

		entry := unmarshalUnloadedBlock(data[offset : offset+entrySize])
		pc.entries = append(pc.entries, entry)
		offset += entrySize
	}

	// Ensure sorted by BaseObjId
	sort.Slice(pc.entries, func(i, j int) bool {
		return pc.entries[i].BaseObjId < pc.entries[j].BaseObjId
	})

	return nil
}

// Close closes the temporary file and cleans up resources.
func (pc *PoolCache) Close() error {
	if pc.tempFile != nil {
		pc.tempFile.Close()
		os.Remove(pc.tempFile.Name())
		pc.tempFile = nil
	}
	return nil
}

// Helper Functions
// marshalUnloadedBlock serializes an UnloadedBlock to bytes.
func marshalUnloadedBlock(entry UnloadedBlock) []byte {
	data := make([]byte, entrySize)
	binary.LittleEndian.PutUint64(data[0:8], uint64(entry.ObjId))
	binary.LittleEndian.PutUint64(data[8:16], uint64(entry.BaseObjId))
	binary.LittleEndian.PutUint64(data[16:24], uint64(entry.BaseFileOffset))
	binary.LittleEndian.PutUint64(data[24:32], uint64(entry.BlockSize))
	binary.LittleEndian.PutUint64(data[32:40], uint64(entry.BlockCount))
	if entry.Available {
		data[40] = 1
	} else {
		data[40] = 0
	}
	return data
}

// unmarshalUnloadedBlock deserializes bytes to UnloadedBlock.
func unmarshalUnloadedBlock(data []byte) UnloadedBlock {
	return UnloadedBlock{
		ObjId:          types.ObjectId(binary.LittleEndian.Uint64(data[0:8])),
		BaseObjId:      types.ObjectId(binary.LittleEndian.Uint64(data[8:16])),
		BaseFileOffset: types.FileOffset(binary.LittleEndian.Uint64(data[16:24])),
		BlockSize:      types.FileSize(binary.LittleEndian.Uint64(data[24:32])),
		BlockCount:     int(binary.LittleEndian.Uint64(data[32:40])),
		Available:      data[40] != 0,
	}
}
