package cache

import (
	"bytes"
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
	headerSize = 8 // version(4) + count(4)
)

// UnloadedBlock represents metadata for a BlockAllocator stored on disk.
// Includes both lightweight metadata and the bitmap state for proper rehydration.
type UnloadedBlock struct {
	ObjId          types.ObjectId   // ObjectId where this block allocator is stored
	BaseObjId      types.ObjectId   // Starting ObjectId this allocator manages
	BaseFileOffset types.FileOffset // Starting FileOffset for this allocator's objects
	BlockSize      types.FileSize   // Size of each object in the allocator
	BlockCount     int              // Number of blocks (slots) this allocator manages
	Available      bool             // Does it have free slots?
	BitmapData     []byte           // Allocation bitmap from BlockAllocator
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

	// Note: Temp file persistence is handled by Marshal/Unmarshal
	// The temp file is no longer used for incremental writes

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
// This accounts for variable-length bitmap data in each entry.
func (pc *PoolCache) SizeInBytes() int {
	size := headerSize // 8 bytes: version + count
	for _, entry := range pc.entries {
		size += 41 // Fixed part: ObjId(8) + BaseObjId(8) + BaseFileOffset(8) + BlockSize(8) + BlockCount(4) + Available(1) + BitmapLength(4)
		size += len(entry.BitmapData)
	}
	return size
}

// Marshal serializes the cache to bytes for persistence.
// Reads from temp file and wraps with header metadata.
// Note: Only includes entries currently in the in-memory list (deleted entries excluded).
func (pc *PoolCache) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	// Write header
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], 0) // version
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(pc.entries)))
	buf.Write(header)

	// Write each entry with variable length (fixed metadata + bitmap data)
	for _, entry := range pc.entries {
		// Fixed part (41 bytes)
		entryData := make([]byte, 41)
		binary.LittleEndian.PutUint64(entryData[0:8], uint64(entry.ObjId))
		binary.LittleEndian.PutUint64(entryData[8:16], uint64(entry.BaseObjId))
		binary.LittleEndian.PutUint64(entryData[16:24], uint64(entry.BaseFileOffset))
		binary.LittleEndian.PutUint64(entryData[24:32], uint64(entry.BlockSize))
		binary.LittleEndian.PutUint32(entryData[32:36], uint32(entry.BlockCount))
		if entry.Available {
			entryData[36] = 1
		} else {
			entryData[36] = 0
		}
		// Bitmap length (4 bytes)
		binary.LittleEndian.PutUint32(entryData[37:41], uint32(len(entry.BitmapData)))
		buf.Write(entryData)

		// Bitmap data (variable length)
		if len(entry.BitmapData) > 0 {
			buf.Write(entry.BitmapData)
		}
	}

	return buf.Bytes(), nil
}

// Unmarshal restores the cache from serialized bytes.
func (pc *PoolCache) Unmarshal(data []byte) error {
	if len(data) < headerSize {
		return ErrInvalidEntry
	}

	// Read header
	_ = binary.LittleEndian.Uint32(data[0:4]) // version (ignored for now)
	count := binary.LittleEndian.Uint32(data[4:8])

	// Clear existing entries
	pc.entries = make([]UnloadedBlock, 0, count)

	// Unmarshal entries (variable length format)
	offset := headerSize
	for range count {
		if offset+41 > len(data) {
			return ErrInvalidEntry
		}

		// Read fixed part (41 bytes)
		entry := UnloadedBlock{
			ObjId:          types.ObjectId(binary.LittleEndian.Uint64(data[offset : offset+8])),
			BaseObjId:      types.ObjectId(binary.LittleEndian.Uint64(data[offset+8 : offset+16])),
			BaseFileOffset: types.FileOffset(binary.LittleEndian.Uint64(data[offset+16 : offset+24])),
			BlockSize:      types.FileSize(binary.LittleEndian.Uint64(data[offset+24 : offset+32])),
			BlockCount:     int(binary.LittleEndian.Uint32(data[offset+32 : offset+36])),
			Available:      data[offset+36] != 0,
		}

		// Bitmap length is at offset+37 in the fixed 41-byte block
		bitmapLen := int(binary.LittleEndian.Uint32(data[offset+37 : offset+41]))
		offset += 41

		// Read bitmap data
		if offset+bitmapLen > len(data) {
			return ErrInvalidEntry
		}
		if bitmapLen > 0 {
			entry.BitmapData = make([]byte, bitmapLen)
			copy(entry.BitmapData, data[offset:offset+bitmapLen])
			offset += bitmapLen
		}

		pc.entries = append(pc.entries, entry)
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
