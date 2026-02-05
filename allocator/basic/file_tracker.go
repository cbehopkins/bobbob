package basic

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"

	"github.com/cbehopkins/bobbob/allocator/types"
)

const (
	magicBytes        = uint32(0x5446494E) // "TFIN"
	trackerVersion    = uint32(1)
	defaultNumBuckets = uint32(4096) // 16KB bucket table
	loadFactorPercent = uint32(75)   // Rehash at 75% full
	headerSize        = 56           // Fixed header size
	entrySize         = 25           // Entry size: IsDeleted(1) + ObjectId(8) + FileSize(8) + Next(8)
)

var (
	ErrCorruptedFile  = errors.New("corrupted tracker file")
	ErrInvalidVersion = errors.New("invalid tracker file version")
)

// trackerHeader represents the file header structure
type trackerHeader struct {
	Magic          uint32
	Version        uint32
	NumBuckets     uint32
	Count          uint32
	LoadFactor     uint32
	NextDataOffset uint64
	FreeListHead   uint64
	Reserved       uint64
}

// trackerEntry represents a single object tracking entry
type trackerEntry struct {
	IsDeleted   uint8
	ObjectId    types.ObjectId
	FileSize    types.FileSize
	NextInChain uint64
}

// TrackerStats provides statistics about the file-based tracker
type TrackerStats struct {
	NumBuckets        uint32  // Number of hash buckets
	LiveEntries       uint32  // Number of live (non-deleted) entries
	DeletedEntries    uint32  // Number of deleted entries in free list
	TotalEntries      uint32  // Total entries (live + deleted)
	MaxChainLength    int     // Longest collision chain
	AvgChainLength    float64 // Average collision chain length
	EmptyBuckets      int     // Number of empty buckets
	LoadFactorPercent float64 // Current load factor (live entries / buckets * 100)
	FileSize          int64   // Total tracker file size in bytes
	DataRegionSize    uint64  // Size of data region
	FreeListLength    int     // Number of entries in free list
}

// fileBasedObjectTracker uses a hash index file for object tracking
type fileBasedObjectTracker struct {
	mu   sync.RWMutex
	file *os.File
	// deleteOnClose signals that the underlying file should be removed after Close.
	deleteOnClose bool
	header        trackerHeader
	buckets       []uint64 // In-memory bucket table (offsets to first entry in chain)
}

// newFileBasedObjectTracker creates a new file-based object tracker
func newFileBasedObjectTracker(file *os.File) (*fileBasedObjectTracker, error) {
	return newFileBasedObjectTrackerWithOptions(file, false)
}

// newFileBasedObjectTrackerWithOptions allows configuring tracker behavior (internal use).
func newFileBasedObjectTrackerWithOptions(file *os.File, deleteOnClose bool) (*fileBasedObjectTracker, error) {
	ft := &fileBasedObjectTracker{
		file:          file,
		deleteOnClose: deleteOnClose,
	}

	// Check if file is empty (new file)
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() == 0 {
		// Initialize new file
		if err := ft.initializeFile(); err != nil {
			return nil, err
		}
	} else {
		// Load existing file
		if err := ft.loadFile(); err != nil {
			return nil, err
		}
	}

	return ft, nil
}

// initializeFile creates a new empty tracker file
func (ft *fileBasedObjectTracker) initializeFile() error {
	ft.header = trackerHeader{
		Magic:          magicBytes,
		Version:        trackerVersion,
		NumBuckets:     defaultNumBuckets,
		Count:          0,
		LoadFactor:     loadFactorPercent,
		NextDataOffset: uint64(headerSize + int(defaultNumBuckets)*8),
		FreeListHead:   0,
		Reserved:       0,
	}

	// Allocate bucket table
	ft.buckets = make([]uint64, defaultNumBuckets)

	// Write header
	if err := ft.writeHeader(); err != nil {
		return err
	}

	// Write empty bucket table
	if err := ft.writeBuckets(); err != nil {
		return err
	}

	return nil
}

// loadFile reads an existing tracker file
func (ft *fileBasedObjectTracker) loadFile() error {
	// Read header
	if err := ft.readHeader(); err != nil {
		return err
	}

	// Validate header
	if ft.header.Magic != magicBytes {
		return ErrCorruptedFile
	}
	if ft.header.Version != trackerVersion {
		return ErrInvalidVersion
	}

	// Read bucket table
	ft.buckets = make([]uint64, ft.header.NumBuckets)
	if err := ft.readBuckets(); err != nil {
		return err
	}

	return nil
}

// readHeader reads the file header
func (ft *fileBasedObjectTracker) readHeader() error {
	buf := make([]byte, headerSize)
	if _, err := ft.file.ReadAt(buf, 0); err != nil {
		return err
	}

	ft.header.Magic = binary.LittleEndian.Uint32(buf[0:4])
	ft.header.Version = binary.LittleEndian.Uint32(buf[4:8])
	ft.header.NumBuckets = binary.LittleEndian.Uint32(buf[8:12])
	ft.header.Count = binary.LittleEndian.Uint32(buf[12:16])
	ft.header.LoadFactor = binary.LittleEndian.Uint32(buf[16:20])
	ft.header.NextDataOffset = binary.LittleEndian.Uint64(buf[20:28])
	ft.header.FreeListHead = binary.LittleEndian.Uint64(buf[28:36])
	ft.header.Reserved = binary.LittleEndian.Uint64(buf[36:44])

	return nil
}

// writeHeader writes the file header
func (ft *fileBasedObjectTracker) writeHeader() error {
	buf := make([]byte, headerSize)

	binary.LittleEndian.PutUint32(buf[0:4], ft.header.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], ft.header.Version)
	binary.LittleEndian.PutUint32(buf[8:12], ft.header.NumBuckets)
	binary.LittleEndian.PutUint32(buf[12:16], ft.header.Count)
	binary.LittleEndian.PutUint32(buf[16:20], ft.header.LoadFactor)
	binary.LittleEndian.PutUint64(buf[20:28], ft.header.NextDataOffset)
	binary.LittleEndian.PutUint64(buf[28:36], ft.header.FreeListHead)
	binary.LittleEndian.PutUint64(buf[36:44], ft.header.Reserved)

	_, err := ft.file.WriteAt(buf, 0)
	return err
}

// readBuckets reads the bucket table
func (ft *fileBasedObjectTracker) readBuckets() error {
	bucketBytes := int(ft.header.NumBuckets) * 8
	buf := make([]byte, bucketBytes)

	if _, err := ft.file.ReadAt(buf, headerSize); err != nil {
		return err
	}

	for i := uint32(0); i < ft.header.NumBuckets; i++ {
		ft.buckets[i] = binary.LittleEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	return nil
}

// writeBuckets writes the bucket table
func (ft *fileBasedObjectTracker) writeBuckets() error {
	bucketBytes := int(ft.header.NumBuckets) * 8
	buf := make([]byte, bucketBytes)

	for i := uint32(0); i < ft.header.NumBuckets; i++ {
		binary.LittleEndian.PutUint64(buf[i*8:(i+1)*8], ft.buckets[i])
	}

	_, err := ft.file.WriteAt(buf, headerSize)
	return err
}

// hash computes hash for an ObjectId
func (ft *fileBasedObjectTracker) hash(objId types.ObjectId) uint32 {
	h := uint64(objId)
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return uint32(h % uint64(ft.header.NumBuckets))
}

// readEntry reads an entry from the given offset
func (ft *fileBasedObjectTracker) readEntry(offset uint64) (*trackerEntry, error) {
	if offset == 0 {
		return nil, nil
	}

	buf := make([]byte, entrySize)
	if _, err := ft.file.ReadAt(buf, int64(offset)); err != nil {
		return nil, err
	}

	entry := &trackerEntry{
		IsDeleted:   buf[0],
		ObjectId:    types.ObjectId(binary.LittleEndian.Uint64(buf[1:9])),
		FileSize:    types.FileSize(binary.LittleEndian.Uint64(buf[9:17])),
		NextInChain: binary.LittleEndian.Uint64(buf[17:25]),
	}

	return entry, nil
}

// writeEntry writes an entry to the given offset
func (ft *fileBasedObjectTracker) writeEntry(offset uint64, entry *trackerEntry) error {
	buf := make([]byte, entrySize)

	buf[0] = entry.IsDeleted
	binary.LittleEndian.PutUint64(buf[1:9], uint64(entry.ObjectId))
	binary.LittleEndian.PutUint64(buf[9:17], uint64(entry.FileSize))
	binary.LittleEndian.PutUint64(buf[17:25], entry.NextInChain)

	_, err := ft.file.WriteAt(buf, int64(offset))
	return err
}

// Get retrieves the size for an ObjectId
func (ft *fileBasedObjectTracker) Get(objId types.ObjectId) (types.FileSize, bool) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	hash := ft.hash(objId)
	entryOffset := ft.buckets[hash]

	for entryOffset != 0 {
		entry, err := ft.readEntry(entryOffset)
		if err != nil {
			return 0, false
		}

		if entry.ObjectId == objId && entry.IsDeleted == 0 {
			return entry.FileSize, true
		}

		entryOffset = entry.NextInChain
	}

	return 0, false
}

// Set stores the size for an ObjectId
func (ft *fileBasedObjectTracker) Set(objId types.ObjectId, size types.FileSize) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	hash := ft.hash(objId)
	entryOffset := ft.buckets[hash]

	// Check if entry already exists
	for entryOffset != 0 {
		entry, err := ft.readEntry(entryOffset)
		if err != nil {
			return
		}

		if entry.ObjectId == objId {
			// Update existing entry
			entry.FileSize = size
			if entry.IsDeleted == 1 {
				entry.IsDeleted = 0
				ft.header.Count++
			}
			ft.writeEntry(entryOffset, entry)
			ft.writeHeader()
			return
		}

		entryOffset = entry.NextInChain
	}

	// Allocate new entry
	var newOffset uint64
	if ft.header.FreeListHead != 0 {
		// Reuse deleted entry
		newOffset = ft.header.FreeListHead
		freeEntry, _ := ft.readEntry(newOffset)
		ft.header.FreeListHead = freeEntry.NextInChain
	} else {
		// Append new entry
		newOffset = ft.header.NextDataOffset
		ft.header.NextDataOffset += entrySize
	}

	// Create new entry
	newEntry := &trackerEntry{
		IsDeleted:   0,
		ObjectId:    objId,
		FileSize:    size,
		NextInChain: ft.buckets[hash],
	}

	// Write entry and update bucket
	ft.writeEntry(newOffset, newEntry)
	ft.buckets[hash] = newOffset
	ft.header.Count++

	// Persist updates
	ft.writeHeader()
	ft.writeBuckets()
}

// Delete removes an ObjectId from tracking
func (ft *fileBasedObjectTracker) Delete(objId types.ObjectId) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	hash := ft.hash(objId)
	entryOffset := ft.buckets[hash]

	// Track previous entry in collision chain for removal
	var prevOffset uint64
	var prevEntry *trackerEntry

	for entryOffset != 0 {
		entry, err := ft.readEntry(entryOffset)
		if err != nil {
			return
		}

		if entry.ObjectId == objId && entry.IsDeleted == 0 {
			// Remove from collision chain
			nextInChain := entry.NextInChain
			if prevOffset == 0 {
				// First entry in bucket - update bucket pointer
				ft.buckets[hash] = nextInChain
				ft.writeBuckets()
			} else {
				// Not first - update previous entry's NextInChain
				prevEntry.NextInChain = nextInChain
				ft.writeEntry(prevOffset, prevEntry)
			}

			// Mark as deleted and add to free list
			entry.IsDeleted = 1
			entry.NextInChain = ft.header.FreeListHead
			ft.header.FreeListHead = entryOffset
			ft.header.Count--

			ft.writeEntry(entryOffset, entry)
			ft.writeHeader()
			return
		}

		prevOffset = entryOffset
		prevEntry = entry
		entryOffset = entry.NextInChain
	}
}

// Len returns the number of tracked objects
func (ft *fileBasedObjectTracker) Len() int {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	return int(ft.header.Count)
}

// Contains returns whether an ObjectId is tracked
func (ft *fileBasedObjectTracker) Contains(objId types.ObjectId) bool {
	_, found := ft.Get(objId)
	return found
}

// ForEach iterates over all tracked objects
func (ft *fileBasedObjectTracker) ForEach(fn func(types.ObjectId, types.FileSize)) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	// Scan entire data region
	bucketEnd := uint64(headerSize + int(ft.header.NumBuckets)*8)
	for offset := bucketEnd; offset < ft.header.NextDataOffset; offset += entrySize {
		entry, err := ft.readEntry(offset)
		if err != nil || entry.IsDeleted == 1 {
			continue
		}

		fn(entry.ObjectId, entry.FileSize)
	}
}

// GetAllObjectIds returns a slice of all tracked ObjectIds
func (ft *fileBasedObjectTracker) GetAllObjectIds() []types.ObjectId {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	objIds := make([]types.ObjectId, 0, ft.header.Count)

	bucketEnd := uint64(headerSize + int(ft.header.NumBuckets)*8)
	for offset := bucketEnd; offset < ft.header.NextDataOffset; offset += entrySize {
		entry, err := ft.readEntry(offset)
		if err != nil || entry.IsDeleted == 1 {
			continue
		}

		objIds = append(objIds, entry.ObjectId)
	}

	return objIds
}

// Clear removes all tracked objects
func (ft *fileBasedObjectTracker) Clear() {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Reset header
	ft.header.Count = 0
	ft.header.NextDataOffset = uint64(headerSize + int(ft.header.NumBuckets)*8)
	ft.header.FreeListHead = 0

	// Zero bucket table
	for i := range ft.buckets {
		ft.buckets[i] = 0
	}

	// Truncate file
	ft.file.Truncate(int64(ft.header.NextDataOffset))

	// Persist
	ft.writeHeader()
	ft.writeBuckets()
}

// Marshal serializes the tracked objects to bytes (for compatibility)
func (ft *fileBasedObjectTracker) Marshal() ([]byte, error) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	numEntries := uint32(ft.header.Count)
	totalSize := 4 + (numEntries * 16) // count + entries
	data := make([]byte, totalSize)

	// Write entry count
	binary.LittleEndian.PutUint32(data[0:4], numEntries)

	// Collect all live entries
	offset := 4
	bucketEnd := uint64(headerSize + int(ft.header.NumBuckets)*8)
	for entryOffset := bucketEnd; entryOffset < ft.header.NextDataOffset; entryOffset += entrySize {
		entry, err := ft.readEntry(entryOffset)
		if err != nil || entry.IsDeleted == 1 {
			continue
		}

		binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(entry.ObjectId))
		binary.LittleEndian.PutUint64(data[offset+8:offset+16], uint64(entry.FileSize))
		offset += 16
	}

	return data, nil
}

// Unmarshal restores tracked objects from bytes
func (ft *fileBasedObjectTracker) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return errors.New("insufficient data for objectTracker unmarshal")
	}

	// Read entry count
	numEntries := binary.LittleEndian.Uint32(data[0:4])
	expectedSize := 4 + (numEntries * 16)

	if len(data) < int(expectedSize) {
		return errors.New("insufficient data for objectTracker entries")
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Clear existing state (inline to avoid locking issues)
	ft.header.Count = 0
	ft.header.NextDataOffset = uint64(headerSize + int(ft.header.NumBuckets)*8)
	ft.header.FreeListHead = 0

	for i := range ft.buckets {
		ft.buckets[i] = 0
	}

	ft.file.Truncate(int64(ft.header.NextDataOffset))
	ft.writeHeader()
	ft.writeBuckets()

	// Restore objects (inline Set logic to avoid locking)
	offset := 4
	for i := uint32(0); i < numEntries; i++ {
		objId := types.ObjectId(binary.LittleEndian.Uint64(data[offset : offset+8]))
		size := types.FileSize(binary.LittleEndian.Uint64(data[offset+8 : offset+16]))

		// Allocate new entry
		newOffset := ft.header.NextDataOffset
		ft.header.NextDataOffset += entrySize

		// Hash and insert
		hash := ft.hash(objId)
		newEntry := &trackerEntry{
			IsDeleted:   0,
			ObjectId:    objId,
			FileSize:    size,
			NextInChain: ft.buckets[hash],
		}

		ft.writeEntry(newOffset, newEntry)
		ft.buckets[hash] = newOffset
		ft.header.Count++

		offset += 16
	}

	// Persist updates
	ft.writeHeader()
	ft.writeBuckets()

	return nil
}

// Close flushes and closes the tracker file
func (ft *fileBasedObjectTracker) Close() error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Capture path before closing in case the file handle becomes invalid.
	trackerPath := ft.file.Name()

	if err := ft.file.Sync(); err != nil {
		return err
	}

	if err := ft.file.Close(); err != nil {
		return err
	}

	if ft.deleteOnClose {
		// Best-effort removal; ignore errors to avoid masking close status.
		_ = os.Remove(trackerPath)
	}

	return nil
}

// Stats returns statistics about the tracker
func (ft *fileBasedObjectTracker) Stats() TrackerStats {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	stats := TrackerStats{
		NumBuckets:        ft.header.NumBuckets,
		LiveEntries:       ft.header.Count,
		TotalEntries:      0,
		DeletedEntries:    0,
		MaxChainLength:    0,
		AvgChainLength:    0,
		EmptyBuckets:      0,
		LoadFactorPercent: float64(ft.header.Count) / float64(ft.header.NumBuckets) * 100,
		DataRegionSize:    ft.header.NextDataOffset - uint64(headerSize+int(ft.header.NumBuckets)*8),
		FreeListLength:    0,
	}

	// Get file size
	if stat, err := ft.file.Stat(); err == nil {
		stats.FileSize = stat.Size()
	}

	// Count total entries and deleted entries
	bucketEnd := uint64(headerSize + int(ft.header.NumBuckets)*8)
	for offset := bucketEnd; offset < ft.header.NextDataOffset; offset += entrySize {
		stats.TotalEntries++
		if entry, err := ft.readEntry(offset); err == nil && entry.IsDeleted == 1 {
			stats.DeletedEntries++
		}
	}

	// Calculate chain statistics
	totalChainLength := 0
	emptyBuckets := 0

	for _, bucketOffset := range ft.buckets {
		if bucketOffset == 0 {
			emptyBuckets++
			continue
		}

		// Traverse chain
		chainLength := 0
		entryOffset := bucketOffset
		for entryOffset != 0 {
			entry, err := ft.readEntry(entryOffset)
			if err != nil {
				break
			}
			if entry.IsDeleted == 0 {
				chainLength++
			}
			entryOffset = entry.NextInChain
		}

		totalChainLength += chainLength
		if chainLength > stats.MaxChainLength {
			stats.MaxChainLength = chainLength
		}
	}

	stats.EmptyBuckets = emptyBuckets
	nonEmptyBuckets := int(ft.header.NumBuckets) - emptyBuckets
	if nonEmptyBuckets > 0 {
		stats.AvgChainLength = float64(totalChainLength) / float64(nonEmptyBuckets)
	}

	// Count free list length
	freeListCount := 0
	freeOffset := ft.header.FreeListHead
	visited := make(map[uint64]bool)
	for freeOffset != 0 && !visited[freeOffset] {
		visited[freeOffset] = true
		freeListCount++
		if entry, err := ft.readEntry(freeOffset); err == nil {
			freeOffset = entry.NextInChain
		} else {
			break
		}
	}
	stats.FreeListLength = freeListCount

	return stats
}

// NeedsRehash returns true if the tracker should be rehashed
func (ft *fileBasedObjectTracker) NeedsRehash() bool {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	threshold := uint32(float64(ft.header.NumBuckets) * float64(ft.header.LoadFactor) / 100.0)
	return ft.header.Count > threshold
}

// Rehash rebuilds the tracker with more buckets
func (ft *fileBasedObjectTracker) Rehash() error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Double the bucket count
	newNumBuckets := ft.header.NumBuckets * 2

	// Collect all live entries
	type liveEntry struct {
		objId types.ObjectId
		size  types.FileSize
	}
	entries := make([]liveEntry, 0, ft.header.Count)

	bucketEnd := uint64(headerSize + int(ft.header.NumBuckets)*8)
	for offset := bucketEnd; offset < ft.header.NextDataOffset; offset += entrySize {
		entry, err := ft.readEntry(offset)
		if err != nil || entry.IsDeleted == 1 {
			continue
		}
		entries = append(entries, liveEntry{
			objId: entry.ObjectId,
			size:  entry.FileSize,
		})
	}

	// Reset header with new bucket count
	ft.header.NumBuckets = newNumBuckets
	ft.header.Count = 0
	ft.header.NextDataOffset = uint64(headerSize + int(newNumBuckets)*8)
	ft.header.FreeListHead = 0

	// Reallocate bucket table
	ft.buckets = make([]uint64, newNumBuckets)

	// Truncate file to new header + buckets
	if err := ft.file.Truncate(int64(ft.header.NextDataOffset)); err != nil {
		return err
	}

	// Write new header and buckets
	if err := ft.writeHeader(); err != nil {
		return err
	}
	if err := ft.writeBuckets(); err != nil {
		return err
	}

	// Reinsert all entries
	for _, entry := range entries {
		hash := ft.hash(entry.objId)
		newOffset := ft.header.NextDataOffset
		ft.header.NextDataOffset += entrySize

		newEntry := &trackerEntry{
			IsDeleted:   0,
			ObjectId:    entry.objId,
			FileSize:    entry.size,
			NextInChain: ft.buckets[hash],
		}

		if err := ft.writeEntry(newOffset, newEntry); err != nil {
			return err
		}

		ft.buckets[hash] = newOffset
		ft.header.Count++
	}

	// Persist final state
	if err := ft.writeHeader(); err != nil {
		return err
	}
	if err := ft.writeBuckets(); err != nil {
		return err
	}

	return nil
}

// Compact reclaims space from deleted entries
func (ft *fileBasedObjectTracker) Compact() error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Collect all live entries
	type liveEntry struct {
		objId types.ObjectId
		size  types.FileSize
	}
	entries := make([]liveEntry, 0, ft.header.Count)

	bucketEnd := uint64(headerSize + int(ft.header.NumBuckets)*8)
	for offset := bucketEnd; offset < ft.header.NextDataOffset; offset += entrySize {
		entry, err := ft.readEntry(offset)
		if err != nil || entry.IsDeleted == 1 {
			continue
		}
		entries = append(entries, liveEntry{
			objId: entry.ObjectId,
			size:  entry.FileSize,
		})
	}

	// Reset data region
	ft.header.Count = 0
	ft.header.NextDataOffset = uint64(headerSize + int(ft.header.NumBuckets)*8)
	ft.header.FreeListHead = 0

	// Clear buckets
	for i := range ft.buckets {
		ft.buckets[i] = 0
	}

	// Truncate file
	if err := ft.file.Truncate(int64(ft.header.NextDataOffset)); err != nil {
		return err
	}

	// Write header and buckets
	if err := ft.writeHeader(); err != nil {
		return err
	}
	if err := ft.writeBuckets(); err != nil {
		return err
	}

	// Reinsert all entries
	for _, entry := range entries {
		hash := ft.hash(entry.objId)
		newOffset := ft.header.NextDataOffset
		ft.header.NextDataOffset += entrySize

		newEntry := &trackerEntry{
			IsDeleted:   0,
			ObjectId:    entry.objId,
			FileSize:    entry.size,
			NextInChain: ft.buckets[hash],
		}

		if err := ft.writeEntry(newOffset, newEntry); err != nil {
			return err
		}

		ft.buckets[hash] = newOffset
		ft.header.Count++
	}

	// Persist final state
	if err := ft.writeHeader(); err != nil {
		return err
	}
	if err := ft.writeBuckets(); err != nil {
		return err
	}

	return nil
}

// NeedsCompaction returns true if compaction would reclaim significant space
func (ft *fileBasedObjectTracker) NeedsCompaction() bool {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	// Count deleted entries
	deletedCount := uint32(0)
	bucketEnd := uint64(headerSize + int(ft.header.NumBuckets)*8)
	for offset := bucketEnd; offset < ft.header.NextDataOffset; offset += entrySize {
		if entry, err := ft.readEntry(offset); err == nil && entry.IsDeleted == 1 {
			deletedCount++
		}
	}

	// Compact if more than 25% of entries are deleted
	totalEntries := ft.header.Count + deletedCount
	if totalEntries == 0 {
		return false
	}

	return float64(deletedCount)/float64(totalEntries) > 0.25
}
