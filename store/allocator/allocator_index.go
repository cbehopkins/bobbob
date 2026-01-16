package allocator

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// AllocatorRange represents a contiguous range of ObjectIds managed by a single allocator.
// Contains all information needed to calculate FileOffset from ObjectId.
type AllocatorRange struct {
	StartObjectId      int64      // inclusive
	EndObjectId        int64      // exclusive
	BlockSize          int        // size of each block in bytes
	StartingFileOffset FileOffset // file offset where the first block begins
}

// AllocatorIndex unifies variable-sized object metadata and fixed-size range metadata
// into a single disk-backed index with a small in-memory cache.
// Two instances are expected in a typical deployment:
//   - One owned by BasicAllocator (objects segment primarily)
//   - One owned by Omni/Block allocators (ranges segment primarily)
type AllocatorIndex struct {
	mu sync.Mutex

	// File backing
	path   string
	loaded bool
	dirty  bool

	// Objects cache: hot map of ObjectId -> allocatedRegion (explicit entries)
	objTable map[ObjectId]allocatedRegion

	// LRU cache for recent on-disk object lookups
	lru *lruCache

	// Ranges cache: always-resident slice used for binary search.
	ranges []*AllocatorRange
}

// NewAllocatorIndex creates a new index using the provided file path.
func NewAllocatorIndex(path string) *AllocatorIndex {
	return &AllocatorIndex{
		path:     path,
		objTable: make(map[ObjectId]allocatedRegion),
		lru:      newLRUCache(4096),
		ranges:   make([]*AllocatorRange, 0),
	}
}

// --- Objects API ---

// AddObject adds or updates an object entry in the index (cached; not immediately persisted).
func (idx *AllocatorIndex) AddObject(id ObjectId, offset FileOffset, size int) {
	idx.mu.Lock()
	idx.objTable[id] = allocatedRegion{fileOffset: offset, size: size}
	if idx.lru != nil {
		idx.lru.put(id, offset, size)
	}
	idx.dirty = true
	idx.mu.Unlock()
}

// BatchAddObjects adds multiple object entries efficiently.
func (idx *AllocatorIndex) BatchAddObjects(entries map[ObjectId]allocatedRegion) {
	idx.mu.Lock()
	for id, r := range entries {
		idx.objTable[id] = r
		if idx.lru != nil {
			idx.lru.put(id, r.fileOffset, r.size)
		}
	}
	if len(entries) > 0 {
		idx.dirty = true
	}
	idx.mu.Unlock()
}

// DeleteObject removes an object entry.
func (idx *AllocatorIndex) DeleteObject(id ObjectId) {
	idx.mu.Lock()
	delete(idx.objTable, id)
	if idx.lru != nil {
		idx.lru.remove(id)
	}
	idx.dirty = true
	idx.mu.Unlock()
}

// Get performs a unified lookup for an ObjectId, supporting both:
// - Range-based lookup (computed from range metadata)
// - Explicit object lookup (from disk-backed table)
// Returns FileOffset, Size, and error. Checks ranges first (fast), then objects.
func (idx *AllocatorIndex) Get(id ObjectId) (FileOffset, int, error) {
	// First check if this ObjectId falls within any registered range (block allocator path)
	// Use binary search since ranges are sorted by StartObjectId
	idx.mu.Lock()
	ranges := idx.ranges
	n := len(ranges)

	if n > 0 {
		// Binary search to find the range that might contain this ObjectId
		// Find the rightmost range where StartObjectId <= id
		i := sort.Search(n, func(i int) bool {
			return ranges[i].StartObjectId > int64(id)
		})

		// Check the range immediately before (if it exists)
		if i > 0 {
			r := ranges[i-1]
			if int64(id) >= r.StartObjectId && int64(id) < r.EndObjectId {
				// Compute offset from range metadata
				offset := r.StartingFileOffset + FileOffset((int64(id)-r.StartObjectId)*int64(r.BlockSize))
				size := r.BlockSize
				idx.mu.Unlock()
				return offset, size, nil
			}
		}
	}
	idx.mu.Unlock()

	// Not in ranges, check explicit objects (variable-size allocator path)
	idx.mu.Lock()
	r, ok := idx.objTable[id]
	idx.mu.Unlock()
	if ok {
		return r.fileOffset, r.size, nil
	}

	// Check LRU cache before hitting disk
	idx.mu.Lock()
	if idx.lru != nil {
		if off, size, ok := idx.lru.get(id); ok {
			idx.mu.Unlock()
			return off, size, nil
		}
	}
	idx.mu.Unlock()

	// Cache miss - search disk
	off, size, err := idx.searchDisk(id)
	if err == nil {
		idx.mu.Lock()
		if idx.lru != nil {
			idx.lru.put(id, off, size)
		}
		idx.mu.Unlock()
	}
	return off, size, err
}

// searchDisk performs a binary search on the sorted objects file for a specific ObjectId.
// This allows point lookups without loading the entire structure into memory.
func (idx *AllocatorIndex) searchDisk(id ObjectId) (FileOffset, int, error) {
	idx.mu.Lock()
	path := idx.path
	idx.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, errors.New("object not found")
		}
		return 0, 0, err
	}
	defer f.Close()

	// Read header to get counts and offsets
	header := make([]byte, 5+4+4) // magic(5) + rangesCount(4) + objectsCount(4)
	if _, err := f.Read(header); err != nil {
		return 0, 0, err
	}
	if string(header[:5]) != idxMagic {
		return 0, 0, errors.New("invalid index file format")
	}

	rangesCount := binary.LittleEndian.Uint32(header[5:9])
	objectsCount := binary.LittleEndian.Uint32(header[9:13])

	// Skip ranges segment
	rangeSegmentSize := int64(rangesCount) * 28 // 8+8+4+8 per range
	objectsStart := 13 + rangeSegmentSize

	// Binary search in objects segment (each entry is 20 bytes: id(8) + offset(8) + size(4))
	left, right := int64(0), int64(objectsCount)
	entrySize := int64(20)

	for left < right {
		mid := (left + right) / 2
		seekPos := objectsStart + mid*entrySize

		if _, err := f.Seek(seekPos, 0); err != nil {
			return 0, 0, err
		}

		var diskId int64
		if err := binary.Read(f, binary.LittleEndian, &diskId); err != nil {
			return 0, 0, err
		}

		if ObjectId(diskId) == id {
			// Found it - read offset and size
			var offset int64
			var size int32
			if err := binary.Read(f, binary.LittleEndian, &offset); err != nil {
				return 0, 0, err
			}
			if err := binary.Read(f, binary.LittleEndian, &size); err != nil {
				return 0, 0, err
			}
			return FileOffset(offset), int(size), nil
		} else if ObjectId(diskId) < id {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return 0, 0, errors.New("object not found")
}

// IterateObjects iterates all cached object entries.
func (idx *AllocatorIndex) IterateObjects(fn func(id ObjectId, offset FileOffset, size int) bool) {
	idx.mu.Lock()
	for id, r := range idx.objTable {
		if !fn(id, r.fileOffset, r.size) {
			break
		}
	}
	idx.mu.Unlock()
}

// --- Ranges API ---

// RegisterRange registers a new fixed-size allocator range.
func (idx *AllocatorIndex) RegisterRange(startObjectId int64, blockSize int, startingFileOffset FileOffset) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	// Prevent overlaps with existing ranges
	endObjectId := startObjectId // updated by UpdateRangeEnd
	for _, r := range idx.ranges {
		if !(endObjectId <= r.StartObjectId || startObjectId >= r.EndObjectId) {
			return errors.New("overlapping allocator ranges")
		}
	}
	idx.ranges = append(idx.ranges, &AllocatorRange{
		StartObjectId:      startObjectId,
		EndObjectId:        endObjectId,
		BlockSize:          blockSize,
		StartingFileOffset: startingFileOffset,
	})
	// Keep sorted by StartObjectId
	idx.sortRanges()
	idx.dirty = true
	return nil
}

// UpdateRangeEnd updates the end for a registered range.
func (idx *AllocatorIndex) UpdateRangeEnd(startObjectId int64, endObjectId int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	var target *AllocatorRange
	for _, r := range idx.ranges {
		if r.StartObjectId == startObjectId {
			target = r
			break
		}
	}
	if target == nil {
		return errors.New("range with startObjectId not found")
	}
	oldEnd := target.EndObjectId
	target.EndObjectId = endObjectId
	// Validate overlaps
	for _, r := range idx.ranges {
		if r == target {
			continue
		}
		if target.StartObjectId < r.EndObjectId && r.StartObjectId < target.EndObjectId {
			// revert
			target.EndObjectId = oldEnd
			return errors.New("overlapping allocator ranges after update")
		}
	}
	idx.dirty = true
	return nil
}

// LookupRange returns the matching range for an objectId.
func (idx *AllocatorIndex) LookupRange(objectId int64) (*AllocatorRange, error) {
	idx.mu.Lock()
	ranges := idx.ranges
	n := len(ranges)
	idx.mu.Unlock()

	if n == 0 {
		return nil, errors.New("ObjectId not found in ranges")
	}

	// Binary search for the range containing this objectId
	// Find the rightmost range where StartObjectId <= objectId
	i := sort.Search(n, func(i int) bool {
		return ranges[i].StartObjectId > objectId
	})

	// Check the range immediately before (if it exists)
	if i > 0 {
		r := ranges[i-1]
		if objectId >= r.StartObjectId && objectId < r.EndObjectId {
			return r, nil
		}
	}

	return nil, errors.New("ObjectId not found in ranges")
}

// --- Persistence ---

const idxMagic = "ALIX1"

// ensureLoaded loads from disk if not already loaded.
func (idx *AllocatorIndex) ensureLoaded() error {
	if idx.loaded {
		return nil
	}
	// For minimal viable version, just ensure file exists; defer full load implementation.
	idx.loaded = true
	return nil
}

// Flush persists both objects and ranges to disk in a simple binary format.
func (idx *AllocatorIndex) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if !idx.dirty {
		return nil
	}
	if err := idx.ensureLoaded(); err != nil {
		return err
	}
	// Ensure directory exists
	if dir := filepath.Dir(idx.path); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	f, err := os.OpenFile(idx.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	// Write magic
	if _, err := bw.Write([]byte(idxMagic)); err != nil {
		_ = f.Close()
		return err
	}
	// Write counts: rangesCount, objectsCount
	var rangesCount uint32 = uint32(len(idx.ranges))
	var objectsCount uint32 = uint32(len(idx.objTable))
	if err := binary.Write(bw, binary.LittleEndian, rangesCount); err != nil {
		_ = f.Close()
		return err
	}
	if err := binary.Write(bw, binary.LittleEndian, objectsCount); err != nil {
		_ = f.Close()
		return err
	}
	// Write ranges
	for _, r := range idx.ranges {
		if err := binary.Write(bw, binary.LittleEndian, r.StartObjectId); err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, r.EndObjectId); err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, int32(r.BlockSize)); err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, int64(r.StartingFileOffset)); err != nil {
			_ = f.Close()
			return err
		}
	}
	// Write objects in sorted order by ObjectId for binary search
	ids := make([]ObjectId, 0, len(idx.objTable))
	for id := range idx.objTable {
		ids = append(ids, id)
	}
	// Use standard library sort for O(n log n) performance
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		r := idx.objTable[id]
		if err := binary.Write(bw, binary.LittleEndian, int64(id)); err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, int64(r.fileOffset)); err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, int32(r.size)); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	idx.dirty = false
	return nil
}

// sortRanges sorts ranges by StartObjectId.
func (idx *AllocatorIndex) sortRanges() {
	if len(idx.ranges) < 2 {
		return
	}
	// simple insertion sort; range count is expected to be small
	for i := 1; i < len(idx.ranges); i++ {
		j := i
		for j > 0 && idx.ranges[j-1].StartObjectId > idx.ranges[j].StartObjectId {
			idx.ranges[j-1], idx.ranges[j] = idx.ranges[j], idx.ranges[j-1]
			j--
		}
	}
}

// --- simple LRU cache for object lookups ---

type lruEntry struct {
	id   ObjectId
	off  FileOffset
	size int
}

type lruCache struct {
	cap   int
	ll    *list.List
	items map[ObjectId]*list.Element
}

func newLRUCache(capacity int) *lruCache {
	if capacity <= 0 {
		capacity = 1024
	}
	return &lruCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[ObjectId]*list.Element),
	}
}

func (c *lruCache) get(id ObjectId) (FileOffset, int, bool) {
	if ele, ok := c.items[id]; ok {
		c.ll.MoveToFront(ele)
		ent := ele.Value.(lruEntry)
		return ent.off, ent.size, true
	}
	return 0, 0, false
}

func (c *lruCache) put(id ObjectId, off FileOffset, size int) {
	if ele, ok := c.items[id]; ok {
		ele.Value = lruEntry{id: id, off: off, size: size}
		c.ll.MoveToFront(ele)
		return
	}
	ele := c.ll.PushFront(lruEntry{id: id, off: off, size: size})
	c.items[id] = ele
	if c.ll.Len() > c.cap {
		c.evict()
	}
}

func (c *lruCache) remove(id ObjectId) {
	if ele, ok := c.items[id]; ok {
		c.ll.Remove(ele)
		delete(c.items, id)
	}
}

func (c *lruCache) evict() {
	if back := c.ll.Back(); back != nil {
		ent := back.Value.(lruEntry)
		delete(c.items, ent.id)
		c.ll.Remove(back)
	}
}
