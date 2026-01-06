package allocator

import (
	"errors"
)

type blockAllocator struct {
	blockSize          int
	blockCount         int
	allocatedList      []bool
	startingFileOffset FileOffset
	startingObjectId   ObjectId
	allAllocated       bool
}

// NewBlockAllocator creates a new block allocator for fixed-size blocks.
// This allocator is useful when you know the size of the blocks you want to allocate.
// It uses a simple boolean array instead of an allocation map, making it more space-efficient.
// blockSize is the size of each block in bytes.
// blockCount is the number of blocks in the allocator.
// startingFileOffset is the file offset where the first block begins.
// startingObjectId is the ObjectId for the first block.
func NewBlockAllocator(blockSize, blockCount int, startingFileOffset FileOffset, startingObjectId ObjectId) *blockAllocator {
	allocatedList := make([]bool, blockCount)
	return &blockAllocator{
		blockSize:          blockSize,
		blockCount:         blockCount,
		allocatedList:      allocatedList,
		startingFileOffset: startingFileOffset,
		startingObjectId:   startingObjectId,
	}
}

// Allocate allocates a block of the specified size.
// The size must match the blockSize configured for this allocator.
// Returns an error if the size doesn't match or if all blocks are allocated.
func (a *blockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	if size != a.blockSize {
		return 0, 0, errors.New("size must match block size")
	}

	// TBD we currently use a list of booleans
	// This is far from the most efficient structure
	// And we could make use of a bitset, but KISS
	if a.allAllocated {
		return 0, 0, AllAllocated
	}
	for i, allocated := range a.allocatedList {
		if !allocated {
			a.allocatedList[i] = true
			fileOffset := a.startingFileOffset + FileOffset(i*a.blockSize)
			objectId := a.startingObjectId + ObjectId(i)
			return objectId, fileOffset, nil
		}
	}
	a.allAllocated = true
	return 0, 0, AllAllocated
}

func (a *blockAllocator) Free(fileOffset FileOffset, size int) error {
	a.allAllocated = false
	if size != a.blockSize {
		return errors.New("invalid block size")
	}
	blockIndex := (fileOffset - a.startingFileOffset) / FileOffset(a.blockSize)
	if blockIndex < 0 || blockIndex >= FileOffset(len(a.allocatedList)) {
		return errors.New("invalid file offset")
	}
	a.allocatedList[blockIndex] = false
	return nil
}

// ContainsObjectId checks if an ObjectId belongs to this block allocator's range.
func (a *blockAllocator) ContainsObjectId(objId ObjectId) bool {
	return objId >= a.startingObjectId && objId < a.startingObjectId+ObjectId(a.blockCount)
}

// GetFileOffset returns the FileOffset for an ObjectId managed by this allocator.
// Returns an error if the ObjectId is not in range or not allocated.
func (a *blockAllocator) GetFileOffset(objId ObjectId) (FileOffset, error) {
	if !a.ContainsObjectId(objId) {
		return 0, errors.New("ObjectId not in range")
	}

	slotIndex := int(objId - a.startingObjectId)
	if slotIndex < 0 || slotIndex >= len(a.allocatedList) {
		return 0, errors.New("invalid slot index")
	}

	if !a.allocatedList[slotIndex] {
		return 0, errors.New("slot not allocated")
	}

	return a.startingFileOffset + FileOffset(slotIndex*a.blockSize), nil
}

func (a *blockAllocator) Marshal() ([]byte, error) {
	byteCount := (a.blockCount + 7) / 8
	data := make([]byte, byteCount)
	for i, allocated := range a.allocatedList {
		if allocated {
			data[i/8] |= 1 << (i % 8)
		}
	}
	return data, nil
}

func (a *blockAllocator) Unmarshal(data []byte) error {
	if len(data) != (a.blockCount+7)/8 {
		return errors.New("invalid data length")
	}
	for i := range a.allocatedList {
		a.allocatedList[i] = (data[i/8] & (1 << (i % 8))) != 0
	}
	return nil
}

type multiBlockAllocator struct {
	blockSize          int
	blockCount         int
	parent             Allocator
	allocators         []*blockAllocator
	startOffsets       []FileOffset
	preParentAllocate  func(size int) error
	postParentAllocate func(ObjectId, FileOffset) error
}

// NewMultiBlockAllocator creates a new multi-block allocator that manages multiple block allocators.
// When all existing block allocators are full, it automatically allocates a new one from the parent.
// blockSize is the size of each block in bytes.
// blockCount is the number of blocks in each sub-allocator.
// parent is the parent allocator used to allocate new block regions.
func NewMultiBlockAllocator(blockSize, blockCount int, parent Allocator) *multiBlockAllocator {
	return &multiBlockAllocator{
		blockSize:  blockSize,
		blockCount: blockCount,
		parent:     parent,
		allocators: []*blockAllocator{},
	}
}

func (m *multiBlockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	// Verify the requested size matches our block size
	if size != m.blockSize {
		return 0, 0, errors.New("size must match block size")
	}

	for _, allocator := range m.allocators {
		// TBD add a bitfield/map/whatever to track which allocators are full
		// Then we can save quering them
		ObjectId, FileOffset, err := allocator.Allocate(m.blockSize)
		if err == nil {
			return ObjectId, FileOffset, nil
		}
		if !errors.Is(err, AllAllocated) {
			return 0, 0, err
		}
	}

	// All allocators are full, create a new one
	if m.preParentAllocate != nil {
		if err := m.preParentAllocate(size); err != nil {
			return 0, 0, err
		}
	}
	parentObjectId, parentFileOffset, err := m.parent.Allocate(m.blockSize * m.blockCount)
	if err != nil {
		return 0, 0, err
	}
	if m.postParentAllocate != nil {
		if err := m.postParentAllocate(parentObjectId, parentFileOffset); err != nil {
			return 0, 0, err
		}
	}

	newAllocator := NewBlockAllocator(m.blockSize, m.blockCount, parentFileOffset, parentObjectId)
	m.allocators = append(m.allocators, newAllocator)
	m.startOffsets = append(m.startOffsets, parentFileOffset)

	return newAllocator.Allocate(m.blockSize)
}

func (m *multiBlockAllocator) Free(fileOffset FileOffset, size int) error {
	if size != m.blockSize {
		return errors.New("invalid block size")
	}
	for i, startOffset := range m.startOffsets {
		if fileOffset >= startOffset && fileOffset < startOffset+FileOffset(m.blockSize*m.blockCount) {
			return m.allocators[i].Free(fileOffset, size)
		}
	}
	return errors.New("invalid file offset")
}

func (m *multiBlockAllocator) Marshal() ([]byte, error) {
	// Serialize the state of the multiBlockAllocator
	data := make([]byte, 0)

	// Serialize blockSize and blockCount
	data = append(data, byte(m.blockSize>>8), byte(m.blockSize))
	data = append(data, byte(m.blockCount>>8), byte(m.blockCount))

	// Serialize the number of allocators
	data = append(data, byte(len(m.allocators)>>8), byte(len(m.allocators)))

	// Serialize each blockAllocator
	for _, allocator := range m.allocators {
		allocatorData, err := allocator.Marshal()
		if err != nil {
			return nil, err
		}
		data = append(data, allocatorData...)
	}

	// Serialize startOffsets
	for _, offset := range m.startOffsets {
		data = append(data, byte(offset>>56), byte(offset>>48), byte(offset>>40), byte(offset>>32),
			byte(offset>>24), byte(offset>>16), byte(offset>>8), byte(offset))
	}

	return data, nil
}

func (m *multiBlockAllocator) Unmarshal(data []byte) error {
	// Deserialize blockSize and blockCount
	if len(data) < 4 {
		return errors.New("invalid data length")
	}
	m.blockSize = int(data[0])<<8 | int(data[1])
	m.blockCount = int(data[2])<<8 | int(data[3])
	data = data[4:]

	// Deserialize the number of allocators
	if len(data) < 2 {
		return errors.New("invalid data length")
	}
	numAllocators := int(data[0])<<8 | int(data[1])
	data = data[2:]

	// Deserialize each blockAllocator
	m.allocators = make([]*blockAllocator, numAllocators)
	for i := range numAllocators {
		allocator := &blockAllocator{
			blockSize:          m.blockSize,
			blockCount:         m.blockCount,
			allocatedList:      make([]bool, m.blockCount), // Properly initialize allocatedList
			startingFileOffset: 0,                          // Placeholder, will be set later
			startingObjectId:   0,                          // Placeholder, will be set later
		}
		allocatorDataSize := (m.blockCount + 7) / 8
		if len(data) < allocatorDataSize {
			return errors.New("invalid data length")
		}
		if err := allocator.Unmarshal(data[:allocatorDataSize]); err != nil {
			return err
		}
		m.allocators[i] = allocator
		data = data[allocatorDataSize:]
	}

	// Deserialize startOffsets
	m.startOffsets = make([]FileOffset, numAllocators)
	for i := range numAllocators {
		if len(data) < 8 {
			return errors.New("invalid data length")
		}
		offset := FileOffset(data[0])<<56 | FileOffset(data[1])<<48 | FileOffset(data[2])<<40 | FileOffset(data[3])<<32 |
			FileOffset(data[4])<<24 | FileOffset(data[5])<<16 | FileOffset(data[6])<<8 | FileOffset(data[7])
		m.startOffsets[i] = offset
		m.allocators[i].startingFileOffset = offset                   // Set startingFileOffset for each allocator
		m.allocators[i].startingObjectId = ObjectId(i * m.blockCount) // Set startingObjectId for each allocator
		data = data[8:]
	}

	return nil
}

type omniBlockAllocator struct {
	blockMap     map[int]*blockAllocator
	blockCount   int
	parent       Allocator
	preAllocate  func(size int) error
	postAllocate func(ObjectId ObjectId, FileOffset FileOffset) error
	preFree      func(FileOffset FileOffset, size int) error
	postFree     func(FileOffset FileOffset, size int) error
	lookupCache  *ObjectIdLookupCache
}

// NewOmniBlockAllocator creates an allocator that manages multiple block sizes.
// It maintains a separate block allocator for each requested size.
// blockSize is a slice of different block sizes to support.
// blockCount is the number of blocks in each sub-allocator.
// parent is the fallback allocator for sizes not in the blockSize slice.
func NewOmniBlockAllocator(blockSize []int, blockCount int, parent Allocator) *omniBlockAllocator {
	blockMap := make(map[int]*blockAllocator)
	cache := NewObjectIdLookupCache()

	// Assign non-overlapping ObjectId ranges to each block allocator.
	// Each allocator gets a range of [baseId, baseId + blockCount)
	for i, size := range blockSize {
		// Calculate a base ObjectId that won't overlap with other allocators.
		// Since each allocator manages blockCount ObjectIds, we can multiply by blockCount.
		// Add 1 to skip ObjectId 0 which is reserved.
		baseObjId := ObjectId(i*blockCount + 1)
		endObjId := baseObjId + ObjectId(blockCount)

		// Pre-allocate file space for this block allocator from the parent.
		// Each block allocator needs blockCount * size bytes of file space.
		totalSize := blockCount * size
		_, fileOffset, err := parent.Allocate(totalSize)
		if err != nil {
			// If allocation fails, continue with a fallback offset.
			// This allows graceful degradation, though it may cause collisions.
			fileOffset = 0
		}

		ba := NewBlockAllocator(size, blockCount, fileOffset, baseObjId)
		blockMap[size] = ba

		// Populate the cache with the allocator range
		_ = cache.AddRange(int64(baseObjId), size, fileOffset)
		_ = cache.UpdateRangeEnd(int64(baseObjId), int64(endObjId))
	}

	return &omniBlockAllocator{
		blockMap:    blockMap,
		blockCount:  blockCount,
		parent:      parent,
		lookupCache: cache,
	}
}

func (o *omniBlockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	if o.preAllocate != nil {
		if err := o.preAllocate(size); err != nil {
			return 0, 0, err
		}
	}

	allocator, ok := o.blockMap[size]
	var ObjectId ObjectId
	var FileOffset FileOffset
	var err error
	if ok {
		ObjectId, FileOffset, err = allocator.Allocate(size)
	} else {
		// Defer to the parent allocator if size is not found
		ObjectId, FileOffset, err = o.parent.Allocate(size)
	}

	if o.postAllocate != nil {
		if postErr := o.postAllocate(ObjectId, FileOffset); postErr != nil {
			return 0, 0, postErr
		}
	}

	return ObjectId, FileOffset, err
}

func (o *omniBlockAllocator) Free(FileOffset FileOffset, size int) error {
	if o.preFree != nil {
		if err := o.preFree(FileOffset, size); err != nil {
			return err
		}
	}

	allocator, ok := o.blockMap[size]
	var err error
	if ok {
		err = allocator.Free(FileOffset, size)
	} else {
		// Defer to the parent allocator if size is not found
		err = o.parent.Free(FileOffset, size)
	}

	if o.postFree != nil {
		if postErr := o.postFree(FileOffset, size); postErr != nil {
			return postErr
		}
	}

	return err
}

// GetObjectInfo returns the FileOffset and Size for an allocated ObjectId.
// It uses a lookup cache for O(log n) performance instead of O(n) linear scan.
// Falls back to parent allocator if ObjectId not found in local allocators.
func (o *omniBlockAllocator) GetObjectInfo(objId ObjectId) (FileOffset, int, error) {
	// Try the cache first for O(log n) lookup
	rangeInfo, err := o.lookupCache.Lookup(int64(objId))
	if err == nil {
		// Calculate the offset directly using the range information
		// offset = startingFileOffset + (objId - startObjectId) * blockSize
		slotIndex := int64(objId) - rangeInfo.StartObjectId
		offset := rangeInfo.StartingFileOffset + FileOffset(slotIndex*int64(rangeInfo.BlockSize))
		return offset, rangeInfo.BlockSize, nil
	}

	// Not in cache, fall back to parent allocator
	if parentHasGetObjectInfo, ok := o.parent.(interface {
		GetObjectInfo(ObjectId) (FileOffset, int, error)
	}); ok {
		return parentHasGetObjectInfo.GetObjectInfo(objId)
	}

	return 0, 0, errors.New("ObjectId not found and parent allocator does not support GetObjectInfo")
}

// Marshal serializes the omniBlockAllocator's state.
// Format: [blockCount:4][numSizes:4][size1:4][size2:4]...[allocator1 data][allocator2 data]...
func (o *omniBlockAllocator) Marshal() ([]byte, error) {
	data := make([]byte, 0)

	// Serialize blockCount
	data = append(data,
		byte(o.blockCount>>24), byte(o.blockCount>>16),
		byte(o.blockCount>>8), byte(o.blockCount))

	// Serialize the number of block sizes
	numSizes := len(o.blockMap)
	data = append(data,
		byte(numSizes>>24), byte(numSizes>>16),
		byte(numSizes>>8), byte(numSizes))

	// Serialize block sizes in deterministic order (we'll use sorted order)
	sizes := make([]int, 0, numSizes)
	for size := range o.blockMap {
		sizes = append(sizes, size)
	}
	// Simple insertion sort for small slices
	for i := 1; i < len(sizes); i++ {
		key := sizes[i]
		j := i - 1
		for j >= 0 && sizes[j] > key {
			sizes[j+1] = sizes[j]
			j--
		}
		sizes[j+1] = key
	}

	// Serialize sizes
	for _, size := range sizes {
		data = append(data,
			byte(size>>24), byte(size>>16),
			byte(size>>8), byte(size))
	}

	// Serialize each allocator's data
	for _, size := range sizes {
		allocator := o.blockMap[size]
		allocatorData, err := allocator.Marshal()
		if err != nil {
			return nil, err
		}
		data = append(data, allocatorData...)
	}

	return data, nil
}

// Unmarshal deserializes the omniBlockAllocator's state.
func (o *omniBlockAllocator) Unmarshal(data []byte) error {
	if len(data) < 8 {
		return errors.New("invalid data length")
	}

	offset := 0

	// Deserialize blockCount
	o.blockCount = int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Deserialize number of sizes
	numSizes := int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Verify we have enough data for sizes
	if len(data) < offset+numSizes*4 {
		return errors.New("invalid data length for sizes")
	}

	// Deserialize sizes
	sizes := make([]int, numSizes)
	for i := 0; i < numSizes; i++ {
		size := int(data[offset])<<24 | int(data[offset+1])<<16 |
			int(data[offset+2])<<8 | int(data[offset+3])
		sizes[i] = size
		offset += 4
	}

	// Initialize blockMap
	o.blockMap = make(map[int]*blockAllocator)

	// Deserialize each allocator
	for i, size := range sizes {
		allocator := &blockAllocator{
			blockSize:          size,
			blockCount:         o.blockCount,
			allocatedList:      make([]bool, o.blockCount),
			startingFileOffset: 0,
			startingObjectId:   ObjectId(i),
		}

		allocatorDataSize := (o.blockCount + 7) / 8
		if len(data) < offset+allocatorDataSize {
			return errors.New("invalid data length for allocator")
		}

		if err := allocator.Unmarshal(data[offset : offset+allocatorDataSize]); err != nil {
			return err
		}

		o.blockMap[size] = allocator
		offset += allocatorDataSize
	}

	return nil
}
