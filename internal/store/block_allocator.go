package store

import (
	"errors"
)

var AllAllocated = errors.New("no free blocks available")

type blockAllocator struct {
	blockSize          int
	blockCount         int
	allocatedList      []bool
	startingFileOffset FileOffset
	startingObjectId   ObjectId
	allAllocated       bool
}

// NewBlockAllocator creates a new block allocator
// This allocator is useful if you know the size of the blocks you want to allocate
// Rather than maintaing an allocation map, it's simply a list of booleans
// So far smaller
// blockSize is the size of each block in bytes
// blockCount is the number of blocks in each allocator
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

func (a *blockAllocator) Allocate() (ObjectId, FileOffset, error) {
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
			return objectId, FileOffset(fileOffset), nil
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

// NewMultiBlockAllocator creates a new multi-block allocator
// This allocator is useful if you know the size of the blocks you want to allocate
// blockSize is the size of each block in bytes
// blockCount is the number of blocks in each allocator
// parent is the parent allocator that will be used to allocate new blocks
func NewMultiBlockAllocator(blockSize, blockCount int, parent Allocator) *multiBlockAllocator {
	return &multiBlockAllocator{
		blockSize:  blockSize,
		blockCount: blockCount,
		parent:     parent,
		allocators: []*blockAllocator{},
	}
}

func (m *multiBlockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	for _, allocator := range m.allocators {
		// TBD add a bitfield/map/whatever to track which allocators are full
		// Then we can save quering them
		objectId, fileOffset, err := allocator.Allocate()
		if err == nil {
			return objectId, fileOffset, nil
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

	return newAllocator.Allocate()
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
	for i := 0; i < numAllocators; i++ {
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
	for i := 0; i < numAllocators; i++ {
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
	postAllocate func(objectId ObjectId, fileOffset FileOffset) error
	preFree      func(fileOffset FileOffset, size int) error
	postFree     func(fileOffset FileOffset, size int) error
}

func NewOmniBlockAllocator(blockSize []int, blockCount int, parent Allocator) *omniBlockAllocator {
	blockMap := make(map[int]*blockAllocator)
	for i, size := range blockSize {
		blockMap[size] = NewBlockAllocator(size, blockCount, 0, ObjectId(i))
	}
	return &omniBlockAllocator{
		blockMap:   blockMap,
		blockCount: blockCount,
		parent:     parent,
	}
}

func (o *omniBlockAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	if o.preAllocate != nil {
		if err := o.preAllocate(size); err != nil {
			return 0, 0, err
		}
	}

	allocator, ok := o.blockMap[size]
	var objectId ObjectId
	var fileOffset FileOffset
	var err error
	if ok {
		objectId, fileOffset, err = allocator.Allocate()
	} else {
		// Defer to the parent allocator if size is not found
		objectId, fileOffset, err = o.parent.Allocate(size)
	}

	if o.postAllocate != nil {
		if postErr := o.postAllocate(objectId, fileOffset); postErr != nil {
			return 0, 0, postErr
		}
	}

	return objectId, fileOffset, err
}

func (o *omniBlockAllocator) Free(fileOffset FileOffset, size int) error {
	if o.preFree != nil {
		if err := o.preFree(fileOffset, size); err != nil {
			return err
		}
	}

	allocator, ok := o.blockMap[size]
	var err error
	if ok {
		err = allocator.Free(fileOffset, size)
	} else {
		// Defer to the parent allocator if size is not found
		err = o.parent.Free(fileOffset, size)
	}

	if o.postFree != nil {
		if postErr := o.postFree(fileOffset, size); postErr != nil {
			return postErr
		}
	}

	return err
}
