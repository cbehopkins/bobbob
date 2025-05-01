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
	for i, allocated := range a.allocatedList {
		if !allocated {
			a.allocatedList[i] = true
			fileOffset := a.startingFileOffset + FileOffset(i*a.blockSize)
			objectId := a.startingObjectId + ObjectId(i)
			return objectId, FileOffset(fileOffset), nil
		}
	}
	return 0, 0, AllAllocated
}

func (a *blockAllocator) Free(fileOffset FileOffset) error {
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
	blockSize    int
	blockCount   int
	parent       Allocator
	allocators   []*blockAllocator
	startOffsets []FileOffset
	preParentAllocate func() error
	postParentAllocate func() error

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

func (m *multiBlockAllocator) Allocate() (ObjectId, FileOffset, error) {
	for _, allocator := range m.allocators {
		objectId, fileOffset, err := allocator.Allocate()
		if err == nil {
			return objectId, fileOffset, nil
		}
		if err != AllAllocated {
			return 0, 0, err
		}
	}

	// All allocators are full, create a new one
	if m.preParentAllocate != nil {
		if err := m.preParentAllocate(); err != nil {
			return 0, 0, err
		}
	}
	parentObjectId, parentFileOffset, err := m.parent.Allocate(m.blockSize * m.blockCount)
	if err != nil {
		return 0, 0, err
	}
	if m.postParentAllocate != nil {
		if err := m.postParentAllocate(); err != nil {
			return 0, 0, err
		}
	}
	

	newAllocator := NewBlockAllocator(m.blockSize, m.blockCount, parentFileOffset, parentObjectId)
	m.allocators = append(m.allocators, newAllocator)
	m.startOffsets = append(m.startOffsets, parentFileOffset)

	return newAllocator.Allocate()
}

func (m *multiBlockAllocator) Free(fileOffset FileOffset) error {
	for i, startOffset := range m.startOffsets {
		if fileOffset >= startOffset && fileOffset < startOffset+FileOffset(m.blockSize*m.blockCount) {
			return m.allocators[i].Free(fileOffset)
		}
	}
	return errors.New("invalid file offset")
}
