package store

import (
	"errors"
)

var AllAllocated = errors.New("no free blocks available")

type BlockAllocator struct {
	blockSize          int
	blockCount         int
	allocatedList      []bool
	startingFileOffset FileOffset
	startingObjectId   ObjectId
}

func NewBlockAllocator(blockSize, blockCount int, startingFileOffset FileOffset, startingObjectId ObjectId) *BlockAllocator {
	allocatedList := make([]bool, blockCount)
	return &BlockAllocator{
		blockSize:          blockSize,
		blockCount:         blockCount,
		allocatedList:      allocatedList,
		startingFileOffset: startingFileOffset,
		startingObjectId:   startingObjectId,
	}
}

func (a *BlockAllocator) Allocate() (ObjectId, FileOffset, error) {
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

func (a *BlockAllocator) Free(fileOffset FileOffset) error {
	blockIndex := (fileOffset - a.startingFileOffset) / FileOffset(a.blockSize)
	if blockIndex < 0 || blockIndex >= FileOffset(len(a.allocatedList)) {
		return errors.New("invalid file offset")
	}
	a.allocatedList[blockIndex] = false
	return nil
}

func (a *BlockAllocator) Marshal() ([]byte, error) {
	byteCount := (a.blockCount + 7) / 8
	data := make([]byte, byteCount)
	for i, allocated := range a.allocatedList {
		if allocated {
			data[i/8] |= 1 << (i % 8)
		}
	}
	return data, nil
}

func (a *BlockAllocator) Unmarshal(data []byte) error {
	if len(data) != (a.blockCount+7)/8 {
		return errors.New("invalid data length")
	}
	for i := range a.allocatedList {
		a.allocatedList[i] = (data[i/8] & (1 << (i % 8))) != 0
	}
	return nil
}
