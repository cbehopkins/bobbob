package store

import (
	"container/heap"
	"io"
	"os"
)

type Gap struct {
	Start int64
	End   int64
}

// GapHeap is a min-heap of Gaps
type GapHeap []Gap

func (h GapHeap) Len() int           { return len(h) }
func (h GapHeap) Less(i, j int) bool { return h[i].End-h[i].Start < h[j].End-h[j].Start }
func (h GapHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *GapHeap) Push(x any) {
	*h = append(*h, x.(Gap))
}

func (h *GapHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

//	type Allocator interface  {
//		Allocate(size int) (ObjectId, FileOffset, error)
//		Free(fileOffset FileOffset, size int) error
//	}
//
// BasicAllocator is a struct to manage the allocation of space in the file
// FIXME freelist should be stored on disk
type BasicAllocator struct {
	freeList GapHeap
	end      int64
}

func NewAllocator(file *os.File) (*BasicAllocator, error) {
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return &BasicAllocator{end: offset, freeList: make(GapHeap, 0)}, nil
}

// RefreshFreeList to refresh the free list
// This is an io intensive function
func (a *BasicAllocator) RefreshFreeList(s *baseStore) error {
	return nil
}

// Allocate to request a new space
func (a *BasicAllocator) Allocate(size int) (ObjectId, FileOffset, error) {
	var gap Gap
	if len(a.freeList) > 0 {
		gap = heap.Pop(&a.freeList).(Gap)
		if gap.End-gap.Start >= int64(size) {
			objId := ObjectId(gap.Start)
			fileOffset := FileOffset(gap.Start)
			if gap.End-gap.Start > int64(size) {
				heap.Push(&a.freeList, Gap{gap.Start + int64(size), gap.End})
			}
			return objId, fileOffset, nil
		}
	}
	objId := ObjectId(a.end)
	fileOffset := FileOffset(a.end)
	a.end += int64(size)
	return objId, fileOffset, nil
}

// Free to mark the space as free
func (a *BasicAllocator) Free(fileOffset FileOffset, size int) error {
	heap.Push(&a.freeList, Gap{int64(fileOffset), int64(fileOffset) + int64(size)})
	return nil
}
