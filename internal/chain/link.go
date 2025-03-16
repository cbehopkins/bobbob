package chain

import (
	"encoding/binary"
	"errors"
	"slices"
	"sort"

	"github.com/cbehopkins/bobbob/internal/store"
)

// Chain represents a list of links with a callback to create new links.
type Chain struct {
	createLink func() *Link
	less       func(i, j any) bool
	store 		store.Storer
	head       *Link
	tail       *Link
	UnSorted   bool // New member to specify if the chain is unsorted
}

// NewChain creates a new Chain with the given callback to create new links and a less function.
func NewChain(createLink func() *Link, less func(i, j any) bool) *Chain {
	return &Chain{
		createLink: createLink,
		less:       less,
	}
}

// AddLink adds a new link to the chain.
func (c *Chain) AddLink(link *Link) *Link{
	if link == nil {
		link = c.createLink()
	}
	if c.head == nil {
		c.head = link
		c.tail = link
	} else {
		c.tail.next = link
		link.prev = c.tail
		c.tail = link
	}
	link.chain = c
	return link
}

// DeleteLink removes a link from the chain.
func (c *Chain) DeleteLink(link *Link) error {
	if link.prev != nil {
		link.prev.next = link.next
	} else {
		c.head = link.next
	}
	if link.next != nil {
		link.next.prev = link.prev
	} else {
		c.tail = link.prev
	}
	link.prev = nil
	link.next = nil
	link.chain = nil
	return link.Delete()
}

// SortMultiple sorts elements across multiple links and creates a new list of links with the sorted elements.
func (c *Chain) SortMultiple(links []*Link) []*Link {
	var allElements []any
	for _, link := range links {
		allElements = append(allElements, link.elements...)
	}

	sort.Slice(allElements, func(i, j int) bool {
		return c.less(allElements[i], allElements[j])
	})

	var sortedLinks []*Link
	currentLink := c.createLink()
	sortedLinks = append(sortedLinks, currentLink)

	for _, element := range allElements {
		if len(currentLink.elements) >= currentLink.maxSize {
			newLink := c.createLink()
			sortedLinks = append(sortedLinks, newLink)
			currentLink.next = newLink
			newLink.prev = currentLink
			currentLink = newLink
		}
		currentLink.elements = append(currentLink.elements, element)
	}

	return sortedLinks
}

// InsertElement inserts an element into the correct link in the chain.
func (c *Chain) InsertElement(element any) error {
	if c.head == nil {
		// If the chain is empty, create a new link and add the element.
		newLink := c.createLink()
		newLink.AddElement(element)
		c.AddLink(newLink)
		return nil
	}

	if c.UnSorted {
		// If the chain is unsorted, insert into the head if there's space, otherwise insert into the tail.
		if len(c.head.elements) < c.head.maxSize {
			return c.head.AddElement(element)
		}
		return c.tail.AddElement(element)
	}

	currentLink := c.head
	for currentLink != nil {
		min, err := currentLink.Min()
		if err != nil {
			return err
		}
		max, err := currentLink.Max()
		if err != nil {
			return err
		}

		if c.less(element, min) {
			// Move to the previous link if the element is less than the minimum.
			if currentLink.prev != nil {
				currentLink = currentLink.prev
			} else {
				return currentLink.AddElement(element)
			}
		} else if c.less(max, element) {
			// Move to the next link if the element is greater than the maximum.
			if currentLink.next != nil {
				currentLink = currentLink.next
			} else {
				// Insert at the end if there is no next link.
				return currentLink.AddElement(element)
			}
		} else {
			// Insert the element into the current link.
			return currentLink.AddElement(element)
		}
	}

	return errors.New("failed to insert element")
}

// I would like to be able to Marshal and Unmarshal the link structure, ready for writing the chain/link to disk
// But this is not trivial.
// There are some fields (e.g. sorted) That should not be part of the marshalled structure and therefore need to be excluded from the process.
// Similarly the prev and next refer to memory addresses, not file offsets - we potentially have a circular dependency that we cannot populate the disk versions prev and next until we marshal, but we also cannot marshal until we have all the values.

// Therefore let's break this problem down. I'm assuming we are using our store to store this. Therefore our Link will need two new fields, prevFileObjId and nextFileObjId. These will be of type ObjectId
// We will also need an elementsFileObjIds - which is an

// Link represents a slice of elements with a maximum size and pointers to the previous and next links.
type Link struct {
	elements []any
	maxSize  int
	prev     *Link
	next     *Link
	chain    *Chain
	sorted   bool
	objectId *store.ObjectId
	elementsFileObjIds []store.ObjectId
}

// NewLink creates a new Link with the given maximum size.
func NewLink(maxSize int) *Link {
	if maxSize <= 0 {
		return nil
	}
	if maxSize > ((1<<31)-1) {
		return nil
	}
	return &Link{
		elements: make([]any, 0, maxSize),
		maxSize:  maxSize,
	}
}
func (l *Link) markStale() {
	// There is a possible improvement here
	// We could keep using the existing object and overwrite it
	// This would save us from having to allocate a new object
	// But that would mean we could get the store in a self inconsistent state
	// if we had an error during the write
	// Given all links should be the same size, we are highly likely to re-use the space
	// So KISS
	if l.objectId == nil {
		return
	}
	if l.chain.store != nil {
		l.chain.store.DeleteObj(*l.objectId)
	}
	l.objectId = nil
}
// AddElement adds an element to the link. If the link exceeds its maximum size, it creates a new link and adds the element there.
func (l *Link) AddElement(element any) error {
	return l.AddElementAndObj(element, store.ObjNotAllocated)
}
// AddElement adds an element to the link. If the link exceeds its maximum size, it creates a new link and adds the element there.
// This allows you to specify the OnjectIfd in the store at the same time
func (l *Link) AddElementAndObj(element any, objId store.ObjectId) error {
	l.markStale()
	if len(l.elements) >= l.maxSize {
		newLink := l.chain.createLink()
		newLink.AddElement(element)
		l.next = newLink
		newLink.prev = l
		l.chain.AddLink(newLink)
		return nil
	}
	l.elements = append(l.elements, element)
	l.elementsFileObjIds = append(l.elementsFileObjIds,objId)
	// To minimise copying, we only sort when necessary
	l.sorted = false
	return nil
}

// DeleteElement deletes an element from the link by index. Returns an error if the index is out of range.
// If the link has no elements remaining after deletion, it removes the link from the chain.
func (l *Link) DeleteElement(index int) error {
	l.markStale()
	if index < 0 || index >= len(l.elements) {
		return errors.New("index out of range")
	}
	l.elements = slices.Delete(l.elements, index, index+1)
	if l.chain.store != nil {
		l.chain.store.DeleteObj(l.elementsFileObjIds[index])
	}
	l.elementsFileObjIds = slices.Delete(l.elementsFileObjIds, index, index+1)
	if len(l.elements) == 0 {
		l.chain.DeleteLink(l)
	}
	return nil
}

// Sort the elements within the link.
func (l *Link) Sort(less func(i, j any) bool) {
    if l.sorted {
        return
    }
    l.markStale()
    if less == nil {
		if l.chain == nil {
			return
		}
        less = l.chain.less
    }
    l.sorted = true

    // Create a slice of indices to sort both elements and elementsFileObjIds
    indices := make([]int, len(l.elements))
    for i := range indices {
        indices[i] = i
    }

    // Sort the indices based on the elements
    sort.Slice(indices, func(i, j int) bool {
        return less(l.elements[indices[i]], l.elements[indices[j]])
    })

    // Create new slices for sorted elements and elementsFileObjIds
    sortedElements := make([]any, len(l.elements))
    sortedElementsFileObjIds := make([]store.ObjectId, len(l.elementsFileObjIds))

    // Populate the new slices based on the sorted indices
    for i, idx := range indices {
        sortedElements[i] = l.elements[idx]
        sortedElementsFileObjIds[i] = l.elementsFileObjIds[idx]
    }

    // Replace the original slices with the sorted ones
    l.elements = sortedElements
    l.elementsFileObjIds = sortedElementsFileObjIds
}

// Min returns the first element in the link
func (l *Link) Min() (any, error) {
	if !l.sorted {
		l.Sort(nil)
	}
	if len(l.elements) == 0 {
		return nil, errors.New("link is empty")
	}
	return l.elements[0], nil
}

// Max returns the last element in the link
func (l *Link) Max() (any, error) {
	if !l.sorted {
		l.Sort(nil)
	}
	if len(l.elements) == 0 {
		return nil, errors.New("link is empty")
	}
	return l.elements[len(l.elements)-1], nil
}
// bytesNeeded returns the number of bytes needed to marshal the link.
// This should be determined soley by the capacity of elements i.e. maxSize
func (l Link) bytesNeeded() int {
	bytesForObjectId := 8
	bytesPerElement := bytesForObjectId
	bytesForElements := l.maxSize * bytesPerElement
	bytesForElementLen := 4
	bytesForMaxSize := 4

	bytesForPrev := bytesForObjectId
	bytesForNext := bytesForObjectId
	bytesForSelf := bytesForObjectId
	return bytesForElements + bytesForElementLen + bytesForMaxSize + bytesForPrev + bytesForNext + bytesForSelf
}
func (l *Link) ChildObjects() []store.ObjectId {
	objs := make([]store.ObjectId, 0, l.maxSize)
	for _, objId := range l.elementsFileObjIds {
		if objId != store.ObjNotAllocated {
			objs = append(objs, objId)
		}
	}
	return objs
}
func (l *Link) Delete() error {
	for _, obj :=  range l.ChildObjects() {
		err:= l.chain.store.DeleteObj(obj)
		if err != nil {
			return err
		}
	}
	return nil
}
func (l *Link) ObjectId() (store.ObjectId, error) {
	if l.objectId != nil {
		return *l.objectId, nil
	}
	objId, err := l.chain.store.NewObj(l.bytesNeeded())
	if err != nil {
		return objId, err
	}
	l.objectId = &objId
	return objId, nil
}
func (l *Link) writeElements() error {
	for i, element := range l.elements {
		if l.elementsFileObjIds[i] == store.ObjNotAllocated {
			objId, err := l.chain.store.WriteGeneric(element)
			if err != nil {
				return err
			}
			l.elementsFileObjIds[i] = objId
		}
	}
	return nil
}
// Marshal the striuct into a fized size byte array
func (l *Link) Marshal() ([]byte, error) {
    // Ensure elementsFileObjIds are populated
    if err := l.writeElements(); err != nil {
        return nil, err
    }

    // Calculate the total size needed for the marshalled data
    size := l.bytesNeeded()
    data := make([]byte, size)
    offset := 0
	
    // Write the number of elements
    binary.LittleEndian.PutUint32(data[offset:4], uint32(len(l.elementsFileObjIds)))
    offset += 4
	// Write the maxSize
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(l.maxSize))
	offset += 4

    // Write the elementsFileObjIds
    for _, objId := range l.elementsFileObjIds {
        binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(objId))
        offset += 8
    }


    // Write the prevObjectId
    prevObjectId := store.ObjNotAllocated
    if l.prev != nil && l.prev.objectId != nil {
        prevObjectId = *l.prev.objectId
    }
    binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(prevObjectId))
    offset += 8

    // Write the nextObjectId
    nextObjectId := store.ObjNotAllocated
    if l.next != nil && l.next.objectId != nil {
        nextObjectId = *l.next.objectId
    }
    binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(nextObjectId))
    offset += 8

    // Write the self objectId
    selfObjectId := store.ObjNotAllocated
    if l.objectId != nil {
        selfObjectId = *l.objectId
    }
    binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(selfObjectId))

    return data, nil
}

func (l *Link) unmarshal(data []byte) error {
	offset :=0

    // Read the number of elements
    numElements := binary.LittleEndian.Uint32(data[offset:4])
    l.elementsFileObjIds = make([]store.ObjectId, numElements)

	// Read the maxSize
	l.maxSize = int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

    // Read the elementsFileObjIds
    offset += 4
    for i := 0; i < int(numElements); i++ {
        l.elementsFileObjIds[i] = store.ObjectId(binary.LittleEndian.Uint64(data[offset : offset+8]))
        offset += 8
    }

    // Read the prevObjectId
    prevObjectId := store.ObjectId(binary.LittleEndian.Uint64(data[offset : offset+8]))
    offset += 8
    if prevObjectId != store.ObjNotAllocated {
        l.prev = &Link{objectId: &prevObjectId}
    }

    // Read the nextObjectId
    nextObjectId := store.ObjectId(binary.LittleEndian.Uint64(data[offset : offset+8]))
    offset += 8
    if nextObjectId != store.ObjNotAllocated {
        l.next = &Link{objectId: &nextObjectId}
    }

    // Read the self objectId
    selfObjectId := store.ObjectId(binary.LittleEndian.Uint64(data[offset : offset+8]))
    if selfObjectId != store.ObjNotAllocated {
        l.objectId = &selfObjectId
    }

    return nil
}
func (l *Link) Unmarshal(data []byte, newObj func() any) error {
	err:=  l.unmarshal(data)
	if err != nil {
		return err
	}
	l.elements = l.elements[:0]
	for _, objId := range l.elementsFileObjIds {
		if objId == store.ObjNotAllocated {
			return errors.New("unmarshal failed: elementsFileObjIds not populated")
		}

		newElm := newObj()
		err := l.chain.store.ReadGeneric(newElm, objId)
		if err != nil {
			return err
		}
		l.elements = append(l.elements, newElm)
	}
	return nil
}