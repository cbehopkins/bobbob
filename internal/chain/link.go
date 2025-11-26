package chain

import (
	"encoding/binary"
	"errors"
	"slices"
	"sort"

	"bobbob/internal/store"
)

// Chain represents a doubly-linked list of Link nodes.
// It maintains references to head and tail, and provides methods
// to insert, delete, and sort elements across links.
type Chain struct {
	createLink func() *Link
	less       func(i, j any) bool
	store      store.Storer
	head       *Link
	tail       *Link
	UnSorted   bool // UnSorted specifies if the chain maintains sorted order
}

// NewChain creates a new Chain with the given link factory and comparison function.
// The createLink function is called when new links need to be created.
// The less function is used to maintain sorted order of elements.
func NewChain(createLink func() *Link, less func(i, j any) bool) *Chain {
	return &Chain{
		createLink: createLink,
		less:       less,
	}
}

// AddLink appends a link to the end of the chain.
// If link is nil, a new link is created using the chain's createLink function.
func (c *Chain) AddLink(link *Link) *Link {
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

// DeleteLink removes a link from the chain and updates the prev/next pointers
// of adjacent links. It also calls Delete on the link to clean up resources.
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

// SortMultiple takes multiple links, merges all their elements, sorts them,
// and redistributes them into a new list of links respecting maxSize constraints.
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

// InsertElement inserts an element into the appropriate link in the chain.
// If the chain is sorted, it finds the correct position based on the less function.
// If unsorted, it inserts into the head or tail link depending on available space.
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

// Link represents a node in a Chain containing a slice of elements.
// Each link has a maximum capacity and maintains pointers to prev/next links.
// Links can be persisted to a store with their elements.
type Link struct {
	elements           []any
	maxSize            int
	prev               *Link
	next               *Link
	chain              *Chain
	sorted             bool
	objectId           *store.ObjectId
	elementsFileObjIds []store.ObjectId
}

// NewLink creates a new Link with the given maximum size.
// Returns nil if maxSize is invalid (<= 0 or > 2^31-1).
func NewLink(maxSize int) *Link {
	if maxSize <= 0 {
		return nil
	}
	if maxSize > ((1 << 31) - 1) {
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

// AddElement adds an element to the link.
// If the link is at capacity, a new link is created and added to the chain.
func (l *Link) AddElement(element any) error {
	return l.AddElementAndObj(element, store.ObjNotAllocated)
}

// AddElementAndObj adds an element to the link with an associated ObjectId.
// This allows you to specify the ObjectId in the store at the same time.
// If the link is at capacity, a new link is created.
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
	l.elementsFileObjIds = append(l.elementsFileObjIds, objId)
	// To minimise copying, we only sort when necessary
	l.sorted = false
	return nil
}

// DeleteElement removes the element at the given index.
// Returns an error if the index is out of range.
// If the link becomes empty after deletion, it is removed from the chain.
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

// Sort sorts the elements within the link using the provided less function.
// If the link is already sorted or less is nil, it uses the chain's less function.
// This also sorts the corresponding elementsFileObjIds.
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

// Min returns the smallest element in the link.
// The link is sorted if necessary before returning the minimum.
func (l *Link) Min() (any, error) {
	if !l.sorted {
		l.Sort(nil)
	}
	if len(l.elements) == 0 {
		return nil, errors.New("link is empty")
	}
	return l.elements[0], nil
}

// Max returns the largest element in the link.
// The link is sorted if necessary before returning the maximum.
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

// ChildObjects returns the ObjectIds of all allocated elements in the link.
// It excludes any elements that have not yet been allocated (ObjNotAllocated).
func (l *Link) ChildObjects() []store.ObjectId {
	objs := make([]store.ObjectId, 0, l.maxSize)
	for _, objId := range l.elementsFileObjIds {
		if objId != store.ObjNotAllocated {
			objs = append(objs, objId)
		}
	}
	return objs
}

// Delete removes all child objects from the store.
// This should be called when removing the link from the chain.
func (l *Link) Delete() error {
	for _, obj := range l.ChildObjects() {
		err := l.chain.store.DeleteObj(obj)
		if err != nil {
			return err
		}
	}
	return nil
}

// ObjectId returns the ObjectId for this link in the store.
// If the link hasn't been persisted yet, it allocates a new object.
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
			objId, err := store.WriteGeneric(l.chain.store, element)
			if err != nil {
				return err
			}
			l.elementsFileObjIds[i] = objId
		}
	}
	return nil
}

// Marshal serializes the link into a fixed-size byte array.
// This includes the element ObjectIds, maxSize, and prev/next/self ObjectIds.
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
	offset := 0

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

// Unmarshal deserializes the link from a byte array and loads all elements.
// The newObj function is called to create instances for each element,
// which are then populated by reading from the store.
func (l *Link) Unmarshal(data []byte, newObj func() any) error {
	err := l.unmarshal(data)
	if err != nil {
		return err
	}
	l.elements = l.elements[:0]
	for _, objId := range l.elementsFileObjIds {
		if objId == store.ObjNotAllocated {
			return errors.New("unmarshal failed: elementsFileObjIds not populated")
		}

		newElm := newObj()
		err := store.ReadGeneric(l.chain.store, newElm, objId)
		if err != nil {
			return err
		}
		l.elements = append(l.elements, newElm)
	}
	return nil
}
