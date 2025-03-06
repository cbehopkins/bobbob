package chain

import (
	"errors"
	"slices"
	"sort"
)

// Chain represents a list of links with a callback to create new links.
type Chain struct {
	createLink func() *Link
	less       func(i, j any) bool
	head       *Link
	tail       *Link
}

// NewChain creates a new Chain with the given callback to create new links and a less function.
func NewChain(createLink func() *Link, less func(i, j any) bool) *Chain {
	return &Chain{
		createLink: createLink,
		less:       less,
	}
}

// AddLink adds a new link to the chain.
func (c *Chain) AddLink(link *Link) {
	if c.head == nil {
		c.head = link
		c.tail = link
	} else {
		c.tail.next = link
		link.prev = c.tail
		c.tail = link
	}
	link.chain = c
}

// DeleteLink removes a link from the chain.
func (c *Chain) DeleteLink(link *Link) {
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
}

// NewLink creates a new Link with the given maximum size.
func NewLink(maxSize int) *Link {
	return &Link{
		elements: make([]any, 0, maxSize),
		maxSize:  maxSize,
	}
}

// AddElement adds an element to the link. If the link exceeds its maximum size, it creates a new link and adds the element there.
func (l *Link) AddElement(element any) error {
	if len(l.elements) >= l.maxSize {
		newLink := l.chain.createLink()
		newLink.AddElement(element)
		l.next = newLink
		newLink.prev = l
		l.chain.AddLink(newLink)
		return nil
	}
	l.elements = append(l.elements, element)
	// To minimise copying, we only sort when necessary
	l.sorted = false
	return nil
}

// DeleteElement deletes an element from the link by index. Returns an error if the index is out of range.
// If the link has no elements remaining after deletion, it removes the link from the chain.
func (l *Link) DeleteElement(index int) error {
	if index < 0 || index >= len(l.elements) {
		return errors.New("index out of range")
	}
	l.elements = slices.Delete(l.elements, index, index+1)
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
	if less == nil {
		less = l.chain.less
	}
	l.sorted = true
	sort.Slice(l.elements, func(i, j int) bool {
		return less(l.elements[i], l.elements[j])
	})
}

// Min returns the first element in the link
func (l *Link) Min() (any, error) {
	if !l.sorted {
		l.Sort(l.chain.less)
		l.sorted = true
	}
	if len(l.elements) == 0 {
		return nil, errors.New("link is empty")
	}
	return l.elements[0], nil
}

// Max returns the last element in the link
func (l *Link) Max() (any, error) {
	if !l.sorted {
		l.Sort(l.chain.less)
		l.sorted = true
	}
	if len(l.elements) == 0 {
		return nil, errors.New("link is empty")
	}
	return l.elements[len(l.elements)-1], nil
}
