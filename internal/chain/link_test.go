package chain

import (
	"os"
	"path/filepath"
	"testing"

	"bobbob/internal/store"
)

func TestChainAndLink(t *testing.T) {
	chain := NewChain(func() *Link {
		return NewLink(3)
	}, func(i, j any) bool {
		return i.(string) < j.(string)
	})

	// Create and add the link to the chain
	link := chain.createLink()
	chain.AddLink(link)

	// Test adding elements to a link
	err := link.AddElement("element1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	err = link.AddElement("element2")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	err = link.AddElement("element3")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Test adding an element that exceeds the link's maximum size
	err = link.AddElement("element4")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if link.next == nil {
		t.Errorf("expected a new link to be created")
	}
	if len(link.next.elements) != 1 || link.next.elements[0] != "element4" {
		t.Errorf("expected element4 to be in the new link")
	}

	// Test deleting an element from a link
	err = link.DeleteElement(1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(link.elements) != 2 || link.elements[0] != "element1" || link.elements[1] != "element3" {
		t.Errorf("unexpected elements in link: %v", link.elements)
	}

	// Test deleting all elements from a link, causing the link to be removed from the chain
	err = link.DeleteElement(0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	err = link.DeleteElement(0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if chain.head == link || chain.tail == link {
		t.Errorf("expected link to be removed from the chain")
	}
}

func TestDeleteLink(t *testing.T) {
	chain := NewChain(func() *Link {
		return NewLink(3)
	}, func(i, j any) bool {
		return i.(string) < j.(string)
	})

	link1 := chain.createLink()
	link2 := chain.createLink()
	link3 := chain.createLink()

	chain.AddLink(link1)
	chain.AddLink(link2)
	chain.AddLink(link3)

	// Test deleting the middle link
	chain.DeleteLink(link2)
	if link1.next != link3 || link3.prev != link1 {
		t.Errorf("expected link1 and link3 to be connected")
	}
	if chain.head != link1 || chain.tail != link3 {
		t.Errorf("unexpected head or tail of the chain")
	}

	// Test deleting the head link
	chain.DeleteLink(link1)
	if chain.head != link3 || link3.prev != nil {
		t.Errorf("expected link3 to be the new head")
	}

	// Test deleting the tail link
	chain.DeleteLink(link3)
	if chain.head != nil || chain.tail != nil {
		t.Errorf("expected the chain to be empty")
	}
}

func TestInsertElement(t *testing.T) {
	chain := NewChain(func() *Link {
		return NewLink(3)
	}, func(i, j any) bool {
		return i.(string) < j.(string)
	})

	// Test inserting elements into an empty chain
	err := chain.InsertElement("element1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if chain.head == nil || chain.head.elements[0] != "element1" {
		t.Errorf("expected element1 to be in the head link")
	}

	// Test inserting elements into a non-empty chain
	err = chain.InsertElement("element2")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	err = chain.InsertElement("element0")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if chain.head.sorted == true {
		t.Errorf("expected head link to be unsorted")
	}
	chain.head.Sort(nil)
	if len(chain.head.elements) != 3 || chain.head.elements[0] != "element0" || chain.head.elements[1] != "element1" || chain.head.elements[2] != "element2" {
		t.Errorf("unexpected elements in head link: %v", chain.head.elements)
	}

	// Test inserting an element that exceeds the link's maximum size
	err = chain.InsertElement("element3")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if chain.head.next == nil || chain.head.next.elements[0] != "element3" {
		t.Errorf("expected element3 to be in the new link")
	}

	// Test inserting elements in sorted order
	err = chain.InsertElement("element4")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	err = chain.InsertElement("element5")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(chain.head.next.elements) != 3 || chain.head.next.elements[0] != "element3" || chain.head.next.elements[1] != "element4" || chain.head.next.elements[2] != "element5" {
		t.Errorf("unexpected elements in second link: %v", chain.head.next.elements)
	}
}

func setupTestStore(t *testing.T) (string, store.Storer) {
	dir, err := os.MkdirTemp("", "store_test")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	filePath := filepath.Join(dir, "testfile.bin")
	store, err := store.NewBasicStore(filePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	return dir, store
}

func TestChainMarshalUnmarshalOneInt(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Create a Chain
	createLink := func() *Link {
		return NewLink(10)
	}
	less := func(i, j any) bool {
		return i.(int) < j.(int)
	}
	c := NewChain(createLink, less)
	c.store = store

	// Add a single integer into the chain via InsertElement
	element := 42
	err := c.InsertElement(element)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Marshal the head link
	data, err := c.head.Marshal()
	if err != nil {
		t.Fatalf("expected no error marshalling link, got %v", err)
	}

	// Unmarshal the data into a new Link
	newLink := NewLink(16)
	newLink.chain = c

	err = newLink.Unmarshal(data, func() any { var i int64; return &i })
	if err != nil {
		t.Fatalf("expected no error unmarshalling link, got %v", err)
	}
	newElement := newLink.elements[0]
	newVal, ok := newElement.(*int64)
	if !ok {
		t.Fatalf("expected element to be an *int64, got %T", newElement)
	}
	if *newVal != 42 {
		t.Fatalf("expected newVal to be 42, got %d", newVal)
	}
}

func TestChainGrowth(t *testing.T) {
	// Create a Chain with a max link size of 4
	createLink := func() *Link {
		return NewLink(4)
	}
	less := func(i, j any) bool {
		return *i.(*int32) < *j.(*int32)
	}
	c := NewChain(createLink, less)

	// Add 12 objects to the chain
	for i := int32(0); i < 12; i++ {
		val := i
		err := c.InsertElement(&val)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	// Count the number of links in the chain
	linkCount := 0
	for link := c.head; link != nil; link = link.next {
		linkCount++
	}

	// Check if the number of links is as expected
	expectedLinks := 3
	if linkCount != expectedLinks {
		t.Errorf("expected %d links, got %d", expectedLinks, linkCount)
	}
}

func TestLinkSort(t *testing.T) {
	// Create a mock mockStore
	dir, mockStore := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer mockStore.Close()

	// Create a chain with a simple less function for integers
	chain := NewChain(func() *Link {
		return NewLink(10)
	}, func(i, j any) bool {
		return i.(int) < j.(int)
	})
	chain.store = mockStore

	elementMapping := make(map[int]int)
	elementMapping[1] = 1
	elementMapping[5] = 9
	elementMapping[3] = 16
	elementMapping[8] = 3
	elementMapping[7] = 8

	// Create a link and add elements
	link := chain.AddLink(nil)

	for key, value := range elementMapping {
		link.AddElementAndObj(key, store.ObjectId(value))
	}

	// Sort the link
	link.Sort(nil)

	// Verify that elements are sorted
	expectedElements := []int{1, 3, 5, 7, 8}
	for i, element := range link.elements {
		if element.(int) != expectedElements[i] {
			t.Errorf("expected element %d, got %d", expectedElements[i], element.(int))
		}
		actualObjId := int(link.elementsFileObjIds[i])
		expectedObjId := elementMapping[element.(int)]
		if actualObjId != expectedObjId {
			t.Errorf("expected object ID %d, got %d", expectedObjId, actualObjId)
		}
	}
}
