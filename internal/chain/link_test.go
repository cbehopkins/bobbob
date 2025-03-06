package chain

import (
	"testing"
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
