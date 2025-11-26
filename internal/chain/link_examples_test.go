package chain

import (
	"fmt"
	"os"
	"path/filepath"

	"bobbob/internal/store"
)

// Example demonstrates creating a chain with integer elements.
func Example() {
	// Create a comparison function for integers
	lessFunc := func(a, b any) bool {
		return a.(int) < b.(int)
	}

	// Create a link factory
	createLink := func() *Link {
		return NewLink(5) // Max 5 elements per link
	}

	// Create a new chain
	chain := NewChain(createLink, lessFunc)

	// Insert some elements
	chain.InsertElement(10)
	chain.InsertElement(5)
	chain.InsertElement(15)
	chain.InsertElement(3)

	// Access the head link
	head := chain.head
	head.Sort(nil)

	// Print sorted elements
	for _, elem := range head.elements {
		fmt.Printf("%d ", elem)
	}
	// Output: 3 5 10 15
}

// ExampleChain_InsertElement demonstrates inserting elements into a sorted chain.
func ExampleChain_InsertElement() {
	lessFunc := func(a, b any) bool {
		return a.(string) < b.(string)
	}

	createLink := func() *Link {
		return NewLink(10) // Larger capacity for simpler example
	}

	chain := NewChain(createLink, lessFunc)

	// Insert elements - they'll be sorted automatically
	chain.InsertElement("dog")
	chain.InsertElement("cat")
	chain.InsertElement("bird")
	chain.InsertElement("ant")

	// Sort and walk through all links printing elements
	currentLink := chain.head
	for currentLink != nil {
		currentLink.Sort(nil)
		for _, elem := range currentLink.elements {
			fmt.Printf("%s ", elem)
		}
		currentLink = currentLink.next
	}
	fmt.Println()
	// Output: ant bird cat dog
}

// ExampleLink_Marshal demonstrates persisting a link to a store.
func ExampleLink_Marshal() {
	tmpFile := filepath.Join(os.TempDir(), "example_link.bin")
	defer os.Remove(tmpFile)

	s, _ := store.NewBasicStore(tmpFile)
	defer s.Close()

	// Create a chain with a store
	lessFunc := func(a, b any) bool {
		return a.(int) < b.(int)
	}

	createLink := func() *Link {
		return NewLink(5)
	}

	chain := NewChain(createLink, lessFunc)
	chain.store = s

	// Add elements
	link := chain.AddLink(nil)
	link.AddElement(42)
	link.AddElement(17)
	link.AddElement(99)

	// Marshal the link (size depends on Link structure)
	data, _ := link.Marshal()

	// Verify we got data
	if len(data) > 0 {
		fmt.Println("Link marshaled successfully")
	}
	// Output: Link marshaled successfully
}

// ExampleChain_sortMultiple demonstrates merging and sorting multiple links.
func ExampleChain_sortMultiple() {
	lessFunc := func(a, b any) bool {
		return a.(int) < b.(int)
	}

	createLink := func() *Link {
		return NewLink(10)
	}

	chain := NewChain(createLink, lessFunc)

	// Create multiple links with unsorted elements
	link1 := NewLink(10)
	link1.elements = []any{5, 2, 8}

	link2 := NewLink(10)
	link2.elements = []any{1, 9, 3}

	link3 := NewLink(10)
	link3.elements = []any{7, 4, 6}

	// Sort and merge all links
	sortedLinks := chain.SortMultiple([]*Link{link1, link2, link3})

	// Print the sorted elements
	for _, link := range sortedLinks {
		for _, elem := range link.elements {
			fmt.Printf("%d ", elem)
		}
	}
	// Output: 1 2 3 4 5 6 7 8 9
}

// ExampleLink_deleteElement demonstrates removing elements from a link.
func ExampleLink_deleteElement() {
	lessFunc := func(a, b any) bool {
		return a.(string) < b.(string)
	}

	createLink := func() *Link {
		return NewLink(5)
	}

	chain := NewChain(createLink, lessFunc)
	link := chain.AddLink(nil)

	// Add some elements
	link.AddElement("apple")
	link.AddElement("banana")
	link.AddElement("cherry")

	// Delete the middle element
	link.DeleteElement(1)

	// Print remaining elements
	for _, elem := range link.elements {
		fmt.Printf("%s ", elem)
	}
	// Output: apple cherry
}
