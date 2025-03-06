package chain

// import (
//     "container/heap"
//     "errors"
//     "sort"
// )

// // ElementHeap is a min-heap of elements with their originating link.
// type ElementHeap struct {
//     elements []heapElement
// }

// type heapElement struct {
//     value interface{}
//     link  *Link
//     index int
// }

// func (h ElementHeap) Len() int           { return len(h.elements) }
// func (h ElementHeap) Less(i, j int) bool { return h.elements[i].value.(int) < h.elements[j].value.(int) }
// func (h ElementHeap) Swap(i, j int)      { h.elements[i], h.elements[j] = h.elements[j], h.elements[i] }

// func (h *ElementHeap) Push(x interface{}) {
//     h.elements = append(h.elements, x.(heapElement))
// }

// func (h *ElementHeap) Pop() interface{} {
//     old := h.elements
//     n := len(old)
//     x := old[n-1]
//     h.elements = old[0 : n-1]
//     return x
// }

// // SortChain sorts the chain using an external merge sort algorithm.
// func (c *Chain) SortChain(chunkSize int) error {
//     if c.head == nil {
//         return errors.New("chain is empty")
//     }

//     // Step 1: Divide the chain into chunks and sort each chunk
//     chunks := []*Link{}
//     current := c.head
//     for current != nil {
//         chunk := []interface{}{}
//         for current != nil && len(chunk) < chunkSize {
//             chunk = append(chunk, current.elements...)
//             current = current.next
//         }
//         // Sort the chunk
//         sort.Slice(chunk, func(i, j int) bool {
//             return chunk[i].(int) < chunk[j].(int)
//         })
//         // Create a new link for the sorted chunk
//         sortedLink := NewLink(len(chunk))
//         sortedLink.elements = chunk
//         chunks = append(chunks, sortedLink)
//     }

//     // Step 2: Merge the sorted chunks
//     h := &ElementHeap{}
//     heap.Init(h)
//     for _, chunk := range chunks {
//         if len(chunk.elements) > 0 {
//             heap.Push(h, heapElement{value: chunk.elements[0], link: chunk, index: 0})
//         }
//     }

//     sortedChain := NewChain(c.createLink)
//     for h.Len() > 0 {
//         smallest := heap.Pop(h).(heapElement)
//         sortedChain.AddElement(smallest.value)
//         if smallest.index+1 < len(smallest.link.elements) {
//             heap.Push(h, heapElement{value: smallest.link.elements[smallest.index+1], link: smallest.link, index: smallest.index + 1})
//         }
//     }

//     // Replace the original chain with the sorted chain
//     c.head = sortedChain.head
//     c.tail = sortedChain.tail

//     return nil
// }

// // AddElement adds an element to the chain.
// func (c *Chain) AddElement(element interface{}) error {
//     if c.tail == nil {
//         newLink := c.createLink()
//         c.AddLink(newLink)
//     }
//     return c.tail.AddElement(element)
// }

// func main() {
//     // Example usage
//     chain := NewChain(func() *Link {
//         return NewLink(3)
//     })

//     // Add elements to the chain
//     chain.AddElement(5)
//     chain.AddElement(1)
//     chain.AddElement(3)
//     chain.AddElement(2)
//     chain.AddElement(4)

//     // Sort the chain with a chunk size of 3
//     err := chain.SortChain(3)
//     if err != nil {
//         fmt.Println(err)
//     }

//     // Print the sorted chain
//     current := chain.head
//     for current != nil {
//         fmt.Println(current.elements)
//         current = current.next
//     }
// }
