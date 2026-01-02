# Yggdrasil Package

## Introduction

The Yggdrasil package provides persistent tree-based data structures for the bobbob project. The name comes from Norse mythology, where Yggdrasil is the immense cosmic tree that connects all nine worlds - a fitting metaphor for a package that stores all kinds of data in tree structures that span between memory and disk.

Just as Yggdrasil's roots and branches stretch through different realms, this package's data structures bridge the gap between in-memory performance and persistent storage, allowing you to work with tree-based collections that are larger than available RAM.

## Architecture Overview

The package is built on a layered architecture where each type embeds and extends the previous:

```
Treap (in-memory)
  ↓ embeds
PersistentTreap (disk-backed)
  ↓ embeds
PersistentPayloadTreap (disk-backed with data)
```

### 1. Treap - The Foundation

A **treap** (tree + heap) is a randomized binary search tree that maintains two orderings simultaneously:
- **BST ordering by key**: Ensures efficient search, insert, and delete (O(log n) expected)
- **Heap ordering by priority**: Random priorities keep the tree balanced without complex rebalancing

```go
type Treap[T any] struct {
    root TreapNodeInterface[T]
    Less func(a, b T) bool  // Key comparison function
}

type TreapNode[T any] struct {
    key      Key[T]
    priority Priority  // Random uint32 for balancing
    left     TreapNodeInterface[T]
    right    TreapNodeInterface[T]
}
```

**Key features:**
- In-memory data structure
- Generic over key type `T`
- Self-balancing via random priorities
- Simple operations: Insert, Delete, Search
- Iteration using Go 1.23+ iterator protocol (iter.Seq)

### 2. PayloadTreap - Adding Data

Extends `Treap` to associate arbitrary data (payload) with each key:

```go
type PayloadTreap[K any, P any] struct {
    Treap[K]  // Embeds base treap
}

type PayloadTreapNode[K any, P any] struct {
    TreapNode[K]
    payload P  // Associated data
}
```

**Use case**: In-memory key-value store where both keys and values can be any type.

### 3. PersistentTreap - Adding Persistence

Extends `Treap` to store nodes on disk using the bobbob store:

```go
type PersistentTreap[T any] struct {
    Treap[T]
    keyTemplate PersistentKey[T]
    Store       store.Storer
}

type PersistentTreapNode[T any] struct {
    TreapNode[T]
    objectId      store.ObjectId  // Where this node is stored
    leftObjectId  store.ObjectId  // Where left child is stored
    rightObjectId store.ObjectId  // Where right child is stored
    Store         store.Storer
}
```

**Key features:**
- Nodes are lazy-loaded from disk
- Only accessed portions of tree kept in memory
- `Persist()`: Save node to disk
- `Flush()`: Save and remove from memory
- Works with trees larger than RAM

**Use case**: Persistent sorted collections that don't fit in memory.

### 4. PersistentPayloadTreap - The Complete Solution

Combines persistence with payloads:

```go
type PersistentPayloadTreap[K any, P PersistentPayload[P]] struct {
    PersistentTreap[K]
}

type PersistentPayloadTreapNode[K any, P PersistentPayload[P]] struct {
    PersistentTreapNode[K]
    payload P
}
```

**Use case**: Persistent key-value store with both keys and values on disk.

## Payload Serialization

For persistent treaps, payloads must implement the `PersistentPayload` interface:

```go
type PersistentPayload[T any] interface {
    Marshal() ([]byte, error)
    Unmarshal([]byte) (UntypedPersistentPayload, error)
    SizeInBytes() int
}
```

### JsonPayload - Automatic Serialization

For convenience, the `JsonPayload[T]` wrapper provides automatic JSON-based serialization for any type, eliminating the need to implement custom Marshal/Unmarshal methods:

```go
type Product struct {
    Name  string
    Price float64
    Stock int
}

// Use JsonPayload wrapper - no Marshal/Unmarshal implementation needed!
treap := NewPersistentPayloadTreap[StringKey, JsonPayload[Product]](
    StringLess,
    (*StringKey)(new(string)),
    store,
)

// Insert wrapped values
treap.Insert(&key, priority, JsonPayload[Product]{
    Value: Product{Name: "Widget", Price: 19.99, Stock: 100},
})

// Access wrapped values
node := treap.Search(&key)
product := node.GetPayload().Value  // Access the Product struct
```

**When to use JsonPayload:**
- Quick prototyping without implementing serialization
- Simple structs that work well with JSON
- Cases where serialization performance is not critical

**When to implement custom Marshal/Unmarshal:**
- Performance-critical code (JSON is slower than binary)
- Need compact binary representation
- Complex data structures with special serialization needs
- See `FileInfo` in examples_test.go for a custom implementation

## Key Types

The package uses a `Key` interface to support various key types:

```go
type Key[T any] interface {
    SizeInBytes() int
    Equals(T) bool
    Value() T
}

type PersistentKey[T any] interface {
    Key[T]
    New() PersistentKey[T]
    MarshalToObjectId(store.Storer) (store.ObjectId, error)
    UnmarshalFromObjectId(store.ObjectId, store.Storer) error
}
```

**Built-in key types:**
- `IntKey` (int32)
- `ShortUIntKey` (uint16)
- `StringKey` (string)

All support both in-memory and persistent treaps.

## Type Mapping System

The `TypeMap` provides efficient type serialization for persistent storage:

```go
type TypeMap struct {
    Types         map[string]typeTuple
    ShortMap      map[ShortCodeType]string
    NextShortCode ShortCodeType
}
```

**Purpose**: Instead of storing full type names, assign compact short codes (uint16) to each type. This saves space when storing type information with each object.

**Usage:**
```go
tm := NewTypeMap()  // Pre-populated with built-in types
tm.AddType(MyCustomType{})
tuple, ok := tm.GetTypeByName("MyCustomType")
tuple, ok := tm.GetTypeByShortCode(shortCode)
```

## Iteration Protocol

The package uses **Go 1.23+ iterator protocol** (`iter.Seq` and `iter.Seq2`) for memory-efficient traversal:

### In-Memory Treaps

Use `iter.Seq[TreapNodeInterface[T]]` for simple iteration:

```go
treap := NewTreap[int](IntLess)
// ... insert data ...

// Range over nodes in sorted order
for node := range treap.Iter() {
    fmt.Printf("Key: %d\n", node.GetKey().Value())
}
```

### Persistent Treaps  

Use `iter.Seq2[TreapNodeInterface[T], error]` with context for I/O error handling:

```go
treap := NewPersistentTreap[int](IntLess, keyTemplate, store)
// ... insert and persist data ...

ctx := context.Background()
for node, err := range treap.Iter(ctx) {
    if err != nil {
        log.Printf("Disk error: %v", err)
        break
    }
    fmt.Printf("Key: %d\n", *node.GetKey().(*IntKey))
}
```

**Key features:**
- **O(height) memory**: Uses explicit stack, not recursion
- **Streaming**: Nodes aren't buffered in memory
- **Cancellation**: Context support for persistent treaps
- **Lazy loading**: Persistent treaps load from disk on-demand
- **No allocations** for in-memory iteration after initial stack setup

## API Reference

### In-Memory Treap

**Creating:**
```go
treap := NewTreap[int](func(a, b int) bool { return a < b })
```

**Operations:**
```go
treap.Insert(key, priority)           // Add or update
treap.Delete(key)                     // Remove
node := treap.Search(key)             // Find

// Iteration using Go iterator protocol (iter.Seq)
for node := range treap.Iter() {
    // Process node in sorted order
}
```

### PayloadTreap

**Creating:**
```go
treap := NewPayloadTreap[string, MyData](StringLess)
```

**Operations:**
```go
treap.Insert(key, priority, payload)  // Add key with data
// Search, Delete, Iter inherited from Treap

// Iterate over entries in sorted order
for node := range treap.Iter() {
    payload := node.GetPayload()
}
```

### PersistentTreap

**Creating:**
```go
store, _ := store.NewBasicStore("data.blob")
keyTemplate := IntKey(0)
treap := NewPersistentTreap[int](IntLess, keyTemplate, store)
```

**Operations:**
```go
// Insert/Delete/Search same as Treap
treap.Insert(key, priority)

// Persistence operations
node.Persist()  // Save node and children to disk
node.Flush()    // Save then remove from memory
objId := node.ObjectId()  // Get disk location

// Iteration with context support (iter.Seq2)
ctx := context.Background()
for node, err := range treap.Iter(ctx) {
    if err != nil {
        // Handle disk I/O errors
        break
    }
    // Process node (may load from disk transiently)
}

// Loading
node, err := NewFromObjectId[int](objId, treap, store)
```

### PersistentPayloadTreap

**Creating:**
```go
store, _ := store.NewBasicStore("data.blob")
keyTemplate := StringKey("")
treap := NewPersistentPayloadTreap[string, MyPayload](StringLess, keyTemplate, store)
```

**Operations:**
```go
treap.Insert(key, priority, payload)  // Add with payload
node := treap.Search(key)
if node != nil {
    payloadNode := node.(*PersistentPayloadTreapNode[string, MyPayload])
    data := payloadNode.GetPayload()
}

// Persistence inherited from PersistentTreap
node.Persist()
node.Flush()
```

### TypeMap

**Creating and registering types:**
```go
tm := NewTypeMap()  // Pre-populated with built-ins
tm.AddType(MyCustomType{})
```

**Looking up types:**
```go
tuple, ok := tm.GetTypeByName("MyCustomType")
if ok {
    shortCode := tuple.ShortCode  // uint16 for storage
}

tuple, ok := tm.GetTypeByShortCode(shortCode)
if ok {
    typeName := tm.ShortMap[shortCode]
}
```

**Serialization:**
```go
data, err := tm.Marshal()   // Save to bytes
err = tm.Unmarshal(data)    // Load from bytes
```

## Memory Management

For persistent treaps working with large datasets:

1. **Lazy loading**: Nodes loaded from disk only when accessed
2. **Explicit persistence**: Call `Persist()` when you want to save
3. **Memory release**: Call `Flush()` to save and free memory
4. **Time-based flushing**: Call `FlushOlderThan()` to evict nodes not accessed recently
5. **Percentile-based flushing**: Call `FlushOldestPercentile()` to evict oldest N% of nodes
6. **Navigation**: Child nodes auto-load on access

**Example workflow:**
```go
// Insert and persist
treap.Insert(key, priority)
treap.root.(PersistentTreapNodeInterface[int]).Persist()

// Work with a portion, then free memory
node := treap.Search(key)
// ... work with node ...
node.(PersistentTreapNodeInterface[int]).Flush()

// Reload later
node, _ := NewFromObjectId[int](savedObjectId, treap, store)
```

**Time-based memory management:**

The `FlushOlderThan()` method allows you to automatically manage memory by flushing nodes that haven't been accessed recently. This is particularly useful for cache-like scenarios where you want to keep frequently accessed data in memory while evicting older data to disk:

```go
// Flush nodes not accessed in the last hour
cutoffTime := time.Now().Unix() - 3600
flushedCount, err := treap.FlushOlderThan(cutoffTime)
// Nodes are still accessible - they'll be reloaded from disk as needed
```

**Percentile-based memory management:**

The `FlushOldestPercentile()` method allows you to flush the oldest N% of nodes based on access time, providing aggressive memory control without relying on specific timestamps:

```go
// Flush the oldest 25% of nodes
flushedCount, err := treap.FlushOldestPercentile(25)
// Flushed nodes can still be accessed - they'll reload from disk
```

**Vault-level automatic memory management:**

For Vault collections, you can enable automatic memory monitoring with convenient high-level APIs:

```go
// Option 1: Time-based - flush nodes older than 10 seconds when over 1000 nodes
vault.SetMemoryBudget(1000, 10)

// Option 2: Percentile-based - flush oldest 25% when over 1000 nodes
vault.SetMemoryBudgetWithPercentile(1000, 25)

// Option 3: Custom monitoring with callbacks
vault.EnableMemoryMonitoring(
    func(stats MemoryStats) bool {
        return stats.TotalInMemoryNodes > 1000
    },
    func(stats MemoryStats) (int, error) {
        return vault.FlushOldestPercentile(30)
    },
)
```

See `ExampleVault_memoryManagement` and `ExampleVault_memoryManagementPercentile` in `examples_test.go` for complete demonstrations.

## Design Patterns

**Embedding hierarchy**: Each level embeds the previous, inheriting and extending functionality:
- `PayloadTreap` embeds `Treap` → adds payloads to in-memory treap
- `PersistentTreap` embeds `Treap` → adds persistence to treap
- `PersistentPayloadTreap` embeds `PersistentTreap` → combines both features

**Lazy evaluation**: Persistent nodes delay loading children until accessed, enabling memory-efficient operation on large trees.

**Explicit resource management**: Unlike garbage collection, you control when nodes are persisted or flushed, giving predictable performance.

## Example: Persistent Key-Value Store

```go
package main

import (
    "bobbob/internal/store"
    "bobbob/internal/yggdrasil"
)

func main() {
    // Setup
    s, _ := store.NewBasicStore("mydata.blob")
    defer s.Close()
    
    // Create persistent treap with string keys and data payloads
    treap := yggdrasil.NewPersistentPayloadTreap[string, MyData](
        yggdrasil.StringLess,
        yggdrasil.StringKey(""),
        s,
    )
    
    // Insert data
    treap.Insert(
        yggdrasil.StringKey("user:123"),
        100,  // priority
        MyData{Name: "Alice", Age: 30},
    )
    
    // Persist to disk
    treap.root.(yggdrasil.PersistentTreapNodeInterface[string]).Persist()
    
    // Search
    node := treap.Search(yggdrasil.StringKey("user:123"))
    if node != nil {
        payloadNode := node.(*yggdrasil.PersistentPayloadTreapNode[string, MyData])
        data := payloadNode.GetPayload()
        // Use data...
    }
    
    // Free memory while keeping data on disk
    node.(yggdrasil.PersistentTreapNodeInterface[string]).Flush()
}
```

## Performance Characteristics

- **Search/Insert/Delete**: O(log n) expected time
- **Space**: O(n) for in-memory, O(accessed nodes) for persistent
- **Disk I/O**: Only when accessing new nodes or persisting/flushing
- **Memory usage**: Controlled by explicit Flush() calls

The treap's randomized balancing ensures good performance without the complexity of AVL or Red-Black trees, making it ideal for the persistence layer where disk I/O dominates rotation overhead.
