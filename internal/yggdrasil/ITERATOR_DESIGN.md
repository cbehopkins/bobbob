# Memory-Efficient Treap Iteration

## Overview

This implementation adds memory-efficient iteration capabilities to the persistent treap structures. It allows you to walk through all nodes in sorted order while:

1. **Loading nodes on-demand from disk** rather than loading the entire tree
2. **Discarding nodes from memory** after they're no longer needed
3. **Maintaining O(log n) memory usage** instead of O(n)

## How It Works

### The Problem

The existing `Search` method loads nodes from disk as it walks down the tree to find a specific key. However, iterating through all nodes would normally require loading the entire tree into memory, which is problematic for large datasets.

### The Solution

The iterator uses a standard iterative in-order traversal with a stack, combined with:

- **Lazy loading**: Nodes are loaded from disk only when needed via the existing `GetLeft()` and `GetRight()` methods
- **Selective flushing**: After visiting a node's left subtree and the node itself, if the node has no right subtree, we can safely flush it from memory
- **Stack-based iteration**: Uses an explicit stack instead of recursion to maintain the current path

### Memory Usage

- **With `KeepInMemory=true`**: Behaves like traditional iteration - all visited nodes remain in memory (O(n))
- **With `KeepInMemory=false`**: Only maintains nodes on the current root-to-node path (O(log n) for balanced trees)

## API

### Iterator Options

```go
type IteratorOptions struct {
    // KeepInMemory: false = flush nodes after visiting (memory-efficient)
    //               true = keep all visited nodes in memory
    KeepInMemory bool
    
    // LoadPayloads: true = load payload data (for PersistentPayloadTreap)
    //               false = only load keys and structure
    LoadPayloads bool
}

// Default options minimize memory usage
opts := DefaultIteratorOptions()  // KeepInMemory=false, LoadPayloads=true
```

### For PersistentTreap (keys only)

```go
// Full node access
err := treap.WalkInOrder(opts, func(node PersistentTreapNodeInterface[K]) error {
    key := node.GetKey()
    // Process node...
    return nil
})

// Keys only (convenience method)
err := treap.WalkInOrderKeys(opts, func(key PersistentKey[K]) error {
    // Process key...
    return nil
})

// Count nodes efficiently
count, err := treap.Count()
```

### For PersistentPayloadTreap

```go
// With payloads
opts.LoadPayloads = true
err := treap.WalkInOrder(opts, func(key PersistentKey[K], payload P, loaded bool) error {
    // Process key and payload...
    return nil
})

// Keys only (no payload loading - faster & less memory)
opts.LoadPayloads = false
err := treap.WalkInOrder(opts, func(key PersistentKey[K], payload P, loaded bool) error {
    // payload will be zero value, loaded will be false
    // Process only the key...
    return nil
})

// Or use the convenience method
err := treap.WalkInOrderKeys(opts, func(key PersistentKey[K]) error {
    // Process key...
    return nil
})
```

## Examples

### Example 1: List all keys (memory-efficient)

```go
opts := DefaultIteratorOptions()
opts.KeepInMemory = false  // Memory-efficient mode
opts.LoadPayloads = false  // Don't load payloads

err := treap.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error {
    fmt.Printf("Key: %v\n", key)
    return nil
})
```

### Example 2: Export all data to JSON

```go
type Record struct {
    Key     int
    Payload string
}

var records []Record
opts := DefaultIteratorOptions()
opts.LoadPayloads = true

err := payloadTreap.WalkInOrder(opts, func(key PersistentKey[IntKey], payload *StringPayload, loaded bool) error {
    intKey := key.(*IntKey)
    records = append(records, Record{
        Key:     int(*intKey),
        Payload: string(*payload),
    })
    return nil
})

jsonData, _ := json.Marshal(records)
```

### Example 3: Count nodes matching a condition

```go
count := 0
opts := DefaultIteratorOptions()
opts.KeepInMemory = false

err := treap.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error {
    intKey := key.(*IntKey)
    if *intKey > 100 {
        count++
    }
    return nil
})

fmt.Printf("Found %d keys > 100\n", count)
```

### Example 4: Early termination

```go
// Find the first 10 keys and stop
found := make([]IntKey, 0, 10)
opts := DefaultIteratorOptions()

err := treap.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error {
    intKey := key.(*IntKey)
    found = append(found, *intKey)
    
    if len(found) >= 10 {
        return fmt.Errorf("found enough")  // Stop iteration
    }
    return nil
})
```

### Example 5: Process large treap in batches

```go
const batchSize = 1000
batch := make([]IntKey, 0, batchSize)
batchNum := 0

opts := DefaultIteratorOptions()
opts.KeepInMemory = false  // Critical for large datasets

err := treap.WalkInOrderKeys(opts, func(key PersistentKey[IntKey]) error {
    intKey := key.(*IntKey)
    batch = append(batch, *intKey)
    
    if len(batch) >= batchSize {
        // Process batch
        fmt.Printf("Processing batch %d with %d items\n", batchNum, len(batch))
        processBatch(batch)
        
        // Clear for next batch
        batch = batch[:0]
        batchNum++
    }
    return nil
})

// Process final partial batch
if len(batch) > 0 {
    processBatch(batch)
}
```

## Performance Characteristics

| Operation | Time Complexity | Memory Usage |
|-----------|----------------|--------------|
| Walk all nodes (KeepInMemory=true) | O(n) | O(n) |
| Walk all nodes (KeepInMemory=false) | O(n) | O(log n)* |
| Count() | O(n) | O(log n)* |
| Walk with LoadPayloads=false | O(n) | Lower I/O |

*Assumes a balanced tree. Worst case for a degenerate tree is O(n).

## Implementation Details

### Node Flushing Strategy

The implementation flushes nodes aggressively to minimize memory usage. In-order traversal processes nodes in this sequence:

1. Visit entire left subtree
2. Visit the node itself  
3. Visit entire right subtree

**The iterator tracks which nodes have been visited** using a `lastVisited` pointer. This allows it to detect when we're "backtracking up" from a right subtree:

- **After visiting a node** (step 2): We keep it in memory while processing its right subtree
- **After finishing the right subtree** (step 3): We detect we're backtracking (because `lastVisited == current.GetRight()`), and **flush the node**
- **If a node has no right children**: We flush it immediately after visiting it (step 2)

This approach flushes **every visited node** once we're completely done with it, not just leaf nodes. The memory usage remains O(log n) because:
- The stack only contains the current path from root to the node being processed
- Nodes are flushed as soon as we backtrack past them
- Only unpersisted nodes remain in memory (which shouldn't happen during read-only iteration)

**Comparison with simpler approach:**
- Simple: Only flush nodes with no right children → some internal nodes remain in memory
- Enhanced: Flush all nodes after processing → only the current path remains in memory

### Stack vs Recursion

The implementation uses an explicit stack rather than recursion because:
1. **Control**: We can easily manage memory by flushing nodes
2. **No stack overflow**: Large trees won't cause stack overflow errors
3. **Interruptible**: Easy to return errors and stop iteration early

### Disk I/O Considerations

Each node is read from disk at most once during iteration. The existing `GetLeft()` and `GetRight()` methods cache loaded children, so:
- Total disk reads: O(n) - one read per node
- With `KeepInMemory=false`: Nodes are flushed after use, allowing the OS to reclaim memory
- With `LoadPayloads=false`: Less data is read from disk

## Testing

Run the tests:

```bash
go test -v ./internal/yggdrasil -run TestSimple
```

The test suite covers:
- Basic in-order traversal
- Key-only iteration
- Counting nodes
- Correctness of sort order
- Memory efficiency

## Future Enhancements

Possible improvements:
1. **Reverse iteration**: Walk in descending key order
2. **Range iteration**: Iterate only over a key range
3. **Parallel iteration**: Multiple goroutines iterating over different subtrees
4. **Cursor-based iteration**: Save position and resume later
5. **More aggressive flushing**: Flush parent nodes when backtracking
