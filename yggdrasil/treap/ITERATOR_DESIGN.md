# Memory-Efficient Treap Iteration - Detailed Design

## Overview

This implementation adds memory-efficient iteration capabilities to persistent treap structures. It allows walking through all nodes in sorted order while:

1. **Loading nodes on-demand from disk** rather than loading the entire tree
2. **Leaving nodes in memory** for as long as the application needs them
3. **Maintaining O(log n) memory usage** for iterating unflushed subtrees

## Core Design Principles

### 1. In-Memory is Authoritative
If an in-memory node exists, its state is authoritative. The disk version (if any) is always considered stale and should not be used if the node is in memory.

### 2. ObjectId Validity Indicates Disk State
- **Valid ObjectId**: Node has been persisted to disk and can be reloaded
- **Invalid ObjectId** (< 0): Node has been mutated in memory but not yet persisted; disk copy (if any) is stale

### 3. ChildObjectId Consistency Requirement
Parent nodes must maintain synchronized references to their children:
- If child has valid ObjectId: parent's `leftObjectId`/`rightObjectId` must point to that same ObjectId (or be invalid if parent hasn't been persisted yet)
- If child has invalid ObjectId: parent's childObjectId reference must also be invalid
- This synchronization is maintained by:
  - Iterator: automatically invalidates parent childObjectIds when child objectId becomes invalid
  - Persist: syncs childObjectIds from in-memory pointers before writing nodes

### 4. Disk Iterator Runs Only on Persisted Subtrees
The Disk Iterator (recursive, uses ObjectIds only) can only safely traverse subtrees where all nodes have been persisted. If a subtree contains nodes with invalid ObjectIds, the Disk Iterator will naturally stop at those boundaries because childObjectIds will be invalid.

## Iterator Architecture

### Three Primitive Iterators

#### 1. In-Memory Iterator
- Walks using **only in-memory pointers**
- Uses stack-based traversal
- Stops when encountering nil child pointers
- Never loads from disk
- Used as the primary mechanism for hybrid iterator

#### 2. Disk Iterator  
- Walks using **only ObjectIds and childObjectIds**
- Unmarshal nodes temporarily from disk (does not cache in memory)
- Uses recursion
- Requires fully persisted subtrees (all ObjectIds valid, no mutations)
- Returns immediately if encountering invalid ObjectId

#### 3. Hybrid Iterator (External API)
- Combines in-memory and disk iterators
- Walks in-memory pointers first (primary mechanism)
- When encountering nil child pointer with valid childObjectId:
  - Recursively invokes Disk Iterator for that subtree
  - Disk Iterator unmarshal nodes temporarily and returns
  - Hybrid iterator continues in-memory from where it left off
- Maintains mutation tracking (for mutating variant)

## Mutation Handling in Iterators

### Before Callback
Iterator records the node's ObjectId (may be valid or invalid)

### After Callback
Iterator checks: did ObjectId change from valid → invalid?
- **YES**: Node was mutated
  - Add node's old ObjectId to trash list
  - Recursively invalidate this ObjectId in all ancestor nodes' childObjectId fields
  - Continue iteration with updated tree state
- **NO**: Node was not mutated; continue normally

### Callback Responsibilities
The callback function **must**:
1. Leave the in-memory tree in a valid state (correctly linked pointers)
2. If it mutates the current node: set that node's ObjectId to invalid
3. If it mutates other nodes (e.g., rotations): set their ObjectIds to invalid
4. Return a list of ObjectIds to delete that it created/owns (e.g., from Delete operations, payload cleanup)
5. Return nil error to continue iteration, non-nil error to stop

The callback is responsible for deleting ObjectIds it explicitly creates. The iterator is responsible for detecting and collecting structural mutations (valid→invalid transitions).

### Iterator Mutation Detection and Trash Collection
- **Before callback**: Record node's ObjectId
- **After callback**: Check if ObjectId changed from valid → invalid
  - **YES**: Iterator adds old ObjectId to trash list automatically
  - Iterator recursively invalidates this ObjectId in all ancestor nodes' childObjectId fields
  - Continue iteration with updated tree state
- **Also**: Iterator collects all ObjectIds returned by the callback
- **At iteration end**: Delete all accumulated trash ObjectIds

This ensures structural mutations (discovered by the iterator) and explicit deletions (returned by the callback) are both handled correctly.

---

## Post-Order Iterator: rangeOverTreapPostOrder

The treap has multiple iteration strategies, each suited to different purposes.

### Definition
**rangeOverTreapPostOrder** traverses the in-memory treap in post-order (children before parents):
- Visit left child first
- Visit right child second  
- Visit current node last

This is a **structural iterator** that only walks in-memory pointers. It cannot load persisted subtrees from disk.

### Signature (Conceptual)
```go
func rangeOverTreapPostOrder(node *TreapNode, callback func(*TreapNode) error) error {
    if node == nil {
        return nil
    }
    if err := rangeOverTreapPostOrder(node.left, callback); err != nil {
        return err
    }
    if err := rangeOverTreapPostOrder(node.right, callback); err != nil {
        return err
    }
    return callback(node)
}
```

### Why Post-Order for Persist and Flush?

Post-order traversal is **required** for Persist and Flush to maintain the childObjectId consistency invariant:

1. **When persisting a node**: Its `leftObjectId` and `rightObjectId` must point to already-persisted child nodes
2. **When flushing a node to disk**: The disk image must contain valid ObjectIds for children
3. **Post-order solves this**:
   - Children persist/flush **FIRST** → their ObjectIds finalized
   - Parent persists/flushes **LAST** → reads already-correct childObjectIds from children
   - No need to update parent references after children persist

### Property: Correctness Without Back-References
Because of post-order:
- When parent's callback executes, child ObjectIds are already synchronized in the parent's fields
- Parent can immediately write the correct childObjectIds to disk
- No second-pass update needed
- No risk of disk state having dangling/incorrect childObjectIds

### Contrast with Other Iteration Orders

**In-Order (left, current, right)**:
- Would violate invariant: parent writes disk image before children are persisted
- Parent might have stale/invalid childObjectIds
- Would require two-pass approach (mark-then-flush)

**Pre-Order (current, left, right)**:
- Same issue: parent processed before children are ready
- Would need back-references or post-processing

**Post-order is canonical for Persist/Flush** because it naturally maintains the "children-before-parents" invariant required for disk state consistency.

---

## Iterator Types Summary

The treap has **four distinct iteration categories**:

1. **In-Memory Iterator** (simple pointer walk)
   - Used by: InOrderVisit, InOrderMutate (primary mechanism)
   - Walks only in-memory pointers; stops at nil
   - Pure tree structure traversal

2. **Disk Iterator** (ObjectId-based load-on-access, **internal use only**)
   - Used internally by: Hybrid iterator when in-memory pointer is nil but ObjectId is valid
   - Recursively loads nodes from disk temporarily (not cached in memory)
   - Returns early if ObjectIds invalid (subtree not persisted)
   - Not exposed to external API; application uses Hybrid iterator instead

3. **Hybrid Iterator** (primary public mutation/read API)
   - Used by: InOrderVisit (read-only), InOrderMutate (with mutations)
   - Combines in-memory pointers (primary) with Disk Iterator (fallback)
   - Detects and accumulates mutations
   - Deletes trash at iteration end
   - Suitable for application-driven traversal

4. **Post-Order Iterator** (rangeOverTreapPostOrder - structural, write-time)
   - Used by: Persist(), flushChild(), and other internal write operations
   - Visits children before parents
   - Maintains childObjectId consistency on write operations
   - **Not suitable for external mutation APIs** (no hybrid loading, no persistence)
   - **Separate concept** from hybrid iteration to keep concerns distinct

### Deletion Queue (Current)
Mutations that invalidate a node’s ObjectId enqueue the old ObjectId for deletion via `pendingDeletes` (or the iterator trash list during `InOrderMutate`). Deletions are flushed after a successful `Persist()` or at the end of `InOrderMutate`, ensuring disk references are not broken mid-operation.

---

## Locking Strategy

The persistent treap uses a single tree-level RWMutex for synchronization:

### Tree-Level RWMutex (Global Synchronization)

#### Read Lock (InOrderVisit)
- **When Held**: Entire duration of InOrderVisit callback
- **What It Protects**:
  - Tree structure immutability (no rotations, insertions, deletions from other threads)
  - ObjectId fields stability (no mutations in other threads)
  - In-memory pointers (safe to follow during iteration)
  - Disk-loaded subtrees (safe to unmarshal temporarily)
- **Why RWLock (Read)**:
  - Multiple InOrderVisit calls can run concurrently (read-only)
  - No mutations, so no conflicts between concurrent readers
- **Boundary**: Released after InOrderVisit completes
- **Note**: Disk I/O may happen during iteration (Disk Iterator loading), but tree state remains unchanged

#### Write Lock (InOrderMutate, Persist)
- **When Held**: Entire duration of InOrderMutate or Persist operation
- **What It Protects**:
  - Tree structure (mutations are serialized; only one writer at a time)
  - ObjectId field assignments (mutation detection depends on consistent view)
  - In-memory pointers (mutations during callback)
  - Ancestor invalidation (modifying parent childObjectIds requires stable tree structure)
  - Trash collection and deletion (consistency of accumulated ObjectIds)
  - Disk write operations (Persist's rangeOverTreapPostOrder)
- **Why Exclusive**:
  - Cannot have concurrent mutations (would corrupt tree state)
  - Cannot have readers during mutation (would see invalid intermediate state)
  - Ancestor invalidation requires knowing parent-child relationships, which can change during rotations
  - Mutation detection (recording ObjectId before callback, checking after) requires consistent tree state
- **Boundary**: Released after InOrderMutate or Persist completes
- **Cost**: InOrderMutate and Persist are exclusive operations. **Persist is especially expensive** (full tree traversal + disk writes).

### 2. Mutation Detection Requires Write Lock
The iterator's mutation detection (checking if ObjectId changed from valid → invalid) happens inside the write lock:

```
Write Lock Acquired
  → rangeOverTreapPostOrder walks tree
  → For each node: record its ObjectId
  → Call callback (may mutate node, invalidate ObjectId)
  → After callback: check if ObjectId changed
  → If changed: invalidate ancestors' childObjectIds (requires stable tree structure)
  → Continue with next node
Write Lock Released
```

If mutations happened outside the write lock, the iterator could miss valid→invalid transitions or see a partially-updated tree state.

### 3. Combining Locking Levels

**InOrderMutate + Persist**:
```
1. Application calls InOrderMutate(callback) 
   - ConcurrentStore acquires write lock
   - Iterator walks tree, running callback
   - Mutations invalidate ObjectIds
   - Trash collected
2. InOrderMutate releases write lock
3. Application calls Persist()
   - ConcurrentStore acquires write lock (again)
   - rangeOverTreapPostOrder syncs childObjectIds from pointers
   - All nodes written to disk
   - pendingDeletes flushed
4. Persist releases write lock
```

**Concurrent Reads During Persist**: Not possible if Persist holds write lock for entire duration. Alternative: Persist could hold lock only during disk writes (releasing for in-memory traversal), but this complicates mutation detection. **Current design: Full write lock for simplicity and correctness.**

### 4. Why Tree-Level Locking?
Using a single tree-level lock (rather than per-node locks) is correct because:

1. **Mutation detection** requires recording ObjectId before callback and checking after callback; needs stable, consistent view
2. **Ancestor invalidation** requires modifying parent childObjectIds; during rotations, parent-child relationships change, so tree structure must be stable
3. **Trash collection** requires consistency across multiple ObjectId deletions
4. **Simplicity and correctness** over fine-grained concurrency; mutations are not the common case

**Design choice**: Single tree-level lock provides safe semantics. InOrderMutate and Persist are exclusive operations, but InOrderVisit (read-only) can run in parallel with multiple concurrent readers.

---

### Read-Only Iteration: InOrderVisit
```go
func (t *PersistentTreap[K]) InOrderVisit(
    callback func(node PersistentTreapNodeInterface[K]) error,
) error
```

- **Lock**: Read lock (nodes are not modified)
- **Behavior**: Hybrid iterator, read-only traversal
- **In-memory nodes**: Traversed via in-memory pointers
- **Flushed subtrees**: Loaded temporarily via Disk Iterator
- **After iteration**: All nodes (in-memory and disk-loaded) remain as they were
- **Result**: O(log n) peak memory if used on partially-flushed tree

### Mutating Iteration: InOrderMutate
```go
func (t *PersistentTreap[K]) InOrderMutate(
    callback func(node PersistentTreapNodeInterface[K]) (trash []ObjectId, err error),
) error
```

- **Lock**: Write lock (tree structure may change)
- **Behavior**: Hybrid iterator with mutation tracking
- **Mutation detection**: Iterator detects valid→invalid ObjectId transitions
- **Ancestor invalidation**: Iterator automatically invalidates ancestor childObjectIds
- **Trash collection**: Iterator accumulates ObjectIds for deletion
- **After iteration**: Tree is in a valid state but may contain nodes with invalid ObjectIds
- **Note**: Persist must be called separately if disk synchronization is needed

### Explicit Persistence: Persist
```go
func (t *PersistentTreap[K]) Persist() error
```

- **Lock**: Write lock
- **Behavior**:
  - Uses rangeOverTreapPostOrder to visit all nodes (post-order: children before parents)
  - For each node: syncs childObjectIds from in-memory pointers
  - Writes all nodes to disk
  - Flushes the pending delete queue
- **Cost**: Expensive; should only be called when needed
- **Result**: All in-memory changes written to disk; tree is ready for disk reloads

## Key Design Decisions

### 1. No Automatic Persist During Iteration
Persist is expensive. Iterator does not trigger persist automatically. Application controls when to persist:
- After batch of mutations: call Persist() explicitly
- Before application shutdown: call Persist() explicitly
- Before disk-only traversal (if needed): call Persist() first

### 2. No Flushing During Iteration
Nodes stay in memory after iteration. Application can explicitly flush nodes (via setting pointers to nil) if memory management is needed, but the iterator does not do this.

### 3. Mutations Must Complete Synchronously
The callback must fully complete the mutation and leave the tree valid. After the callback returns, the iterator handles:
- Mutation detection (valid→invalid)
- Ancestor invalidation
- Continued iteration

### 4. Trash Collection via Iterator
The iterator is responsible for:
- Detecting mutations
- Collecting trash ObjectIds
- Deleting trash at iteration end

This is the primary mechanism for orphaned object cleanup.

## Example Workflows

### Read-Only Full Tree Traversal
```
1. Application calls InOrderVisit(callback)
2. Iterator acquires read lock
3. Walks in-memory pointers, calling callback for each node
4. When hitting nil child with valid childObjectId:
   - Disk Iterator loads and traverses that subtree (temporarily)
   - Unmarshal, process, discard
5. Iterator releases lock
6. Result: Read-only view of tree; all in-memory nodes stay in memory
```

### Mutating Traversal with Persist
```
1. Application calls InOrderMutate(callback)
2. Iterator acquires write lock
3. Walks nodes, calling callback for each
4. Callback mutates node → sets ObjectId invalid
5. Iterator detects mutation, invalidates ancestors' childObjectIds
6. Iterator accumulates old ObjectIds in trash
7. Iteration continues with updated tree state
8. After iteration: Delete trash ObjectIds
9. Iterator releases lock
10. Application calls Persist()
11. Persist syncs childObjectIds and writes all to disk
12. Persist flushes pending deletes
```

### Partial Disk Reloads
```
1. Tree has some nodes in memory, some flushed to disk
2. Application calls Persist() first (writes all in-memory changes)
3. Application calls InOrderVisit()
4. Hybrid iterator walks in-memory nodes
5. When hitting nil child with valid childObjectId:
   - Disk Iterator temporarily loads that subtree
   - Result: Efficient traversal with O(log n) memory
```

---

## Detailed Iterator API Specifications

### 1. In-Memory Iterator

**Purpose**: Walk only in-memory pointers; used as primary mechanism in hybrid iterator.

**Implementation Note**: Uses **stack-based iteration** (not recursion) to avoid stack overflow on deep trees and enable mutation detection with ancestor tracking.

**Pseudo Code**:
```go
// Stack-based in-order traversal
function walkInMemoryInOrder(root, callback, trackMutations):
    if root == nil:
        return nil
    
    stack = []  // Stack of nodes to visit
    ancestorStack = []  // Track ancestors for mutation invalidation
    trashList = []  // Accumulate ObjectIds to delete
    current = root
    
    while current != nil or stack not empty:
        // Traverse to leftmost node
        while current != nil:
            stack.push(current)
            current = current.left
        
        // Process top of stack
        current = stack.pop()
        ancestorStack = stack.copy()  // Ancestors are everything in stack above us
        
        // MUTATION DETECTION (if enabled)
        oldObjectId = -1
        if trackMutations:
            oldObjectId = current.ObjectId
        
        // Callback
        if trackMutations:
            callbackTrash, err = callback(current)
            if err != nil:
                return err, trashList
            trashList.append(callbackTrash)
        else:
            err = callback(current)
            if err != nil:
                return err
        
        // MUTATION DETECTION: Check if ObjectId changed
        if trackMutations and oldObjectId >= 0 and current.ObjectId < 0:
            // Mutation detected! ObjectId went from valid → invalid
            trashList.append(oldObjectId)
            
            // Invalidate this ObjectId in all ancestors' childObjectIds
            for ancestor in ancestorStack:
                if ancestor.leftObjectId == oldObjectId:
                    ancestor.leftObjectId = ObjNotAllocated
                if ancestor.rightObjectId == oldObjectId:
                    ancestor.rightObjectId = ObjNotAllocated
        
        // Move to right subtree
        current = current.right
    
    return nil, trashList
```

**Characteristics**:
- **Stack-based**: Explicit stack; no recursion
- **Ancestor tracking**: Copy stack to track which nodes are ancestors
- **Mutation detection**: Optional; controlled by `trackMutations` flag
- **Returns trash**: If tracking mutations, returns list of ObjectIds to delete

---

### 2. Disk Iterator (Internal Only)

**Purpose**: Load and traverse persisted subtrees temporarily; not exposed to applications.

**Implementation Note**: Uses **recursion** (acceptable here because we're loading temporarily from disk and don't need mutation tracking).

**Pseudo Code**:
```go
// Recursive in-order traversal using ObjectIds
function rangeOverDiskObjects(store, objectId, callback):
    // Base case: invalid ObjectId means subtree not persisted
    if objectId < 0:
        return nil
    
    // Load node from disk (temporarily; not cached)
    node, err = unmarshalNodeFromDisk(store, objectId)
    if err != nil:
        return err
    
    // In-order traversal using childObjectIds (not pointers)
    // Left child
    if node.leftObjectId >= 0:
        err = rangeOverDiskObjects(store, node.leftObjectId, callback)
        if err != nil:
            return err
    
    // Current node
    err = callback(node)
    if err != nil:
        return err
    
    // Right child
    if node.rightObjectId >= 0:
        err = rangeOverDiskObjects(store, node.rightObjectId, callback)
        if err != nil:
            return err
    
    // Node goes out of scope; not kept in memory
    return nil
```

**Characteristics**:
- **Recursive**: Uses call stack (simpler for temporary loading)
- **Temporary loading**: Nodes not cached; loaded, processed, discarded
- **No mutation tracking**: Read-only operation

---

### 3. Hybrid Iterator - Shared Implementation

**Purpose**: Both InOrderVisit and InOrderMutate use the same underlying hybrid walk; mutation tracking controlled by flag.

**API Signatures**:
```go
// Read-only variant
func (t *PersistentTreap[K]) InOrderVisit(
    callback func(node PersistentTreapNodeInterface[K]) error,
) error

// Mutating variant
func (t *PersistentTreap[K]) InOrderMutate(
    callback func(node PersistentTreapNodeInterface[K]) (trash []ObjectId, err error),
) error
```

**Shared Implementation Pseudo Code**:
```go
// Both InOrderVisit and InOrderMutate call this
function hybridInOrderWalk(root, callback, trackMutations):
    if trackMutations:
        acquireWriteLock()
    else:
        acquireReadLock()
    defer releaseLock()
    
    if root == nil:
        return nil
    
    stack = []
    ancestorStack = []
    trashList = []
    current = root
    
    while current != nil or stack not empty:
        // Traverse to leftmost node (hybrid: memory or disk)
        while current != nil:
            stack.push(current)
            current = current.left  // Follow in-memory pointer
        
        // If left pointer was nil, check for disk-backed subtree
        if stack not empty:
            parent = stack.peek()
            if parent.left == nil and parent.leftObjectId >= 0:
                // Disk fallback: load left subtree temporarily
                err = rangeOverDiskObjects(store, parent.leftObjectId, callback)
                if err != nil:
                    return err
        
        // Process current node
        current = stack.pop()
        ancestorStack = stack.copy()
        
        // MUTATION DETECTION
        oldObjectId = -1
        if trackMutations:
            oldObjectId = current.ObjectId
        
        // CALLBACK (signature differs based on trackMutations)
        if trackMutations:
            callbackTrash, err = callback(current)
            if err != nil:
                return err
            trashList.append(callbackTrash)
            
            // Check for mutation
            if oldObjectId >= 0 and current.ObjectId < 0:
                trashList.append(oldObjectId)
                
                // Invalidate in ancestors
                for ancestor in ancestorStack:
                    if ancestor.leftObjectId == oldObjectId:
                        ancestor.leftObjectId = ObjNotAllocated
                    if ancestor.rightObjectId == oldObjectId:
                        ancestor.rightObjectId = ObjNotAllocated
        else:
            err = callback(current)
            if err != nil:
                return err
        
        // Move to right subtree (hybrid: memory or disk)
        if current.right != nil:
            current = current.right  // In-memory pointer
        else if current.rightObjectId >= 0:
            // Disk fallback: load right subtree temporarily
            err = rangeOverDiskObjects(store, current.rightObjectId, callback)
            if err != nil:
                return err
            current = nil  // Continue iteration
        else:
            current = nil
    
    // Delete accumulated trash (if tracking mutations)
    if trackMutations:
        for objectId in trashList:
            store.DeleteObj(objectId)
    
    return nil
```

**Key Design Decision**:
- **Same function, different modes**: `trackMutations` flag switches between read-only and mutating behavior
- **Callback signature varies**: Type system ensures correct callback type for each mode
- **Lock type varies**: Read lock for InOrderVisit, write lock for InOrderMutate

---

### 4. Post-Order Persist Iterator

**Purpose**: Write all in-memory nodes to disk in children-before-parents order.

**API Signature**:
```go
func (t *PersistentTreap[K]) Persist() error
```

**Pseudo Code**:
```go
function Persist(root):
    acquireWriteLock()
    defer releaseWriteLock()
    
    return rangeOverTreapPostOrder(root, persistNode)

// Post-order traversal using recursion (acceptable for persist)
function rangeOverTreapPostOrder(node, callback):
    if node == nil:
        return nil
    
    // Post-order: left, right, current
    if node.left != nil:
        err = rangeOverTreapPostOrder(node.left, callback)
        if err != nil:
            return err
    
    if node.right != nil:
        err = rangeOverTreapPostOrder(node.right, callback)
        if err != nil:
            return err
    
    // Current node (children already processed)
    err = callback(node)
    return err

function persistNode(node):
    // DIRTY TRACKING: Mark this node as needing persistence
    node.invalidateDirty()
    
    // Sync childObjectIds from in-memory pointers
    if node.left != nil:
        node.leftObjectId = node.left.ObjectId
    else:
        node.leftObjectId = ObjNotAllocated
    
    if node.right != nil:
        node.rightObjectId = node.right.ObjectId
    else:
        node.rightObjectId = ObjNotAllocated
    
    // Write to disk
    if node.ObjectId < 0:
        // New node: allocate new ObjectId
        node.ObjectId, err = store.NewObj(nodeSize)
        if err != nil:
            return err
    
    // Serialize and write
    bytes = node.Marshal()
    err = store.WriteBytesToObj(node.ObjectId, bytes)
    if err != nil:
        return err
    
    // Deletions are queued elsewhere; pendingDeletes flushed after persist
    
    return nil
```

**Implementation Notes**:
- **Recursion acceptable**: Persist is not called during iteration; separate operation
- **Dirty tracking**: `invalidateDirty()` marks node as needing disk write
- **Post-order required**: Ensures children persisted before parents (childObjectIds valid)

---

## Complex Scenarios: Diagrams and ObjectId States

### Scenario 1: Fully In-Memory Tree

**State**: All nodes in memory; none persisted yet.

```
Tree Structure:
                [50] ptr=0x1000, objId=-1, L=-1, R=-1
               /    \
         [30] ptr=0x2000          [70] ptr=0x3000
         objId=-1, L=-1, R=-1     objId=-1, L=-1, R=-1
        /    \                   /    \
   [20]      [40]           [60]      [80]
   ptr=0x4000  ptr=0x5000   ptr=0x6000  ptr=0x7000
   objId=-1    objId=-1     objId=-1    objId=-1
   L=-1, R=-1  L=-1, R=-1   L=-1, R=-1  L=-1, R=-1

Legend:
  ptr = in-memory pointer address
  objId = ObjectId field (-1 = invalid/not persisted)
  L = leftObjectId, R = rightObjectId
```

**Iterator Behavior**:
- **InOrderVisit**: Walks via in-memory pointers only; never calls Disk Iterator
- **InOrderMutate**: Can mutate any node; no old ObjectIds to trash (all invalid)
- **Persist**: Traverses post-order, allocates ObjectIds, writes all nodes

---

### Scenario 2: Fully Persisted Tree (After First Persist)

**State**: All nodes written to disk; ObjectIds valid.

```
Tree Structure:
                [50] ptr=0x1000, objId=100, L=200, R=300
               /    \
         [30] ptr=0x2000          [70] ptr=0x3000
         objId=200, L=400, R=500  objId=300, L=600, R=700
        /    \                   /    \
   [20]      [40]           [60]      [80]
   ptr=0x4000  ptr=0x5000   ptr=0x6000  ptr=0x7000
   objId=400   objId=500    objId=600   objId=700
   L=-1, R=-1  L=-1, R=-1   L=-1, R=-1  L=-1, R=-1
```

**Key Properties**:
- All nodes have valid ObjectIds (≥ 0)
- childObjectIds synchronized with children's ObjectIds
- In-memory pointers still valid (nodes still cached)

**Iterator Behavior**:
- **InOrderVisit**: Still walks in-memory pointers (faster than disk)
- **InOrderMutate**: If node mutated, old ObjectId added to trash
- **Persist**: Detects ObjectIds already valid; re-writes nodes (idempotent)

---

### Scenario 3: Partially Flushed Tree (Memory Pressure)

**State**: Some nodes flushed from memory; pointers nil but ObjectIds valid.

```
Tree Structure:
                [50] ptr=0x1000, objId=100, L=200, R=300
               /    \
         [30] ptr=nil           [70] ptr=0x3000
         objId=200 (disk)        objId=300, L=600, R=700
        /    \                   /    \
   [20]      [40]           [60]      [80]
   ptr=nil    ptr=nil        ptr=nil    ptr=nil
   objId=400  objId=500      objId=600  objId=700
   (disk)     (disk)         (disk)     (disk)

Node [50] in-memory state:
  - left pointer: nil
  - leftObjectId: 200 (valid)
  - right pointer: 0x3000 (in-memory)
  - rightObjectId: 300 (valid)

Node [70] in-memory state:
  - left pointer: nil
  - leftObjectId: 600 (valid)
  - right pointer: nil
  - rightObjectId: 700 (valid)
```

**Key Properties**:
- Root and some nodes in memory (pointers valid)
- Other nodes flushed (pointers nil, ObjectIds valid)
- Can reload from disk using ObjectIds

**Iterator Behavior**:
- **InOrderVisit**: 
  - Walks to [50] via memory
  - Sees left=nil but leftObjectId=200 → **Disk Iterator** loads [30] subtree temporarily
  - Walks to [70] via memory pointer
  - Sees left=nil but leftObjectId=600 → **Disk Iterator** loads [60] temporarily
  - Sees right=nil but rightObjectId=700 → **Disk Iterator** loads [80] temporarily
- **Hybrid boundaries**: Seamlessly switches between in-memory and disk

---

### Scenario 4: Partially Hydrated by Search

**State**: Application searched for key=40; path to it loaded from disk back into memory.

```
Before Search (Partially Flushed):
                [50] ptr=0x1000, objId=100, L=200, R=300
               /    \
         [30] ptr=nil           [70] ptr=0x3000
         objId=200              objId=300
        /    \
   [20]      [40]
   ptr=nil    ptr=nil
   objId=400  objId=500

After Search (Path to 40 Hydrated):
                [50] ptr=0x1000, objId=100, L=200, R=300
               /    \
         [30] ptr=0x2000        [70] ptr=0x3000
         objId=200, L=400, R=500   objId=300, L=600, R=700
        /    \                   /    \
   [20]      [40]           [60]      [80]
   ptr=nil    ptr=0x5000     ptr=nil    ptr=nil
   objId=400  objId=500      objId=600  objId=700
   (disk)     (in-memory!)   (disk)     (disk)

Nodes Rehydrated:
  - [30]: Loaded from ObjectId 200, pointer now 0x2000
  - [40]: Loaded from ObjectId 500, pointer now 0x5000
```

**Key Properties**:
- Search operation loaded [30] and [40] back into memory
- [20], [60], [80] remain flushed (not on search path)
- Tree is now partially in-memory, partially on disk

**Iterator Behavior**:
- **InOrderVisit**:
  - [50] → in-memory pointer to [30] → walk via pointer
  - [30] → left=nil but leftObjectId=400 → **Disk Iterator** loads [20]
  - [30] → right pointer to [40] → walk via pointer
  - [50] → right pointer to [70] → walk via pointer
  - [70] → left=nil but leftObjectId=600 → **Disk Iterator** loads [60]
  - [70] → right=nil but rightObjectId=700 → **Disk Iterator** loads [80]

---

### Scenario 5: Mutation After Partial Hydration

**State**: Application mutated node [40] (e.g., updated payload); ObjectId becomes invalid.

```
Before Mutation:
         [30] ptr=0x2000, objId=200, L=400, R=500
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000
   objId=400  objId=500

After Mutation (e.g., UpdatePayload on [40]):
         [30] ptr=0x2000, objId=200, L=400, R=500 (stale!)
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000
   objId=400  objId=-1 (INVALID!)
              oldObjectId=500 queued for delete

Problem:
  - [30].rightObjectId = 500 (points to old disk version)
  - [40].ObjectId = -1 (mutated, not yet persisted)
  - Disk still has old [40] at ObjectId 500
```

**Key Properties**:
- [40] mutated → ObjectId invalid (-1)
- [40] queued old ObjectId (500) for cleanup later
- [30]'s rightObjectId (500) now **stale** (points to old version)
- **Invariant violated**: Parent references incorrect child ObjectId

**Iterator Mutation Detection**:
```
InOrderMutate walks to [40]:
  1. Record oldObjectId = 500
  2. Callback mutates [40] → sets ObjectId = -1 (queues old ObjectId for delete)
  3. Iterator detects: oldObjectId=500 valid, newObjectId=-1 invalid → MUTATION!
  4. Iterator adds 500 to trash list
  5. Iterator walks ancestor stack, finds [30]
  6. Iterator invalidates: [30].rightObjectId = -1
  7. Continue iteration...
  8. At iteration end: delete ObjectId 500 from disk

After Iterator Completes:
         [30] ptr=0x2000, objId=200, L=400, R=-1 (invalidated!)
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000
   objId=400  objId=-1
```

**Result**:
- [30]'s rightObjectId correctly invalidated (matches child state)
- Old disk ObjectId 500 deleted (no orphans)
- Invariant restored: parent childObjectId matches child state

---

### Scenario 6: Persist After Mutation

**State**: After mutation, application calls Persist to sync to disk.

```
Before Persist:
         [30] ptr=0x2000, objId=200, L=400, R=-1
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000
   objId=400  objId=-1 (needs persist)

Persist Traversal (Post-Order):
  1. Visit [20]: ptr=nil, skip (not in memory)
  2. Visit [40]: objId=-1 → Allocate new ObjectId 800
                 → Write [40] to disk at ObjectId 800
                 → [40].ObjectId = 800
  3. Visit [30]: Sync childObjectIds from pointers:
                 → left=nil → leftObjectId=-1 (was 400, but [20] not in memory)
                 → right=[40] → rightObjectId=800 ([40]'s new ObjectId)
                 → [30] already has ObjectId=200 → Re-write to disk
                 → [30].ObjectId = 200 (unchanged, but disk updated)

After Persist:
         [30] ptr=0x2000, objId=200, L=-1, R=800
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000
   objId=400  objId=800 (newly persisted!)

Disk State:
  - ObjectId 200: Node [30] with leftObjectId=-1, rightObjectId=800
  - ObjectId 400: Node [20] (unchanged)
  - ObjectId 500: DELETED (was old [40])
  - ObjectId 800: Node [40] (new version)
```

**Key Properties**:
- [40] gets new ObjectId (800) because it was mutated
- [30] syncs its rightObjectId to 800 before writing
- [30]'s leftObjectId becomes invalid (-1) because [20] not in memory (pointer nil)
  - This is correct: [20] remains on disk at ObjectId 400, but [30] doesn't track it
  - If [20] is loaded later, [30].left pointer becomes valid again
- **Invariant maintained**: childObjectIds synchronized from pointers before disk write

---

### Scenario 7: Full Cycle (Persist → Flush → Mutate → Persist)

**Complete lifecycle showing all ObjectId transitions**:

```
Step 1: Initial In-Memory Tree
         [30] objId=-1
        /    \
   [20]      [40]
   objId=-1   objId=-1

Step 2: After First Persist
         [30] objId=200, L=400, R=500
        /    \
   [20]      [40]
   objId=400  objId=500

Step 3: After Flush (Memory Pressure)
         [30] ptr=nil, objId=200
        /    \
   [20]      [40]
   ptr=nil    ptr=nil
   objId=400  objId=500
   (all on disk)

Step 4: After Search for [40] (Partial Hydration)
         [30] ptr=0x2000, objId=200, L=400, R=500
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000, objId=500
   objId=400

Step 5: After Mutate [40]
         [30] ptr=0x2000, objId=200, L=400, R=-1 (iterator invalidated!)
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000, objId=-1
   objId=400  old ObjectId=500 queued → deleted at iteration end

Step 6: After Second Persist
         [30] ptr=0x2000, objId=200, L=-1, R=800 (synced from pointer)
        /    \
   [20]      [40]
   ptr=nil    ptr=0x5000, objId=800 (new!)
   objId=400

Disk State Timeline:
  Initial: Empty
  After Step 2: [200→Node30(L=400,R=500), 400→Node20, 500→Node40]
  After Step 5: [200→Node30(L=400,R=500), 400→Node20, 500→DELETED]
  After Step 6: [200→Node30(L=-1,R=800), 400→Node20, 800→Node40]
```

**Key Insights**:
- ObjectIds are stable until mutation
- Mutations create new ObjectIds (old ones deleted)
- childObjectIds only synchronized during Persist (post-order ensures correctness)
- Hybrid iterator handles all these states transparently

---

## Summary: ObjectId State Machine

```
Node State Transitions:

[New Node]
  objId = -1 (invalid)
  leftObjectId = -1, rightObjectId = -1
  ↓
[First Persist]
  objId = allocated (e.g., 100)
  childObjectIds synced from pointers
  ↓
[Flushed from Memory]
  pointer = nil (but can be reloaded from objId=100)
  ↓
[Rehydrated by Search]
  pointer = valid (loaded from disk via objId=100)
  ↓
[Mutated]
  objId = -1 (invalid)
  old ObjectId queued for deletion
  Parent's childObjectId invalidated by iterator
  ↓
[Persist Again]
  objId = allocated (e.g., 200, new ObjectId)
  pendingDeletes flushed (old ObjectId removed from disk)
  Parent's childObjectId synced to 200
```

**Invariants Maintained**:
1. **In-memory is authoritative**: Pointers always correct if node is in memory
2. **ObjectId validity**: Valid (≥0) = on disk; Invalid (<0) = mutated
3. **ChildObjectId consistency**: Synced during Persist; invalidated during mutation
4. **No orphans**: Old ObjectIds deleted when mutations detected or persist completes
