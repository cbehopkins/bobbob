# Treap Package Design

This package provides treap  data structures with support for both in-memory and persistent (disk-backed) implementations.

## API Design Philosophy

The treap package is designed around **interface-based composition** to eliminate code duplication across different treap types. Core operations (insert, delete, search, rotate, compare) are implemented as **package-level functions** that operate on `TreapNodeInterface[T]`, allowing a single implementation to serve all treap variants.

### Design Principles

1. **One Implementation Per Operation**: Operations like `insert()`, `delete()`, `search()`, and `Compare()` are package-level functions, not type-specific methods. This eliminates duplication across `Treap[T]`, `PayloadTreap[K,P]`, `PersistentTreap[T]`, and `PersistentPayloadTreap[K,P]`.

2. **Interface-Driven**: All operations work on `TreapNodeInterface[T]`, which defines the minimal contract for treap nodes (GetKey, GetPriority, GetLeft/Right, SetLeft/Right).

3. **Layered Persistence**: Persistent variants extend the base interface with persistence-specific methods (ObjectId, Persist, Flush) via `PersistentTreapNodeInterface[T]`.

4. **Type Safety**: Generic type parameters ensure compile-time type safety while maintaining flexibility.

## Core API

### Package-Level Functions (Shared Operations)

These functions operate on any node implementing `TreapNodeInterface[T]`:

```go
// InsertNode adds a new node to the treap, maintaining BST and heap properties.
// Returns the new root of the subtree.
func InsertNode[T any](
    node TreapNodeInterface[T],
    newNode TreapNodeInterface[T],
    less func(a, b T) bool,
    releaser NodeReleaser[T],
) (TreapNodeInterface[T], error)

// DeleteNode removes a node with the given key from the treap.
// Returns the new root of the subtree.
func DeleteNode[T any](
    node TreapNodeInterface[T],
    key T,
    less func(a, b T) bool,
    releaser NodeReleaser[T],
) (TreapNodeInterface[T], error)

// SearchNode finds the node with the given key.
// Optional callback is invoked for each visited node (for LRU tracking, etc.).
func SearchNodeComplex[T any](
    node TreapNodeInterface[T], 
    key T,
    less func(a, b T) bool,
    callback func(TreapNodeInterface[T]) error,
) (TreapNodeInterface[T], error)

// SearchNode finds the node with the given key without a callback.
func SearchNode[T any](
  node TreapNodeInterface[T],
  key T,
  less func(a, b T) bool,
) TreapNodeInterface[T]

// RotateLeft/RotateRight perform tree rotations to maintain heap property.
func RotateLeft[T any](node TreapNodeInterface[T]) (TreapNodeInterface[T], error)
func RotateRight[T any](node TreapNodeInterface[T]) (TreapNodeInterface[T], error)

// inOrderWalk traverses the treap in sorted order.
func inOrderWalk[T any](
    node TreapNodeInterface[T], 
    callback func(TreapNodeInterface[T]) error,
) error

// reverseOrderWalk traverses the treap in reverse sorted order.
func reverseOrderWalk[T any](
    node TreapNodeInterface[T], 
    callback func(TreapNodeInterface[T]) error,
) error
```

### Interface Hierarchy

```go
// TreapNodeInterface: Minimal contract for all treap nodes
type TreapNodeInterface[T any] interface {
    GetKey() types.Key[T]
    GetPriority() Priority
    SetPriority(Priority)
    GetLeft() TreapNodeInterface[T]
    GetRight() TreapNodeInterface[T]
    SetLeft(TreapNodeInterface[T]) error
    SetRight(TreapNodeInterface[T]) error
    IsNil() bool
}

// PersistentTreapNodeInterface: Adds persistence support
type PersistentTreapNodeInterface[T any] interface {
    TreapNodeInterface[T]
    ObjectId() (store.ObjectId, error)
    SetObjectId(store.ObjectId)
    IsObjectIdInvalid() bool
    Persist() error
    Flush() error
}

// PersistentPayloadNodeInterface: Adds payload support
type PersistentPayloadNodeInterface[T any, P any] interface {
    PersistentTreapNodeInterface[T]
    GetPayload() P
    SetPayload(P)
}
```

### Treap Container Types

The package provides several container types that wrap a root node and provide high-level operations:

```go
// Treap[T]: In-memory treap
type Treap[T any] struct {
    root TreapNodeInterface[T]
    Less func(a, b T) bool
}

// Methods delegate to package-level functions:
func (t *Treap[T]) Insert(value T)
func (t *Treap[T]) Delete(value T)
func (t *Treap[T]) Search(value T) TreapNodeInterface[T]
func (t *Treap[T]) Compare(other *Treap[T], ...) error

// PayloadTreap[K, P]: In-memory treap with payloads
// PersistentTreap[T]: Disk-backed treap
// PersistentPayloadTreap[K, P]: Disk-backed treap with payloads
```

### Iteration API

See [ITERATOR_DESIGN.md](ITERATOR_DESIGN.md) for complete iterator design. Summary:

```go
// Visitor callbacks for read-only iteration
type VisitCallback[T any] func(node TreapNodeInterface[T]) error

// Mutating callbacks return list of ObjectIds to delete
type MutatingCallback[T any] func(node TreapNodeInterface[T]) (trash []store.ObjectId, error)

// External iterator interfaces (acquire treap mutex)
func (t *Treap[T]) InOrderVisit(callback VisitCallback[T]) error
func (t *PersistentTreap[T]) InOrderMutate(callback MutatingCallback[T]) error

// Iterator implementations (internal)
// - rangeOverTreapPostOrder: Post-order (for mutations)
// - inMemoryIterator: In-order using memory pointers only
// - diskIterator: In-order loading from ObjectIds
// - hybridIterator: Memory first, disk fallback for nil pointers
```

## Recent Refactor Findings (Feb 2026)

1. Polymorphic post-order traversal now supports payload nodes. The generic walker accepts any node implementing PersistentNodeWalker and returns dirty nodes when a valid ObjectId transitions to invalid.
2. PersistentPayloadTreapNode now explicitly implements the walker methods for cached children and transient loading so payload post-order traversal can use automatic dirty tracking.
3. The IsNil override on PersistentPayloadTreapNode is required for typed-nil interface safety; do not remove it.
4. RangeOverTreapPayloadPostOrder now returns the dirty list, mirroring the PersistentTreap behavior.

## Implementation History

This package underwent a major refactoring (completed Feb 2026) to eliminate code duplication across treap variants through interface-based composition.

### Phase 1: In-Memory Treap Refactoring

Core operations (`insert`, `delete`, `search`, `rotate`) were extracted from type-specific methods into package-level functions operating on `TreapNodeInterface[T]`. This established the foundation for code reuse across all treap types.

**Key changes:**
- Package-level functions now handle all tree manipulation logic
- `Treap[T]` methods became thin wrappers calling package functions
- Node allocation abstracted through factory pattern for pool management
- Comprehensive test suite established covering basic operations, treap properties (BST + heap), iteration, comparison, and stress testing

### Phase 2: Payload Treap Implementation

`PayloadTreap[K, P]` was implemented using the same package-level functions, proving the interface design successfully eliminated duplication. All operations (insert, delete, search) work identically to `Treap[T]` without any duplicated logic.

**Key validation:**
- Zero code duplication between `Treap[T]` and `PayloadTreap[K,P]`
- Payload operations (retrieval, update, SetPayload) tested exhaustively
- Payloads correctly preserved through tree rotations and restructuring

### Phase 3: Persistent Treaps

Persistence support added to both base and payload variants through `PersistentTreapNodeInterface[T]`. Package-level functions remained unchanged; persistence logic lives in node implementations and container wrappers.

**Key mechanisms:**
- Dirty tracking via `trackDirty` slice during mutations - all modified nodes collected as operations recurse
- ObjectId invalidation through `invalidateDirty()` - marks all tracked nodes invalid and queues old ObjectIds for deletion
- Deletion queue (`pendingDeletes`) - old ObjectIds queued during invalidation, flushed after successful `Persist()` or at end of `InOrderMutate`
- Lazy loading of child nodes from disk when needed
- Post-order traversal for persistence (children before parents)

**Iterator implementation:**
- `rangeOverTreapPostOrder`: Post-order traversal with automatic dirty tracking
- `inMemoryIterator`: In-order using memory pointers only (zero disk I/O)
- `diskIterator`: In-order loading from ObjectIds (memory-efficient)
- `hybridIterator`: Memory first, disk fallback (balanced approach)
- `InOrderVisit`: Read-only iteration using hybrid iterator
- `InOrderMutate`: Mutating iteration with trash collection

**Persistence testing covered:**
- In-memory operations, persist/flush cycles, rehydration from disk
- ObjectId invalidation, dirty tracking, iterator memory efficiency
- Concurrent operations, allocator integration, space reclamation

### Phase 4: Finalization

**Code cleanup:**
- All duplicate implementations removed
- Consistent error handling across all treap types
- Unused helper functions eliminated

**Documentation:**
- API documentation updated with interface contracts
- Godoc examples added for common operations
- ITERATOR_DESIGN.md finalized with iterator patterns
- Persistence design rules documented (see below)

**Performance:**
- Benchmarking confirmed no regression from refactoring
- Reduced duplication improved maintainability without sacrificing performance

## Persistence Design Notes

This section captures persistence assumptions and design rules for the disk-backed treap implementation.

## Scope
- Applies to `PersistentTreap` and `PersistentPayloadTreap`.
- Focuses on persistence, object IDs, and mutation effects.
- Does not cover allocator or store internals (see top-level README for those).

## Core Assumptions
1. **Mutations are in-memory first.** Insert/Delete/Rotate operate on in-memory nodes. Nodes not in memory may be loaded to perform the mutation.
2. **ObjectIds are only used for rehydration.** If a node is in memory, traversal uses pointers, not ObjectIds.
3. **Flushing can make persisted ObjectIds active again.** When nodes are flushed (e.g., iteration with `KeepInMemory=false`), parent nodes may later rehydrate children via persisted ObjectIds.

## Persistence Rules
1. **Mutations MUST invalidate ALL ancestors.**
   - When a node is modified (Insert, Delete, SetPayload, SetPriority, etc.), ALL ancestors up to the root must be marked dirty.
   - This is because parents store `leftObjectId`/`rightObjectId` references that become stale when children change.
   - Use the `dirty` tracking mechanism: collect modified nodes during recursion, then `invalidateDirty()` marks them all invalid.
2. **Invalidation means: set ObjectId to -1 and queue old ObjectIds for deletion.**
   - `objectId = ObjNotAllocated` marks the node for re-persistence.
   - Old ObjectIds are queued via `pendingDeletes` (or iterator trash) and flushed after a successful `Persist()` or at the end of `InOrderMutate`.
   - This happens in `invalidateDirty()` and in mutation helpers that already know the old ObjectId.
3. **Never call DeleteObj directly in mutation methods.**
   - Methods like `SetPayload`, `SetPriority`, `SetLeft`, `SetRight` should only invalidate and/or queue the old ObjectId.
   - Let `pendingDeletes` + `flushPendingDeletes()` handle deletion after persistence.
   - Early deletion breaks if a persisted ancestor still references the old ObjectId.
4. **Child ObjectId caches must reflect in-memory pointers.**
   - If a child pointer changes, its cached `leftObjectId`/`rightObjectId` must be updated to match the child's current `objectId` (even if invalid).
5. **Persist is responsible for generating new ObjectIds.**
   - During post-order persistence, children are persisted first, then parent ObjectIds are synced and persisted.

## Critical Gap: Mutations and Persistence
- **Mutations are in-memory; Persist is deferred.** There can be a long gap between modifying a node and calling Persist().
- During this gap, nodes have `objectId = -1` (invalid) but are fully functional in memory via pointers.
- **This is why ancestor invalidation is critical:** Parents must also be marked invalid so Persist knows to rewrite them with updated child references.

## Consequences
- **Missing ancestor tracking = data loss.** If a parent isn't marked dirty, it won't be re-persisted with the new child ObjectId. Later rehydration from disk will load stale references.
- **Garbage is queued during invalidation.** Old ObjectIds are queued for deletion and flushed after a successful `Persist()` or at the end of `InOrderMutate`.
- **Deferred cleanup is bounded.** Garbage accumulates only in `pendingDeletes` until the next flush point.

## Practical Guidance
- When adding or modifying mutation paths (insert, delete, rotate, payload update):
  - Mark the modified node dirty (add to `dirty` slice during recursion).
  - Ensure all ancestors are also added to `dirty` as recursion unwinds.
  - Call `invalidateDirty(dirty)` at operation end to invalidate all tracked nodes.
  - NEVER call `DeleteObj` directly in mutation methods - queue deletes and let `flushPendingDeletes()` handle it after persistence.
- If a future GC is introduced, ensure it only deletes objects no longer referenced by any persisted ancestor.

## Open Questions / Future Work
- Formal invariants or a verification pass to ensure no persisted references point to deleted ObjectIds.
- Performance optimization: batch DeleteObj calls during invalidation.
