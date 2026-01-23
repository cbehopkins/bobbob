# Allocator Overview
bobbob makes extensive use of allocators to manage access to the store. The store
notionally is a single file of potentially infinite size. The Allocator functionality
should manage the allocation of objects within this file.

## Pre-Refactor Notes (COMPLETED)

The following pre-refactor changes have been successfully implemented and validated with full test suite passing:

1. ✅ **Added DeleteObj(ObjectId) method:** BasicAllocator and OmniBlockAllocator now support ObjectId-based deletion. Internally resolves ObjectId to FileOffset/size via GetObjectInfo, then delegates to existing Free(FileOffset, size) method.

2. ✅ **Removed async persistence infrastructure:**
   - Removed `StartBackgroundFlush()` / `StopBackgroundFlush()` methods from BasicAllocator
   - Removed `FlushCacheToBacking()` method from BasicAllocator
   - Removed `StartBackgroundPersist()` / `StopBackgroundPersist()` methods from OmniBlockAllocator
   - Removed background persist loop and related infrastructure
   - Removed TestOmniBlockAllocatorBackgroundPersist test (documented as comment)
   - Updated base_store.go to remove calls to removed methods

3. ✅ **Verified AllocateRun signature:** Current signature `([]ObjectId, []FileOffset, error)` is correct and consistently used throughout codebase.

**Result:** All tests pass. Pre-refactor changes validate that the new API design (explicit Marshal/Unmarshal persistence, no background loops, DeleteObj support) is compatible with existing codebase patterns.

Objects - normally referring to a block of data in the file have an ObjectId, FileOffset, and Size.
Most consumers simply maintain the ObjectId and leave the Allocator to track the FileOffset and Size.
Typically the Store is asked to fetch and Object ID and will query the Allocator to obtain the FileOffset & Size so that it may fetch that block of data for the consumer.

A naive allocator would maintain a map from ObjectId to FilleOffset and Size. This would consume a 
large amount of memory so we typically want to have temporary file structures that maintain
a searchable index of these strcutures rather than hold the entire structure in memory.

Similarly we support object deletion. Once an object is deleted it should be placed onto a FreeList.
If a request is made to allocate a new object it should ideally come from this FreeList
As above this list can be quite large so typpically will also have a temporary file backing it.

It is a hard design requirement that all the data required by a Store is contained within a single file. 
Whilst the implementation is allowed to create temporary files to assist in its operation these must not be
relied upon inbetween instances of a Run.

We allow multiple Allocators in the Allocator architecture. In the initial design there is a BasicAllocator and the PoolAllocator. The 
PoolAllocator exists because we know it is common for objects to be of a similar size and 
there are optimisations one can make by Pooling together these requests - one 
can request your parent allocator (in this case the BasicAllocator) for a large Block of data and allocate from within there. 
When it comes time to Persist the structure fully to disk the PoolAllocator is able to Marshal itself to a []byte
A block of data this size is then requested from its Parent Allocator (the BasicAllocator) and it is written.
On the Load, then the allocator is marshelled back from this block.

An Allocator should be able to Persist itself to the store to a single Object ID
This could create a dependency loop though as the Allocator manages ObjectIds - so how can we request them.

We introduce the concept of PrimeObjects. These can only be created the very first time a Store/Allocator is created.
Once any other object is Allocated new PrimeObjects are forbidden.
A PrimeObject has two parts. Each allocator will at first creation time Register a PrimeObjectId. This will be a table (the PrimeTable) stored as the first Item in the file. e.g. BasicAllocator  is hard coded as the first PrimeObject, Pool Alllocator the second etc.
When it comes time to Perist the Allocators the last allocator will go first. It will Marshal itself
and create a block of data. It will repquest an ObjectId for this from its parent alloctor. It will be provided with an ObjectId that the data are written to.
The PrimeTable then has the entry associated with the PoolAllocator updated to contain the ObjectId used to
write the object.
Finally the BasicAllocator will Marshal itself out. One of the entries it will Marshal out is the current Length of the file. It will then take the bytes from the Marshal of self and append this to the file. (note that the final length of the file will therefore be the previous length of the file plus the size of the BasicAllocator Marshalled data)
The Prime Table will then be updated to have the entry associated with the BasicAllocator point to the FileOffset at which writing the BasicAllocator data commenced - the Previous File Length + 1

To restore the Allocator status when loading from a file the process can be reversed. First we Load the Basic Allocator from the first PrimeObject, then we load the PoolAllocator from the second PrimeObject.
After the Objects 

Conceptually each Allocator is going to need to maintain two tables.
A ObjectId->{FileOffset, Size} and a FreeList
It is up to the individual allocator how these lists are stored. It is up to the individual allocator
if there are any rules it enforces so that the properties can be calculated rather than stored.

# Implementation Details

## Allocator API

Every allocator implements a common interface. This section describes the core operations.

### Common Types

```go
// ObjectId uniquely identifies an object in the file
type ObjectId int64

// FileOffset is a byte position in the file
type FileOffset int64

// FileSize represents the size of data in bytes
type FileSize int64
```

### Core Interface

Each allocator (OmniAllocator, PoolAllocator, BlockAllocator, BasicAllocator) implements these operations:

#### Allocate(size int) → (ObjectId, FileOffset, error)

Requests a new object allocation of the given size.

**Behavior by allocator type:**

- **OmniAllocator**:
  - Routes based on size to the smallest PoolAllocator that fits
  - If no pool fits, delegates to parent BasicAllocator
  - Returns the ObjectId from the chosen allocator
  
- **PoolAllocator**:
  - Size must match the pool's block size (or error)
  - Searches available BlockAllocators for a free slot
  - If none found, creates a new BlockAllocator from parent
  - Returns ObjectId from the BlockAllocator
  
- **BlockAllocator**:
  - Finds first available (false) slot in allocation bitmap
  - Marks slot as allocated (true)
  - Returns calculated ObjectId (startingObjectId + slotIndex)
  
- **BasicAllocator**:
  - Searches FreeList for a gap large enough
  - If found, allocates from the gap
  - If not found, appends to end of file
  - Stores ObjectId → (FileOffset, Size) in map
  - Returns the ObjectId

**Errors:**
- `ErrAllocationFailed`: Unable to allocate (file full, etc.)
- `ErrSizeNotSupported`: Size doesn't fit in any pool (BasicAllocator may handle it, but could fail)

#### AllocateRun(size int, count int) → ([]ObjectId, []FileOffset, error)

Allocates multiple contiguous ObjectIds efficiently. Primarily used when structure needs many fixed-size objects.

**Behavior by allocator type:**

- **OmniAllocator**:
  - Routes to appropriate PoolAllocator
  - Delegates AllocateRun to that pool
  
- **PoolAllocator**:
  - Requests contiguous block of `count` ObjectIds
  - Delegates to a single BlockAllocator (creates new one if needed)
  - Returns all ObjectIds from that one BlockAllocator
  
- **BlockAllocator**:
  - Scans allocation bitmap for `count` consecutive free slots
  - If found, marks all as allocated, returns all ObjectIds
  - If not found: returns partial run of whatever contiguous slots ARE available (may be 1 or more)
  - Caller may need to call again on another BlockAllocator to get remaining ObjectIds
  
- **BasicAllocator**:
  - `AllocateRun` not typically supported (returns error)
  - Caller must use individual `Allocate` calls

**Errors:**
- `ErrAllocationFailed`: Not enough contiguous space
- `ErrUnsupported`: Allocator doesn't support run allocation

#### Delete(ObjectId) → error

Frees an object and makes its space available for reuse.

**Behavior by allocator type:**

- **OmniAllocator**:
  - Determines which allocator owns this ObjectId
  - Routes to that allocator
  - Delegates Delete
  
- **PoolAllocator**:
  - Determines which BlockAllocator owns this ObjectId
  - Routes to that allocator
  
- **BlockAllocator**:
  - Calculates slot from ObjectId (ObjectId - startingObjectId)
  - Marks slot as free (false) in bitmap
  - Potentially returns block to parent if completely empty (future optimization)
  
- **BasicAllocator**:
  - Looks up ObjectId in map
  - Removes entry from map
  - Adds gap to FreeList
  - Special case: if ObjectId is at end of file, truncate file

**Errors:**
- `ErrInvalidObjectId`: ObjectId not found or already deleted
- `ErrOwnershipNotFound`: OmniAllocator can't determine which sub-allocator owns this

#### GetObjectInfo(ObjectId) → (FileOffset, FileSize, error)

Returns the location and size of an object without modifying anything.

**Behavior by allocator type:**

- **OmniAllocator**:
  - Routes to appropriate allocator by ObjectId
  - Returns (FileOffset, FileSize) from sub-allocator
  
- **PoolAllocator**:
  - Routes to BlockAllocator based on ObjectId
  
- **BlockAllocator**:
  - Calculates: slotIndex = ObjectId - startingObjectId
  - Verifies slot is allocated in bitmap
  - Calculates: FileOffset = startingFileOffset + (slotIndex × blockSize)
  - Returns: (FileOffset, blockSize)
  
- **BasicAllocator**:
  - Looks up ObjectId in map
  - Returns stored (FileOffset, FileSize)

**Errors:**
- `ErrInvalidObjectId`: ObjectId not found or marked as free

#### SetOnAllocate(callback func(ObjectId, FileOffset, size int))

Registers a callback to be invoked whenever an allocation occurs. Useful for tracking allocations externally (e.g., ObjectMap updates, statistics).

**Behavior:**
- Callback is invoked synchronously during Allocate() or AllocateRun()
- Called once per ObjectId allocated
- If callback is nil, no-op (clears any previous callback)

**Use cases:**
- ObjectMap maintenance (Store layer needs to track ObjectId → FileOffset mappings)
- Allocation statistics/debugging
- External index updates

#### GetFile() → *os.File

Returns the file handle associated with this allocator. Required for allocators to perform direct I/O.

**Note:** File handle is provided during allocator construction; this is read-only access for operations like reading/writing serialized allocator data.

#### ContainsObjectId(ObjectId) → bool

Quick ownership check without loading full allocator state. Returns true if this allocator (or one of its children) owns the given ObjectId.

**Behavior by allocator type:**

- **OmniAllocator:** Checks all PoolAllocators and BasicAllocator (may require loading PoolCache metadata)
- **PoolAllocator:** Checks range against all owned BlockAllocators (available and full)
- **BlockAllocator:** Checks if ObjectId falls within [startingObjectId .. startingObjectId + blockCount)
- **BasicAllocator:** Checks if ObjectId exists in allocation map

**Use case:** Fast routing before expensive GetObjectInfo call; prevents unnecessary allocator loads.

#### GetObjectIdsInAllocator(blockSize int, allocatorIndex int) → []ObjectId

Returns all ObjectIds currently allocated within a specific BlockAllocator. Used for compaction and migration.

**Parameters:**
- `blockSize`: The pool size (e.g., 1024 for 1K pool)
- `allocatorIndex`: Index of the BlockAllocator within that pool

**Returns:** Slice of ObjectIds (may be empty if all slots are free)

**Behavior:**
- Iterates the BlockAllocator's bitmap, returning ObjectIds for allocated (true) slots
- May trigger lazy-load of the BlockAllocator if not in memory

**Use case:** Compaction logic needs to enumerate all objects in suboptimal allocators to migrate them.

---

## Allocator Lifecycle and File I/O

All allocators are self-contained and interact only with `os.File`. No dependency on the Store package.

### Constructor

Each allocator type has a constructor:

```go
// Create new allocator for fresh file
func NewBasicAllocator(file *os.File) (*BasicAllocator, error)
func NewOmniBlockAllocator(blockSizes []int, blockCount int, parent Allocator, file *os.File, opts ...OmniBlockAllocatorOption) (*omniBlockAllocator, error)
func NewPoolAllocator(blockSize int, parent Allocator, file *os.File) (*PoolAllocator, error)
func NewBlockAllocator(blockSize int, blockCount int, startingFileOffset FileOffset, parentObjectId ObjectId, file *os.File) (*BlockAllocator, error)
```

**Constructor responsibilities:**
- Accept file handle for all I/O operations
- Initialize internal state (maps, bitmaps, etc.)
- Do NOT read from file (fresh state only)
- Parent allocator must be provided for child allocators (hierarchy must be manually constructed)

### Load from Existing File

To open an existing allocator from file:

```go
// Pattern: Allocate → Unmarshal → Ready
alloc := NewBasicAllocator(file)  // Fresh state
data := readFromFile(file, offset, size)  // Read serialized state from file
err := alloc.Unmarshal(data)  // Restore state from bytes
// alloc is now ready with restored ObjectMap, FreeList, etc.
```

**Load sequence (entire hierarchy):**

1. **Read PrimeTable** from fixed file location (offset 0)
   - Tells us where each allocator's serialized data lives

2. **Load BasicAllocator:**
   - Read ObjectId/FileOffset/Size from PrimeTable
   - Read serialized bytes from file at that location
   - Create fresh BasicAllocator
   - Call Unmarshal(bytes)
   - BasicAllocator.ObjectMap and FreeList are restored

3. **Load OmniAllocator:**
   - Read ObjectId/FileOffset/Size from PrimeTable
   - Read serialized bytes from file
   - Create fresh OmniAllocator with parent=BasicAllocator
   - Call Unmarshal(bytes) — gets PoolAllocator ObjectIds but doesn't load them yet
   - OmniAllocator is ready (lazy-loads pools on-demand)

4. **PoolAllocators loaded on-demand:**
   - When OmniAllocator routes a request to a pool not in memory
   - Read pool's serialized bytes from file using ObjectId
   - Create fresh PoolAllocator with parent=BasicAllocator
   - Call Unmarshal(bytes) — gets BlockAllocator ObjectIds
   - Ready to serve requests

### Save to File

To persist allocator state:

```go
// Pattern: Marshal → Write to File → Update PrimeTable
data, err := alloc.Marshal()  // Serialize to bytes
objId, fileOffset, err := parentAlloc.Allocate(len(data))  // Get space
_, err := file.WriteAt(data, int64(fileOffset))  // Write bytes
err := primeTable.Store(allocatorType, objId, fileOffset, len(data))  // Record location
```

**Save sequence (entire hierarchy):**

1. **Each PoolAllocator persists:**
   - Tell all BlockAllocators to persist first
   - Marshal own state → bytes
   - Request ObjectId from parent (BasicAllocator)
   - Write bytes to file
   - Record ObjectId in own parent's metadata

2. **OmniAllocator persists:**
   - Tell all PoolAllocators to persist first (recursively)
   - Marshal own state (now with PoolAllocator ObjectIds) → bytes
   - Request ObjectId from parent (BasicAllocator)
   - Write bytes to file
   - Record ObjectId in PrimeTable[OmniAllocator]

3. **BasicAllocator persists:**
   - Marshal own state → bytes
   - Allocate space from itself (special: end of file)
   - Write bytes to file
   - Record ObjectId in PrimeTable[BasicAllocator]

4. **PrimeTable writes last:**
   - Serialize PrimeTable → bytes
   - Write at fixed offset (e.g., offset 0)
   - This is the atomic "commit" point

**Key insight:** PrimeTable written last makes persistence atomic. If crash before PrimeTable write, old state is valid. If crash after, new state is valid.

---

### Persistence Operations

#### Marshal() → ([]byte, error)

Serializes the allocator's state to a byte slice. Low-level operation; typically used by Save() in the persistence sequence.

**What gets serialized:**

- **OmniAllocator**:
  - Metadata: array of PoolAllocator ObjectIds (one per pool)
  - Array of PoolAllocator pointers being managed
  
- **PoolAllocator**:
  - Metadata: blockSize, blockCount, next planned blockCount
  - Array of owned BlockAllocator ObjectIds (for both available and full)
  
- **BlockAllocator**:
  - Metadata: startingObjectId, startingFileOffset, blockSize, blockCount
  - Bitmap: allocated slots (packed bitset)
  
- **BasicAllocator**:
  - Metadata: current file length
  - Map entries: ObjectId → (FileOffset, FileSize) for each allocation
  - FreeList entries: all gaps

**Returns:** Serialized bytes ready to be written to a single object in the file

**Relationship to Save():** Save() calls Marshal(), writes bytes to file, and updates PrimeTable. Marshal() is the low-level primitive.

#### Unmarshal(data []byte) → error

Deserializes allocator state from a byte slice and restores internal state. Low-level operation; typically used by Load() in the restore sequence.

**What gets restored:**

- Reconstruction of internal maps and lists
- Validation of ObjectId ranges don't overlap
- Validation that all FileOffsets are within file bounds

**Note:** The actual child allocators (BlockAllocators) are NOT deserialized in-memory. Instead, their ObjectIds are stored, and they are loaded on-demand (lazy loading).

---

### Persistence Abstraction Levels

**Marshal/Unmarshal (Low-level primitives):**
- Convert between in-memory state ↔ serialized bytes
- Standalone operations: don't know about files, offsets, or PrimeTable
- Used internally by Save/Load sequences
- **Example:** `bytes, err := alloc.Marshal()`

**Save/Load (High-level API):**
- Manage file I/O, ObjectId allocation, PrimeTable updates
- Coordinate persistence across allocator hierarchy
- Handle atomic commit (PrimeTable written last)
- **Pattern:** Marshal → Allocate storage → Write to file → Update PrimeTable → Commit

**In new design:**
- Allocators implement Marshal/Unmarshal
- File I/O and PrimeTable management handled at higher level (likely new Store layer)
- Clear separation: allocators are stateless about persistence details

---

### Prime Object Registration

Each allocator registers itself in the PrimeTable at startup:

```go
type PrimeTable struct {
    Entries []PrimeEntry  // indexed by allocator type
}

type PrimeEntry struct {
    ObjectId    ObjectId   // Canonical ObjectId
    FileOffset  FileOffset // FileOffset
    Size        Size
}
```

**Current plan:**
- PrimeEntry[0]: BasicAllocator
- PrimeEntry[1]: OmniAllocator (parent knows where its children live)
- PrimeEntry[2+]: Reserved for future use

---

### Persist Sequence

When saving allocator state to disk:

1. **BasicAllocator persists last** (it will allocate storage for others):
   - Serializes own state → []byte
   - Calls `Allocate(len(bytes))` → gets ObjectId
   - Writes serialized bytes to file at that ObjectId's FileOffset
   - Updates PrimeTable[BasicAllocator] = ObjectId

2. **OmniAllocator persists**:
   - Tells all PoolAllocators to Persist first
   - Serializes own state (now including PoolAllocator ObjectIds) → []byte
   - Calls parent `BasicAllocator.Allocate(len(bytes))` → gets ObjectId
   - Writes serialized bytes to file
   - Updates PrimeTable[OmniAllocator] = ObjectId

3. **Each PoolAllocator persists**:
   - Tells all owned BlockAllocators to Persist first
   - Serializes own state (now with BlockAllocator ObjectIds) → []byte
   - Calls parent `Allocate(len(bytes))` → gets ObjectId
   - Writes serialized bytes
   - Returns ObjectId to OmniAllocator

4. **Each BlockAllocator persists**:
   - Serializes bitmap and metadata → []byte
   - Calls parent (PoolAllocator's parent, i.e., BasicAllocator) `Allocate()` → gets ObjectId
   - Writes serialized bytes
   - Returns ObjectId to PoolAllocator

5. **PrimeTable writes last**:
   - Serializes updated PrimeTable → []byte
   - Writes at fixed offset (e.g., file offset 0)
   - This is the "commit" point

**Key insight:** Each allocator's serialized state fits in ONE object, and child allocators are persisted before parent (breadth-first or post-order traversal).

### Load Sequence

When opening an existing allocation file:

1. **Read PrimeTable** from fixed file location (offset 0)
2. **Load BasicAllocator**:
   - Read from file at ObjectId stored in PrimeTable[BasicAllocator]
   - Unmarshal into BasicAllocator instance
3. **Load OmniAllocator**:
   - Read from file at ObjectId stored in PrimeTable[OmniAllocator]
   - Unmarshal into OmniAllocator instance
   - Receives ObjectIds of PoolAllocators but does NOT load them yet (lazy)
4. **PoolAllocators** loaded on-demand:
   - When OmniAllocator routes a request, if PoolAllocator not in memory, load it
   - Read from file and Unmarshal
   - BlockAllocators within PoolAllocator also loaded on-demand

---

### Future: PoolCache (Unloaded Block Tracking)

When many BlockAllocators are unloaded from memory but their ObjectIds are still tracked:

- **GetObjectInfo** must still work without loading full BlockAllocator
- **Delete** must still work efficiently
- Metadata stored in PoolCache to enable this

This is deferred for now but should be designed with it in mind (e.g., BlockAllocator can return enough metadata for PoolCache to calculate FileOffset without full deserialize).

## Prime Table
A slice of ObjectIds. Marshals out to a constant sized structure (for number of entries).
A uint32 of number of entries, then one entry for each PrimeObject in the table. An entry will consist of a FileOffset and Size.
### API
```go
type Size int64
    type FileInfo {
        objId ObjectId
        fo FileOffset
        sz Size
    }
type PrimeObjectIdTable {
    currentOffset int
    []FileInfo
}
func (p PrimeObjectIdTable) Add() {}
func (p PrimeObjectIdTable) Store(FileInfo) {}
func (p PrimeObjectIdTable) Load() (FileInfo) {}
func (p PrimeObjectIdTable) Marshal() []byte {}
func (p *PrimeObjectIdTable) Unmarshal([]byte) {}
func (p *PrimeObjectIdTable) SizeInBytes() Size {}
```
The PrimeObjectIdTable is the first object written to the file in the store. It is always at FileOffset 0.
How it works depends on if it is Creating a new file from scratch, or loading an existing store

### Create From Scratch
Each Allocator that needs one will call Add. This order will be fixed. The Return argument will typically be ignored, but is there for testing.
With the table having been created the Basic Allocator will take over. It will query the SizeInBytes and set its first/next available ObjectId to this value. e.g. if it take 1024 bytes to store the table, then BasicAllocator is allowed to begin writing new objects at offset 1024.
The table is able to calculate its SizeInBytes because it knows how many entires it has - it knows how many times
Add has been called.

### Saving A Store
As the OmniAllocator saves its state it will generate a []byte that it needs to save to the store. It will request the BasicAllocator for an ObjectId suitable for a data block of that size. It will write the data to that block. It will then call Store on the PrimeObjectIdTable informing it of the ObjectId & FileOffset it was given and the data block size.
The Table knows which entry to use in Store, because it starts at the Last Entry.
If there are other allocators in the chain, they may perform a similar process.
Then it will be the BasicAllocator's turn to write. It will Allocate itself a block (typically the end of the file so it can simply append) and Marshall out the data that represents its state. It will then call Store on the PrimeObjectIdTable informing it of the Object Id, File Offset, in this case the file offset of the end of the file when it started the write.
Finally the PrimeObjectTable will write itself out to the file (at offset 0). (Of course it will be Store that is doing this, but you get the idea). The last byte  of this write will be adjacent to the first byte of the first object allocated by the BasicAllocator. We are guaranteed to not corrupt as the size of the table was fixed by the last Add happening before the Basic Allocator starts.

### Load An Existing Store
This will be a reverse of the above process. When Load is called it will read from the File and retrieve the
FileOffset and Size BasicAllocator provided. BasicAllocator can then Unmarshal Itself. Having done that it can revert the FileOffset back to where it started reading its data from so that the file is truincated effectively deleting the ObjId that was used to save its state.
Store will then create the OmniAllocator, again Load will be called and the ObjectId, FileOffset and Size will
be retrieved. This can then be used to retrieve the bytes needed to UnMarshal the OmniAllocator.
Once this is done, the ObjectId used can be deleted - this time from the Basic Store

This should then illustrate the pattern an allocator creates an entry in the prime table and uses that as a way to store a reference to its data. an allocator's parent being responsible for this allocation and deletion of this object.


## Basic Allocator
The BasicAllocator is the lowest level Allocator. In the worst case scenario, one can always allocate
a new object by appending to the file.
The ObjectId is always the FileOffset. This is always True for the Basic Allocator. Later layers may break this rule, but for the BasicAllocator it is a 121 mapping.
When an object is deleted it is added to the FreeList.
Special case when deleting the last object in the file, we Truncate the file.

The Basic Allocator maintains a table that Maps ObjectId/FileOffset to Size. This Table is what is Marshalled/Unmarshalled on Save/Load.
The Free List is not saved on Save/Load. It is reconstructed by traversing the Allocation table on Load. Since we know the ObjectId/FileOffset and Size we can calculate the gaps and add them to the freelist as they are discovered.

Note: Existing version uses a gap heap and this is generally a mess - we should start again from scratch here.

### Free List

**Structure:** Sorted vector of gaps by FileOffset, enabling efficient coalescing.

```go
type Gap struct {
    FileOffset FileOffset
    Size       Size
}

type FreeList struct {
    gaps []Gap  // sorted by FileOffset
}
```

**Operations:**

- **Insert gap** (on delete): Binary search to find position, insert, then coalesce with adjacent gaps in O(n) worst case
- **Find gap** (on allocate): Linear scan for FirstFit or BestFit; gaps are rare in practice so acceptable
- **Coalesce:** Trivial on sorted list—check neighbors and merge if adjacent
- **Persist:** Not persisted to store; reconstructed from ObjectMap on load by identifying gaps between allocations

**Why not a heap?** Heaps excel at priority ordering but fail at spatial coalescing—gaps are scattered and you lose locality information. A sorted list naturally preserves FileOffset ordering, making adjacent coalescing O(1).

**Scaling:** For typical systems with <10K gaps, linear scans are negligible. If needed, a size index (map of Size → gap indices) can be added without redesign.

**Minimum gap size:** Gaps smaller than 4 bytes are not added to the FreeList (overhead not worth it). When inserting a gap from a deletion:
- If gap size >= 4 bytes: add to FreeList
- If gap size < 4 bytes: discard (wasted space, but acceptable for practical systems)

This avoids polluting the FreeList with unusable gaps.

## Compacting Allocator
This is here as a holding pad for a concept. This is not yet implemented.
This sits between the Pool Allocator at the Basic Allocator.
For the Basic Allocator ObjectId directly maps to FileOffset and vice versa.
To allow objects to be re-ordered on disk we would like to be able to compact objects - move the file blocks around in the store.
This would require a table to store the new mappings.

As I say - not implemented...


---

## ObjectId Hierarchy and Uniqueness

ObjectId uniqueness is maintained throughout the hierarchy by respecting allocation ranges. Each allocator receives a contiguous ObjectId range from its parent and subdivides it to clients.

**Example: 3-level hierarchy**

```
BasicAllocator (manages entire file)
│
├─ Allocates ObjectId=1000, Size=1M to a 1K PoolAllocator
│  └─ PoolAllocator subdivides [1000 .. 2047] (1048 slots)
│     │
│     ├─ BlockAllocator[0] manages [1000 .. 2023] (1024 slots)
│     │  └─ Issues ObjectIds: 1000, 1001, 1002, ... (each slot = 1 ObjectId)
│     │
│     └─ BlockAllocator[1] manages [2024 .. 2047] (24 slots, for future growth)
│
└─ Allocates ObjectId=3000, Size=512K to a 512B PoolAllocator
   └─ PoolAllocator subdivides [3000 .. 4023] (1024 slots)
      └─ BlockAllocator manages [3000 .. 4023]
         └─ Issues ObjectIds: 3000, 3001, 3002, ...
```

**Uniqueness guarantees:**
- **BasicAllocator:** Owns all ObjectIds in the file
- **PoolAllocator:** Each pool owns a disjoint range (no overlap with other pools)
- **BlockAllocator:** Each allocator owns a contiguous sub-range (no overlap with sibling BlockAllocators)
- **At any level:** When routing a request (e.g., GetObjectInfo for ObjectId=2500):
  1. OmniAllocator routes to the PoolAllocator that owns 2500 (binary search on pool ranges)
  2. PoolAllocator routes to the BlockAllocator that owns 2500
  3. BlockAllocator directly calculates the FileOffset (no ambiguity)

**Hierarchy matters:** Direct access to BasicAllocator with ObjectId=2500 would fail (or worse, alias to the wrong pool's allocator storage), because BasicAllocator doesn't know about the pool's subdivision. Clients must respect the hierarchy: requests go through OmniAllocator → PoolAllocator → BlockAllocator.

---


An Omni Allocator is (at the moment) our highest level allocator concept.
All allocator requests are sent here. Depending on the size requested the Omni Allocator kmay delegate this to one of its children Pool allocators.
Alternatively it may pass the request up to the parent Allocaotor.
It is worth defining terms here.
An allocation Request will request a DataBlock of a certain Size.
We have BlockAllocators that are optimised to allocate data blocks of a certain size. That is they will be capable of allocating N * Data Blocks of Size N.
To create a Block Allocator we request a data block from a parent allocator.
We have Pool Allocators. Each pool is responsible for data blocks of a certain size.

A Pool Allocator can only allocate blocks of Size N. It is however optimised to do this very efficiently as it can make many assumptions about the block of data.

The OmniAllocator will be pre-configured with a number of sizes of pool to allocate. When a request
for an allocation comes in, it will try to find the **smallest pool whose block size can fit the request**.
If no pool exists that is large enough, it will fall back to the parent BasicAllocator.

**Example:** Configured with pools of block sizes: 4K, 2K, 1K, 512B
- Request for 1135 bytes → routes to 2K pool (smallest that fits)
- Request for 300 bytes → routes to 512B pool
- Request for 5000 bytes → falls back to BasicAllocator (no pool large enough)

Note: The actual request size (1135 bytes) is NOT tracked; the allocator returns a 2K block. This is safe because the consumer only cares about the ObjectId, not the block size.
(Note: existing implementation gets this wrong - we need to fix this mistake)

The ObjectId range a BlockAllocator uses is determined by the ObjectId the Parent allocator provides it with. the BlockAllocator sub-divdes the range to clients downstream.
Note that in this situation the hierarchy matters. An extarnal client requsting ObjectId X will be serviced by one of the BlockAllocators. Was that request to hit the BasicAllocator then it would either miss and not be seen as a valid objectId, or alias to the underlying objectBlock allocator uses to implement that storage and therefore return the wrong object.
This Hierarchy allows the PoolCache to work as well - it has to check across allocation ranges to check for a hit on one of the objects.

## Pool Allocator
The pool allocator maintains a number of child allocators. Normally these are Block Allocators.
A pool allocator maintains two slices of block allocators. One for blocks that are completely full
and one for blocks that have space in them for new allocations.
When there are no entries left in an allocator slice and a request comes in then we will create a new one.

**Block count evolution:** The Pool Allocator creates BlockAllocators with an initial block count of 1024 (i.e., each BlockAllocator manages 1024 slots).

Since optimal block count varies by allocation size and runtime patterns, the pool adapts: each time a new BlockAllocator is needed, the next one doubles the slot count (1024 → 2048 → 4096 → ...).

Benefit: Larger BlockAllocators mean fewer allocators to manage, reducing O(n) search cost.

**Maximum block count:** Capped at 32,768 slots per BlockAllocator to bound worst-case O(n) search time when scanning the allocation bitmap for AllocateRun.

Doubling sequence: 1024 → 2048 → 4096 → 8192 → 16384 → 32768 (stop).

A Pool Allocator may unload an allocator and thereby delegate responsibility for it to the Pool Cache.
Typically when we are peristed to disk, this is achieved by delegating all allocators to the PoolCache.
When we are loaded typically the PoolCache will search itself for Allocators that are not fully allocated and delegate these to the Pool Allocator

An agent owned by the Omni Allocator will intermittently Delegate full allocators from a Pool to the Pool Cache.

When re-loading from disk the pool allocator will start with a blank slate of an empty full and available Block Allocator list. It is the responsibility of the PoolCache to discover any partially used allocators it finds and delegate them to the appropriate Pool Allocator.


## Block Allocator

Creating a new BlockAllocator requests an object from the parent store.
The Block we are given will have an ObjectId, a FileOffset and a Size.
We are safe to treat the ObjectId and Size that we are given as an address space in which we can allocate objects.
Within a BlockAllocator all objects will be the same size. We therefore check for allocation with a bool slice. This is referred to as the allocation slot
The ObjectId we issue is is therefore calculated from the allocated slot. Likewise when we perform a lookup
we can calculate the FileOffset from the ObjectId we are being queried about and our fixed FileOffset.

## Pool Cache
An OmniAllocator will have a pool cache
When a BlockAllocator is unloaded from a pool allocator it will delegate responsibility to the PoolCache.
When a PoolCache has a request to modify one of the Block Allocators (an Allocate or Delete request to one of the objects it is responsible for) it will Load the Block Allocator and Delegate responsibility back to the appropriate Pool Allocator.

```go
type unloadedBlock struct {
    objId           ObjectId

    baseObjId       ObjectId
    BaseFileOffset  FileOffset
    blockSize       int64
    available       bool    // <- I'm not 100% sure this is needed - Let's see
}
```

For Unloaded Objects we track
* ObjectId the stuct is stored at. We can Unmarshal From this ObjectId to get the object back in memory
* BaseObjectId - the starting Block Id that this Allocator manages
* BaseFileOffset - the starting File offset that this Allocator manages
* BlockSize - The size of the Object Id space that this allocator Tracks.


### File-Backed Structure

The PoolCache is backed by a temporary file holding unloaded block metadata.

**File format:**
```
[Header: magic="POOL", version, count]
[Offset table: seek positions for random access]
[Data section: serialized unloadedBlock entries]
```

**In-memory indexes:**
- `unloadedBlocks []unloadedBlock` sorted by `baseObjId` (enables binary search by ObjectId)
- `bySize map[Size][]int` for availability queries
- `byAvailability []int` for finding blocks with free slots

**Key operations:**

- **Insert(Block allocator context):** Binary search to maintain sort order, update indices, append to temp file
- **Query(ObjectId):** Binary search by `baseObjId` range (no disk I/O—metadata already in memory). Returns block metadata. Caller loads full BlockAllocator if modification needed.
- **Remove(Block Allocator Context):** Delete from indices; mark as deleted in temp file (rebuild on persist)

**Persistence:** Marshal to single Store ObjectId. On load, parse file and rebuild in-memory indices (O(n)).

**Scaling:** Simple format scales to thousands of unloaded blocks. If query performance becomes critical, add B-tree index on-disk (future enhancement).

With this data structure, PoolCache answers metadata queries (GetObjectInfo) without loading BlockAllocators into memory. Only when modifying (Allocate/Delete) does the cache load the full BlockAllocator back into a PoolAllocator.

**Load behavior:** When PoolCache loads, any blocks with `available==true` are delegated to appropriate PoolAllocators. Blocks in PoolCache are kept unloaded until needed.

---

## Complete Example: New, Save, Load Workflows

### Scenario 1: Creating a fresh store and persisting it

```go
// FRESH: Create new file
file, _ := os.Create("store.dat")
defer file.Close()

// FRESH: Create allocator hierarchy (from bottom up)
basicAlloc, _ := allocator.NewBasicAllocator(file)
omniAlloc, _ := allocator.NewOmniBlockAllocator(
    []int{64, 256, 1024},  // Pool sizes
    1024,                  // Initial block count per pool
    basicAlloc,            // Parent allocator
    file,
)

// USE: Application does allocations
id1, offset1, _ := omniAlloc.Allocate(200)  // → goes to 256B pool
id2, offset2, _ := omniAlloc.Allocate(50)   // → goes to 64B pool
id3, offset3, _ := omniAlloc.Allocate(5000) // → goes to BasicAllocator

// SAVE: Persist to disk (executed by higher-level Save() orchestrator)
primeTable := allocator.NewPrimeTable()

// Step 1: Persist PoolAllocators (which persist their BlockAllocators first)
for _, pool := range omniAlloc.blockMap {
    if pool != nil {
        poolData, _ := pool.Marshal()
        poolObjId, _, _ := basicAlloc.Allocate(len(poolData))
        file.WriteAt(poolData, int64(poolObjId))  // Write at calculated offset
        primeTable.Store(allocator.PoolAllocatorIndex, poolObjId, ...)
    }
}

// Step 2: Persist OmniAllocator
omniData, _ := omniAlloc.Marshal()
omniObjId, _, _ := basicAlloc.Allocate(len(omniData))
file.WriteAt(omniData, int64(omniObjId))
primeTable.Store(allocator.OmniAllocatorIndex, omniObjId, ...)

// Step 3: Persist BasicAllocator
basicData, _ := basicAlloc.Marshal()
basicObjId, _, _ := basicAlloc.Allocate(len(basicData))
file.WriteAt(basicData, int64(basicObjId))
primeTable.Store(allocator.BasicAllocatorIndex, basicObjId, ...)

// Step 4: Persist PrimeTable (ATOMIC COMMIT)
tableData, _ := primeTable.Marshal()
file.WriteAt(tableData, 0)  // Always at offset 0
```

**Result:** store.dat now contains:
- [Offset 0]: PrimeTable (references to all allocators)
- [Offset X]: BasicAllocator state
- [Offset Y]: OmniAllocator state
- [Offset Z...]: PoolAllocator and BlockAllocator states

---

### Scenario 2: Opening an existing store

```go
// LOAD: Open file
file, _ := os.Open("store.dat")
defer file.Close()

// Step 1: Read PrimeTable from offset 0
tableData := make([]byte, PrimeTableSize)
file.ReadAt(tableData, 0)
primeTable, _ := allocator.UnmarshalPrimeTable(tableData)

// Step 2: Load BasicAllocator
basicEntry := primeTable.Get(allocator.BasicAllocatorIndex)
basicData := make([]byte, basicEntry.Size)
file.ReadAt(basicData, int64(basicEntry.FileOffset))
basicAlloc, _ := allocator.NewBasicAllocator(file)
basicAlloc.Unmarshal(basicData)

// Step 3: Load OmniAllocator
omniEntry := primeTable.Get(allocator.OmniAllocatorIndex)
omniData := make([]byte, omniEntry.Size)
file.ReadAt(omniData, int64(omniEntry.FileOffset))
omniAlloc, _ := allocator.NewOmniBlockAllocator(nil, 1024, basicAlloc, file)
omniAlloc.Unmarshal(omniData)  // Gets PoolAllocator ObjectIds, doesn't load them yet

// READY: Allocator hierarchy is restored and ready to use
// Allocations will work immediately; pools load on-demand

// Example: This triggers lazy-load of the 256B pool
id4, offset4, _ := omniAlloc.Allocate(300)
```

**Result:** Allocator state is fully restored from disk without loading all pools into memory upfront.

---

### Scenario 3: Deleting and reallocating (using existing allocator)

```go
// Suppose we have omniAlloc already loaded (from Scenario 2)

// Delete an object
omniAlloc.DeleteObj(id1)  // Frees space, adds to FreeList

// Reallocate: Will try to reuse freed space
id5, offset5, _ := omniAlloc.Allocate(150)  // May reuse id1's freed space

// Save again (same pattern as Scenario 1)
// PrimeTable is updated to point to new allocator states
```

---

### Key Insights

1. **Constructor = Fresh state:**
   - `New*()` always creates a fresh allocator, never reads from file
   - If you want to restore, call `New*()` then `Unmarshal()`

2. **Hierarchy must be manually wired:**
   - Parent allocator passed explicitly to children
   - Allows flexibility: can test with mock parents, or swap implementations

3. **File I/O owned by allocator, not caller:**
   - Allocators have `os.File` reference
   - Allocators do NOT do file I/O themselves (only Marshal/Unmarshal)
   - Higher-level orchestrator (new Store layer) handles file operations

4. **Lazy loading for efficiency:**
   - PrimeTable tells you where allocators live, but not all are loaded
   - OmniAllocator loads pools on-demand
   - PoolAllocator loads BlockAllocators on-demand
   - Only actively-used allocators consume RAM
