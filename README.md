# bobbob Project

Bobbob (Bunch Of Binary BlOBs) is a simple Go application that provides a binary file storage system. It allows users to create a binary file, write objects to it, and read objects from it.

Do you find yourself dealing with data structures that are larger than available memory?
Is working with these without using all your memory more important to you than performance? (i.e. are you willing to spend disk bandwidth to ensure you don't use physical memory)
Need lists that can grow to be larger than available memory?
Need a map storing lots of objects.
Don't want to implement a full database?

BunchOfBinaryBlOBs might be for you.

FWIW the name bobbob comes from when I can't think of a name for something I often use foo and bar like everyone else. But sometimes Bob seems to work better and of course [Bob was](https://galactanet.com/comic/view.php?strip=530)  [there too](https://galactanet.com/comic/view.php?strip=517)

## Core Concepts

The foundation is the Store type. At its most simple level it's trivial. A single file that you write []bytes to.
You write an []byte and get back an integer. This integer is the ObjectId - a unique value that can be used to access the bytes again in the future.

In the most trivial cases the ObjectId is the file offset that the bytes are stored to. (Told you it was a simple thing.)

The store also tracks the object locations and their sizes, policing the reads and writes so that one object does not interfere with others.

So far, so boring. But Store supports deletion of objects, so you can end up with holes in your used space.
The sensible thing to do is to therefore track those holes and rather than write new objects to the end of the file, use these holes for new objects.

Therefore the ObjectId is not actually the file offset (although it can often be).
It is in fact just a handle that we look up the actual file offset from. The file offset can change after a Free/Compact session and therefore there are safeguards in place.

## API Design Pattern

The bobbob API follows a consistent pattern where **"Late" methods are the fundamental primitives**:

### Late Methods (Fundamental Primitives)
These methods provide streaming I/O with explicit resource management via `Finisher` callbacks:

- **`LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error)`** - Allocate and get a writer for streaming data to disk
- **`LateReadObj(id ObjectId) (io.Reader, Finisher, error)`** - Get a reader for streaming data from disk
- **`WriteToObj(objectId ObjectId) (io.Writer, Finisher, error)`** - Get a writer to update an existing object

The `Finisher` callback must be called when you're done with the reader/writer to release resources properly.

### Convenience Wrappers
These are built on top of the Late methods for common use cases:

- **`NewObj(size int) (ObjectId, error)`** - Allocate without immediate write access (wraps `LateWriteNewObj`)
- **`ReadBytesFromObj(objId ObjectId) ([]byte, error)`** - Read entire object into memory (wraps `LateReadObj`)
- **`WriteNewObjFromBytes(data []byte) (ObjectId, error)`** - Write in-memory data to new object (wraps `LateWriteNewObj`)

### Interface Hierarchy

```go
// BasicStorer - object lifecycle (allocate, delete, close)
type BasicStorer interface {
    NewObj(size int) (ObjectId, error)
    DeleteObj(objId ObjectId) error
    Close() error
}

// ObjReader - streaming reads (Late method)
type ObjReader interface {
    LateReadObj(id ObjectId) (io.Reader, Finisher, error)
}

// ObjWriter - streaming writes (Late methods)
type ObjWriter interface {
    LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error)
    WriteToObj(objectId ObjectId) (io.Writer, Finisher, error)
}

// Storer - complete interface (combines all above)
type Storer interface {
    BasicStorer
    ObjReader
    ObjWriter
}
```

### Usage Examples

**Streaming I/O (Late methods - for large objects):**
```go
// Write large data without loading into memory
objId, writer, finisher, err := store.LateWriteNewObj(size)
if err != nil {
    return err
}
defer finisher()

_, err = io.Copy(writer, largeDataSource)

// Read large data without loading into memory
reader, finisher, err := store.LateReadObj(objId)
if err != nil {
    return err
}
defer finisher()

_, err = io.Copy(destination, reader)
```

**In-Memory I/O (Convenience wrappers):**
```go
// Write in-memory data
data := []byte("hello world")
objId, err := store.WriteNewObjFromBytes(data)

// Read into memory
data, err := store.ReadBytesFromObj(objId)
```

## Higher-Level Data Structures

Further Data structures are then built upon this foundation.

For arbitrary length lists we have Link and Chain. These work together as a doubly linked list to provide lists that are larger than can be fit into memory.

This comes in a sorted and unsorted variant. Fundamentally they are the same structure, but if one follows certain rules, you will always get a sorted list, if one follows a different set of rules it will be an unsorted list.

If one desires a map/dict like object there is the yggdrasil package. This builds upon store to create a map/dict behaviour on top of a store. This provides peristent set/key/value pair behaviour for arbitrary data types, for arbitrarily large data sets regardless of available system memory (uses a treap and lazy loading)

There is clearly much to be done...
