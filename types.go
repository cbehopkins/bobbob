package bobbob

import (
	"encoding/binary"
	"errors"
	"io"
)

// ObjectId is an identifier unique within the store for an object.
type ObjectId int64

var (
	ObjNotWritten      = ObjectId(-1)
	ObjNotAllocated    = ObjectId(-2)
	ObjNotPreAllocated = ObjectId(-3)
)

// FileOffset represents a byte offset within a file.
type FileOffset int64
type FileSize int64

// SizeInBytes returns the number of bytes required to marshal this ObjectId.
// It must satisfy the PersistentKey interface.
func (id ObjectId) SizeInBytes() int {
	return 8
}

// Equals reports whether this ObjectId equals another.
func (id ObjectId) Equals(other ObjectId) bool {
	return id == other
}

// Marshal converts the ObjectId into a fixed length bytes encoding
func (id ObjectId) Marshal() ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(id))
	return buf, nil
}

// Unmarshal converts a fixed length bytes encoding into an ObjectId
func (id *ObjectId) Unmarshal(data []byte) error {
	if len(data) < 8 {
		return errors.New("invalid data length for ObjectId")
	}
	*id = ObjectId(binary.LittleEndian.Uint64(data[:8]))
	return nil
}

// MarshalSimple is an interface for basic types that can be marshalled in one step.
type MarshalSimple interface {
	Marshal() ([]byte, error)
}

// UnmarshalSimple is an interface for basic types that can be unmarshalled in one step.
type UnmarshalSimple interface {
	Unmarshal([]byte) error
}

type LateMarshaler interface {
	// LateMarshal returns the ObjectId where the payload will be written,
	// the logical size of the payload in bytes, and a Finisher to call when
	// the I/O has completed.
	LateMarshal(s Storer) (ObjectId, int, Finisher)
}

type LateUnmarshaler interface {
	// LateUnmarshal reads a payload stored under id with the provided logical size.
	// size may be 0 when unknown; implementations should handle that case.
	LateUnmarshal(id ObjectId, size int, s Storer) Finisher
}

// ErrRePreAllocate is returned when an object needs more ObjectIds than initially allocated
var ErrRePreAllocate = errors.New("object requires re-preallocation")

// ErrStringStorerNotSupported is returned when a store doesn't support StringStorer operations
var ErrStringStorerNotSupported = errors.New("store does not support StringStorer interface")

// Finisher is a callback that must be called when you have finished
// with the associated I/O operation. It releases resources and ensures
// data is properly flushed or locks are released.
type Finisher func() error

// BasicStorer provides basic object lifecycle management.
// It handles allocation, deletion, and closing of the store.
// NewObj is a convenience wrapper around the Late methods.
type BasicStorer interface {
	// NewObj allocates a new object of the given size and returns its ID.
	// This is a convenience wrapper around LateWriteNewObj for when you don't
	// need immediate write access.
	NewObj(size int) (ObjectId, error)
	// DeleteObj removes the object with the given ID.
	DeleteObj(objId ObjectId) error
	// Close flushes any pending writes and closes the store.
	Close() error
	// PrimeObject returns the ObjectId reserved for application metadata.
	// This is typically the first object allocated after the store's internal
	// objects. The store allocates this object on first access if it doesn't
	// exist. Applications can use this to store their root metadata (like a
	// collection registry or type map).
	PrimeObject(size int) (ObjectId, error)
}

// ObjReader provides streaming read access to stored objects.
// The Late methods are the fundamental primitives for I/O operations.
type ObjReader interface {
	// LateReadObj returns a reader for streaming access to the object.
	// This is the fundamental read primitive. The Finisher must be called
	// when reading is complete to release resources.
	LateReadObj(id ObjectId) (io.Reader, Finisher, error)
}

// ObjWriter provides streaming write access to stored objects.
// The Late methods are the fundamental primitives for I/O operations.
type ObjWriter interface {
	// LateWriteNewObj is the fundamental allocation and write primitive.
	// It creates a new object of the given size and returns its ID along with
	// a writer. The Finisher must be called when writing is complete.
	LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error)
	// WriteToObj returns a writer for an existing object.
	// This is a Late method for streaming writes. The Finisher must be called
	// when writing is complete.
	WriteToObj(objectId ObjectId) (io.Writer, Finisher, error)
	// WriteBatchedObjs writes data to multiple consecutive objects in a single operation.
	// This is a performance optimization for writing multiple small objects that are
	// adjacent in the file, reducing system call overhead.
	//
	// The objects must be consecutive in the file (adjacent with no gaps). Each object
	// is verified to exist and the total data written must match the sum of object sizes.
	//
	// Parameters:
	//   - objIds: Slice of ObjectIds to write to (must be consecutive in file)
	//   - data: The complete byte slice containing all object data concatenated
	//   - sizes: Slice of sizes for each object (must match objIds length and sum to len(data))
	//
	// Returns an error if:
	//   - Objects are not consecutive in the file
	//   - Sum of sizes doesn't match len(data)
	//   - Length of objIds doesn't match length of sizes
	//   - Any object doesn't exist
	//   - Write operation fails
	WriteBatchedObjs(objIds []ObjectId, data []byte, sizes []int) error
}

// Storer is the primary interface for object storage.
// It combines basic lifecycle management with streaming read/write operations.
//
// Storer includes all methods from BasicStorer (including Close() for resource cleanup),
// ObjReader (for streaming reads), and ObjWriter (for streaming writes).
// This ensures that any Storer can be properly closed by callers without type assertions.
type Storer interface {
	BasicStorer
	ObjReader
	ObjWriter
}

// StringStorer is an optional interface that stores may implement to provide
// specialized string storage. Implementations use type assertions to check for this interface.
// If not available, callers fall back to generic object allocation.
//
// This pattern allows stores like StringStore (via MultiStore) to optimize string handling
// without breaking backward compatibility with stores that don't support it.
type StringStorer interface {
	// NewStringObj stores a string and returns its ObjectId.
	// Returns error if string is too large or storage fails.
	NewStringObj(data string) (ObjectId, error)

	// StringFromObjId retrieves a string by ObjectId.
	// Returns error if objId doesn't exist or is not a string object.
	StringFromObjId(objId ObjectId) (string, error)

	// HasStringObj checks if a given ObjectId is a string object in this store.
	// Used by DeleteObj to determine routing.
	HasStringObj(objId ObjectId) bool
}
