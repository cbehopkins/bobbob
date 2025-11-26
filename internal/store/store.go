// Package store provides a persistent binary object storage system.
//
// # Architecture
//
// The store API is built on a "Late method" pattern where streaming I/O
// primitives (methods prefixed with "Late") form the foundation, and
// convenience wrappers are built on top for common use cases.
//
// # Late Methods (Fundamental Primitives)
//
// Late methods provide streaming I/O with explicit resource management:
//   - LateWriteNewObj: allocate and stream data to a new object
//   - LateReadObj: stream data from an existing object
//   - WriteToObj: stream data to update an existing object
//
// All Late methods return a Finisher callback that must be called when
// the I/O operation completes to properly release resources.
//
// # Convenience Wrappers
//
// Built on top of Late methods for common patterns:
//   - NewObj: allocate without immediate write (wraps LateWriteNewObj)
//   - ReadBytesFromObj: read entire object into memory (wraps LateReadObj)
//   - WriteNewObjFromBytes: write in-memory data (wraps LateWriteNewObj)
//
// # Usage Example
//
//	// Create a store
//	store, err := NewBasicStore("data.blob")
//	if err != nil {
//	    return err
//	}
//	defer store.Close()
//
//	// Write using Late method (streaming)
//	objId, writer, finisher, err := store.LateWriteNewObj(1024)
//	if err != nil {
//	    return err
//	}
//	defer finisher()
//	io.Copy(writer, dataSource)
//
//	// Write using convenience wrapper (in-memory)
//	objId, err = WriteNewObjFromBytes(store, []byte("hello"))
//
//	// Read using Late method (streaming)
//	reader, finisher, err := store.LateReadObj(objId)
//	if err != nil {
//	    return err
//	}
//	defer finisher()
//	io.Copy(destination, reader)
//
//	// Read using convenience wrapper (in-memory)
//	data, err := ReadBytesFromObj(store, objId)
package store

import (
	"errors"
	"io"
)

var (
	ObjNotWritten      = ObjectId(-1)
	ObjNotAllocated    = ObjectId(-2)
	objNotPreAllocated = ObjectId(-3)
)

// IsValidObjectId reports whether the given ObjectId is valid.
// Object 0 is reserved for the store's internal use (typically a config table),
// so valid IDs must be greater than 0.
func IsValidObjectId(objId ObjectId) bool {
	return objId > 0
}

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
	// Close closes the store and releases all resources.
	Close() error
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
}

// Storer is the primary interface for object storage.
// It combines basic lifecycle management with streaming read/write operations.
type Storer interface {
	BasicStorer
	ObjReader
	ObjWriter
}

// ObjectInfo contains metadata about a stored object,
// including its file offset and size in bytes.
type ObjectInfo struct {
	Offset FileOffset
	Size   int
}

// WriteNewObjFromBytes is a convenience wrapper around LateWriteNewObj that writes
// a byte slice from memory to a new object in the store.
// This is useful when you have the data in memory and want a simple write operation.
// For large data or streaming scenarios, use LateWriteNewObj directly.
// If an error occurs during writing, the allocated object is automatically deleted.
func WriteNewObjFromBytes(s Storer, data []byte) (ObjectId, error) {
	size := len(data)
	objId, writer, finisher, err := s.LateWriteNewObj(size)
	if err != nil {
		return 0, err
	}
	if finisher != nil {
		defer finisher()
	}

	n, err := writer.Write(data)
	if err != nil {
		s.DeleteObj(objId) // Clean up allocated object on write error
		return 0, err
	}
	if n != size {
		s.DeleteObj(objId) // Clean up allocated object on incomplete write
		return 0, errors.New("did not write all the data")
	}

	return objId, nil
}

// WriteBytesToObj is a convenience wrapper around WriteToObj that writes a byte slice
// from memory to an existing object in the store.
// It is preferred to create a new object, write to it, and then delete the old object.
// This allows for less risk of file corruption.
// For large data or streaming scenarios, use WriteToObj directly.
func WriteBytesToObj(s Storer, data []byte, objectId ObjectId) error {
	writer, closer, err := s.WriteToObj(objectId)
	if err != nil {
		return err
	}
	defer closer()

	n, err := writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("did not write all the data")
	}
	return nil
}

// ReadBytesFromObj is a convenience wrapper around LateReadObj that reads
// all bytes from an object into memory.
// For large objects or streaming scenarios, use LateReadObj directly.
func ReadBytesFromObj(s Storer, objId ObjectId) ([]byte, error) {
	objReader, finisher, err := s.LateReadObj(objId)
	if err != nil {
		return nil, err
	}
	if finisher != nil {
		defer finisher()
	}

	return io.ReadAll(objReader)
}

// WriteGeneric writes a generic object to the store
func WriteGeneric(s Storer, obj any) (ObjectId, error) {
	switch v := obj.(type) {
	case MarshalComplex:
		return writeComplexTypes(s, v)
	case MarshalSimple:
		data, err := v.Marshal()
		if err != nil {
			return ObjNotWritten, err
		}
		return WriteNewObjFromBytes(s, data)
	default:
		return marshalGeneric(s, obj)
	}
}

// ReadGeneric reads a generic object from the store
func ReadGeneric(s Storer, obj any, objId ObjectId) error {
	switch v := obj.(type) {
	case UnmarshalComplex:
		return unmarshalComplexObj(s, v, objId)
	case UnmarshalSimple:
		return unmarshalSimpleObj(s, v, objId)
	default:
		return unmarshalGeneric(s, obj, objId)
	}
}
