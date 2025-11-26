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

// ObjReader provides read access to stored objects.
type ObjReader interface {
	// LateReadObj returns a reader for the object with the given ID.
	// The Finisher must be called when reading is complete.
	LateReadObj(id ObjectId) (io.Reader, Finisher, error)
}

// ObjWriter provides write access to stored objects.
type ObjWriter interface {
	// LateWriteNewObj creates a new object of the given size and returns
	// its ID and a writer. The Finisher must be called when writing is complete.
	LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error)
	// WriteToObj returns a writer for an existing object.
	// The Finisher must be called when writing is complete.
	WriteToObj(objectId ObjectId) (io.Writer, Finisher, error)
}

// Storer is the primary interface for object storage.
// It provides methods to create, read, write, and delete objects.
type Storer interface {
	// NewObj allocates a new object of the given size and returns its ID.
	NewObj(size int) (ObjectId, error)
	// DeleteObj removes the object with the given ID.
	DeleteObj(objId ObjectId) error
	// Close closes the store and releases all resources.
	Close() error
	ObjReader
	ObjWriter
}

// ObjectInfo contains metadata about a stored object,
// including its file offset and size in bytes.
type ObjectInfo struct {
	Offset FileOffset
	Size   int
}

// WriteNewObjFromBytes writes a new object to the store from a byte slice
// This is useful when you have the data in memory and you want to write it to the store
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
		return objId, err
	}
	if n != size {
		return objId, errors.New("did not write all the data")
	}

	return objId, err
}

// WriteBytesToObj writes a byte slice to an existing object in the store.
// It is preferred to create a new object, write to it, and then delete the old object.
// This allows for less risk of file corruption.
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

// ReadBytesFromObj reads all bytes from an object in the store.
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
