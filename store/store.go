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
	"fmt"
	"io"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/allocator/types"
)

type Storer bobbob.Storer
type FileSize bobbob.FileSize

// IsValidObjectId reports whether the given ObjectId is valid.
// Object 0 is reserved for the store's internal use (typically a config table),
// so valid IDs must be greater than 0.
func IsValidObjectId(objId ObjectId) bool {
	return objId > 0
}

// RunAllocator is an optional extension for stores that can allocate a
// contiguous run of equally-sized objects in a single request.
type RunAllocator interface {
	AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error)
}

// ErrAllocateRunUnsupported indicates the store cannot guarantee contiguous run allocation.
var ErrAllocateRunUnsupported = errors.New("allocate run unsupported")

// AllocatorProvider exposes access to the underlying allocator used by a store.
// This enables external consumers (like vault sessions) to configure allocation
// callbacks without needing direct access to the store implementation type.
type AllocatorProvider interface {
	Allocator() types.Allocator
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

	n, err := writer.Write(data)
	if err != nil {
		if finisher != nil {
			_ = finisher()
		}
		if err := s.DeleteObj(objId); err != nil {
			// Clean up allocated object on write error (best effort)
		}
		return 0, err
	}
	if n != size {
		if finisher != nil {
			_ = finisher()
		}
		if err := s.DeleteObj(objId); err != nil {
			// Clean up allocated object on incomplete write (best effort)
		}
		return 0, errors.New("did not write all the data")
	}

	if finisher != nil {
		if err := finisher(); err != nil {
			if delErr := s.DeleteObj(objId); delErr != nil {
				return 0, fmt.Errorf("finisher failed after write: %w (cleanup delete also failed: %v)", err, delErr)
			}
			return 0, fmt.Errorf("finisher failed after write: %w", err)
		}
	}

	return objId, nil
}

// LateWriteNewObjFromBytes is a convenience wrapper around LateWriteNewObj that writes
// a byte slice from memory to a new object in the store.
// This is useful when you have the data in memory and want a simple write operation.
// For large data or streaming scenarios, use LateWriteNewObj directly.
// If an error occurs during writing, the allocated object is automatically deleted.
func LateWriteNewObjFromBytes(s Storer, data []byte) (ObjectId, func() error) {
	size := len(data)
	objId, writer, finisher, err := s.LateWriteNewObj(size)
	if err != nil {
		return 0, func() error { return err }
	}
	lateWriter := func() error {
		// log.Printf("store: LateWriteNewObjFromBytes start obj=%d size=%d", objId, size)
		n, err := writer.Write(data)
		// log.Printf("store: LateWriteNewObjFromBytes done obj=%d n=%d err=%v", objId, n, err)
		if err != nil {
			if finisher != nil {
				_ = finisher()
			}
			if err := s.DeleteObj(objId); err != nil {
				// Clean up allocated object on write error (best effort)
			}
			return err
		}
		if n != size {
			if finisher != nil {
				_ = finisher()
			}
			if err := s.DeleteObj(objId); err != nil {
				// Clean up allocated object on incomplete write (best effort)
			}
			return errors.New("did not write all the data")
		}
		if finisher != nil {
			if err := finisher(); err != nil {
				if delErr := s.DeleteObj(objId); delErr != nil {
					return fmt.Errorf("finisher failed after write: %w (cleanup delete also failed: %v)", err, delErr)
				}
				return fmt.Errorf("finisher failed after write: %w", err)
			}
		}
		return nil
	}
	return objId, lateWriter
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

	// log.Printf("store: WriteBytesToObj start obj=%d size=%d", objectId, len(data))
	n, err := writer.Write(data)
	// log.Printf("store: WriteBytesToObj done obj=%d n=%d err=%v", objectId, n, err)
	if err != nil {
		if closer != nil {
			_ = closer()
		}
		return err
	}
	if n != len(data) {
		if closer != nil {
			_ = closer()
		}
		return errors.New("did not write all the data")
	}
	if closer != nil {
		if err := closer(); err != nil {
			return fmt.Errorf("failed to finalize write: %w", err)
		}
	}
	return nil
}

// ReadBytesFromObj is a convenience wrapper around LateReadObj that reads
// all bytes from an object into memory.
// For large objects or streaming scenarios, use LateReadObj directly.
func ReadBytesFromObj(s Storer, objId ObjectId) ([]byte, error) {
	if !IsValidObjectId(objId) {
		return nil, errors.New("invalid object ID")
	}
	objReader, finisher, err := s.LateReadObj(objId)
	if err != nil {
		return nil, err
	}

	// log.Printf("store: ReadBytesFromObj start obj=%d", objId)
	data, err := io.ReadAll(objReader)
	// log.Printf("store: ReadBytesFromObj done obj=%d n=%d err=%v", objId, len(data), err)
	if finisher != nil {
		finishErr := finisher()
		if err != nil {
			if finishErr != nil {
				return nil, fmt.Errorf("read failed: %w; finisher failed: %v", err, finishErr)
			}
			return nil, err
		}
		if finishErr != nil {
			return nil, fmt.Errorf("read finisher failed: %w", finishErr)
		}
	}
	return data, err
}

// WriteGeneric writes a generic object to the store
func WriteGeneric(s Storer, obj any) (ObjectId, error) {
	switch v := obj.(type) {
	case MarshalSimple:
		data, err := v.Marshal()
		if err != nil {
			return bobbob.ObjNotWritten, err
		}
		return WriteNewObjFromBytes(s, data)
	default:
		return marshalGeneric(s, obj)
	}
}

// ReadGeneric reads a generic object from the store
func ReadGeneric(s Storer, obj any, objId ObjectId) error {
	switch v := obj.(type) {
	case UnmarshalSimple:
		return unmarshalSimpleObj(s, v, objId)
	default:
		return unmarshalGeneric(s, obj, objId)
	}
}
