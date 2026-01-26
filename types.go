package bobbob

import (
	"encoding/binary"
	"errors"
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

// PreMarshal returns the sizes of sub-objects needed to store the ObjectId.
// For ObjectId, this is a single 8-byte value.
func (id ObjectId) PreMarshal() []int {
	return []int{8}
}

// MarshalSimple is an interface for basic types that can be marshalled in one step.
type MarshalSimple interface {
	Marshal() ([]byte, error)
}

// UnmarshalSimple is an interface for basic types that can be unmarshalled in one step.
type UnmarshalSimple interface {
	Unmarshal([]byte) error
}

// ErrRePreAllocate is returned when an object needs more ObjectIds than initially allocated
var ErrRePreAllocate = errors.New("object requires re-preallocation")
