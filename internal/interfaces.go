package internal

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

// MarshalComplexWithRetry marshals a MarshalComplex object with automatic retry on ErrRePreAllocate.
// allocFunc should allocate ObjectIds for the given sizes.
// Returns the identity ObjectId and any error.
func MarshalComplexWithRetry(obj MarshalComplex, allocFunc func([]int) ([]ObjectId, error), maxRetries int) (ObjectId, []ObjectAndByteFunc, error) {
	sizes, err := obj.PreMarshal()
	if err != nil {
		return 0, nil, err
	}

	objectIds, err := allocFunc(sizes)
	if err != nil {
		return 0, nil, err
	}

	for i := 0; i < maxRetries; i++ {
		identityFunction, objectAndByteFuncs, err := obj.MarshalMultiple(objectIds)
		if err == nil {
			return identityFunction(), objectAndByteFuncs, nil
		}
		// Retry if re-preallocation is needed
		if errors.Is(err, ErrRePreAllocate) {
			// Re-allocate objects as needed
			sizes, err := obj.PreMarshal()
			if err != nil {
				return 0, nil, err
			}

			objectIds, err = allocFunc(sizes)
			if err != nil {
				return 0, nil, err
			}
			continue
		}
		return 0, nil, err
	}
	return 0, nil, errors.New("too many re-preallocation attempts")
}

// MarshalComplex defines the multi-step marshaling contract.
type MarshalComplex interface {
	PreMarshal() ([]int, error)
	MarshalMultiple([]ObjectId) (func() ObjectId, []ObjectAndByteFunc, error)
	Delete() error
}

// UnmarshalComplex is an interface for complex types that need to be unmarshalled in multiple steps.
type UnmarshalComplex interface {
	UnmarshalMultiple(objData io.Reader, reader any) error
}

// ObjectAndByteFunc pairs an ObjectId with a byte-producing function.
type ObjectAndByteFunc struct {
	ObjectId ObjectId
	ByteFunc func() ([]byte, error)
}
