package internal

import (
	"errors"
	"io"

	"github.com/cbehopkins/bobbob"
)

// MarshalComplexWithRetry marshals a MarshalComplex object with automatic retry on ErrRePreAllocate.
// allocFunc should allocate ObjectIds for the given sizes.
// Returns the identity ObjectId and any error.
func MarshalComplexWithRetry(obj MarshalComplex, allocFunc func([]int) ([]bobbob.ObjectId, error), maxRetries int) (bobbob.ObjectId, []ObjectAndByteFunc, error) {
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
		if errors.Is(err, bobbob.ErrRePreAllocate) {
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
	MarshalMultiple([]bobbob.ObjectId) (func() bobbob.ObjectId, []ObjectAndByteFunc, error)
	Delete() error
}

// UnmarshalComplex is an interface for complex types that need to be unmarshalled in multiple steps.
type UnmarshalComplex interface {
	UnmarshalMultiple(objData io.Reader, reader any) error
}

// ObjectAndByteFunc pairs an ObjectId with a byte-producing function.
type ObjectAndByteFunc struct {
	ObjectId bobbob.ObjectId
	ByteFunc func() ([]byte, error)
}
