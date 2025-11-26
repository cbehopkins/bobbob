package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

var ErrorNoData = fmt.Errorf("no data to unmarshal")

// MarshalSimple is an interface for basic types that can be marshalled in one step.
// Types implementing this interface can convert themselves to a byte slice.
type MarshalSimple interface {
	// Marshal converts the object to a byte slice.
	Marshal() ([]byte, error)
}

// UnmarshalSimple is an interface for basic types that can be unmarshalled in one step.
// Types implementing this interface can populate themselves from a byte slice.
type UnmarshalSimple interface {
	// Unmarshal populates the object from a byte slice.
	Unmarshal([]byte) error
}

// MarshalComplex is an interface for complex types that need to be marshalled in multiple steps.
// This is used for types that contain references to other objects in the store.
type MarshalComplex interface {
	// PreMarshal returns the sizes of sub-objects that need to be allocated.
	PreMarshal() ([]int, error)
	// MarshalMultiple returns functions to marshal this object and its children.
	// It takes the pre-allocated ObjectIds for the sub-objects.
	MarshalMultiple([]ObjectId) (func() ObjectId, []ObjectAndByteFunc, error)
	// Delete removes this object and all its children from the store.
	Delete() error
}

// UnmarshalComplex is an interface for complex types that need to be unmarshalled in multiple steps.
type UnmarshalComplex interface {
	// UnmarshalMultiple populates the object by reading from the store.
	UnmarshalMultiple(objData io.Reader, reader ObjReader) error
}

// ObjectAndByteFunc holds an ObjectId and a function that returns the bytes to write to it.
// This is used during complex marshaling to associate pre-allocated objects with their data.
type ObjectAndByteFunc struct {
	ObjectId ObjectId
	ByteFunc func() ([]byte, error)
}

// allocateObjects allocates objects in the store and returns a list of ObjectIds
func allocateObjects(s Storer, sizes []int) ([]ObjectId, error) {
	if sizes == nil {
		return []ObjectId{objNotPreAllocated}, nil
	}

	var objectIds []ObjectId
	for _, size := range sizes {
		if size < 0 {
			objectIds = append(objectIds, objNotPreAllocated)
			continue
		}
		// FIXME refactor to get mutex once
		objId, err := s.NewObj(size)
		if err != nil {
			return nil, err
		}
		objectIds = append(objectIds, objId)
	}

	return objectIds, nil
}

// writeObjects writes the objects to the store using the provided ObjectAndByteFunc list
func writeObjects(s Storer, objects []ObjectAndByteFunc) error {
	// FIXME Can we detect the objects are consecutive and write them in one go?
	// FIXME do we even need to - write a benchmark!
	for _, obj := range objects {
		data, err := obj.ByteFunc()
		if err != nil {
			return err
		}
		objId := obj.ObjectId
		if objId == objNotPreAllocated {
			_, err := WriteNewObjFromBytes(s, data)
			if err != nil {
				return err
			}
			continue
		}
		// FIXME can we farm this out into a series of workers?
		err = WriteBytesToObj(s, data, objId)
		if err != nil {
			return err
		}

	}

	return nil
}

// writeComplexTypes writes complex types to the store
func writeComplexTypes(s Storer, obj MarshalComplex) (ObjectId, error) {
	sizes, err := obj.PreMarshal()
	if err != nil {
		return ObjNotAllocated, err
	}

	objectIds, err := allocateObjects(s, sizes)
	if err != nil {
		return ObjNotAllocated, err
	}

	identityFunction, objectAndByteFuncs, err := obj.MarshalMultiple(objectIds)
	if err != nil {
		return ObjNotWritten, err
	}

	return identityFunction(), writeObjects(s, objectAndByteFuncs)
}

// marshalGeneric marshals a generic object
func marshalGeneric(s Storer, obj any) (ObjectId, error) {
	switch v := obj.(type) {
	case int:
		return marshalFixedSize(s, int64(v))
	case uint:
		return marshalFixedSize(s, uint64(v))
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return marshalFixedSize(s, v)
	default:
		return ObjNotWritten, fmt.Errorf("Object cannot be generically marshalled")
	}
}

// marshalFixedSize marshals a fixed size object
func marshalFixedSize(s Storer, v any) (ObjectId, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, v)
	if err != nil {
		return ObjNotWritten, err
	}
	return WriteNewObjFromBytes(s, buf.Bytes())
}

// unmarshalComplexObj unmarshals a complex object
func unmarshalComplexObj(s Storer, obj UnmarshalComplex, objId ObjectId) error {
	objReader, finisher, err := s.LateReadObj(objId)
	if err != nil {
		return err
	}
	defer finisher()
	return obj.UnmarshalMultiple(objReader, s)
}

// unmarshalSimpleObj unmarshals a simple object
func unmarshalSimpleObj(s Storer, obj UnmarshalSimple, objId ObjectId) error {
	data, err := ReadBytesFromObj(s, objId)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return ErrorNoData
	}
	return obj.Unmarshal(data)
}

// unmarshalGeneric unmarshals a generic object
func unmarshalGeneric(s Storer, obj any, objId ObjectId) error {
	switch v := obj.(type) {
	case *int:
		var temp int64
		objReader, finisher, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		err = finisher()
		if err != nil {
			return err
		}
		err = binary.Read(objReader, binary.LittleEndian, &temp)
		if err != nil {
			return err
		}
		*v = int(temp)
		return nil
	case *uint:
		var temp uint64
		objReader, finisher, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		err = binary.Read(objReader, binary.LittleEndian, &temp)
		if err != nil {
			return err
		}
		err = finisher()
		if err != nil {
			return err
		}
		*v = uint(temp)
		return nil
	case *int8, *int16, *int32, *int64, *uint8, *uint16, *uint32, *uint64:
		objReader, finisher, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		err = finisher()
		if err != nil {
			return err
		}
		return binary.Read(objReader, binary.LittleEndian, v)
	default:
		return fmt.Errorf("Unsupported object type for unmarshalling generically, The type is a %T", v)
	}
}
