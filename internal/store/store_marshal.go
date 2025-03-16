package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// MarshalSimple is an interface for basic types that can be marshalled in one step
type MarshalSimple interface {
	Marshal() ([]byte, error)
}
type UnmarshalSimple interface {
	Unmarshal([]byte) error
}

// MarshalComplex is an interface for complex types that need to be marshalled in multiple steps
type MarshalComplex interface {
	PreMarshal() ([]int, error)
	MarshalMultiple([]ObjectId) (func() ObjectId, []ObjectAndByteFunc, error)
	Delete() error
}

type UnmarshalComplex interface {
	UnmarshalMultiple(objData io.Reader, reader ObjReader) error
}

// ObjectAndByteFunc holds an ObjectId and a function that returns a slice of bytes
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
	objReader, err := s.LateReadObj(objId)
	if err != nil {
		return err
	}
	return obj.UnmarshalMultiple(objReader, s)
}

// unmarshalSimpleObj unmarshals a simple object
func unmarshalSimpleObj(s Storer, obj UnmarshalSimple, objId ObjectId) error {
	objReader, err := s.LateReadObj(objId)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(objReader)
	if err != nil {
		return err
	}
	return obj.Unmarshal(data)
}

// unmarshalGeneric unmarshals a generic object
func unmarshalGeneric(s Storer, obj any, objId ObjectId) error {
	switch v := obj.(type) {
	case *int:
		var temp int64
		objReader, err := s.LateReadObj(objId)
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
		objReader, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		err = binary.Read(objReader, binary.LittleEndian, &temp)
		if err != nil {
			return err
		}
		*v = uint(temp)
		return nil
	case *int8, *int16, *int32, *int64, *uint8, *uint16, *uint32, *uint64:
		objReader, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		return binary.Read(objReader, binary.LittleEndian, v)
	default:
		return fmt.Errorf("Unsupported object type for unmarshalling generically, The type is a %T", v)
	}
}
