package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MarshalSimple is an interface for basic types that can be marshalled in one step
type MarshalSimple interface {
	Marshal() ([]byte, error)
}
type UnmarshalSimple interface {
	Unmarshal([]byte) (error)
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
func (s *store) allocateObjects(sizes []int) ([]ObjectId, error) {
	if sizes == nil {
		return []ObjectId{objNotPreAllocated}, nil
	}

	var objectIds []ObjectId
	for _, size := range sizes {
		if size < 0 {
			objectIds = append(objectIds, objNotPreAllocated)
			continue
		}
		objId, fileOffset, err := s.allocator.Allocate(size)
		if err != nil {
			return nil, err
		}
		// FIXME refactor to get mutex once
		s.objectMap.Set(objId, ObjectInfo{Offset: fileOffset, Size: size})
		objectIds = append(objectIds, objId)
	}

	return objectIds, nil
}

// writeObjects writes the objects to the store using the provided ObjectAndByteFunc list
func (s *store) writeObjects(objects []ObjectAndByteFunc) error {
	// FIXME Can we detect the objects are consecutive and write them in one go?
	for _, obj := range objects {
		data, err := obj.ByteFunc()
		if err != nil {
			return err
		}
		objId := obj.ObjectId
		if objId == objNotPreAllocated {
			_, err := s.WriteNewObjFromBytes(data)
			if err != nil {
				return err
			}
			continue
		}
		// FIXME can we farm this out into a series of workers?
		objInfo, found := s.objectMap.Get(obj.ObjectId)
		if !found {
			return errors.New("object not found in object map")
		}
		n, err := s.file.WriteAt(data, int64(objInfo.Offset))
		if err != nil {
			return err
		}
		if n != len(data) {
			// FIXME we should retry here
			return errors.New("failed to write all bytes")
		}
	}

	return nil
}


func (s *store) WriteGeneric(obj any) (ObjectId, error) {
	switch v := obj.(type) {
	case MarshalComplex:
		return s.WriteComplexTypes(v)
	case MarshalSimple:
		data, err := v.Marshal()
		if err != nil {
			return	ObjNotWritten, err
		}
		return s.WriteNewObjFromBytes(data)
	default:
		return s.marshalGeneric(obj)
	}
}

func (s *store) ReadGeneric(obj any, objId ObjectId) error {
	switch v := obj.(type) {
	case UnmarshalComplex:
		return s.unmarshalComplexObj(v, objId)
	case UnmarshalSimple:
		return s.unmarshalSimpleObj(v, objId)
	default:
		return s.unmarshalGeneric(obj, objId)
	}
}
// WriteComplexTypes writes complex types to the store
func (s *store) WriteComplexTypes(obj MarshalComplex) (ObjectId, error) {
	sizes, err := obj.PreMarshal()
	if err != nil {
		return 0, err
	}

	objectIds, err := s.allocateObjects(sizes)
	if err != nil {
		return 0, err
	}

	identityFunction, objectAndByteFuncs, err := obj.MarshalMultiple(objectIds)
	if err != nil {
		return 0, err
	}

	return identityFunction(), s.writeObjects(objectAndByteFuncs)
}

func (s *store) MarshalMultipleGeneric(obj MarshalComplex, objectIds []ObjectId) (func() ObjectId, []ObjectAndByteFunc, error) {
	return obj.MarshalMultiple(objectIds)
}

func (s *store) marshalGeneric(obj any) (ObjectId,  error) {
	switch v := obj.(type) {
	case int:
		return s.marshalFixedSize(int64(v))
	case uint:
		return s.marshalFixedSize(uint64(v))
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return s.marshalFixedSize(v)
	default:
		return ObjNotWritten, fmt.Errorf("Object cannot be generically marshalled")
}}

func (s *store) marshalFixedSize(v any) (ObjectId,  error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, v)
	if err != nil {
		return ObjNotWritten, err
	}
	return s.WriteNewObjFromBytes(buf.Bytes())
}

func  (s *store) unmarshalComplexObj(obj UnmarshalComplex, objId ObjectId) error {
	objReader, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		return obj.UnmarshalMultiple(objReader, s)
}
func  (s *store) unmarshalSimpleObj(obj UnmarshalSimple, objId ObjectId) error {
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
func (s *store) unmarshalGeneric(obj any, objId ObjectId) error {
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
		return fmt.Errorf("Unsupporeted object type for unmarshalling generically, The type is a %T", v)
	}
}
