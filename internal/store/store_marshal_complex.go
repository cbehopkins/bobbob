package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ObjectAndByteFunc holds an ObjectId and a function that returns a slice of bytes
type ObjectAndByteFunc struct {
	ObjectId ObjectId
	ByteFunc func() ([]byte, error)
}

// AllocateObjects allocates objects in the store and returns a list of ObjectIds
func (s *Store) AllocateObjects(sizes []int) ([]ObjectId, error) {
	s.objectMapLock.Lock()
	defer s.objectMapLock.Unlock()

	var objectIds []ObjectId
	for _, size := range sizes {
		objId, fileOffset, err := s.allocator.Allocate(size)
		if err != nil {
			return nil, err
		}
		s.objectMap[objId] = ObjectInfo{Offset: fileOffset, Size: size}
		objectIds = append(objectIds, objId)
	}

	return objectIds, nil
}

// WriteObjects writes the objects to the store using the provided ObjectAndByteFunc list
func (s *Store) WriteObjects(objects []ObjectAndByteFunc) error {
	// FIXME Can we detect the objects are consecutive and write them in one go?
	for _, obj := range objects {
		data, err := obj.ByteFunc()
		if err != nil {
			return err
		}
		// FIXME can we farm this out into a series of workers?
		n, err := s.file.WriteAt(data, int64(s.objectMap[obj.ObjectId].Offset))
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

// MarshalSimple is an interface for simple types that can be marshalled in one step
type MarshalSimple interface {
	PreMarshal() ([]int, error)
	Marshal() ([]byte, error)
}
// MarshalBasic support is not yet implemented.
// FIXME to make this work we need AllocateObjects to be okay with nil as a size
// And then the allocation to happen on the fly in WriteObjects
type MarshalBasic interface {
	Marshal() ([]byte, error)
}
type UnmarshalSimple interface {
	Unmarshal([]byte) (error)
}
// MarshalComplex is an interface for complex types that need to be marshalled in multiple steps
type MarshalComplex interface {
	PreMarshal() ([]int, error)
	MarshalMultiple([]ObjectId) (func() ObjectId, []ObjectAndByteFunc, error)
}

type UnmarshalComplex interface {
	UnmarshalMultiple(objData io.Reader, reader ObjReader) error
}

// WriteComplexTypes writes complex types to the store
func (s *Store) WriteComplexTypes(obj MarshalComplex) (ObjectId, error) {
	sizes, err := obj.PreMarshal()
	if err != nil {
		return 0, err
	}

	objectIds, err := s.AllocateObjects(sizes)
	if err != nil {
		return 0, err
	}

	identityFunction, objectAndByteFuncs, err := obj.MarshalMultiple(objectIds)
	if err != nil {
		return 0, err
	}

	return identityFunction(), s.WriteObjects(objectAndByteFuncs)
}

func integerSize(obj any) int {
	switch v := obj.(type) {
	case int8:
		return binary.Size(v)
	case uint8:
		return binary.Size(v)
	case int16:
		return binary.Size(v)
	case uint16:
		return binary.Size(v)
	case int32:
		return binary.Size(v)
	case uint32:
		return binary.Size(v)
	case int64:
		return binary.Size(v)
	case uint64:
		return binary.Size(v)
	case int:
		return binary.Size(int64(v))
	case uint:
		return binary.Size(uint64(v))
	default:
		return -1
	}
}
func (s *Store) PreMarshalGeneric(obj any) ([]int, error) {
	switch v := obj.(type) {
	case MarshalComplex:
		return v.PreMarshal()
    case MarshalSimple:
        return v.PreMarshal()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		size := integerSize(v)
		if size < 0 {
			return nil, errors.New("unsupported integer type")
		}
		return []int{size}, nil
	default:
		return nil, fmt.Errorf("object does not support MarshalComplex interface or is not a supported integer type")
	}
}

func (s *Store) MarshalMultipleGeneric(obj any, objectIds []ObjectId) (func() ObjectId, []ObjectAndByteFunc, error) {
	switch v := obj.(type) {
	case MarshalComplex:
		return v.MarshalMultiple(objectIds)
    case MarshalSimple:
        data, err := v.Marshal()
        if err != nil {
            return nil, nil, err
        }
        objIdFunc :=  func() ObjectId { return objectIds[0] }
        objByteFunc := []ObjectAndByteFunc{
            {
                ObjectId: objectIds[0],
                ByteFunc: func() ([]byte, error) {
                    return data, nil
                },
            },
        }   
        return objIdFunc, objByteFunc, nil
	case int:
		return s.marshalFixedSize(int64(v), objectIds)
	case uint:
		return s.marshalFixedSize(uint64(v), objectIds)
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return s.marshalFixedSize(v, objectIds)
	default:
		return nil, nil, fmt.Errorf("object does not support MarshalComplex interface or is not a supported integer type")
	}
}

func (s *Store) marshalFixedSize(v any, objectIds []ObjectId) (func() ObjectId, []ObjectAndByteFunc, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, v)
	if err != nil {
		return nil, nil, err
	}
	objectAndByteFuncs := []ObjectAndByteFunc{
		{
			ObjectId: objectIds[0],
			ByteFunc: func() ([]byte, error) {
				return buf.Bytes(), nil
			},
		},
	}
	return func() ObjectId { return objectIds[0] }, objectAndByteFuncs, nil
}

func (s *Store) UnmarshalMultipleGeneric(obj any, objId ObjectId) error {
	switch v := obj.(type) {
	case UnmarshalComplex:
		objReader, err := s.ReadObj(objId)
		if err != nil {
			return err
		}
		return v.UnmarshalMultiple(objReader, s)
    case UnmarshalSimple:
            objReader, err := s.ReadObj(objId)
            if err != nil {
                return err
            }
            data, err := io.ReadAll(objReader)
            if err != nil {
                return err
            }
            return v.Unmarshal(data)
	case *int:
		var temp int64
		objReader, err := s.ReadObj(objId)
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
		objReader, err := s.ReadObj(objId)
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
		objReader, err := s.ReadObj(objId)
		if err != nil {
			return err
		}
		return binary.Read(objReader, binary.LittleEndian, v)
	default:
		return fmt.Errorf("Unsupporeted object type for unmarshalling")
	}
}
