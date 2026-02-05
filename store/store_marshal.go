package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cbehopkins/bobbob"
)

var ErrorNoData = fmt.Errorf("no data to unmarshal")

// Type aliases for clarity at call sites (internal types directly)
type MarshalSimple = bobbob.MarshalSimple
type UnmarshalSimple = bobbob.UnmarshalSimple

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
		return bobbob.ObjNotWritten, fmt.Errorf("Object cannot be generically marshalled")
	}
}

// marshalFixedSize marshals a fixed size object
func marshalFixedSize(s Storer, v any) (ObjectId, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, v)
	if err != nil {
		return bobbob.ObjNotWritten, err
	}
	return WriteNewObjFromBytes(s, buf.Bytes())
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
