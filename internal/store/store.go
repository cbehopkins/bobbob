package store

import (
	"errors"
	"io"
)

var ObjNotWritten = ObjectId(-1)
var ObjNotAllocated = ObjectId(-2)
var objNotPreAllocated = ObjectId(-3)
func IsValidObjectId(objId ObjectId) bool {
	// Note object 0 is reserved for a store's internal use
	// usually this will be used as some sort of config table
	// for that is in the store
	return objId > 0
}
// Finisher is a callback that must be called when you have finished
// With the request in question
type Finisher func() error

// ObjReader is an interface for getting an io.Reader for an object
type ObjReader interface {
	LateReadObj(id ObjectId) (io.Reader, Finisher, error)
}
type ObjWriter interface {
	LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error)
	WriteToObj(objectId ObjectId) (io.Writer, Finisher, error)
}
type Storer interface {
	NewObj(size int) (ObjectId, error)
	DeleteObj(objId ObjectId) error
	Close() error
	ObjReader
	ObjWriter
}


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

// WriteBytesToObj writes a byte slice to an existing object in the store
// It is preferred to create a new object, write to it and then delete the old object
// This allows for less risk of file corruption
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
