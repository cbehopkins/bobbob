package store

import (
	"bytes"
	"errors"

	"github.com/cbehopkins/bobbob/internal"
)

// ObjectId is an identifier unique within the store for an object.
// A user refers to an object by its ObjectId.
type ObjectId = internal.ObjectId

// FileOffset represents a byte offset within a file.
type FileOffset = internal.FileOffset

// ObjectIdLut represents a lookup table of ObjectIds.
// It provides marshaling support for a slice of ObjectIds.
type ObjectIdLut struct {
	Ids []ObjectId
}

// Marshal converts the ObjectIdLut into a fixed length bytes encoding
func (lut *ObjectIdLut) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, id := range lut.Ids {
		idBytes, err := id.Marshal()
		if err != nil {
			return nil, err
		}
		buf.Write(idBytes)
	}
	return buf.Bytes(), nil
}

// Unmarshal converts a fixed length bytes encoding into an ObjectIdLut
func (lut *ObjectIdLut) Unmarshal(data []byte) error {
	if len(data)%8 != 0 {
		return errors.New("invalid data length for ObjectIdLut")
	}
	lut.Ids = make([]ObjectId, len(data)/8)
	for i := range lut.Ids {
		if err := lut.Ids[i].Unmarshal(data[i*8 : (i+1)*8]); err != nil {
			return err
		}
	}
	return nil
}
