package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
)

type ObjectId int64

type FileOffset int64

func (id ObjectId) SizeInBytes() int {
	return 8
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

// PreMarshal specifies how many objects of how many bytes are
// needed to store the ObjectId
func (id ObjectId) PreMarshal() []int {
	return []int{8}
}

// ObjectIdLut represents a lookup table of ObjectIds
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

type ObjectMap map[ObjectId]ObjectInfo

func (om ObjectMap) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(om)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (om *ObjectMap) Deserialize(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(om)
}

// Marshal serializes the ObjectMap into a byte slice.
func Marshal(v any) ([]byte, error) {
	if om, ok := v.(ObjectMap); ok {
		return om.Serialize()
	}
	return nil, errors.New("unsupported type")
}

// Unmarshal deserializes the byte slice into the ObjectMap.
func Unmarshal(data []byte, v any) error {
	if om, ok := v.(*ObjectMap); ok {
		return om.Deserialize(data)
	}
	return errors.New("unsupported type")
}
