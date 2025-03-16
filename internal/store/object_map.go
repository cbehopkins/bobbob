package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"slices"
	"sync"
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

type ObjectMap struct {
	mu    sync.RWMutex
	store map[ObjectId]ObjectInfo
}

func NewObjectMap() *ObjectMap {
	return &ObjectMap{
		store: make(map[ObjectId]ObjectInfo),
	}
}

func (om *ObjectMap) Get(id ObjectId) (ObjectInfo, bool) {
	om.mu.RLock()
	defer om.mu.RUnlock()
	obj, found := om.store[id]
	return obj, found
}

func (om *ObjectMap) Set(id ObjectId, info ObjectInfo) {
	om.mu.Lock()
	defer om.mu.Unlock()
	om.store[id] = info
}

func (om *ObjectMap) Delete(id ObjectId) {
	om.mu.Lock()
	defer om.mu.Unlock()
	delete(om.store, id)
}

func (om *ObjectMap) GetAndDelete(id ObjectId, callback func(ObjectInfo)) (ObjectInfo, bool) {
	om.mu.Lock()
	defer om.mu.Unlock()
	obj, found := om.store[id]
	if found {
		callback(obj)
		delete(om.store, id)
	}
	return obj, found
}

func (om *ObjectMap) Serialize() ([]byte, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(om.store)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (om *ObjectMap) Deserialize(data []byte) error {
	om.mu.Lock()
	defer om.mu.Unlock()
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(&om.store)
}

func (om *ObjectMap) Marshal() ([]byte, error) {
	return om.Serialize()
}

func (om *ObjectMap) Unmarshal(data []byte) error {
	return om.Deserialize(data)
}

func (om *ObjectMap) FindGaps() <-chan Gap {
	gapChan := make(chan Gap)
	go func() {
		defer close(gapChan)
		om.mu.RLock()
		defer om.mu.RUnlock()
		var offsets []int64
		for _, obj := range om.store {
			offsets = append(offsets, int64(obj.Offset))
		}
		slices.Sort(offsets)
		for i := 1; i < len(offsets); i++ {
			prevObj := om.store[ObjectId(offsets[i-1])]
			currOffset := offsets[i]
			prevEnd := int64(prevObj.Offset) + int64(prevObj.Size)
			if currOffset > prevEnd {
				gapChan <- Gap{Start: prevEnd, End: currOffset}
			}
		}
	}()
	return gapChan
}
