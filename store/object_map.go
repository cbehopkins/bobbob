package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"slices"
	"sync"

	"github.com/cbehopkins/bobbob/internal"
	"github.com/cbehopkins/bobbob/store/allocator"
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

// ObjectMap maps ObjectIds to ObjectInfo, providing thread-safe access
// to object metadata including file offset and size.
type ObjectMap struct {
	mu    sync.RWMutex
	store map[ObjectId]ObjectInfo
}

// NewObjectMap creates a new empty ObjectMap.
func NewObjectMap() *ObjectMap {
	return &ObjectMap{
		store: make(map[ObjectId]ObjectInfo),
	}
}

// Get retrieves the ObjectInfo for the given ObjectId.
// Returns the info and a boolean indicating whether the object was found.
func (om *ObjectMap) Get(id ObjectId) (ObjectInfo, bool) {
	om.mu.RLock()
	defer om.mu.RUnlock()
	obj, found := om.store[id]
	return obj, found
}

// Set stores the ObjectInfo for the given ObjectId.
func (om *ObjectMap) Set(id ObjectId, info ObjectInfo) {
	om.mu.Lock()
	defer om.mu.Unlock()
	om.store[id] = info
}

// Delete removes the ObjectId from the map.
func (om *ObjectMap) Delete(id ObjectId) {
	om.mu.Lock()
	defer om.mu.Unlock()
	delete(om.store, id)
}

// Len returns the number of objects in the map.
func (om *ObjectMap) Len() int {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return len(om.store)
}

// GetAndDelete atomically retrieves and removes an object.
// The callback is invoked with the ObjectInfo before deletion.
// Returns the info and a boolean indicating whether the object was found.
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

// Serialize encodes the ObjectMap to bytes using gob encoding.
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

// Deserialize decodes the ObjectMap from an io.Reader using gob encoding.
func (om *ObjectMap) Deserialize(r io.Reader) error {
	om.mu.Lock()
	defer om.mu.Unlock()
	decoder := gob.NewDecoder(r)
	return decoder.Decode(&om.store)
}

// Marshal encodes the ObjectMap to bytes (alias for Serialize).
func (om *ObjectMap) Marshal() ([]byte, error) {
	return om.Serialize()
}

// Unmarshal decodes the ObjectMap from bytes by wrapping in a reader.
func (om *ObjectMap) Unmarshal(data []byte) error {
	return om.Deserialize(bytes.NewReader(data))
}

// FindGaps returns a channel that yields gaps (unused regions) in the file.
// The gaps are computed by sorting all object offsets and finding regions
// between consecutive objects.
func (om *ObjectMap) FindGaps() <-chan allocator.Gap {
	gapChan := make(chan allocator.Gap)
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
				gapChan <- allocator.Gap{Start: prevEnd, End: currOffset}
			}
		}
	}()
	return gapChan
}
