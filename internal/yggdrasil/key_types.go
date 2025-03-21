package yggdrasil

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/cbehopkins/bobbob/internal/store"
)


type Key interface {
	// Size in bytes is the size in the store of the key. 
	// This should be constant for all objects of the same type.
	SizeInBytes() int
	Equals(Key) bool
}

type PersistentKey interface {
	Key
	New() PersistentKey
	MarshalToObjectId() (store.ObjectId, error)
	UnmarshalFromObjectId(store.ObjectId) error
}

type IntKey int32
func IntLess(a, b any) bool {
	return *a.(*IntKey) < *b.(*IntKey)
}
func (k IntKey) New() PersistentKey {
	v := IntKey(-1)
	return &v
}
func (k IntKey) SizeInBytes() int {
	return 4
}

func (k IntKey) MarshalToObjectId() (store.ObjectId, error) {
	return store.ObjectId(k), nil
}
func (k *IntKey) UnmarshalFromObjectId(id store.ObjectId) error {
	*k = IntKey(id)
	return nil
}
func (k IntKey) Marshal() ([]byte, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(k))
	return buf, nil
}

func (k *IntKey) Unmarshal(data []byte) error {
	if len(data) != 4 {
		return errors.New("invalid data length for MockKey")
	}
	*k = IntKey(binary.LittleEndian.Uint32(data))
	return nil
}

func (k IntKey) Equals(other Key) bool {
	otherKey, ok := other.(*IntKey)
	if !ok {
		return false
	}
	return k == *otherKey
}

type StringKey string

func StringLess(a, b any) bool {
	return *a.(*StringKey) < *b.(*StringKey)
}

func (k StringKey) SizeInBytes() int {
	return store.ObjectId(0).SizeInBytes()
}

func (k StringKey) Marshal() ([]byte, error) {
	return []byte(k), nil
}

func (k *StringKey) Unmarshal(data []byte) error {
	*k = StringKey(data)
	return nil
}

func (k StringKey) Equals(other Key) bool {
	otherKey, ok := other.(*StringKey)
	if !ok {
		return false
	}
	return k == *otherKey
}

// Custom struct for testing

type exampleCustomKey struct {
	ID   int
	Name string
}

func (k exampleCustomKey) SizeInBytes() int {
	return 8
}

func (k exampleCustomKey) Marshal() ([]byte, error) {
	return json.Marshal(k)
}

func (k *exampleCustomKey) Unmarshal(data []byte) error {
	return json.Unmarshal(data, k)
}
func (k exampleCustomKey) Equals(other Key) bool {
	otherKey, ok := other.(*exampleCustomKey)
	if !ok {
		return false
	}
	return k.ID == otherKey.ID && k.Name == otherKey.Name
}

func customKeyLess(a, b any) bool {
	ka := a.(*exampleCustomKey)
	kb := b.(*exampleCustomKey)
	if ka.ID == kb.ID {
		return ka.Name < kb.Name
	}
	return ka.ID < kb.ID
}
