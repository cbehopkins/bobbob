package yggdrasil

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"bobbob/internal/store"
)

type Key[T any] interface {
	// Size in bytes is the size in the store of the key.
	// This should be constant for all objects of the same type.
	SizeInBytes() int
	Equals(T) bool
	Value() T
}

type PersistentKey[T any] interface {
	Key[T]
	New() PersistentKey[T]
	MarshalToObjectId(store.Storer) (store.ObjectId, error)
	UnmarshalFromObjectId(store.ObjectId, store.Storer) error
}

type IntKey int32

func (k IntKey) Equals(other IntKey) bool {
	return k == other
}

func (k IntKey) Value() IntKey {
	return k
}

func IntLess(a, b IntKey) bool {
	return a < b
}

func (k IntKey) New() PersistentKey[IntKey] {
	v := IntKey(-1)
	return &v
}

func (k IntKey) SizeInBytes() int {
	return store.ObjectId(0).SizeInBytes()
}

func (k IntKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	// Here we cheat because we know IntKey will fit into the ObjectId storage space
	// I do not reccomend this trick, but as long as Unmarshal knows about it, it is ok
	return store.ObjectId(k), nil
}

func (k *IntKey) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
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

type StringKey string

func (k StringKey) Value() StringKey {
	return k
}

func StringLess(a, b StringKey) bool {
	return a < b
}

func (k StringKey) SizeInBytes() int {
	return len([]byte(k))
}

func (k StringKey) Marshal() ([]byte, error) {
	return []byte(k), nil
}

func (k *StringKey) Unmarshal(data []byte) error {
	*k = StringKey(data)
	return nil
}

func (k StringKey) Equals(other StringKey) bool {
	return k == other
}

func (k StringKey) New() PersistentKey[StringKey] {
	v := StringKey("")
	return &v
}

func (k StringKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	marshalled, _ := k.Marshal()
	return store.WriteNewObjFromBytes(stre, marshalled)
}

func (k *StringKey) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}

// Custom struct for testing
type exampleCustomKey struct {
	ID   int
	Name string
}

func (k exampleCustomKey) Value() exampleCustomKey {
	return k
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

func (k exampleCustomKey) Equals(other exampleCustomKey) bool {
	return k.ID == other.ID && k.Name == other.Name
}

func customKeyLess(a, b exampleCustomKey) bool {
	ka := a
	kb := b
	if ka.ID == kb.ID {
		return ka.Name < kb.Name
	}
	return ka.ID < kb.ID
}
