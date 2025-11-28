package treap

import (
	"encoding/binary"
	"encoding/json"

	"bobbob/internal/store"
)

// Test helper types - these are simple implementations for testing purposes
// Production code should use types from the types package

// IntKey is a simple integer key for testing
type IntKey int32

func (k IntKey) Equals(other IntKey) bool { return k == other }
func (k IntKey) Value() IntKey            { return k }
func (k IntKey) SizeInBytes() int         { return store.ObjectId(0).SizeInBytes() }

func IntLess(a, b IntKey) bool { return a < b }

func (k IntKey) New() PersistentKey[IntKey] {
	v := IntKey(-1)
	return &v
}

func (k IntKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
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
	*k = IntKey(binary.LittleEndian.Uint32(data))
	return nil
}

// exampleCustomKey is a custom test key type
type exampleCustomKey struct {
	ID   int
	Name string
}

func (k exampleCustomKey) Value() exampleCustomKey { return k }
func (k exampleCustomKey) SizeInBytes() int        { return 8 }

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
	if a.ID == b.ID {
		return a.Name < b.Name
	}
	return a.ID < b.ID
}

// StringKey for testing
type StringKey string

func (k StringKey) Value() StringKey            { return k }
func (k StringKey) SizeInBytes() int            { return len([]byte(k)) }
func (k StringKey) Equals(other StringKey) bool { return k == other }

func StringLess(a, b StringKey) bool { return a < b }

func (k StringKey) Marshal() ([]byte, error) {
	return []byte(k), nil
}

func (k *StringKey) Unmarshal(data []byte) error {
	*k = StringKey(data)
	return nil
}

func (k StringKey) New() PersistentKey[StringKey] {
	v := StringKey("")
	return &v
}

func (k StringKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	marshalled, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, marshalled)
}

func (k *StringKey) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}
