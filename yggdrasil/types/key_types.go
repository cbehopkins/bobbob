package types

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/treap"
)

// IntKey is a 32-bit integer key for use in treap data structures.
// It implements both Key and PersistentKey interfaces.
type IntKey int32

// Equals reports whether this IntKey equals another.
func (k IntKey) Equals(other IntKey) bool {
	return k == other
}

// Value returns the underlying int32 value.
func (k IntKey) Value() IntKey {
	return k
}

// IntLess is a comparison function for IntKey values.
func IntLess(a, b IntKey) bool {
	return a < b
}

// New creates a new IntKey instance initialized to -1.
func (k IntKey) New() treap.PersistentKey[IntKey] {
	v := IntKey(-1)
	return &v
}

// SizeInBytes returns the size of an ObjectId since IntKey fits within it.
func (k IntKey) SizeInBytes() int {
	return store.ObjectId(0).SizeInBytes()
}

// MarshalToObjectId stores the IntKey by casting it to an ObjectId.
// This is a special optimization since IntKey fits in the ObjectId space.
func (k IntKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	// Here we cheat because we know IntKey will fit into the ObjectId storage space
	// I do not recommend this trick, but as long as Unmarshal knows about it, it is ok
	return store.ObjectId(k), nil
}

// UnmarshalFromObjectId loads the IntKey by casting from an ObjectId.
func (k *IntKey) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	*k = IntKey(id)
	return nil
}

// Marshal encodes the IntKey to a 4-byte little-endian representation.
func (k IntKey) Marshal() ([]byte, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(k))
	return buf, nil
}

// Unmarshal decodes the IntKey from a 4-byte little-endian representation.
func (k *IntKey) Unmarshal(data []byte) error {
	if len(data) != 4 {
		return errors.New("invalid data length for MockKey")
	}
	*k = IntKey(binary.LittleEndian.Uint32(data))
	return nil
}

// ShortUIntKey is a 16-bit unsigned integer key for use in treap data structures.
// It implements both Key and PersistentKey interfaces, similar to IntKey but smaller.
type ShortUIntKey uint16

// Equals reports whether this ShortUIntKey equals another.
func (k ShortUIntKey) Equals(other ShortUIntKey) bool {
	return k == other
}

// Value returns the underlying uint16 value.
func (k ShortUIntKey) Value() ShortUIntKey {
	return k
}

// New creates a new ShortUIntKey instance initialized to 0.
func (k ShortUIntKey) New() treap.PersistentKey[ShortUIntKey] {
	v := ShortUIntKey(0)
	return &v
}

// SizeInBytes returns 2 for the 16-bit key.
func (k ShortUIntKey) SizeInBytes() int {
	return 2
}

// MarshalToObjectId stores the ShortUIntKey in the lower bytes of an ObjectId.
func (k ShortUIntKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	// Store as uint16 in ObjectId's lower bytes
	return store.ObjectId(k), nil
}

// UnmarshalFromObjectId loads the ShortUIntKey from an ObjectId's lower bytes.
func (k *ShortUIntKey) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	*k = ShortUIntKey(uint16(id))
	return nil
}

// Marshal encodes the ShortUIntKey to a 2-byte little-endian representation.
func (k ShortUIntKey) Marshal() ([]byte, error) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(k))
	return buf, nil
}

// Unmarshal decodes the ShortUIntKey from a 2-byte little-endian representation.
func (k *ShortUIntKey) Unmarshal(data []byte) error {
	if len(data) != 2 {
		return errors.New("invalid data length for ShortUIntKey")
	}
	*k = ShortUIntKey(binary.LittleEndian.Uint16(data))
	return nil
}

// StringKey is a string-based key for use in treap data structures.
// It implements both Key and PersistentKey interfaces.
type StringKey string

// Value returns the underlying string value.
func (k StringKey) Value() StringKey {
	return k
}

// StringLess is a comparison function for StringKey values.
func StringLess(a, b StringKey) bool {
	return a < b
}

// SizeInBytes returns the byte length of the string.
func (k StringKey) SizeInBytes() int {
	return len([]byte(k))
}

// Marshal encodes the StringKey to bytes.
func (k StringKey) Marshal() ([]byte, error) {
	return []byte(k), nil
}

// Unmarshal decodes the StringKey from bytes.
func (k *StringKey) Unmarshal(data []byte) error {
	*k = StringKey(data)
	return nil
}

// Equals reports whether this StringKey equals another.
func (k StringKey) Equals(other StringKey) bool {
	return k == other
}

// New creates a new empty StringKey instance.
func (k StringKey) New() treap.PersistentKey[StringKey] {
	v := StringKey("")
	return &v
}

// MarshalToObjectId stores the StringKey as a new object in the store.
func (k StringKey) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	marshalled, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, marshalled)
}

// UnmarshalFromObjectId loads the StringKey from an object in the store.
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
