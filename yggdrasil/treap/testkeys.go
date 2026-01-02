package treap

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cbehopkins/bobbob/store"
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

// MD5Key represents a 16-byte MD5 hash that can be used as a treap key.
// It implements PriorityProvider to use the hash value itself as the priority,
// since MD5 hashes are uniformly distributed and make excellent priorities.
type MD5Key [16]byte

func (k MD5Key) Value() MD5Key            { return k }
func (k MD5Key) SizeInBytes() int         { return 16 }
func (k MD5Key) Equals(other MD5Key) bool { return k == other }

// Priority implements PriorityProvider by using the first 4 bytes of the MD5 hash.
// This provides a well-distributed priority without needing random number generation.
func (k MD5Key) Priority() Priority {
	return Priority(binary.LittleEndian.Uint32(k[0:4]))
}

func MD5Less(a, b MD5Key) bool {
	for i := 0; i < 16; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

func (k MD5Key) Marshal() ([]byte, error) {
	return k[:], nil
}

func (k *MD5Key) Unmarshal(data []byte) error {
	if len(data) != 16 {
		return errors.New("MD5Key must be exactly 16 bytes")
	}
	copy(k[:], data)
	return nil
}

func (k MD5Key) New() PersistentKey[MD5Key] {
	v := MD5Key{}
	return &v
}

// MD5KeyFromString converts a 32-character hex string to a 16-byte MD5Key.
// Returns an error if the string is not exactly 32 characters or contains invalid hex.
func MD5KeyFromString(md5Hex string) (MD5Key, error) {
	var key MD5Key
	if len(md5Hex) != 32 {
		return key, fmt.Errorf("invalid md5 hex string length: got %d, want 32", len(md5Hex))
	}
	for i := range 16 {
		n, err := fmt.Sscanf(md5Hex[i*2:i*2+2], "%02x", &key[i])
		if err != nil {
			return key, fmt.Errorf("failed to parse hex at position %d: %w", i*2, err)
		}
		if n != 1 {
			return key, fmt.Errorf("expected to parse 1 byte at position %d, got %d", i*2, n)
		}
	}
	return key, nil
}

// Md5KeyFromBase64String parses an unpadded base64 MD5 digest into an MD5Key.
func Md5KeyFromBase64String(s string) (MD5Key, error) {
	var key MD5Key

	// RawStdEncoding handles no-padding base64
	n, err := base64.RawStdEncoding.Decode(key[:], []byte(s))
	if err != nil {
		return MD5Key{}, fmt.Errorf("invalid base64 md5 key %q: %w", s, err)
	}
	if n != md5.Size {
		return MD5Key{}, fmt.Errorf("invalid base64 md5 key %q: expected %d bytes, got %d", s, md5.Size, n)
	}
	return key, nil
}

func (k MD5Key) MarshalToObjectId(stre store.Storer) (store.ObjectId, error) {
	marshalled, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, marshalled)
}

func (k *MD5Key) UnmarshalFromObjectId(id store.ObjectId, stre store.Storer) error {
	return store.ReadGeneric(stre, k, id)
}
