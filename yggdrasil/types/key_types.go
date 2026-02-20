package types

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
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
func (k IntKey) New() PersistentKey[IntKey] {
	v := IntKey(-1)
	return &v
}

// SizeInBytes returns the size of an ObjectId since IntKey fits within it.
func (k IntKey) SizeInBytes() int {
	return bobbob.ObjectId(0).SizeInBytes()
}

// MarshalToObjectId stores the IntKey by casting it to an ObjectId.
// This is a special optimization since IntKey fits in the ObjectId space.
func (k IntKey) MarshalToObjectId(stre bobbob.Storer) (bobbob.ObjectId, error) {
	// Here we cheat because we know IntKey will fit into the ObjectId storage space
	// I do not recommend this trick, but as long as Unmarshal knows about it, it is ok
	return bobbob.ObjectId(k), nil
}
func (k IntKey) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	oid, err := k.MarshalToObjectId(stre)
	if err != nil {
		return 0, 0, func() error { return err }
	}
	return oid, k.SizeInBytes(), nil
}
func (k *IntKey) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error { return k.UnmarshalFromObjectId(id, stre) }
}

// UnmarshalFromObjectId loads the IntKey by casting from an ObjectId.
func (k *IntKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre bobbob.Storer) error {
	*k = IntKey(id)
	return nil
}

// DependentObjectIds returns the list of ObjectIds that this key depends on.
// IntKey stores its value directly in the ObjectId, so it has no dependent allocations.
func (k IntKey) DependentObjectIds() []bobbob.ObjectId {
	return nil
}

// DeleteDependents is a no-op for IntKey since it stores its value directly in the ObjectId.
func (k IntKey) DeleteDependents(stre bobbob.Storer) error {
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
func (k ShortUIntKey) New() PersistentKey[ShortUIntKey] {
	v := ShortUIntKey(0)
	return &v
}

// SizeInBytes returns 2 for the 16-bit key.
func (k ShortUIntKey) SizeInBytes() int {
	return 2
}

// MarshalToObjectId stores the ShortUIntKey in the lower bytes of an ObjectId.
func (k ShortUIntKey) MarshalToObjectId(stre bobbob.Storer) (bobbob.ObjectId, error) {
	// Store as uint16 in ObjectId's lower bytes
	return bobbob.ObjectId(k), nil
}
func (k ShortUIntKey) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	oid, err := k.MarshalToObjectId(stre)
	if err != nil {
		return 0, 0, func() error { return err }
	}
	return oid, k.SizeInBytes(), nil
}

// UnmarshalFromObjectId loads the ShortUIntKey from an ObjectId's lower bytes.
func (k *ShortUIntKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre bobbob.Storer) error {
	*k = ShortUIntKey(uint16(id))
	return nil
}
func (k ShortUIntKey) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error { return k.UnmarshalFromObjectId(id, stre) }
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

// DeleteDependents is a no-op for ShortUIntKey since it stores its value directly in the ObjectId.
func (k ShortUIntKey) DeleteDependents(stre bobbob.Storer) error {
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

// SizeInBytes returns the byte length of the string plus 4 bytes for length prefix.
func (k StringKey) SizeInBytes() int {
	return 4 + len([]byte(k))
}

// Marshal encodes the StringKey to bytes with a length prefix.
func (k StringKey) Marshal() ([]byte, error) {
	s := string(k)
	length := uint32(len(s))
	buf := make([]byte, 4+len(s))
	binary.LittleEndian.PutUint32(buf[0:4], length)
	copy(buf[4:], s)
	return buf, nil
}

// Unmarshal decodes the StringKey from bytes with a length prefix.
func (k *StringKey) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("StringKey data too short: %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	if int(length) > len(data)-4 {
		return fmt.Errorf("StringKey length %d exceeds data size %d", length, len(data)-4)
	}
	*k = StringKey(data[4 : 4+length])
	return nil
}

// Equals reports whether this StringKey equals another.
func (k StringKey) Equals(other StringKey) bool {
	return k == other
}

// New creates a new empty StringKey instance.
func (k StringKey) New() PersistentKey[StringKey] {
	v := StringKey("")
	return &v
}

// MarshalToObjectId stores the StringKey as a new object in the store.
func (k StringKey) MarshalToObjectId(stre bobbob.Storer) (bobbob.ObjectId, error) {
	marshalled, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, marshalled)
}

// LateMarshal stores the StringKey as a new object in the store.
func (k StringKey) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	// Try to use StringStorer interface if available
	if ss, ok := stre.(bobbob.StringStorer); ok {
		value := string(k)
		objId, err := ss.NewStringObj(value)
		if err != nil {
			return objId, 0, func() error { return err }
		}
		return objId, len(value), func() error { return nil }
	}

	// Fallback to generic allocation with length prefix
	marshalled, err := k.Marshal()
	if err != nil {
		return 0, 0, func() error { return err }
	}
	objId, fin := store.LateWriteNewObjFromBytes(stre, marshalled)
	return objId, len(marshalled), fin
}

// UnmarshalFromObjectId loads the StringKey from an object in the store.
// Note: StringKey does NOT implement DependentObjectIds, so it is responsible
// for cleaning up its own backing ObjectId. This cleanup must happen BEFORE
// any new allocation during a subsequent update, but the backing object itself
// is owned by this key and should be managed by higher layers if needed.
func (k *StringKey) UnmarshalFromObjectId(id bobbob.ObjectId, stre bobbob.Storer) error {
	return store.ReadGeneric(stre, k, id)
}
func (k *StringKey) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error {
		if ss, ok := stre.(bobbob.StringStorer); ok {
			value, err := ss.StringFromObjId(id)
			if err != nil {
				return err
			}
			*k = StringKey(value)
			return nil
		}
		return k.UnmarshalFromObjectId(id, stre)
	}
}

// DeleteDependents is a no-op for StringKey.
// StringKey creates a new backing object each time it's marshalled,
// so there's no persistent dependent object to clean up.
func (k StringKey) DeleteDependents(stre bobbob.Storer) error {
	return nil
}

// MD5Key represents a 16-byte MD5 hash that can be used as a treap key.
// It implements PriorityProvider to use the hash value itself as the priority,
// since MD5 hashes are uniformly distributed and make excellent priorities.
type MD5Key [16]byte

// Value returns the MD5Key itself.
func (k MD5Key) Value() MD5Key {
	return k
}

func (k MD5Key) String() string {
	return fmt.Sprintf("%x", k[:])
}

// SizeInBytes returns 16 for the MD5 hash.
func (k MD5Key) SizeInBytes() int {
	return 16
}

// Equals reports whether this MD5Key equals another.
func (k MD5Key) Equals(other MD5Key) bool {
	return k == other
}

// Priority implements PriorityProvider by using the first 4 bytes of the MD5 hash.
// This provides a well-distributed priority without needing random number generation.
func (k MD5Key) Priority() uint32 {
	return binary.LittleEndian.Uint32(k[0:4])
}

// MD5Less is a comparison function for MD5Key values.
func MD5Less(a, b MD5Key) bool {
	for i := range 16 {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// Marshal encodes the MD5Key to bytes.
func (k MD5Key) Marshal() ([]byte, error) {
	return k[:], nil
}

// Unmarshal decodes the MD5Key from bytes.
func (k *MD5Key) Unmarshal(data []byte) error {
	if len(data) != 16 {
		return errors.New("MD5Key must be exactly 16 bytes")
	}
	copy(k[:], data[:16])
	return nil
}

// New creates a new zero-value MD5Key instance.
func (k MD5Key) New() PersistentKey[MD5Key] {
	v := MD5Key{}
	return &v
}

// MarshalToObjectId stores the MD5Key as a new object in the store.
func (k MD5Key) MarshalToObjectId(stre bobbob.Storer) (bobbob.ObjectId, error) {
	marshalled, err := k.Marshal()
	if err != nil {
		return 0, err
	}
	return store.WriteNewObjFromBytes(stre, marshalled)
}
func (k MD5Key) LateMarshal(stre bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	marshalled, err := k.Marshal()
	if err != nil {
		return 0, 0, func() error { return err }
	}
	objId, fin := store.LateWriteNewObjFromBytes(stre, marshalled)
	return objId, len(marshalled), fin

}

// UnmarshalFromObjectId loads the MD5Key from an object in the store.
func (k *MD5Key) UnmarshalFromObjectId(id bobbob.ObjectId, stre bobbob.Storer) error {
	return store.ReadGeneric(stre, k, id)
}
func (k *MD5Key) LateUnmarshal(id bobbob.ObjectId, size int, stre bobbob.Storer) bobbob.Finisher {
	return func() error { return k.UnmarshalFromObjectId(id, stre) }
}

// DeleteDependents is a no-op for MD5Key.
// MD5Key creates a new backing object each time it's marshalled,
// so there's no persistent dependent object to clean up.
func (k MD5Key) DeleteDependents(stre bobbob.Storer) error {
	return nil
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
