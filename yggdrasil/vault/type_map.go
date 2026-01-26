package vault

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// Type Map is a mapping of types to a unique identifier
// The intent is that one can add to the store a mapping of
// What type an object is so that when reading back from the store
// One can know what type to unmarshal into

// A program will first register all the types it will use
// There will be generated a unique identifier for each type
// When a persistent object is stored, is can store this type identifier
// When reading back, the type identifier can be used to determine
// What type to unmarshal into

type ShortCodeType = types.ShortUIntKey

var ShortCodeLess = func(a, b ShortCodeType) bool {
	return a < b
}

type (
	typeIdType []byte
	typeTuple  struct {
		// TypeID is a sha256 hash of the type name
		// Its a way to guarantee uniqueness of the type across builds
		TypeId typeIdType `json:"type_id"`
		// Short code is a small integer that can be used to identify the type
		// This is what will be stored in the persistent treap
		// It will be mapped back to the type name using the TypeMap
		ShortCode ShortCodeType `json:"short_code"`
		// typeRef is a reference to the type itself
		// This is not serialized, its just for convenience
		// to be able to create new instances of the type
		typeRef any
	}
)

// TypeMap maintains a bidirectional mapping between type names and short codes.
// It allows efficient serialization of type information by using compact short codes
// instead of full type names.
type TypeMap struct {
	Types         map[string]typeTuple     `json:"types"`
	ShortMap      map[ShortCodeType]string `json:"short_map"`
	NextShortCode ShortCodeType            `json:"next_short_code"`
}

// NewTypeMap creates a new TypeMap pre-populated with common built-in types.
// This includes basic integer types, strings, and the key types used in yggdrasil.
func NewTypeMap() *TypeMap {
	tmp := &TypeMap{}
	tmp.AddType(tmp)
	tmp.AddType("")
	tmp.AddType(int(0))
	tmp.AddType(int8(0))
	tmp.AddType(int16(0))
	tmp.AddType(uint8(0))
	tmp.AddType(uint16(0))
	tmp.AddType(types.IntKey(0))
	tmp.AddType(types.StringKey(""))
	tmp.AddType(types.ShortUIntKey(0))
	return tmp
}

// AddType registers a new type in the type map with a unique short code.
// If the type is already registered, it updates the type reference.
// Each type is assigned a monotonically increasing short code.
func (tm *TypeMap) AddType(t any) {
	if tm.Types == nil {
		tm.Types = make(map[string]typeTuple)
	}
	if tm.ShortMap == nil {
		tm.ShortMap = make(map[ShortCodeType]string)
	}

	typeName := getTypeName(t)
	// Check if type already exists
	if typeTuple, exists := tm.Types[typeName]; exists {
		// make sure we have a reference to the type in this
		// exact runtime
		typeTuple.typeRef = t
		tm.Types[typeName] = typeTuple
		return
	}
	typeId := hashTypeName(typeName)
	tm.Types[typeName] = typeTuple{
		TypeId:    typeId,
		ShortCode: tm.NextShortCode,
		typeRef:   t,
	}
	tm.ShortMap[tm.NextShortCode] = typeName
	tm.NextShortCode++
}

// GetTypeByName retrieves the type tuple for a given type name.
// Returns the tuple and a boolean indicating whether the type was found.
func (tm TypeMap) GetTypeByName(typeName string) (typeTuple, bool) {
	tuple, exists := tm.Types[typeName]
	return tuple, exists
}

// GetTypeByShortCode returns the typeTuple for a given ShortCodeType
// When objects are stored in the persistent treap they will be stored with a ShortCodeType
// from that one will lookup the type in question
// We will then be able to use typeRef to create a new instance of the type
func (tm TypeMap) GetTypeByShortCode(shortCode ShortCodeType) (typeTuple, bool) {
	typeName, exists := tm.ShortMap[shortCode]
	if !exists {
		return typeTuple{}, false
	}
	return tm.Types[typeName], true
}

//	func hashType(t any) typeIdType {
//		return hashTypeName(getTypeName(t))
//	}
func hashTypeName(typeName string) typeIdType {
	h := sha256.New()
	h.Write([]byte(typeName))
	return h.Sum(nil)
}

func getTypeName(t any) string {
	return fmt.Sprintf("%T", t)
}

// GetShortCode returns the short code for a given type.
// If the type is not registered, it returns an error.
func (tm TypeMap) GetShortCode(t any) (ShortCodeType, error) {
	tuple, exists := tm.Types[getTypeName(t)]
	if !exists {
		return 0, fmt.Errorf("type not found")
	}
	return tuple.ShortCode, nil
}

func (tm TypeMap) Marshal() ([]byte, error) {
	return json.Marshal(tm)
}

func (tm *TypeMap) Unmarshal(data []byte) error {
	other := TypeMap{}
	// Allocated size may be larger than written size; trim trailing zeros for JSON
	data = bytes.TrimRight(data, "\x00")
	err := json.Unmarshal(data, &other)
	if err != nil {
		return err
	}
	return tm.merge(other)
}

// merge integrates types from another TypeMap into this one.
// This is used when loading a TypeMap from disk after local types have been registered.
//
// Behavior:
//   - Types in both maps: overwrites local short code with the persisted one
//   - Types only in 'other': adds them to preserve persisted type registrations
//   - Types only in 'this': keeps them with their current short codes
//   - Validates type IDs match (detects incompatible type definitions)
//   - Updates NextShortCode to prevent collisions
//
// This design ensures persisted short codes remain stable across sessions,
// even if types are registered in a different order.
func (tm *TypeMap) merge(other TypeMap) error {
	// merge a child TypeMap into this one
	// useful when loading in a typemap after we have locally registered types
	// i.e. overwrite the short codes with the ones from the other map
	if tm == nil {
		return errors.New("cannot merge into nil TypeMap")
	}
	if other.Types == nil {
		return nil // nothing to merge
	}
	if tm.Types == nil {
		tm.Types = other.Types
		tm.ShortMap = other.ShortMap
		tm.NextShortCode = other.NextShortCode
		return nil
	}
	for typeName, otherTuple := range other.Types {
		if existingTuple, exists := tm.Types[typeName]; exists {
			if !bytes.Equal(existingTuple.TypeId, otherTuple.TypeId) {
				return fmt.Errorf("type ID mismatch for type %s", typeName)
			}
			// Overwrite the short code with the one from the other map
			// Remove old short code mapping only if it's still pointing to this type
			if existingTypeName, ok := tm.ShortMap[existingTuple.ShortCode]; ok && existingTypeName == typeName {
				delete(tm.ShortMap, existingTuple.ShortCode)
			}
			existingTuple.ShortCode = otherTuple.ShortCode
			tm.Types[typeName] = existingTuple
			tm.ShortMap[otherTuple.ShortCode] = typeName
		} else {
			// Type exists in loaded map but not in current map - add it
			// This preserves types that were registered in the previous session
			tm.Types[typeName] = otherTuple
			tm.ShortMap[otherTuple.ShortCode] = typeName
		}
	}
	// Update NextShortCode to be at least as high as the other map's
	// to prevent short code collisions
	if other.NextShortCode > tm.NextShortCode {
		tm.NextShortCode = other.NextShortCode
	}
	return nil
}
