package yggdrasil

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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

type ShortCodeType = ShortUIntKey

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

type TypeMap struct {
	Types         map[string]typeTuple     `json:"types"`
	ShortMap      map[ShortCodeType]string `json:"short_map"`
	NextShortCode ShortCodeType            `json:"next_short_code"`
}

func NewTypeMap() *TypeMap {
	tmp := &TypeMap{}
	tmp.AddType(tmp)
	tmp.AddType("")
	tmp.AddType(int(0))
	tmp.AddType(int8(0))
	tmp.AddType(int16(0))
	tmp.AddType(uint8(0))
	tmp.AddType(uint16(0))
	tmp.AddType(IntKey(0))
	tmp.AddType(StringKey(""))
	tmp.AddType(ShortUIntKey(0))
	return tmp
}

// AddType adds a new type to the type map
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

func (tm TypeMap) getShortCode(t any) (ShortCodeType, error) {
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
	err := json.Unmarshal(data, &other)
	if err != nil {
		return err
	}
	return tm.merge(other)
}

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
			existingTuple.ShortCode = otherTuple.ShortCode
			tm.Types[typeName] = existingTuple
			tm.ShortMap[otherTuple.ShortCode] = typeName
		}
	}
	return nil
}
