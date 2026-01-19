package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cbehopkins/bobbob/internal"
)

var ErrorNoData = fmt.Errorf("no data to unmarshal")

// Type aliases for clarity at call sites (internal types directly)
type MarshalSimple = internal.MarshalSimple
type UnmarshalSimple = internal.UnmarshalSimple
type MarshalComplex = internal.MarshalComplex
type UnmarshalComplex = internal.UnmarshalComplex
type ObjectAndByteFunc = internal.ObjectAndByteFunc

// allocateObjects allocates objects in the store and returns a list of ObjectIds
func allocateObjects(s Storer, sizes []int) ([]ObjectId, error) {
	if sizes == nil {
		return []ObjectId{objNotPreAllocated}, nil
	}

	var objectIds []ObjectId
	for _, size := range sizes {
		if size < 0 {
			objectIds = append(objectIds, objNotPreAllocated)
			continue
		}
		objId, err := s.NewObj(size)
		if err != nil {
			return nil, err
		}
		objectIds = append(objectIds, objId)
	}

	return objectIds, nil
}

// writeObjects writes the objects to the store using the provided ObjectAndByteFunc list
func writeObjects(s Storer, objects []ObjectAndByteFunc) error {
	if len(objects) == 0 {
		return nil
	}

	// Detect consecutive object groups and use batched writes when possible
	groups := detectConsecutiveObjectGroups(s, objects)

	for _, group := range groups {
		if len(group) > 1 {
			// Multiple consecutive objects - use batched write
			if err := writeBatchedGroup(s, group); err != nil {
				return err
			}
		} else {
			// Single object - use regular write
			if err := writeSingleObject(s, group[0]); err != nil {
				return err
			}
		}
	}

	return nil
}

// detectConsecutiveObjectGroups groups objects by whether they are consecutive in the file
func detectConsecutiveObjectGroups(s Storer, objects []ObjectAndByteFunc) [][]ObjectAndByteFunc {
	if len(objects) == 0 {
		return nil
	}

	// We need access to objectInfo to check consecutiveness
	// Define an interface for stores that support this optimization
	type objectInfoGetter interface {
		GetObjectInfo(ObjectId) (ObjectInfo, bool)
	}

	// Check if the store supports getting object info
	getter, ok := s.(objectInfoGetter)
	if !ok {
		// Store doesn't support optimization, treat as single group
		return [][]ObjectAndByteFunc{objects}
	}

	var groups [][]ObjectAndByteFunc
	currentGroup := []ObjectAndByteFunc{objects[0]}

	for i := 1; i < len(objects); i++ {
		prevObj, prevFound := getter.GetObjectInfo(objects[i-1].ObjectId)
		currObj, currFound := getter.GetObjectInfo(objects[i].ObjectId)

		if !prevFound || !currFound {
			// Can't determine consecutiveness, start new group
			groups = append(groups, currentGroup)
			currentGroup = []ObjectAndByteFunc{objects[i]}
			continue
		}

		// Check if current object immediately follows previous object
		expectedOffset := prevObj.Offset + FileOffset(prevObj.Size)
		if currObj.Offset == expectedOffset {
			// Consecutive - add to current group
			currentGroup = append(currentGroup, objects[i])
		} else {
			// Not consecutive - start new group
			groups = append(groups, currentGroup)
			currentGroup = []ObjectAndByteFunc{objects[i]}
		}
	}

	// Add the last group
	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

// writeBatchedGroup writes a group of consecutive objects using WriteBatchedObjs
// Only batches if all objects are fully filled (no gaps)
func writeBatchedGroup(s Storer, group []ObjectAndByteFunc) error {
	// Get object info to check if objects are fully filled
	type objectInfoGetter interface {
		GetObjectInfo(ObjectId) (ObjectInfo, bool)
	}

	getter, ok := s.(objectInfoGetter)
	if !ok {
		// Can't optimize, write individually
		for _, obj := range group {
			if err := writeSingleObject(s, obj); err != nil {
				return err
			}
		}
		return nil
	}

	// Collect all data, sizes, and object IDs
	// Also check if all objects will be fully filled
	var allData []byte
	sizes := make([]int, len(group))
	objIds := make([]ObjectId, len(group))
	canBatch := true

	for i, obj := range group {
		data, err := obj.ByteFunc()
		if err != nil {
			return err
		}

		// Check if this object will be fully filled
		objInfo, found := getter.GetObjectInfo(obj.ObjectId)
		if !found || objInfo.Size != len(data) {
			// Object not fully filled, can't batch
			canBatch = false
		}

		allData = append(allData, data...)
		sizes[i] = len(data)
		objIds[i] = obj.ObjectId
	}

	// If we can batch (all objects fully filled), use batched write
	if canBatch && len(group) > 1 {
		return s.WriteBatchedObjs(objIds, allData, sizes)
	}

	// Otherwise write individually
	for _, obj := range group {
		if err := writeSingleObject(s, obj); err != nil {
			return err
		}
	}
	return nil
}

// writeSingleObject writes a single object using the standard method
func writeSingleObject(s Storer, obj ObjectAndByteFunc) error {
	data, err := obj.ByteFunc()
	if err != nil {
		return err
	}
	objId := obj.ObjectId
	if objId == objNotPreAllocated {
		_, err := WriteNewObjFromBytes(s, data)
		return err
	}
	return WriteBytesToObj(s, data, objId)
}

// writeComplexTypes writes complex types to the store
func writeComplexTypes(s Storer, obj MarshalComplex) (ObjectId, error) {
	sizes, err := obj.PreMarshal()
	if err != nil {
		return ObjNotAllocated, err
	}

	objectIds, err := allocateObjects(s, sizes)
	if err != nil {
		return ObjNotAllocated, err
	}

	identityFunction, objectAndByteFuncs, err := obj.MarshalMultiple(objectIds)
	if err != nil {
		return ObjNotWritten, err
	}

	return identityFunction(), writeObjects(s, objectAndByteFuncs)
}

// marshalGeneric marshals a generic object
func marshalGeneric(s Storer, obj any) (ObjectId, error) {
	switch v := obj.(type) {
	case int:
		return marshalFixedSize(s, int64(v))
	case uint:
		return marshalFixedSize(s, uint64(v))
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return marshalFixedSize(s, v)
	default:
		return ObjNotWritten, fmt.Errorf("Object cannot be generically marshalled")
	}
}

// marshalFixedSize marshals a fixed size object
func marshalFixedSize(s Storer, v any) (ObjectId, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, v)
	if err != nil {
		return ObjNotWritten, err
	}
	return WriteNewObjFromBytes(s, buf.Bytes())
}

// unmarshalComplexObj unmarshals a complex object
func unmarshalComplexObj(s Storer, obj UnmarshalComplex, objId ObjectId) error {
	objReader, finisher, err := s.LateReadObj(objId)
	if err != nil {
		return err
	}
	defer func() {
		if err := finisher(); err != nil {
			// Log error but continue - unmarshaling may still succeed
		}
	}()
	return obj.UnmarshalMultiple(objReader, s)
}

// unmarshalSimpleObj unmarshals a simple object
func unmarshalSimpleObj(s Storer, obj UnmarshalSimple, objId ObjectId) error {
	data, err := ReadBytesFromObj(s, objId)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return ErrorNoData
	}
	return obj.Unmarshal(data)
}

// unmarshalGeneric unmarshals a generic object
func unmarshalGeneric(s Storer, obj any, objId ObjectId) error {
	switch v := obj.(type) {
	case *int:
		var temp int64
		objReader, finisher, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		err = finisher()
		if err != nil {
			return err
		}
		err = binary.Read(objReader, binary.LittleEndian, &temp)
		if err != nil {
			return err
		}
		*v = int(temp)
		return nil
	case *uint:
		var temp uint64
		objReader, finisher, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		err = binary.Read(objReader, binary.LittleEndian, &temp)
		if err != nil {
			return err
		}
		err = finisher()
		if err != nil {
			return err
		}
		*v = uint(temp)
		return nil
	case *int8, *int16, *int32, *int64, *uint8, *uint16, *uint32, *uint64:
		objReader, finisher, err := s.LateReadObj(objId)
		if err != nil {
			return err
		}
		err = finisher()
		if err != nil {
			return err
		}
		return binary.Read(objReader, binary.LittleEndian, v)
	default:
		return fmt.Errorf("Unsupported object type for unmarshalling generically, The type is a %T", v)
	}
}
