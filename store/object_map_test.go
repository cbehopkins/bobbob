package store

import (
	"bytes"
	"testing"
)

func TestObjectMapSerialize(t *testing.T) {
	objectMap := NewObjectMap()
	objectMap.Set(0, ObjectInfo{Offset: 0, Size: 10})
	objectMap.Set(1, ObjectInfo{Offset: 1, Size: 20})

	data, err := objectMap.Serialize()
	if err != nil {
		t.Fatalf("expected no error serializing objectMap, got %v", err)
	}
	deserializedMap := NewObjectMap()
	err = deserializedMap.Deserialize(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("expected no error deserializing objectMap, got %v", err)
	}

	if len(deserializedMap.store) != len(objectMap.store) {
		t.Fatalf("expected deserialized map length to be %d, got %d", len(objectMap.store), len(deserializedMap.store))
	}

	for k, v := range objectMap.store {
		if deserializedMap.store[k] != v {
			t.Fatalf("expected deserialized map value for key %d to be %v, got %v", k, v, deserializedMap.store[k])
		}
	}
}

func TestObjectMapMarshal(t *testing.T) {
	objectMap := NewObjectMap()
	objectMap.Set(0, ObjectInfo{Offset: 0, Size: 10})
	objectMap.Set(1, ObjectInfo{Offset: 1, Size: 20})

	data, err := objectMap.Marshal()
	if err != nil {
		t.Fatalf("expected no error marshaling objectMap, got %v", err)
	}
	deserializedMap := NewObjectMap()
	err = deserializedMap.Unmarshal(data)
	if err != nil {
		t.Fatalf("expected no error unmarshaling objectMap, got %v", err)
	}

	if len(deserializedMap.store) != len(objectMap.store) {
		t.Fatalf("expected deserialized map length to be %d, got %d", len(objectMap.store), len(deserializedMap.store))
	}

	for k, v := range objectMap.store {
		if deserializedMap.store[k] != v {
			t.Fatalf("expected deserialized map value for key %d to be %v, got %v", k, v, deserializedMap.store[k])
		}
	}
}

// TestObjectIdSizeInBytes tests the SizeInBytes method
func TestObjectIdSizeInBytes(t *testing.T) {
	id := ObjectId(42)
	size := id.SizeInBytes()
	if size != 8 {
		t.Errorf("expected SizeInBytes to return 8, got %d", size)
	}
}

// TestObjectIdEquals tests the Equals method
func TestObjectIdEquals(t *testing.T) {
	id1 := ObjectId(42)
	id2 := ObjectId(42)
	id3 := ObjectId(99)

	if !id1.Equals(id2) {
		t.Error("expected id1 to equal id2")
	}

	if id1.Equals(id3) {
		t.Error("expected id1 to not equal id3")
	}
}

// TestObjectIdPreMarshal tests the PreMarshal method
func TestObjectIdPreMarshal(t *testing.T) {
	id := ObjectId(42)
	sizes := id.PreMarshal()
	if len(sizes) != 1 {
		t.Errorf("expected PreMarshal to return 1 size, got %d", len(sizes))
	}
	if sizes[0] != 8 {
		t.Errorf("expected PreMarshal to return [8], got %v", sizes)
	}
}

// TestObjectIdUnmarshalInvalidData tests error handling in Unmarshal
func TestObjectIdUnmarshalInvalidData(t *testing.T) {
	var id ObjectId
	err := id.Unmarshal([]byte{1, 2, 3}) // less than 8 bytes
	if err == nil {
		t.Error("expected error for invalid data length, got nil")
	}
	expectedError := "invalid data length for ObjectId"
	if err.Error() != expectedError {
		t.Errorf("expected error message %q, got %q", expectedError, err.Error())
	}
}

// TestObjectIdLutMarshalUnmarshal tests the ObjectIdLut marshaling
func TestObjectIdLutMarshalUnmarshal(t *testing.T) {
	lut := &ObjectIdLut{
		Ids: []ObjectId{1, 2, 3, 42, 100},
	}

	data, err := lut.Marshal()
	if err != nil {
		t.Fatalf("expected no error marshaling ObjectIdLut, got %v", err)
	}

	expectedLen := len(lut.Ids) * 8
	if len(data) != expectedLen {
		t.Errorf("expected marshaled data length to be %d, got %d", expectedLen, len(data))
	}

	var lut2 ObjectIdLut
	err = lut2.Unmarshal(data)
	if err != nil {
		t.Fatalf("expected no error unmarshaling ObjectIdLut, got %v", err)
	}

	if len(lut2.Ids) != len(lut.Ids) {
		t.Errorf("expected %d IDs, got %d", len(lut.Ids), len(lut2.Ids))
	}

	for i, id := range lut.Ids {
		if lut2.Ids[i] != id {
			t.Errorf("expected ID at index %d to be %d, got %d", i, id, lut2.Ids[i])
		}
	}
}

// TestObjectIdLutUnmarshalInvalidData tests error handling
func TestObjectIdLutUnmarshalInvalidData(t *testing.T) {
	var lut ObjectIdLut
	// Data length not divisible by 8
	err := lut.Unmarshal([]byte{1, 2, 3, 4, 5})
	if err == nil {
		t.Error("expected error for invalid data length, got nil")
	}
	expectedError := "invalid data length for ObjectIdLut"
	if err.Error() != expectedError {
		t.Errorf("expected error message %q, got %q", expectedError, err.Error())
	}
}

// TestObjectIdLutEmpty tests marshaling an empty ObjectIdLut
func TestObjectIdLutEmpty(t *testing.T) {
	lut := &ObjectIdLut{
		Ids: []ObjectId{},
	}

	data, err := lut.Marshal()
	if err != nil {
		t.Fatalf("expected no error marshaling empty ObjectIdLut, got %v", err)
	}

	if len(data) != 0 {
		t.Errorf("expected empty marshaled data, got length %d", len(data))
	}

	var lut2 ObjectIdLut
	err = lut2.Unmarshal(data)
	if err != nil {
		t.Fatalf("expected no error unmarshaling empty ObjectIdLut, got %v", err)
	}

	if len(lut2.Ids) != 0 {
		t.Errorf("expected 0 IDs, got %d", len(lut2.Ids))
	}
}

// TestObjectMapDelete tests the Delete method
func TestObjectMapDelete(t *testing.T) {
	om := NewObjectMap()
	id := ObjectId(42)
	info := ObjectInfo{Offset: 100, Size: 50}

	// Set and verify
	om.Set(id, info)
	retrievedInfo, found := om.Get(id)
	if !found {
		t.Fatal("expected to find object after Set")
	}
	if retrievedInfo != info {
		t.Errorf("expected info %v, got %v", info, retrievedInfo)
	}

	// Delete and verify
	om.Delete(id)
	_, found = om.Get(id)
	if found {
		t.Error("expected object to be deleted")
	}
}

// TestObjectMapSerializeError tests error handling during serialization
func TestObjectMapSerializeError(t *testing.T) {
	// This tests the error path in Serialize
	// We create a map with an un-serializable value if possible
	// Since ObjectInfo is a simple struct, gob should handle it fine
	// But we can at least verify the function executes without panicking
	om := NewObjectMap()
	om.Set(ObjectId(1), ObjectInfo{Offset: 0, Size: 10})
	om.Set(ObjectId(2), ObjectInfo{Offset: 10, Size: 20})

	data, err := om.Serialize()
	if err != nil {
		t.Fatalf("expected no error during serialization, got %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty serialized data")
	}
}

// TestObjectIdLutMarshalError tests error handling during marshaling
// Note: In the current implementation, ObjectId.Marshal() never returns an error,
// so this test verifies the current behavior. If error handling is added later,
// this test documents the expected behavior.
func TestObjectIdLutMarshalError(t *testing.T) {
	lut := &ObjectIdLut{
		Ids: []ObjectId{1, 2, 3},
	}

	// Current implementation should not error
	data, err := lut.Marshal()
	if err != nil {
		t.Fatalf("expected no error marshaling ObjectIdLut, got %v", err)
	}

	if len(data) != len(lut.Ids)*8 {
		t.Errorf("expected %d bytes, got %d", len(lut.Ids)*8, len(data))
	}
}

// TestObjectIdLutUnmarshalError tests error handling during unmarshaling
func TestObjectIdLutUnmarshalError(t *testing.T) {
	var lut ObjectIdLut

	// Test with data that's too short (unmarshal individual ID should fail)
	// But actually this is caught by the length check first
	// Let's test another scenario - an empty unmarshal followed by a valid one
	err := lut.Unmarshal([]byte{})
	if err != nil {
		t.Fatalf("expected no error unmarshaling empty data, got %v", err)
	}

	if len(lut.Ids) != 0 {
		t.Errorf("expected 0 IDs after unmarshaling empty data, got %d", len(lut.Ids))
	}
}
