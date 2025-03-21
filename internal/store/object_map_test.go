package store

import (
	"fmt"
	"os"
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
	err = deserializedMap.Deserialize(data)
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

func TestFindGapsAfterDeletions(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)

	// Write multiple objects
	data1 := []byte("object1")
	objId1 := createObject(t, store, data1)
	data2 := []byte("object2")
	objId2 := createObject(t, store, data2)
	data3 := []byte("object3")
	objId3 := createObject(t, store, data3)
	data4 := []byte("object4")
	objId4 := createObject(t, store, data4)

	// Delete the first and third objects
	fmt.Println("Deleting object 1 and 3", objId1, objId3)
	err := store.DeleteObj(ObjectId(objId1))
	if err != nil {
		t.Fatalf("expected no error deleting object, got %v", err)
	}

	err = store.DeleteObj(ObjectId(objId3))
	if err != nil {
		t.Fatalf("expected no error deleting object, got %v", err)
	}

	// Close and re-open the store
	err = store.Close()
	if err != nil {
		t.Fatalf("expected no error closing store, got %v", err)
	}

	store, err = LoadBaseStore(store.filePath)
	if err != nil {
		t.Fatalf("expected no error loading store, got %v", err)
	}
	defer store.Close()

	// Find gaps
	gapChan := store.objectMap.FindGaps()
	var gaps []Gap
	for gap := range gapChan {
		gaps = append(gaps, gap)
	}

	// Verify the gaps
	expectedGaps := []Gap{
		{Start: int64(objId1), End: int64(objId2)},
		{Start: int64(objId3), End: int64(objId4)},
	}

	if len(gaps) != len(expectedGaps) {
		t.Fatalf("expected %d gaps, got %d", len(expectedGaps), len(gaps))
	}

	for i, gap := range gaps {
		if gap != expectedGaps[i] {
			t.Fatalf("expected gap %v, got %v", expectedGaps[i], gap)
		}
	}
}
