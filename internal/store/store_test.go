package store

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func setupTestStore(t *testing.T) (string, *Store) {
	dir, err := os.MkdirTemp("", "store_test")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	filePath := filepath.Join(dir, "testfile.bin")
	store, err := NewBob(filePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	return dir, store
}

func TestNewBob(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	if _, err := os.Stat(store.filePath); os.IsNotExist(err) {
		t.Fatalf("expected file to be created, but it does not exist")
	}

	// Verify the initial offset is zero using ReadObj
	reader, err := store.ReadObj(0)
	if err != nil {
		t.Fatalf("expected no error reading initial offset, got %v", err)
	}

	var initialOffset int64
	err = binary.Read(reader, binary.LittleEndian, &initialOffset)
	if err != nil {
		t.Fatalf("expected no error reading initial offset, got %v", err)
	}

	if initialOffset != 0 {
		t.Fatalf("expected initial offset to be 0, got %d", initialOffset)
	}
}

func TestWriteNewObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	offset, writer, err := store.WriteNewObj(10)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data := []byte("testdata")
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	if offset != 8 { // Initial offset is 8 bytes
		t.Fatalf("expected offset to be 8, got %d", offset)
	}

	if len(store.objectMap) != 2 {
		t.Fatalf("expected objectMap length to be 2, got %d", len(store.objectMap))
	}

	obj, found := store.objectMap[offset]
	if !found {
		t.Fatalf("expected object to be found in objectMap")
	}

	if obj.Offset != offset {
		t.Fatalf("expected object offset to be %d, got %d", offset, obj.Offset)
	}

	if obj.Size != 10 {
		t.Fatalf("expected object size to be 10, got %d", obj.Size)
	}
}

func TestReadObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	data := []byte("testdata")
	offset, writer, err := store.WriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	reader, err := store.ReadObj(offset)
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}

	readData := make([]byte, len(data))
	if _, err := io.ReadFull(reader, readData); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}

	if !bytes.Equal(data, readData) {
		t.Fatalf("expected read data to be %v, got %v", data, readData)
	}
}

func TestWriteToObj(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	data := []byte("testdata")
	offset, writer, err := store.WriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	// Write to the existing object
	newData := []byte("newdata")
	writer, err = store.WriteToObj(offset)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := writer.Write(newData); err != nil {
		t.Fatalf("expected no error writing new data, got %v", err)
	}

	// Read back the updated object
	reader, err := store.ReadObj(offset)
	if err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}

	readData := make([]byte, len(data))
	if _, err := io.ReadFull(reader, readData); err != nil {
		t.Fatalf("expected no error reading data, got %v", err)
	}

	if !bytes.Equal(newData, readData[:len(newData)]) {
		t.Fatalf("expected read data to be %v, got %v", newData, readData[:len(newData)])
	}
}

func TestObjectMapSerialize(t *testing.T) {
	objectMap := ObjectMap{
		0: {Offset: 0, Size: 10},
		1: {Offset: 1, Size: 20},
	}

	data, err := objectMap.Serialize()
	if err != nil {
		t.Fatalf("expected no error serializing objectMap, got %v", err)
	}
	var deserializedMap ObjectMap
	err = deserializedMap.Deserialize(data)
	if err != nil {
		t.Fatalf("expected no error deserializing objectMap, got %v", err)
	}

	if len(deserializedMap) != len(objectMap) {
		t.Fatalf("expected deserialized map length to be %d, got %d", len(objectMap), len(deserializedMap))
	}

	for k, v := range objectMap {
		if deserializedMap[k] != v {
			t.Fatalf("expected deserialized map value for key %d to be %v, got %v", k, v, deserializedMap[k])
		}
	}
}

func TestLoadStore(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	data := []byte("testdata")
	offset, writer, err := store.WriteNewObj(len(data))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := writer.Write(data); err != nil {
		t.Fatalf("expected no error writing data, got %v", err)
	}

	store.Close()

	loadedStore, err := LoadStore(store.filePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer loadedStore.Close()

	obj, found := loadedStore.objectMap[offset]
	if !found {
		t.Fatalf("expected object to be found in objectMap")
	}

	if obj.Offset != offset {
		t.Fatalf("expected object offset to be %d, got %d", offset, obj.Offset)
	}

	if obj.Size != len(data) {
		t.Fatalf("expected object size to be %d, got %d", len(data), obj.Size)
	}
}