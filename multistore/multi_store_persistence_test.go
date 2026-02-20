package multistore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
)

func TestMultiStoreStringStorePersistenceAcrossSessions(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "string_persistence.dat")
	metaPath := filePath + ".strings.meta"

	ms1, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create multiStore: %v", err)
	}

	obj1, err := ms1.NewStringObj("alpha")
	if err != nil {
		t.Fatalf("NewStringObj alpha failed: %v", err)
	}
	obj2, err := ms1.NewStringObj("beta")
	if err != nil {
		t.Fatalf("NewStringObj beta failed: %v", err)
	}

	if err := ms1.Close(); err != nil {
		t.Fatalf("Failed to close multiStore: %v", err)
	}

	if _, err := os.Stat(metaPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no sidecar metadata file at %s", metaPath)
	}

	ms2, err := LoadMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to load multiStore: %v", err)
	}
	defer ms2.Close()

	v1, err := ms2.StringFromObjId(obj1)
	if err != nil {
		t.Fatalf("StringFromObjId alpha failed: %v", err)
	}
	v2, err := ms2.StringFromObjId(obj2)
	if err != nil {
		t.Fatalf("StringFromObjId beta failed: %v", err)
	}

	if v1 != "alpha" {
		t.Fatalf("alpha mismatch: got %q", v1)
	}
	if v2 != "beta" {
		t.Fatalf("beta mismatch: got %q", v2)
	}
}

func TestLoadMultiStoreLegacyMetadataCompatibility(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "legacy_metadata.dat")

	ms1, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create multiStore: %v", err)
	}

	// Simulate legacy store metadata (non-BBMSTATE payload) in the metadata slot.
	legacyMeta := []byte("legacy-metadata-format")
	primeObjId, err := ms1.PrimeObject(len(legacyMeta))
	if err != nil {
		t.Fatalf("PrimeObject failed: %v", err)
	}
	if err := store.WriteBytesToObj(ms1, legacyMeta, primeObjId); err != nil {
		t.Fatalf("Failed to write legacy metadata payload: %v", err)
	}

	// Persist a regular object so we can verify normal data survives reload.
	objData := []byte("legacy-object-payload")
	objId, err := ms1.NewObj(len(objData))
	if err != nil {
		t.Fatalf("Failed to allocate object: %v", err)
	}
	if err := store.WriteBytesToObj(ms1, objData, objId); err != nil {
		t.Fatalf("Failed to write object data: %v", err)
	}

	if err := ms1.Close(); err != nil {
		t.Fatalf("Failed to close multiStore: %v", err)
	}

	ms2, err := LoadMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("LoadMultiStore should tolerate legacy metadata payload, got: %v", err)
	}
	defer ms2.Close()

	readBack, err := store.ReadBytesFromObj(ms2, objId)
	if err != nil {
		t.Fatalf("Failed to read persisted object after load: %v", err)
	}
	readBack = bytes.TrimRight(readBack, "\x00")
	if string(readBack) != string(objData) {
		t.Fatalf("object payload mismatch after load: got %q, want %q", string(readBack), string(objData))
	}
}

func TestLoadMultiStoreNoMetadataObjectCompatibility(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "no_metadata_object.dat")

	ms1, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create multiStore: %v", err)
	}

	payload := []byte("plain-data-without-metadata")
	objId, err := ms1.NewObj(len(payload))
	if err != nil {
		t.Fatalf("Failed to allocate object: %v", err)
	}
	if err := store.WriteBytesToObj(ms1, payload, objId); err != nil {
		t.Fatalf("Failed to write object payload: %v", err)
	}

	if err := ms1.Close(); err != nil {
		t.Fatalf("Failed to close multiStore: %v", err)
	}

	ms2, err := LoadMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("LoadMultiStore should tolerate missing metadata object, got: %v", err)
	}
	defer ms2.Close()

	readBack, err := store.ReadBytesFromObj(ms2, objId)
	if err != nil {
		t.Fatalf("Failed to read object after load: %v", err)
	}
	readBack = bytes.TrimRight(readBack, "\x00")
	if string(readBack) != string(payload) {
		t.Fatalf("object payload mismatch after load: got %q, want %q", string(readBack), string(payload))
	}
}

func TestMultiStorePersistence(t *testing.T) {
	// Create a temporary file for the test
	filePath := "test_multistore_persistence.dat"
	defer testutil.CleanupTempFile(t, filePath)

	// Create a new multiStore and write some objects
	ms1, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create multiStore: %v", err)
	}

	// Create some test objects with different sizes
	testData := []struct {
		data []byte
		id   store.ObjectId
	}{
		{data: []byte("Hello, World!")},
		{data: []byte("This is a test object")},
		{data: []byte("Short")},
		{data: []byte("A longer test object with more content to fill the space")},
	}

	// Write objects
	for i := range testData {
		objId, err := ms1.NewObj(len(testData[i].data))
		if err != nil {
			t.Fatalf("Failed to create object %d: %v", i, err)
		}
		testData[i].id = objId

		writer, finisher, err := ms1.WriteToObj(objId)
		if err != nil {
			t.Fatalf("Failed to get writer for object %d: %v", i, err)
		}

		n, err := writer.Write(testData[i].data)
		if err != nil {
			t.Fatalf("Failed to write object %d: %v", i, err)
		}
		if n != len(testData[i].data) {
			t.Fatalf("Wrote %d bytes, expected %d", n, len(testData[i].data))
		}

		if err := finisher(); err != nil {
			t.Fatalf("Failed to finish writing object %d: %v", i, err)
		}
	}

	// Close the store (this should marshal allocators only)
	if err := ms1.Close(); err != nil {
		t.Fatalf("Failed to close multiStore: %v", err)
	}

	// Load the store from disk
	ms2, err := LoadMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to load multiStore: %v", err)
	}
	defer ms2.Close()

	// Verify all objects can be read back with the same data
	for i, test := range testData {
		reader, finisher, err := ms2.LateReadObj(test.id)
		if err != nil {
			t.Fatalf("Failed to read object %d: %v", i, err)
		}

		readData := make([]byte, len(test.data))
		n, err := reader.Read(readData)
		if err != nil {
			t.Fatalf("Failed to read data from object %d: %v", i, err)
		}
		if n != len(test.data) {
			t.Fatalf("Read %d bytes, expected %d", n, len(test.data))
		}

		if string(readData) != string(test.data) {
			t.Errorf("Object %d data mismatch: got %q, expected %q", i, string(readData), string(test.data))
		}

		if err := finisher(); err != nil {
			t.Fatalf("Failed to finish reading object %d: %v", i, err)
		}
	}

	// Create a new object in the loaded store to ensure allocators work
	newData := []byte("New object after reload")
	newObjId, err := ms2.NewObj(len(newData))
	if err != nil {
		t.Fatalf("Failed to create new object after reload: %v", err)
	}

	writer, finisher, err := ms2.WriteToObj(newObjId)
	if err != nil {
		t.Fatalf("Failed to get writer for new object: %v", err)
	}

	n, err := writer.Write(newData)
	if err != nil {
		t.Fatalf("Failed to write new object: %v", err)
	}
	if n != len(newData) {
		t.Fatalf("Wrote %d bytes, expected %d", n, len(newData))
	}

	if err := finisher(); err != nil {
		t.Fatalf("Failed to finish writing new object: %v", err)
	}

	// Verify the new object can be read
	reader, finisher, err := ms2.LateReadObj(newObjId)
	if err != nil {
		t.Fatalf("Failed to read new object: %v", err)
	}

	readData := make([]byte, len(newData))
	n, err = reader.Read(readData)
	if err != nil {
		t.Fatalf("Failed to read data from new object: %v", err)
	}
	if n != len(newData) {
		t.Fatalf("Read %d bytes, expected %d", n, len(newData))
	}

	if string(readData) != string(newData) {
		t.Errorf("New object data mismatch: got %q, expected %q", string(readData), string(newData))
	}

	if err := finisher(); err != nil {
		t.Fatalf("Failed to finish reading new object: %v", err)
	}
}

func TestMultiStorePersistenceWithDeletion(t *testing.T) {
	// Create a temporary file for the test
	filePath := "test_multistore_deletion.dat"
	defer testutil.CleanupTempFile(t, filePath)

	// Create a new multiStore
	ms1, err := NewMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to create multiStore: %v", err)
	}

	// Create several objects
	objIds := make([]store.ObjectId, 5)
	for i := 0; i < 5; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		objId, err := ms1.NewObj(len(data))
		if err != nil {
			t.Fatalf("Failed to create object %d: %v", i, err)
		}
		objIds[i] = objId

		writer, finisher, err := ms1.WriteToObj(objId)
		if err != nil {
			t.Fatalf("Failed to get writer for object %d: %v", i, err)
		}

		if _, err := writer.Write(data); err != nil {
			t.Fatalf("Failed to write object %d: %v", i, err)
		}
		if err := finisher(); err != nil {
			t.Fatalf("Failed to finish writing object %d: %v", i, err)
		}
	}

	// Delete some objects
	if err := ms1.DeleteObj(objIds[1]); err != nil {
		t.Fatalf("Failed to delete object 1: %v", err)
	}
	if err := ms1.DeleteObj(objIds[3]); err != nil {
		t.Fatalf("Failed to delete object 3: %v", err)
	}

	// Close and reload
	if err := ms1.Close(); err != nil {
		t.Fatalf("Failed to close multiStore: %v", err)
	}

	ms2, err := LoadMultiStore(filePath, 0)
	if err != nil {
		t.Fatalf("Failed to load multiStore: %v", err)
	}
	defer ms2.Close()

	// Verify deleted objects are not accessible
	for i, objId := range objIds {
		_, _, err := ms2.LateReadObj(objId)
		if i == 1 || i == 3 {
			// These were deleted, should fail
			if err == nil {
				t.Errorf("Expected error reading deleted object %d, got none", i)
			}
		} else {
			// These should still exist
			if err != nil {
				t.Errorf("Failed to read object %d that should exist: %v", i, err)
			}
		}
	}
}
