package store

import (
	"path/filepath"
	"testing"
)

// TestLateWriteNewObjSeeksBug demonstrates that LateWriteNewObj doesn't seek to the allocated offset
func TestLateWriteNewObjSeeksBug(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_seek.bin")

	store, err := NewBasicStore(tempFile)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write first object
	data1 := []byte("first_object_data")
	objId1, err := WriteNewObjFromBytes(store, data1)
	if err != nil {
		t.Fatalf("Failed to write first object: %v", err)
	}

	// Write second object
	data2 := []byte("second_object_data")
	objId2, err := WriteNewObjFromBytes(store, data2)
	if err != nil {
		t.Fatalf("Failed to write second object: %v", err)
	}

	// Write third object
	data3 := []byte("third_object_data")
	objId3, err := WriteNewObjFromBytes(store, data3)
	if err != nil {
		t.Fatalf("Failed to write third object: %v", err)
	}

	// Now read them back and verify
	// Note: ReadBytesFromObj may return more bytes than written (allocated size includes padding)
	// So we only compare the prefix
	readData1, err := ReadBytesFromObj(store, objId1)
	if err != nil {
		t.Fatalf("Failed to read first object: %v", err)
	}
	if len(readData1) < len(data1) || string(readData1[:len(data1)]) != string(data1) {
		t.Errorf("First object data mismatch. Expected %q, got %q", string(data1), string(readData1))
	}

	readData2, err := ReadBytesFromObj(store, objId2)
	if err != nil {
		t.Fatalf("Failed to read second object: %v", err)
	}
	if len(readData2) < len(data2) || string(readData2[:len(data2)]) != string(data2) {
		t.Errorf("Second object data mismatch. Expected %q, got %q", string(data2), string(readData2))
	}

	readData3, err := ReadBytesFromObj(store, objId3)
	if err != nil {
		t.Fatalf("Failed to read third object: %v", err)
	}
	if len(readData3) < len(data3) || string(readData3[:len(data3)]) != string(data3) {
		t.Errorf("Third object data mismatch. Expected %q, got %q", string(data3), string(readData3))
	}
}
