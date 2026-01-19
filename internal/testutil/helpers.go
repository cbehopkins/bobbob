package testutil

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

// CleanupTempFile removes a temporary file and its associated allocator metadata files.
// This ensures all test artifacts are cleaned up, including:
// - The data file itself (.dat)
// - The allocation store (.allocs.json)
// - The allocator index (.allocs.idx)
func CleanupTempFile(tb testing.TB, filePath string) {
	tb.Helper()

	files := []string{
		filePath,
		filePath + ".allocs.json",
		filePath + ".allocs.idx",
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			tb.Logf("warning: failed to remove %s: %v", f, err)
		}
	}
}

// WriteObject is a helper that writes data to a new object in the store.
// Returns the ObjectId of the created object.
func WriteObject(tb testing.TB, s store.Storer, data []byte) store.ObjectId {
	tb.Helper()

	objId, writer, finisher, err := s.LateWriteNewObj(len(data))
	if err != nil {
		tb.Fatalf("failed to write object: %v", err)
	}
	if finisher != nil {
		defer func() {
			if err := finisher(); err != nil {
				tb.Errorf("failed to finalize write: %v", err)
			}
		}()
	}

	n, err := writer.Write(data)
	if err != nil {
		tb.Fatalf("failed to write data: %v", err)
	}
	if n != len(data) {
		tb.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
	}

	return objId
}

// ReadObject is a helper that reads data from an object in the store.
// Returns the data read from the object.
func ReadObject(tb testing.TB, s store.Storer, objId store.ObjectId) []byte {
	tb.Helper()

	reader, finisher, err := s.LateReadObj(objId)
	if err != nil {
		tb.Fatalf("failed to read object %v: %v", objId, err)
	}
	if finisher != nil {
		defer func() {
			if err := finisher(); err != nil {
				tb.Errorf("failed to finalize read: %v", err)
			}
		}()
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		tb.Fatalf("failed to read data from object %v: %v", objId, err)
	}

	return data
}

// VerifyObject reads an object and verifies its contents match the expected data.
func VerifyObject(tb testing.TB, s store.Storer, objId store.ObjectId, expected []byte) {
	tb.Helper()

	actual := ReadObject(tb, s, objId)
	if !bytes.Equal(expected, actual) {
		tb.Errorf("object %v data mismatch:\nexpected: %v\nactual:   %v", objId, expected, actual)
	}
}

// AllocateObject allocates an object of the specified size without writing to it.
// Returns the ObjectId of the allocated object.
func AllocateObject(tb testing.TB, s store.Storer, size int) store.ObjectId {
	tb.Helper()

	objId, err := s.NewObj(size)
	if err != nil {
		tb.Fatalf("failed to allocate object: %v", err)
	}

	return objId
}

// UpdateObject overwrites an existing object with new data.
func UpdateObject(tb testing.TB, s store.Storer, objId store.ObjectId, data []byte) {
	tb.Helper()

	writer, finisher, err := s.WriteToObj(objId)
	if err != nil {
		tb.Fatalf("failed to get writer for object %v: %v", objId, err)
	}
	if finisher != nil {
		defer func() {
			if err := finisher(); err != nil {
				tb.Errorf("failed to finalize write: %v", err)
			}
		}()
	}

	n, err := writer.Write(data)
	if err != nil {
		tb.Fatalf("failed to write data to object %v: %v", objId, err)
	}
	if n != len(data) {
		tb.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
	}
}

// DeleteObject deletes an object from the store.
func DeleteObject(tb testing.TB, s store.Storer, objId store.ObjectId) {
	tb.Helper()

	err := s.DeleteObj(objId)
	if err != nil {
		tb.Fatalf("failed to delete object %v: %v", objId, err)
	}
}
