package testutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

// WriteObject is a helper that writes data to a new object in the store.
// Returns the ObjectId of the created object.
func WriteObject(tb testing.TB, s store.Storer, data []byte) store.ObjectId {
	tb.Helper()

	objId, writer, finisher, err := s.LateWriteNewObj(len(data))
	if err != nil {
		tb.Fatalf("failed to write object: %v", err)
	}
	if finisher != nil {
		defer finisher()
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
		defer finisher()
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
		defer finisher()
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
