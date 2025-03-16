package store

import (
	"encoding/json"
	"os"
	"testing"
)

// MockMarSimple is a mock implementation of the MarshalSimple and UnmarshalSimple interfaces
type MockMarSimple struct {
    StringValue string
}

// Marshal marshals the MockMarSimple to JSON
func (m *MockMarSimple) Marshal() ([]byte, error) {
    return json.Marshal(m)
}

// Unmarshal unmarshals the MockMarSimple from JSON
func (m *MockMarSimple) Unmarshal(data []byte) error {
    return json.Unmarshal(data, m)
}

func TestMarshalSimple(t *testing.T) {
    dir, store := setupTestStore(t)
    defer os.RemoveAll(dir)
    defer store.Close()

    // Create a MockMarSimple instance
    mock := &MockMarSimple{
        StringValue: "Hello, World!",
    }

    // Write the simple type to the store
    objectId, err := WriteGeneric(store, mock)
    if err != nil {
        t.Fatalf("marshalSimpleObj failed: %v", err)
    }

    // Read back the object
    newMock := &MockMarSimple{}
    err = ReadGeneric(store, newMock, objectId)
    if err != nil {
        t.Fatalf("ReadGeneric failed: %v", err)
    }

    // Verify the unmarshalled data
    if newMock.StringValue != "Hello, World!" {
        t.Errorf("Expected StringValue 'Hello, World!', got '%s'", newMock.StringValue)
    }
}

func TestInterfaceCruft(t *testing.T) {
    dir, store := setupTestStore(t)
    defer os.RemoveAll(dir)
    defer store.Close()

    objId, err := WriteGeneric(store, int64(42))
    if err != nil {
        t.Fatalf("WriteGeneric failed: %v", err)
    }
	newObj := func() any {
		var i int64
        // Because we unmarshal into it
        // It must be a pointer so it can be modified
		return &i
	}

    err = ReadGeneric(store, newObj(), objId)
    if err != nil {
        t.Fatal(err)
    }
}