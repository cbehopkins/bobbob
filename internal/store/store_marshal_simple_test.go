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

// PreMarshal returns the size needed by AllocateObjects
func (m *MockMarSimple) PreMarshal() ([]int, error) {
    data, err := m.Marshal()
    if err != nil {
        return nil, err
    }
    return []int{len(data)}, nil
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
    objectSizes, err := store.PreMarshalGeneric(mock)
    if err != nil {
        t.Fatalf("PreMarshalGeneric failed: %v", err)
    }
    objectIds, err := store.AllocateObjects(objectSizes)
    if err != nil {
        t.Fatalf("AllocateObjects failed: %v", err)
    }

    _, objectAndByteFuncs, err := store.MarshalMultipleGeneric(mock, objectIds)
    if err != nil {
        t.Fatalf("MarshalMultipleGeneric failed: %v", err)
    }

    err = store.WriteObjects(objectAndByteFuncs)
    if err != nil {
        t.Fatalf("WriteObjects failed: %v", err)
    }

    // Read back the object
    newMock := &MockMarSimple{}
    err = store.UnmarshalMultipleGeneric(newMock, objectIds[0])
    if err != nil {
        t.Fatalf("UnmarshalMultipleGeneric failed: %v", err)
    }

    // Verify the unmarshalled data
    if newMock.StringValue != "Hello, World!" {
        t.Errorf("Expected StringValue 'Hello, World!', got '%s'", newMock.StringValue)
    }
}