package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"testing"
)

// MockObjReader is a mock implementation of the ObjReader interface
type MockObjReader struct {
	data map[ObjectId][]byte
}

// NewMockObjReader creates a new MockObjReader with the given data
func NewMockObjReader(data map[ObjectId][]byte) *MockObjReader {
	return &MockObjReader{data: data}
}

// ReadObj reads the object data for the given ObjectId
func (r *MockObjReader) ReadObj(id ObjectId) (io.Reader, error) {
	if data, ok := r.data[id]; ok {
		return bytes.NewReader(data), nil
	}
	return nil, errors.New("object not found")
}

// MockStruct is a mock implementation of the MarshalComplex interface
type MockStruct struct {
	IntValue  int
	BoolValue bool
}

// PreMarshal returns the list of sizes needed by AllocateObjects
func (m *MockStruct) PreMarshal() ([]int, error) {
	// For demonstration, we assume that the integer and boolean each need 8 bytes
	return []int{16, 8, 8}, nil
}

// MarshalMultiple receives the list of ObjectIds provided by AllocateObjects and returns the list of ObjectAndByteFunc needed by WriteObjects
func (m *MockStruct) MarshalMultiple(objectIds []ObjectId) (func() ObjectId, []ObjectAndByteFunc, error) {
	if len(objectIds) != 3 {
		return nil, nil, errors.New("expected 3 ObjectIds")
	}
	lut := &ObjectIdLut{Ids: objectIds[1:]}
	// Create the ObjectAndByteFunc list
	objectAndByteFuncs := []ObjectAndByteFunc{
		{
			ObjectId: objectIds[0],
			ByteFunc: func() ([]byte, error) {
				return lut.Marshal()
			},
		},
		{
			ObjectId: objectIds[1],
			ByteFunc: func() ([]byte, error) {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.Encode(m.IntValue); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
		{
			ObjectId: objectIds[2],
			ByteFunc: func() ([]byte, error) {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.Encode(m.BoolValue); err != nil {
					return nil, err
				}
				return buf.Bytes(), nil
			},
		},
	}

	return func() ObjectId { return objectIds[0] }, objectAndByteFuncs, nil
}
func (m *MockStruct) Delete() error {
	return nil
}

// UnmarshalMultiple unmarshals the MockStruct from the store using the ObjectId LUT
func (m *MockStruct) UnmarshalMultiple(objReader io.Reader, reader ObjReader) error {
	var lut ObjectIdLut
	dataBuf := new(bytes.Buffer)
	if _, err := dataBuf.ReadFrom(objReader); err != nil {
		return err
	}
	data := dataBuf.Bytes()
	if err := lut.Unmarshal(data); err != nil {
		return err
	}

	if len(lut.Ids) != 2 {
		return errors.New("expected 2 ObjectIds in LUT")
	}

	// Fetch and unmarshal the integer value
	intReader, err := reader.ReadObj(lut.Ids[0])
	if err != nil {
		return err
	}
	intBuf := new(bytes.Buffer)
	if _, err := intBuf.ReadFrom(intReader); err != nil {
		return err
	}
	intDec := gob.NewDecoder(intBuf)
	if err := intDec.Decode(&m.IntValue); err != nil {
		return err
	}

	// Fetch and unmarshal the boolean value
	boolReader, err := reader.ReadObj(lut.Ids[1])
	if err != nil {
		return err
	}
	boolBuf := new(bytes.Buffer)
	if _, err := boolBuf.ReadFrom(boolReader); err != nil {
		return err
	}
	boolDec := gob.NewDecoder(boolBuf)
	if err := boolDec.Decode(&m.BoolValue); err != nil {
		return err
	}

	return nil
}

func TestMockStructMarshalMultiple(t *testing.T) {
	mock := &MockStruct{
		IntValue:  42,
		BoolValue: true,
	}

	objectIds := []ObjectId{1, 2, 3}
	_, objectAndByteFuncs, err := mock.MarshalMultiple(objectIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}

	if len(objectAndByteFuncs) != 3 {
		t.Fatalf("Expected 3 ObjectAndByteFunc, got %d", len(objectAndByteFuncs))
	}

	// Verify the LUT
	lutData, err := objectAndByteFuncs[0].ByteFunc()
	if err != nil {
		t.Fatalf("ByteFunc for LUT failed: %v", err)
	}
	expectedLut := &ObjectIdLut{Ids: objectIds[1:]}
	expectedLutData, err := expectedLut.Marshal()
	if err != nil {
		t.Fatalf("Expected LUT Marshal failed: %v", err)
	}
	if !bytes.Equal(lutData, expectedLutData) {
		t.Errorf("Expected LUT data %v, got %v", expectedLutData, lutData)
	}

	// Verify the IntValue
	intData, err := objectAndByteFuncs[1].ByteFunc()
	if err != nil {
		t.Fatalf("ByteFunc for IntValue failed: %v", err)
	}
	var intValue int
	intBuf := bytes.NewBuffer(intData)
	intDec := gob.NewDecoder(intBuf)
	if err := intDec.Decode(&intValue); err != nil {
		t.Fatalf("Decode IntValue failed: %v", err)
	}
	if intValue != 42 {
		t.Errorf("Expected IntValue 42, got %d", intValue)
	}

	// Verify the BoolValue
	boolData, err := objectAndByteFuncs[2].ByteFunc()
	if err != nil {
		t.Fatalf("ByteFunc for BoolValue failed: %v", err)
	}
	var boolValue bool
	boolBuf := bytes.NewBuffer(boolData)
	boolDec := gob.NewDecoder(boolBuf)
	if err := boolDec.Decode(&boolValue); err != nil {
		t.Fatalf("Decode BoolValue failed: %v", err)
	}
	if boolValue != true {
		t.Errorf("Expected BoolValue true, got %v", boolValue)
	}
}

func TestMockStructUnmarshalMultiple(t *testing.T) {
	// Prepare the data
	intValue := 42
	boolValue := true

	var intBuf bytes.Buffer
	intEnc := gob.NewEncoder(&intBuf)
	if err := intEnc.Encode(intValue); err != nil {
		t.Fatalf("Encode IntValue failed: %v", err)
	}

	var boolBuf bytes.Buffer
	boolEnc := gob.NewEncoder(&boolBuf)
	if err := boolEnc.Encode(boolValue); err != nil {
		t.Fatalf("Encode BoolValue failed: %v", err)
	}

	objectIds := []ObjectId{1, 2, 3}
	lut := &ObjectIdLut{Ids: objectIds[1:]}
	lutData, err := lut.Marshal()
	if err != nil {
		t.Fatalf("LUT Marshal failed: %v", err)
	}

	mockReader := NewMockObjReader(map[ObjectId][]byte{
		objectIds[0]: lutData,
		objectIds[1]: intBuf.Bytes(),
		objectIds[2]: boolBuf.Bytes(),
	})

	// Unmarshal the MockStruct
	mock := &MockStruct{}
	lutDataBuffer := bytes.NewReader(lutData)
	err = mock.UnmarshalMultiple(lutDataBuffer, mockReader)
	if err != nil {
		t.Fatalf("UnmarshalMultiple failed: %v", err)
	}

	// Verify the unmarshalled data
	if mock.IntValue != 42 {
		t.Errorf("Expected IntValue 42, got %d", mock.IntValue)
	}
	if mock.BoolValue != true {
		t.Errorf("Expected BoolValue true, got %v", mock.BoolValue)
	}
}

func TestWriteComplexTypes(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Create a MockStruct instance
	mock := &MockStruct{
		IntValue:  42,
		BoolValue: true,
	}

	// Write the complex type to the store
	mockObjId, err := store.WriteComplexTypes(mock)
	if err != nil {
		t.Fatalf("Failed to write complex types: %v", err)
	}

	// Read the LUT object
	lutReader, err := store.ReadObj(mockObjId)
	if err != nil {
		t.Fatalf("Failed to read LUT object: %v", err)
	}
	lutBuf := make([]byte, 16)
	_, err = lutReader.Read(lutBuf)
	if err != nil {
		t.Fatalf("Failed to read LUT data: %v", err)
	}

	// Create a new MockStruct instance and unmarshal it using the LUT
	newMock := &MockStruct{}
	lutBufReader := bytes.NewReader(lutBuf)
	err = newMock.UnmarshalMultiple(lutBufReader, store)
	if err != nil {
		t.Fatalf("Failed to unmarshal complex types: %v", err)
	}

	// Verify the unmarshalled data
	if newMock.IntValue != 42 {
		t.Errorf("Expected IntValue 42, got %d", newMock.IntValue)
	}
	if newMock.BoolValue != true {
		t.Errorf("Expected BoolValue true, got %v", newMock.BoolValue)
	}
}

func TestMarshalMultipleGeneric(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Test with a MarshalComplex type
	mock := &MockStruct{
		IntValue:  42,
		BoolValue: true,
	}
	objectLengths, err := mock.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}
	objectIds, err := store.allocateObjects(objectLengths)
	if err != nil {
		t.Fatalf("AllocateObjects failed: %v", err)
	}

	_, objectAndByteFuncs, err := mock.MarshalMultiple(objectIds)
	if err != nil {
		t.Fatalf("MarshalMultipleGeneric failed: %v", err)
	}

	err = store.writeObjects(objectAndByteFuncs)
	if err != nil {
		t.Fatalf("WriteObjects failed: %v", err)
	}
}


func TestUnmarshalMultipleGenericMockStruct(t *testing.T) {
	dir, store := setupTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	// Prepare the data for a MarshalComplex type
	mock := &MockStruct{
		IntValue:  42,
		BoolValue: true,
	}
	objectLengths, err := mock.PreMarshal()
	if err != nil {
		t.Fatalf("PreMarshal failed: %v", err)
	}
	objectIds, err := store.allocateObjects(objectLengths)
	if err != nil {
		t.Fatalf("AllocateObjects failed: %v", err)
	}

	identityFunc, objectAndByteFuncs, err := mock.MarshalMultiple(objectIds)
	if err != nil {
		t.Fatalf("MarshalMultiple failed: %v", err)
	}
	err = store.writeObjects(objectAndByteFuncs)
	if err != nil {
		t.Fatalf("WriteObjects failed: %v", err)
	}

	// Test with a MarshalComplex type
	newMock := &MockStruct{}
	err = store.unmarshalComplexObj(newMock, identityFunc())
	if err != nil {
		t.Fatalf("unmarshalComplexObj failed: %v", err)
	}
	if newMock.IntValue != 42 {
		t.Errorf("Expected IntValue 42, got %d", newMock.IntValue)
	}
	if newMock.BoolValue != true {
		t.Errorf("Expected BoolValue true, got %v", newMock.BoolValue)
	}
}

func equalIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
