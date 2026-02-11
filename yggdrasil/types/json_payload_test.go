package types

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

type SimpleStruct struct {
	Name  string
	Count int
}

type NestedStruct struct {
	Simple SimpleStruct
	Active bool
}

type ComplexStruct struct {
	ID       int
	Name     string
	Tags     []string
	Metadata map[string]any
}

// TestJsonPayloadMarshal verifies that JsonPayload can serialize a struct to JSON bytes
// and that the marshaled data is valid JSON.
func TestJsonPayloadMarshal(t *testing.T) {
	payload := JsonPayload[SimpleStruct]{
		Value: SimpleStruct{Name: "test", Count: 42},
	}

	data, err := payload.Marshal()
	if err != nil {
		t.Fatalf("expected no error marshaling, got %v", err)
	}

	if len(data) == 0 {
		t.Error("expected non-empty marshaled data")
	}

	if len(data) < 4 {
		t.Fatal("expected length-prefixed payload")
	}
	jsonLen := binary.LittleEndian.Uint32(data[:4])
	if int(jsonLen) != len(data)-4 {
		t.Fatalf("expected length %d to match payload size %d", jsonLen, len(data)-4)
	}
	jsonData := data[4:]
	// Verify the JSON portion contains expected values
	if !bytes.Contains(jsonData, []byte("test")) {
		t.Error("expected marshaled JSON to contain 'test'")
	}
	if !bytes.Contains(jsonData, []byte("42")) {
		t.Error("expected marshaled JSON to contain '42'")
	}
}

// TestJsonPayloadUnmarshal verifies that JsonPayload can deserialize JSON bytes back into
// the original struct, preserving all field values.
func TestJsonPayloadUnmarshal(t *testing.T) {
	original := JsonPayload[SimpleStruct]{
		Value: SimpleStruct{Name: "unmarshal test", Count: 123},
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Create empty payload for unmarshaling
	empty := JsonPayload[SimpleStruct]{}
	result, err := empty.Unmarshal(data)
	if err != nil {
		t.Fatalf("expected no error unmarshaling, got %v", err)
	}

	restored, ok := result.(JsonPayload[SimpleStruct])
	if !ok {
		t.Fatal("expected result to be JsonPayload[SimpleStruct]")
	}

	if restored.Value.Name != original.Value.Name {
		t.Errorf("expected Name %q, got %q", original.Value.Name, restored.Value.Name)
	}
	if restored.Value.Count != original.Value.Count {
		t.Errorf("expected Count %d, got %d", original.Value.Count, restored.Value.Count)
	}
}

// TestJsonPayloadRoundTrip verifies that various types (structs, primitives, slices, maps)
// can be marshaled and unmarshaled without data loss.
func TestJsonPayloadRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"simple struct", SimpleStruct{Name: "Alice", Count: 100}},
		{"nested struct", NestedStruct{
			Simple: SimpleStruct{Name: "Bob", Count: 50},
			Active: true,
		}},
		{"string", "hello world"},
		{"int", 42},
		{"slice", []int{1, 2, 3, 4, 5}},
		{"map", map[string]int{"one": 1, "two": 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			payload := JsonPayload[any]{Value: tt.value}
			data, err := payload.Marshal()
			if err != nil {
				t.Fatalf("marshal failed: %v", err)
			}

			// Unmarshal
			empty := JsonPayload[any]{}
			result, err := empty.Unmarshal(data)
			if err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}

			restored := result.(JsonPayload[any])
			if restored.Value == nil {
				t.Error("expected non-nil value after unmarshal")
			}
		})
	}
}

// TestJsonPayloadSizeInBytes verifies that SizeInBytes returns the correct byte count
// of the marshaled JSON data.
func TestJsonPayloadSizeInBytes(t *testing.T) {
	payload := JsonPayload[SimpleStruct]{
		Value: SimpleStruct{Name: "size test", Count: 999},
	}

	size := payload.SizeInBytes()
	if size <= 0 {
		t.Errorf("expected positive size, got %d", size)
	}

	// Size should match marshaled data length
	data, _ := payload.Marshal()
	if size != len(data) {
		t.Errorf("expected size %d to match data length %d", size, len(data))
	}
}

// TestJsonPayloadSizeInBytesEmpty verifies that an empty struct serializes to "{}" with
// a size of 2 bytes.
func TestJsonPayloadSizeInBytesEmpty(t *testing.T) {
	payload := JsonPayload[SimpleStruct]{}

	size := payload.SizeInBytes()
	if size <= 0 {
		t.Errorf("expected positive size for empty struct, got %d", size)
	}
}

// TestJsonPayloadComplexStruct verifies that nested structs with various field types
// (strings, numbers, arrays, nested objects) marshal and unmarshal correctly.
func TestJsonPayloadComplexStruct(t *testing.T) {
	original := JsonPayload[ComplexStruct]{
		Value: ComplexStruct{
			ID:   12345,
			Name: "complex",
			Tags: []string{"tag1", "tag2", "tag3"},
			Metadata: map[string]any{
				"version": 1.0,
				"enabled": true,
				"count":   100,
			},
		},
	}

	// Marshal
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Unmarshal
	empty := JsonPayload[ComplexStruct]{}
	result, err := empty.Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	restored := result.(JsonPayload[ComplexStruct])

	// Verify fields
	if restored.Value.ID != original.Value.ID {
		t.Errorf("ID mismatch: expected %d, got %d", original.Value.ID, restored.Value.ID)
	}
	if restored.Value.Name != original.Value.Name {
		t.Errorf("Name mismatch: expected %q, got %q", original.Value.Name, restored.Value.Name)
	}
	if len(restored.Value.Tags) != len(original.Value.Tags) {
		t.Errorf("Tags length mismatch: expected %d, got %d", len(original.Value.Tags), len(restored.Value.Tags))
	}
	if len(restored.Value.Metadata) != len(original.Value.Metadata) {
		t.Errorf("Metadata length mismatch: expected %d, got %d", len(original.Value.Metadata), len(restored.Value.Metadata))
	}
}

// TestJsonPayloadUnmarshalInvalidData verifies that unmarshaling invalid JSON data
// returns an appropriate error.
func TestJsonPayloadUnmarshalInvalidData(t *testing.T) {
	payload := JsonPayload[SimpleStruct]{}

	invalidData := []byte("this is not valid JSON {]")
	_, err := payload.Unmarshal(invalidData)
	if err == nil {
		t.Error("expected error unmarshaling invalid JSON, got nil")
	}
}

// TestJsonPayloadUnmarshalWithPaddingGarbage verifies length-prefixed JSON ignores
// trailing garbage bytes (e.g., block-aligned storage not zeroed).
func TestJsonPayloadUnmarshalWithPaddingGarbage(t *testing.T) {
	original := JsonPayload[string]{Value: "payload"}
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Simulate block-aligned storage with garbage bytes after JSON.
	data = append(data, 0x00, 0xFF, 0x7A)

	_, err = original.Unmarshal(data)
	if err != nil {
		t.Fatalf("expected unmarshal to tolerate padding garbage, got error: %v", err)
	}
}

// TestJsonPayloadUnmarshalEmptyData verifies that unmarshaling empty byte slice
// returns an error rather than silently succeeding.
func TestJsonPayloadUnmarshalEmptyData(t *testing.T) {
	payload := JsonPayload[SimpleStruct]{}

	_, err := payload.Unmarshal([]byte{})
	if err == nil {
		t.Error("expected error unmarshaling empty data, got nil")
	}
}

// TestJsonPayloadUnmarshalWrongType verifies that unmarshaling into the wrong type
// (e.g., JSON array into struct) returns an error.
func TestJsonPayloadUnmarshalWrongType(t *testing.T) {
	// Marshal as one type
	original := JsonPayload[string]{Value: "this is a string"}
	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Try to unmarshal as different type
	wrongType := JsonPayload[int]{}
	_, err = wrongType.Unmarshal(data)
	if err == nil {
		t.Error("expected error unmarshaling wrong type, got nil")
	}
}

// TestJsonPayloadZeroValue verifies that zero-value structs (with default field values)
// marshal and unmarshal correctly.
func TestJsonPayloadZeroValue(t *testing.T) {
	// Test marshaling zero value
	payload := JsonPayload[SimpleStruct]{}

	data, err := payload.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Unmarshal back
	result, err := payload.Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	restored := result.(JsonPayload[SimpleStruct])
	if restored.Value.Name != "" {
		t.Errorf("expected empty Name, got %q", restored.Value.Name)
	}
	if restored.Value.Count != 0 {
		t.Errorf("expected Count 0, got %d", restored.Value.Count)
	}
}

// TestJsonPayloadPointerTypes verifies that structs containing pointer fields
// marshal and unmarshal correctly, preserving pointer values.
func TestJsonPayloadPointerTypes(t *testing.T) {
	type PointerStruct struct {
		Name  *string
		Count *int
	}

	name := "pointer test"
	count := 42

	original := JsonPayload[PointerStruct]{
		Value: PointerStruct{
			Name:  &name,
			Count: &count,
		},
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	empty := JsonPayload[PointerStruct]{}
	result, err := empty.Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	restored := result.(JsonPayload[PointerStruct])
	if restored.Value.Name == nil {
		t.Fatal("expected non-nil Name pointer")
	}
	if *restored.Value.Name != name {
		t.Errorf("expected Name %q, got %q", name, *restored.Value.Name)
	}
	if restored.Value.Count == nil {
		t.Fatal("expected non-nil Count pointer")
	}
	if *restored.Value.Count != count {
		t.Errorf("expected Count %d, got %d", count, *restored.Value.Count)
	}
}

// TestJsonPayloadNilPointers verifies that nil pointer fields are handled correctly,
// serializing as null in JSON.
func TestJsonPayloadNilPointers(t *testing.T) {
	type PointerStruct struct {
		Name  *string
		Count *int
	}

	original := JsonPayload[PointerStruct]{
		Value: PointerStruct{
			Name:  nil,
			Count: nil,
		},
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	empty := JsonPayload[PointerStruct]{}
	result, err := empty.Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	restored := result.(JsonPayload[PointerStruct])
	if restored.Value.Name != nil {
		t.Error("expected nil Name pointer")
	}
	if restored.Value.Count != nil {
		t.Error("expected nil Count pointer")
	}
}

// TestJsonPayloadLargeData verifies that large data structures (e.g., struct with 1000 items)
// marshal and unmarshal efficiently without errors.
func TestJsonPayloadLargeData(t *testing.T) {
	// Test with larger data structure
	type LargeStruct struct {
		Items []string
	}

	items := make([]string, 1000)
	for i := range items {
		items[i] = "item-" + string(rune('0'+i%10))
	}

	original := JsonPayload[LargeStruct]{
		Value: LargeStruct{Items: items},
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	size := original.SizeInBytes()
	if size != len(data) {
		t.Errorf("SizeInBytes %d doesn't match actual size %d", size, len(data))
	}

	empty := JsonPayload[LargeStruct]{}
	result, err := empty.Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	restored := result.(JsonPayload[LargeStruct])
	if len(restored.Value.Items) != 1000 {
		t.Errorf("expected 1000 items, got %d", len(restored.Value.Items))
	}
}

// TestJsonPayloadMultipleUnmarshalCalls verifies that calling Unmarshal multiple times
// on the same payload works correctly, updating the value each time.
func TestJsonPayloadMultipleUnmarshalCalls(t *testing.T) {
	original := JsonPayload[SimpleStruct]{
		Value: SimpleStruct{Name: "test", Count: 42},
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// Unmarshal multiple times with same instance
	payload := JsonPayload[SimpleStruct]{}

	for i := 0; i < 3; i++ {
		result, err := payload.Unmarshal(data)
		if err != nil {
			t.Fatalf("unmarshal %d failed: %v", i, err)
		}

		restored := result.(JsonPayload[SimpleStruct])
		if restored.Value.Name != "test" {
			t.Errorf("iteration %d: expected Name 'test', got %q", i, restored.Value.Name)
		}
		if restored.Value.Count != 42 {
			t.Errorf("iteration %d: expected Count 42, got %d", i, restored.Value.Count)
		}
	}
}

// TestJsonPayloadLateMarshalUnmarshalSimple verifies LateMarshal/LateUnmarshal
// round-trips a simple struct using MockStore.
func TestJsonPayloadLateMarshalUnmarshalSimple(t *testing.T) {
	mockStore := store.NewMockStore()

	original := JsonPayload[SimpleStruct]{
		Value: SimpleStruct{Name: "late", Count: 7},
	}

	objId, _, finisher := original.LateMarshal(mockStore)
	if finisher == nil {
		t.Fatal("LateMarshal returned nil finisher")
	}
	if err := finisher(); err != nil {
		t.Fatalf("LateMarshal finisher failed: %v", err)
	}

	var restored JsonPayload[SimpleStruct]
	unmarshalFinisher := restored.LateUnmarshal(objId, 0, mockStore)
	if unmarshalFinisher == nil {
		t.Fatal("LateUnmarshal returned nil finisher")
	}
	if err := unmarshalFinisher(); err != nil {
		t.Fatalf("LateUnmarshal finisher failed: %v", err)
	}

	if restored.Value.Name != original.Value.Name {
		t.Errorf("expected Name %q, got %q", original.Value.Name, restored.Value.Name)
	}
	if restored.Value.Count != original.Value.Count {
		t.Errorf("expected Count %d, got %d", original.Value.Count, restored.Value.Count)
	}
}

// TestJsonPayloadLateMarshalUnmarshalComplex verifies LateMarshal/LateUnmarshal
// round-trips a more complex structure using MockStore.
func TestJsonPayloadLateMarshalUnmarshalComplex(t *testing.T) {
	mockStore := store.NewMockStore()

	original := JsonPayload[ComplexStruct]{
		Value: ComplexStruct{
			ID:   99,
			Name: "late-complex",
			Tags: []string{"alpha", "beta"},
			Metadata: map[string]any{
				"enabled": true,
				"count":   123,
			},
		},
	}

	objId, _, finisher := original.LateMarshal(mockStore)
	if finisher == nil {
		t.Fatal("LateMarshal returned nil finisher")
	}
	if err := finisher(); err != nil {
		t.Fatalf("LateMarshal finisher failed: %v", err)
	}

	var restored JsonPayload[ComplexStruct]
	unmarshalFinisher := restored.LateUnmarshal(objId, 0, mockStore)
	if unmarshalFinisher == nil {
		t.Fatal("LateUnmarshal returned nil finisher")
	}
	if err := unmarshalFinisher(); err != nil {
		t.Fatalf("LateUnmarshal finisher failed: %v", err)
	}

	if restored.Value.ID != original.Value.ID {
		t.Errorf("expected ID %d, got %d", original.Value.ID, restored.Value.ID)
	}
	if restored.Value.Name != original.Value.Name {
		t.Errorf("expected Name %q, got %q", original.Value.Name, restored.Value.Name)
	}
	if len(restored.Value.Tags) != len(original.Value.Tags) {
		t.Errorf("expected %d tags, got %d", len(original.Value.Tags), len(restored.Value.Tags))
	}
	if len(restored.Value.Metadata) != len(original.Value.Metadata) {
		t.Errorf("expected %d metadata entries, got %d", len(original.Value.Metadata), len(restored.Value.Metadata))
	}
}
