package types

import (
	"testing"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

// InstrumentedStringPayload wraps StringPayload and tracks Late method calls
type InstrumentedStringPayload struct {
	value              StringPayload
	lateMarshalCalls   int
	lateUnmarshalCalls int
}

func NewInstrumentedStringPayload(value string) InstrumentedStringPayload {
	return InstrumentedStringPayload{
		value: StringPayload(value),
	}
}

func (isp InstrumentedStringPayload) Marshal() ([]byte, error) {
	return isp.value.Marshal()
}

func (isp InstrumentedStringPayload) SizeInBytes() int {
	return isp.value.SizeInBytes()
}

func (isp *InstrumentedStringPayload) Unmarshal(data []byte) error {
	return isp.value.Unmarshal(data)
}

// LateMarshal increments the call counter and delegates to StringPayload
func (isp *InstrumentedStringPayload) LateMarshal(s bobbob.Storer) (bobbob.ObjectId, bobbob.Finisher) {
	isp.lateMarshalCalls++
	return isp.value.LateMarshal(s)
}

// LateUnmarshal increments the call counter and delegates to StringPayload
func (isp *InstrumentedStringPayload) LateUnmarshal(id bobbob.ObjectId, s bobbob.Storer) bobbob.Finisher {
	isp.lateUnmarshalCalls++
	return isp.value.LateUnmarshal(id, s)
}

func (isp InstrumentedStringPayload) GetLateMarshalCalls() int {
	return isp.lateMarshalCalls
}

func (isp InstrumentedStringPayload) GetLateUnmarshalCalls() int {
	return isp.lateUnmarshalCalls
}

// TestInstrumentedStringPayloadDetectsLateMethods verifies that LateMarshal/LateUnmarshal are called
func TestInstrumentedStringPayloadDetectsLateMethods(t *testing.T) {
	mockStore := store.NewMockStore()
	instrumented := &InstrumentedStringPayload{value: StringPayload("hello instrumented world")}

	// Verify initial state
	if instrumented.GetLateMarshalCalls() != 0 {
		t.Fatalf("expected 0 LateMarshal calls initially, got %d", instrumented.GetLateMarshalCalls())
	}

	// Call LateMarshal
	objId, marshalFinisher := instrumented.LateMarshal(mockStore)
	if instrumented.GetLateMarshalCalls() != 1 {
		t.Fatalf("expected 1 LateMarshal call after calling LateMarshal, got %d", instrumented.GetLateMarshalCalls())
	}

	// Execute the finisher
	if err := marshalFinisher(); err != nil {
		t.Fatalf("LateMarshal finisher failed: %v", err)
	}

	// Call LateUnmarshal
	var restored InstrumentedStringPayload
	unmarshalFinisher := restored.LateUnmarshal(objId, mockStore)
	if restored.GetLateUnmarshalCalls() != 1 {
		t.Fatalf("expected 1 LateUnmarshal call after calling LateUnmarshal, got %d", restored.GetLateUnmarshalCalls())
	}

	// Execute the finisher
	if err := unmarshalFinisher(); err != nil {
		t.Fatalf("LateUnmarshal finisher failed: %v", err)
	}

	// Verify the round-trip
	if restored.value != instrumented.value {
		t.Fatalf("round-trip mismatch: expected %q, got %q", instrumented.value, restored.value)
	}
}

// TestInstrumentedStringPayloadMultipleCalls verifies multiple marshal/unmarshal cycles
func TestInstrumentedStringPayloadMultipleCalls(t *testing.T) {
	mockStore := store.NewMockStore()

	tests := []struct {
		name  string
		value string
	}{
		{"simple", "hello"},
		{"with spaces", "hello world"},
		{"empty", ""},
		{"long", "this is a much longer string with more characters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instrumented := &InstrumentedStringPayload{value: StringPayload(tt.value)}

			// Marshal
			objId, marshalFinisher := instrumented.LateMarshal(mockStore)
			if err := marshalFinisher(); err != nil {
				t.Fatalf("marshal finisher failed: %v", err)
			}

			// Unmarshal
			restored := &InstrumentedStringPayload{}
			unmarshalFinisher := restored.LateUnmarshal(objId, mockStore)
			if err := unmarshalFinisher(); err != nil {
				t.Fatalf("unmarshal finisher failed: %v", err)
			}

			// Verify calls were tracked
			if instrumented.GetLateMarshalCalls() != 1 {
				t.Errorf("expected 1 LateMarshal call, got %d", instrumented.GetLateMarshalCalls())
			}
			if restored.GetLateUnmarshalCalls() != 1 {
				t.Errorf("expected 1 LateUnmarshal call, got %d", restored.GetLateUnmarshalCalls())
			}

			// Verify round-trip
			if restored.value != instrumented.value {
				t.Errorf("round-trip mismatch: expected %q, got %q", instrumented.value, restored.value)
			}
		})
	}
}
