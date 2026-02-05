package types

import (
	"testing"

	"github.com/cbehopkins/bobbob/store"
)

func TestStringPayloadMarshalUnmarshalWithMockStore(t *testing.T) {
	mockStore := store.NewMockStore()

	original := StringPayload("hello mock store")

	objId, finisher := original.LateMarshal(mockStore)

	if finisher == nil {
		t.Fatal("LateMarshal returned nil finisher")
	}
	if err := finisher(); err != nil {
		t.Fatalf("LateMarshal finisher failed: %v", err)
	}

	var restored StringPayload
	unmarshalFinisher := restored.LateUnmarshal(objId, mockStore)

	if unmarshalFinisher == nil {
		t.Fatal("LateUnmarshal returned nil finisher")
	}
	if err := unmarshalFinisher(); err != nil {
		t.Fatalf("LateUnmarshal finisher failed: %v", err)
	}

	if restored != original {
		t.Fatalf("round-trip mismatch: got %q, want %q", restored, original)
	}
}
