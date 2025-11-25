package yggdrasil

import (
	"reflect"
	"testing"
)

type (
	dummyTypeA struct{}
	dummyTypeB struct{}
)

type dummyTypeC struct {
	Field int
}

func TestTypeMap_SerializationAndLookup(t *testing.T) {
	tm := &TypeMap{}

	// Register types
	tm.AddType(dummyTypeA{})
	tm.AddType(dummyTypeB{})
	tm.AddType(dummyTypeC{})

	// Serialize
	data, err := tm.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Deserialize into a new TypeMap
	var tm2 TypeMap
	err = tm2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Re-register types to restore typeRef (not serialized)
	tm2.AddType(dummyTypeA{})
	tm2.AddType(dummyTypeC{})
	tm2.AddType(dummyTypeB{})

	// Lookup by name
	tuple, ok := tm2.GetTypeByName("yggdrasil.dummyTypeC")
	if !ok {
		t.Fatalf("Type yggdrasil.dummyTypeC not found after unmarshal")
	}
	if reflect.TypeOf(tuple.typeRef) != reflect.TypeOf(dummyTypeC{}) {
		t.Errorf("typeRef is not correct type after re-registration")
	}

	// Lookup by short code
	// Find short code for dummyTypeB
	tupleB, ok := tm2.GetTypeByName("yggdrasil.dummyTypeB")
	if !ok {
		t.Fatalf("Type yggdrasil.dummyTypeB not found after unmarshal")
	}
	tupleByShort, ok := tm2.GetTypeByShortCode(tupleB.ShortCode)
	if !ok {
		t.Fatalf("ShortCode lookup failed after unmarshal")
	}
	if reflect.TypeOf(tupleByShort.typeRef) != reflect.TypeOf(dummyTypeB{}) {
		t.Errorf("typeRef is not correct type for short code lookup")
	}
}

func TestTypeMap_SerializationAndLookup_alt_order(t *testing.T) {
	tm := &TypeMap{}

	// Register types
	tm.AddType(dummyTypeA{})
	tm.AddType(dummyTypeB{})
	tm.AddType(dummyTypeC{})

	// Serialize
	data, err := tm.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Deserialize into a new TypeMap
	var tm2 TypeMap
	// Re-register types to before Unmarshal
	tm2.AddType(dummyTypeA{})
	tm2.AddType(dummyTypeC{})
	tm2.AddType(dummyTypeB{})

	err = tm2.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Lookup by name
	tuple, ok := tm2.GetTypeByName("yggdrasil.dummyTypeC")
	if !ok {
		t.Fatalf("Type yggdrasil.dummyTypeC not found after unmarshal")
	}
	if reflect.TypeOf(tuple.typeRef) != reflect.TypeOf(dummyTypeC{}) {
		t.Errorf("typeRef is not correct type after re-registration")
	}

	// Lookup by short code
	// Find short code for dummyTypeB
	tupleB, ok := tm2.GetTypeByName("yggdrasil.dummyTypeB")
	if !ok {
		t.Fatalf("Type yggdrasil.dummyTypeB not found after unmarshal")
	}
	tupleByShort, ok := tm2.GetTypeByShortCode(tupleB.ShortCode)
	if !ok {
		t.Fatalf("ShortCode lookup failed after unmarshal")
	}
	if reflect.TypeOf(tupleByShort.typeRef) != reflect.TypeOf(dummyTypeB{}) {
		t.Errorf("typeRef is not correct type for short code lookup")
	}
}
