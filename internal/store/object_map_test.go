package store

import "testing"

func TestObjectMapSerialize(t *testing.T) {
	objectMap := ObjectMap{
		0: {Offset: 0, Size: 10},
		1: {Offset: 1, Size: 20},
	}

	data, err := objectMap.Serialize()
	if err != nil {
		t.Fatalf("expected no error serializing objectMap, got %v", err)
	}
	var deserializedMap ObjectMap
	err = deserializedMap.Deserialize(data)
	if err != nil {
		t.Fatalf("expected no error deserializing objectMap, got %v", err)
	}

	if len(deserializedMap) != len(objectMap) {
		t.Fatalf("expected deserialized map length to be %d, got %d", len(objectMap), len(deserializedMap))
	}

	for k, v := range objectMap {
		if deserializedMap[k] != v {
			t.Fatalf("expected deserialized map value for key %d to be %v, got %v", k, v, deserializedMap[k])
		}
	}
}
func TestObjectMapMarshal(t *testing.T) {
	objectMap := ObjectMap{
		0: {Offset: 0, Size: 10},
		1: {Offset: 1, Size: 20},
	}

	data, err := Marshal(objectMap)
	if err != nil {
		t.Fatalf("expected no error marshaling objectMap, got %v", err)
	}
	var deserializedMap ObjectMap
	err = Unmarshal(data, &deserializedMap)
	if err != nil {
		t.Fatalf("expected no error unmarshaling objectMap, got %v", err)
	}

	if len(deserializedMap) != len(objectMap) {
		t.Fatalf("expected deserialized map length to be %d, got %d", len(objectMap), len(deserializedMap))
	}

	for k, v := range objectMap {
		if deserializedMap[k] != v {
			t.Fatalf("expected deserialized map value for key %d to be %v, got %v", k, v, deserializedMap[k])
		}
	}
}
