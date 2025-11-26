package yggdrasil

import (
	"encoding/json"
)

// JsonPayload is a wrapper that provides PersistentPayload interface for any type
// by using JSON marshaling/unmarshaling. This allows using structs without
// implementing custom Marshal/Unmarshal methods.
//
// Usage:
//
//	type MyStruct struct {
//	    Name string
//	    Age  int
//	}
//
//	treap := NewPersistentPayloadTreap[StringKey, JsonPayload[MyStruct]](
//	    StringLess,
//	    (*StringKey)(new(string)),
//	    store,
//	)
//
//	treap.Insert(&key, priority, JsonPayload[MyStruct]{
//	    Value: MyStruct{Name: "Alice", Age: 30},
//	})
//
// For types that require optimal performance or compact storage, implement
// custom Marshal/Unmarshal methods instead of using JsonPayload.
type JsonPayload[T any] struct {
	Value T
}

// Marshal implements PersistentPayload using JSON encoding.
func (j JsonPayload[T]) Marshal() ([]byte, error) {
	return json.Marshal(j.Value)
}

// Unmarshal implements PersistentPayload using JSON decoding.
func (j JsonPayload[T]) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		return nil, err
	}
	return JsonPayload[T]{Value: value}, nil
}

// SizeInBytes returns the size of the JSON-encoded payload.
// Note: This marshals the value to get the exact size.
func (j JsonPayload[T]) SizeInBytes() int {
	data, err := json.Marshal(j.Value)
	if err != nil {
		return 0
	}
	return len(data)
}
