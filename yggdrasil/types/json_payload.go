package types

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/cbehopkins/bobbob"
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
	// Allocated size may be larger than written size; trim trailing zeros for JSON
	data = bytes.TrimRight(data, "\x00")
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

// LateMarshal writes the JSON payload to the store and returns the ObjectId and finisher.
func (j JsonPayload[T]) LateMarshal(s bobbob.Storer) (bobbob.ObjectId, bobbob.Finisher) {
	payload, err := j.Marshal()
	if err != nil {
		return 0, func() error { return err }
	}
	size := len(payload)
	objId, err := s.NewObj(size)
	if err != nil {
		return objId, func() error { return err }
	}
	f := func() error {
		writer, finisher, err := s.WriteToObj(objId)
		if err != nil {
			return err
		}
		_, err = writer.Write(payload)
		if err != nil {
			return err
		}
		return finisher()
	}
	return objId, f
}

// LateUnmarshal reads the JSON payload from the store and updates the receiver.
func (j *JsonPayload[T]) LateUnmarshal(id bobbob.ObjectId, s bobbob.Storer) bobbob.Finisher {
	f := func() error {
		reader, finisher, err := s.LateReadObj(id)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		// Allocated size may be larger than written size; trim trailing zeros for JSON
		data = bytes.TrimRight(data, "\x00")
		var value T
		err = json.Unmarshal(data, &value)
		if err != nil {
			return err
		}
		j.Value = value
		return finisher()
	}
	return f
}
