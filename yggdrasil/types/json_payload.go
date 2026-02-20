package types

import (
	"bytes"
	"encoding/binary"
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

// Marshal implements PersistentPayload using JSON encoding with a length prefix.
// This prevents JSON parsing errors when stored in block-aligned objects that
// may include trailing garbage bytes.
func (j JsonPayload[T]) Marshal() ([]byte, error) {
	payload, err := json.Marshal(j.Value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf, nil
}

// Unmarshal implements PersistentPayload using JSON decoding.
func (j JsonPayload[T]) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	// Allocated size may be larger than written size; trim trailing zeros
	data = bytes.TrimRight(data, "\x00")
	if len(data) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	length := binary.LittleEndian.Uint32(data[:4])
	if int(length) > len(data)-4 {
		return nil, io.ErrUnexpectedEOF
	}
	var value T
	err := json.Unmarshal(data[4:4+length], &value)
	if err != nil {
		return nil, err
	}
	return JsonPayload[T]{Value: value}, nil
}

// SizeInBytes returns the size of the JSON-encoded payload.
// Should return -1 if the size cannot be determined without marshaling, which is the case for JsonPayload.
func (j JsonPayload[T]) SizeInBytes() int {
	return -1
}

// LateMarshal writes the JSON payload to the store and returns the ObjectId and finisher.
func (j JsonPayload[T]) LateMarshal(s bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	payload, err := j.Marshal()
	if err != nil {
		return 0, 0, func() error { return err }
	}

	// Try to use StringStorer interface if available
	// JsonPayload marshals to bytes, which we can store as a string for better batching
	if ss, ok := s.(bobbob.StringStorer); ok {
		objId, err := ss.NewStringObj(string(payload))
		if err != nil {
			return objId, 0, func() error { return err }
		}
		return objId, len(payload), func() error { return nil }
	}

	// Fallback to generic allocation
	size := len(payload)
	objId, err := s.NewObj(size)
	if err != nil {
		return objId, 0, func() error { return err }
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
	return objId, size, f
}

// LateUnmarshal reads the JSON payload from the store and updates the receiver.
func (j *JsonPayload[T]) LateUnmarshal(id bobbob.ObjectId, size int, s bobbob.Storer) bobbob.Finisher {
	f := func() error {
		// Try to use StringStorer interface if available
		if ss, ok := s.(bobbob.StringStorer); ok {
			data, err := ss.StringFromObjId(id)
			if err != nil {
				return err
			}
			payload := []byte(data)
			
			// Allocated size may be larger than written size; trim trailing zeros
			payload = bytes.TrimRight(payload, "\x00")
			if len(payload) < 4 {
				return io.ErrUnexpectedEOF
			}
			length := binary.LittleEndian.Uint32(payload[:4])
			if int(length) > len(payload)-4 {
				return io.ErrUnexpectedEOF
			}
			var value T
			err = json.Unmarshal(payload[4:4+length], &value)
			if err != nil {
				return err
			}
			j.Value = value
			return nil
		}

		// Fallback to generic read
		reader, finisher, err := s.LateReadObj(id)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		if size > 0 && size <= len(data) {
			data = data[:size]
		}
		// Allocated size may be larger than written size; trim trailing zeros
		data = bytes.TrimRight(data, "\x00")
		if len(data) < 4 {
			return io.ErrUnexpectedEOF
		}
		length := binary.LittleEndian.Uint32(data[:4])
		if int(length) > len(data)-4 {
			return io.ErrUnexpectedEOF
		}
		var value T
		err = json.Unmarshal(data[4:4+length], &value)
		if err != nil {
			return err
		}
		j.Value = value
		return finisher()
	}
	return f
}
