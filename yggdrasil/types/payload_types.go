package types

import (
	"io"

	"github.com/cbehopkins/bobbob"
)

type StringPayload string

func (sp StringPayload) Marshal() ([]byte, error) {
	return []byte(sp), nil
}

func (sp StringPayload) SizeInBytes() int {
	return len(sp)
}

func (sp StringPayload) Unmarshal(data []byte) (UntypedPersistentPayload, error) {
	return StringPayload(data), nil
}
func (sp StringPayload) LateMarshal(s bobbob.Storer) (bobbob.ObjectId, int, bobbob.Finisher) {
	// Try to use StringStorer interface if available
	if ss, ok := s.(bobbob.StringStorer); ok {
		value := string(sp)
		objId, err := ss.NewStringObj(value)
		if err != nil {
			return objId, 0, func() error { return err }
		}
		return objId, len(value), func() error { return nil }
	}

	// Fallback to generic allocation
	size := sp.SizeInBytes()
	objId, err := s.NewObj(size)
	if err != nil {
		return objId, 0, func() error { return err }
	}
	f := func() error {
		writer, finisher, err := s.WriteToObj(objId)
		if err != nil {
			return err
		}
		_, err = writer.Write([]byte(sp))
		if err != nil {
			return err
		}
		return finisher()
	}
	return objId, size, f
}
func (sp *StringPayload) LateUnmarshal(id bobbob.ObjectId, size int, s bobbob.Storer) bobbob.Finisher {
	f := func() error {
		if ss, ok := s.(bobbob.StringStorer); ok {
			value, err := ss.StringFromObjId(id)
			if err != nil {
				return err
			}
			*sp = StringPayload(value)
			return nil
		}

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
		*sp = StringPayload(data)
		return finisher()
	}
	return f
}

// Delete removes the string object from the store.
// If the store supports StringStorer and the object is a string, it deletes via that interface.
// Otherwise, it falls back to regular DeleteObj.
// This is useful when StringPayload is used outside of a treap context where automatic
// cleanup isn't available.
func (sp StringPayload) Delete(objId bobbob.ObjectId, s bobbob.Storer) error {
	if objId < 0 {
		return nil // Nothing to delete (invalid ObjectId)
	}
	return s.DeleteObj(objId)
}

// DeleteDependents implements the payloadDeleter interface for treap integration.
// For StringPayload, all data is in the main payloadObjectId (already deleted by node),
// so this is a no-op.
func (sp StringPayload) DeleteDependents(s bobbob.Storer) error {
	// No dependent objects to clean up - the payloadObjectId itself is handled by the node
	return nil
}
