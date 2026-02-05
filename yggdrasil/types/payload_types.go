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

func (sp *StringPayload) Unmarshal(data []byte) error {
	*sp = StringPayload(data)
	return nil
}
func (sp StringPayload) LateMarshal(s bobbob.Storer) (bobbob.ObjectId, bobbob.Finisher) {
	size := sp.SizeInBytes()
	objId, err := s.NewObj(size)
	if err != nil {
		return objId, func() error { return err }
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
	return objId, f
}
func (sp *StringPayload) LateUnmarshal(id bobbob.ObjectId, s bobbob.Storer) bobbob.Finisher {
	f := func() error {
		reader, finisher, err := s.LateReadObj(id)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		*sp = StringPayload(data)
		return finisher()
	}
	return f
}
func (sp *StringPayload) Delete(s bobbob.Storer) error {
	// FIXME: We need to delete the object from the store, but we don't have access to it here.
	return nil
}
