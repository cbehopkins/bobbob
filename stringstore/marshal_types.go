package stringstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cbehopkins/bobbob"
)

// ShardRecord represents a single shard's metadata in persistent storage.
// These records are used during marshalling/unmarshalling to track shard state and data locations.
type ShardRecord struct {
	// StateObjId is the BaseStore ObjectId where shard metadata lives.
	// This includes config, slot table, freelist, and other shard state.
	StateObjId bobbob.ObjectId

	// StrdataObjId is the BaseStore ObjectId where the shard's backing file data lives.
	// On first marshal, this is allocated. On subsequent marshals, it may be reused
	// (if shard unmodified) or replaced with a new ObjectId (if shard modified).
	StrdataObjId bobbob.ObjectId

	// StrdataSize is the size of the data stored at StrdataObjId.
	// Used for validation and reconstruction during unmarshalling.
	StrdataSize int64
}

// WrapperState represents the StringStore wrapper's persistent state.
// This includes the list of all shards and their associated ObjectIds.
// The wrapper state itself is stored in a reserved ObjectId in the BaseStore.
type WrapperState struct {
	// Version is the version number of this state format.
	// Used for forward compatibility when decoding.
	Version uint8

	// ShardRecords contains metadata for each shard, in order.
	// Index into this array corresponds to shard index in the wrapper.
	ShardRecords []ShardRecord
}

func marshalWrapperState(state WrapperState) ([]byte, error) {
	var buf bytes.Buffer

	if state.Version == 0 {
		state.Version = 1
	}
	if err := binary.Write(&buf, binary.LittleEndian, state.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(state.ShardRecords))); err != nil {
		return nil, err
	}
	for _, rec := range state.ShardRecords {
		if err := binary.Write(&buf, binary.LittleEndian, rec.StateObjId); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, rec.StrdataObjId); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, rec.StrdataSize); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func unmarshalWrapperState(data []byte) (WrapperState, error) {
	buf := bytes.NewReader(data)
	var state WrapperState

	if err := binary.Read(buf, binary.LittleEndian, &state.Version); err != nil {
		return WrapperState{}, fmt.Errorf("read wrapper state version: %w", err)
	}
	if state.Version != 1 {
		return WrapperState{}, fmt.Errorf("unsupported wrapper state version: %d", state.Version)
	}

	var count uint32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return WrapperState{}, fmt.Errorf("read shard record count: %w", err)
	}

	state.ShardRecords = make([]ShardRecord, 0, count)
	for range count {
		var rec ShardRecord
		if err := binary.Read(buf, binary.LittleEndian, &rec.StateObjId); err != nil {
			return WrapperState{}, fmt.Errorf("read StateObjId: %w", err)
		}
		if err := binary.Read(buf, binary.LittleEndian, &rec.StrdataObjId); err != nil {
			return WrapperState{}, fmt.Errorf("read StrdataObjId: %w", err)
		}
		if err := binary.Read(buf, binary.LittleEndian, &rec.StrdataSize); err != nil {
			return WrapperState{}, fmt.Errorf("read StrdataSize: %w", err)
		}
		state.ShardRecords = append(state.ShardRecords, rec)
	}

	return state, nil
}
