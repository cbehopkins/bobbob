package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/cbehopkins/bobbob"
)

type mockStringStorer struct {
	mu      sync.RWMutex
	strings map[bobbob.ObjectId]string
	objects map[bobbob.ObjectId][]byte
	next    bobbob.ObjectId
}

func newMockStringStorer() *mockStringStorer {
	return &mockStringStorer{
		strings: make(map[bobbob.ObjectId]string),
		objects: make(map[bobbob.ObjectId][]byte),
		next:    1000,
	}
}

func (m *mockStringStorer) NewStringObj(data string) (bobbob.ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	objId := m.next
	m.next++
	m.strings[objId] = data
	return objId, nil
}

func (m *mockStringStorer) StringFromObjId(objId bobbob.ObjectId) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.strings[objId]
	if !ok {
		return "", fmt.Errorf("string object not found: %d", objId)
	}
	return val, nil
}

func (m *mockStringStorer) HasStringObj(objId bobbob.ObjectId) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.strings[objId]
	return ok
}

func (m *mockStringStorer) NewObj(size int) (bobbob.ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	objId := m.next
	m.next++
	m.objects[objId] = make([]byte, size)
	return objId, nil
}

func (m *mockStringStorer) DeleteObj(objId bobbob.ObjectId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.strings, objId)
	delete(m.objects, objId)
	return nil
}

func (m *mockStringStorer) Close() error {
	return nil
}

func (m *mockStringStorer) LateReadObj(id bobbob.ObjectId) (io.Reader, bobbob.Finisher, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[id]
	if !ok {
		return nil, nil, fmt.Errorf("object not found: %d", id)
	}
	return bytes.NewReader(data), func() error { return nil }, nil
}

func (m *mockStringStorer) LateWriteNewObj(size int) (bobbob.ObjectId, io.Writer, bobbob.Finisher, error) {
	objId, err := m.NewObj(size)
	if err != nil {
		return bobbob.ObjNotAllocated, nil, nil, err
	}
	buf := &bytes.Buffer{}
	finisher := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		data := buf.Bytes()
		if len(data) < size {
			padded := make([]byte, size)
			copy(padded, data)
			data = padded
		}
		m.objects[objId] = data
		return nil
	}
	return objId, buf, finisher, nil
}

func (m *mockStringStorer) WriteToObj(objectId bobbob.ObjectId) (io.Writer, bobbob.Finisher, error) {
	buf := &bytes.Buffer{}
	finisher := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.objects[objectId] = buf.Bytes()
		return nil
	}
	return buf, finisher, nil
}

func (m *mockStringStorer) WriteBatchedObjs(objIds []bobbob.ObjectId, data []byte, sizes []int) error {
	return fmt.Errorf("WriteBatchedObjs not implemented in mockStringStorer")
}

func (m *mockStringStorer) PrimeObject(size int) (bobbob.ObjectId, error) {
	return m.NewObj(size)
}

type mockBaseStorer struct {
	mu      sync.RWMutex
	objects map[bobbob.ObjectId][]byte
	next    bobbob.ObjectId
}

func newMockBaseStorer() *mockBaseStorer {
	return &mockBaseStorer{
		objects: make(map[bobbob.ObjectId][]byte),
		next:    2000,
	}
}

func (m *mockBaseStorer) NewObj(size int) (bobbob.ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	objId := m.next
	m.next++
	m.objects[objId] = make([]byte, size)
	return objId, nil
}

func (m *mockBaseStorer) DeleteObj(objId bobbob.ObjectId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, objId)
	return nil
}

func (m *mockBaseStorer) Close() error {
	return nil
}

func (m *mockBaseStorer) LateReadObj(id bobbob.ObjectId) (io.Reader, bobbob.Finisher, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[id]
	if !ok {
		return nil, nil, fmt.Errorf("object not found: %d", id)
	}
	return bytes.NewReader(data), func() error { return nil }, nil
}

func (m *mockBaseStorer) LateWriteNewObj(size int) (bobbob.ObjectId, io.Writer, bobbob.Finisher, error) {
	objId, err := m.NewObj(size)
	if err != nil {
		return bobbob.ObjNotAllocated, nil, nil, err
	}
	buf := &bytes.Buffer{}
	finisher := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		data := buf.Bytes()
		if len(data) < size {
			padded := make([]byte, size)
			copy(padded, data)
			data = padded
		}
		m.objects[objId] = data
		return nil
	}
	return objId, buf, finisher, nil
}

func (m *mockBaseStorer) WriteToObj(objectId bobbob.ObjectId) (io.Writer, bobbob.Finisher, error) {
	buf := &bytes.Buffer{}
	finisher := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.objects[objectId] = buf.Bytes()
		return nil
	}
	return buf, finisher, nil
}

func (m *mockBaseStorer) WriteBatchedObjs(objIds []bobbob.ObjectId, data []byte, sizes []int) error {
	return fmt.Errorf("WriteBatchedObjs not implemented in mockBaseStorer")
}

func (m *mockBaseStorer) PrimeObject(size int) (bobbob.ObjectId, error) {
	return m.NewObj(size)
}

func TestStringPayload_StringStorer(t *testing.T) {
	store := newMockStringStorer()
	payload := StringPayload("test string")

	objId, size, finisher := payload.LateMarshal(store)
	if objId <= 0 {
		t.Fatalf("expected positive ObjectId, got %d", objId)
	}
	if size != len(payload) {
		t.Fatalf("expected size %d, got %d", len(payload), size)
	}
	if err := finisher(); err != nil {
		t.Fatalf("finisher: %v", err)
	}

	val, err := store.StringFromObjId(objId)
	if err != nil {
		t.Fatalf("StringFromObjId: %v", err)
	}
	if val != string(payload) {
		t.Fatalf("expected '%s', got '%s'", payload, val)
	}
}

func TestStringPayload_Fallback(t *testing.T) {
	store := newMockBaseStorer()
	payload := StringPayload("fallback test")

	objId, size, finisher := payload.LateMarshal(store)
	if objId <= 0 {
		t.Fatalf("expected positive ObjectId, got %d", objId)
	}
	if size != len(payload) {
		t.Fatalf("expected size %d, got %d", len(payload), size)
	}
	if err := finisher(); err != nil {
		t.Fatalf("finisher: %v", err)
	}

	reader, _, err := store.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(data) < len(payload) {
		t.Fatalf("expected at least %d bytes, got %d", len(payload), len(data))
	}
	if string(data[:len(payload)]) != string(payload) {
		t.Fatalf("payload mismatch: expected '%s', got '%s'", payload, string(data))
	}
}

func TestStringKey_StringStorer(t *testing.T) {
	store := newMockStringStorer()
	key := StringKey("key-test")

	objId, size, finisher := key.LateMarshal(store)
	if objId <= 0 {
		t.Fatalf("expected positive ObjectId, got %d", objId)
	}
	if size != len(key) {
		t.Fatalf("expected size %d, got %d", len(key), size)
	}
	if err := finisher(); err != nil {
		t.Fatalf("finisher: %v", err)
	}

	val, err := store.StringFromObjId(objId)
	if err != nil {
		t.Fatalf("StringFromObjId: %v", err)
	}
	if val != string(key) {
		t.Fatalf("expected '%s', got '%s'", key, val)
	}
}

func TestStringKey_Fallback(t *testing.T) {
	store := newMockBaseStorer()
	key := StringKey("key-fallback")

	objId, size, finisher := key.LateMarshal(store)
	if objId <= 0 {
		t.Fatalf("expected positive ObjectId, got %d", objId)
	}
	if size == 0 {
		t.Fatalf("expected non-zero size, got %d", size)
	}
	if err := finisher(); err != nil {
		t.Fatalf("finisher: %v", err)
	}

	reader, _, err := store.LateReadObj(objId)
	if err != nil {
		t.Fatalf("LateReadObj: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(data) < 4 {
		t.Fatalf("expected length prefix, got %d bytes", len(data))
	}
	length := binary.LittleEndian.Uint32(data[:4])
	if int(length) != len(key) {
		t.Fatalf("expected length %d, got %d", len(key), length)
	}
	if string(data[4:4+length]) != string(key) {
		t.Fatalf("expected '%s', got '%s'", key, string(data[4:4+length]))
	}
}

func TestStringPayload_LateUnmarshal_StringStorer(t *testing.T) {
	store := newMockStringStorer()
	objId, err := store.NewStringObj("late-unmarshal")
	if err != nil {
		t.Fatalf("NewStringObj: %v", err)
	}

	var payload StringPayload
	finisher := payload.LateUnmarshal(objId, 0, store)
	if err := finisher(); err != nil {
		t.Fatalf("finisher: %v", err)
	}
	if payload != "late-unmarshal" {
		t.Fatalf("expected 'late-unmarshal', got '%s'", payload)
	}
}

func TestStringKey_LateUnmarshal_StringStorer(t *testing.T) {
	store := newMockStringStorer()
	objId, err := store.NewStringObj("late-key")
	if err != nil {
		t.Fatalf("NewStringObj: %v", err)
	}

	var key StringKey
	finisher := key.LateUnmarshal(objId, 0, store)
	if err := finisher(); err != nil {
		t.Fatalf("finisher: %v", err)
	}
	if key != "late-key" {
		t.Fatalf("expected 'late-key', got '%s'", key)
	}
}
