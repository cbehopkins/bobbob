package stringstore

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/cbehopkins/bobbob"
)

// MockStringStore is an in-memory implementation of StringStorer for testing.
// It stores strings with auto-incrementing ObjectIds.
type MockStringStore struct {
	mu   sync.RWMutex
	data map[bobbob.ObjectId]string
	objects map[bobbob.ObjectId][]byte
	next    bobbob.ObjectId
}

func NewMockStringStore() *MockStringStore {
	return &MockStringStore{
		data:    make(map[bobbob.ObjectId]string),
		objects: make(map[bobbob.ObjectId][]byte),
		next:    1000,
	}
}

func (m *MockStringStore) NewStringObj(data string) (bobbob.ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	objId := m.next
	m.next++
	m.data[objId] = data
	return objId, nil
}

func (m *MockStringStore) StringFromObjId(objId bobbob.ObjectId) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.data[objId]
	if !ok {
		return "", fmt.Errorf("string object not found: %d", objId)
	}
	return val, nil
}

func (m *MockStringStore) HasStringObj(objId bobbob.ObjectId) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.data[objId]
	return ok
}

// Storer interface methods (minimal implementations for tests).
func (m *MockStringStore) NewObj(size int) (bobbob.ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	objId := m.next
	m.next++
	m.objects[objId] = make([]byte, size)
	return objId, nil
}

func (m *MockStringStore) DeleteObj(objId bobbob.ObjectId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, objId)
	delete(m.objects, objId)
	return nil
}

func (m *MockStringStore) Close() error {
	return nil
}

func (m *MockStringStore) LateReadObj(id bobbob.ObjectId) (io.Reader, bobbob.Finisher, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[id]
	if !ok {
		return nil, nil, fmt.Errorf("object not found: %d", id)
	}
	return bytes.NewReader(data), func() error { return nil }, nil
}

func (m *MockStringStore) LateWriteNewObj(size int) (bobbob.ObjectId, io.Writer, bobbob.Finisher, error) {
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

func (m *MockStringStore) WriteToObj(objectId bobbob.ObjectId) (io.Writer, bobbob.Finisher, error) {
	buf := &bytes.Buffer{}
	finisher := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.objects[objectId] = buf.Bytes()
		return nil
	}
	return buf, finisher, nil
}

// MockBaseStore simulates a BaseStore for testing marshalling without real I/O.
type MockBaseStore struct {
	mu          sync.RWMutex
	objects     map[bobbob.ObjectId][]byte
	nextObjId   bobbob.ObjectId
	AllocCalls  []int // Track allocation requests
	DeleteCalls []bobbob.ObjectId
	WriteHist   []struct {
		ObjId bobbob.ObjectId
		Size  int
	} // Track writes
}

func NewMockBaseStore() *MockBaseStore {
	return &MockBaseStore{
		objects:     make(map[bobbob.ObjectId][]byte),
		nextObjId:   bobbob.ObjectId(100),
		AllocCalls:  make([]int, 0),
		DeleteCalls: make([]bobbob.ObjectId, 0),
		WriteHist:   make([]struct{ ObjId bobbob.ObjectId; Size int }, 0),
	}
}

func (m *MockBaseStore) NewObj(size int) (bobbob.ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.AllocCalls = append(m.AllocCalls, size)
	objId := m.nextObjId
	m.nextObjId++
	m.objects[objId] = make([]byte, size)
	return objId, nil
}

func (m *MockBaseStore) WriteToObj(objId bobbob.ObjectId) (io.Writer, bobbob.Finisher, error) {
	m.mu.RLock()
	_, ok := m.objects[objId]
	m.mu.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("object not found: %d", objId)
	}

	buf := &bytes.Buffer{}
	finisher := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		data := buf.Bytes()
		m.objects[objId] = data
		m.WriteHist = append(m.WriteHist, struct {
			ObjId bobbob.ObjectId
			Size  int
		}{objId, len(data)})
		return nil
	}
	return buf, finisher, nil
}

func (m *MockBaseStore) DeleteObj(objId bobbob.ObjectId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeleteCalls = append(m.DeleteCalls, objId)
	delete(m.objects, objId)
	return nil
}

func (m *MockBaseStore) GetObjectData(objId bobbob.ObjectId) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[objId]
	if !ok {
		return nil, fmt.Errorf("object not found: %d", objId)
	}
	return data, nil
}

// Stubs to satisfy Storer interface (not fully implemented for basic mock)
func (m *MockBaseStore) LateReadObj(id bobbob.ObjectId) (io.Reader, bobbob.Finisher, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[id]
	if !ok {
		return nil, nil, fmt.Errorf("object not found: %d", id)
	}
	return bytes.NewReader(data), func() error { return nil }, nil
}

func (m *MockBaseStore) LateWriteNewObj(size int) (bobbob.ObjectId, io.Writer, bobbob.Finisher, error) {
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
		m.WriteHist = append(m.WriteHist, struct {
			ObjId bobbob.ObjectId
			Size  int
		}{objId, len(data)})
		return nil
	}
	return objId, buf, finisher, nil
}

func (m *MockBaseStore) WriteBatchedObjs(objIds []bobbob.ObjectId, data []byte, sizes []int) error {
	return fmt.Errorf("WriteBatchedObjs not implemented in MockBaseStore")
}

func (m *MockBaseStore) Close() error {
	return nil
}

func (m *MockBaseStore) PrimeObject(size int) (bobbob.ObjectId, error) {
	return m.NewObj(size)
}
