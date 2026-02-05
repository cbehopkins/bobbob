package store

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/cbehopkins/bobbob"
)

// MockStore is an in-memory implementation of store.Storer for fast testing.
// It uses Go maps instead of disk I/O, making tests 10-100x faster.
//
// Use MockStore for:
// - Unit tests focused on logic (not persistence)
// - Vault tests that don't need disk serialization
// - Fast iteration during development
//
// Use real stores (NewBasicStore/NewMultiStore) for:
// - Integration tests
// - Persistence/serialization tests
// - Benchmark tests measuring disk I/O
type MockStore struct {
	objects map[ObjectId][]byte
	nextId  ObjectId
	mu      sync.RWMutex
	closed  bool
}

// NewMockStore creates a new in-memory mock store.
func NewMockStore() *MockStore {
	return &MockStore{
		objects: make(map[ObjectId][]byte),
		nextId:  ObjectId(76), // Start after PrimeTable (3 slots * 24 + 4 byte header = 76 bytes)
	}
}

// NewObj allocates a new object of the given size and returns its ID.
func (m *MockStore) NewObj(size int) (ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return bobbob.ObjNotAllocated, io.ErrClosedPipe
	}

	objId := m.nextId
	m.nextId++
	m.objects[objId] = make([]byte, size)
	return objId, nil
}

// DeleteObj removes the object with the given ID.
func (m *MockStore) DeleteObj(objId ObjectId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return io.ErrClosedPipe
	}

	if _, exists := m.objects[objId]; !exists {
		return io.ErrUnexpectedEOF
	}

	delete(m.objects, objId)
	return nil
}

// Close marks the store as closed. Further operations will fail.
func (m *MockStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}

// PrimeObject returns a dedicated ObjectId for application metadata.
// Align with real stores by using the PrimeTable-derived start offset.
func (m *MockStore) PrimeObject(size int) (ObjectId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return bobbob.ObjNotAllocated, io.ErrClosedPipe
	}

	// PrimeTable size: 4 byte header + 3 slots * 24 bytes = 76 bytes
	primeObjectId := ObjectId(76)

	// Check if prime object already exists
	if _, exists := m.objects[primeObjectId]; exists {
		return primeObjectId, nil
	}

	// Allocate prime object
	m.objects[primeObjectId] = make([]byte, size)

	// Ensure nextId is past the prime object
	if m.nextId <= primeObjectId {
		m.nextId = primeObjectId + 1
	}

	return primeObjectId, nil
}

// LateReadObj returns a reader for the object with the given ID.
func (m *MockStore) LateReadObj(id ObjectId) (io.Reader, bobbob.Finisher, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, nil, io.ErrClosedPipe
	}

	data, exists := m.objects[id]
	if !exists {
		return nil, nil, io.ErrUnexpectedEOF
	}

	// Return a copy to avoid race conditions when reader is used outside lock
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	reader := bytes.NewReader(dataCopy)
	finisher := func() error { return nil }
	return reader, finisher, nil
}

// LateWriteNewObj allocates a new object and returns a writer for it.
func (m *MockStore) LateWriteNewObj(size int) (ObjectId, io.Writer, bobbob.Finisher, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return bobbob.ObjNotAllocated, nil, nil, io.ErrClosedPipe
	}

	objId := m.nextId
	m.nextId++
	m.objects[objId] = make([]byte, size)

	// Create a buffer writer that writes directly to our map
	writer := &mockWriter{
		store:  m,
		objId:  objId,
		buffer: bytes.NewBuffer(m.objects[objId][:0]),
	}

	finisher := func() error { return nil }
	return objId, writer, finisher, nil
}

// WriteToObj returns a writer for an existing object.
func (m *MockStore) WriteToObj(objectId ObjectId) (io.Writer, bobbob.Finisher, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, nil, io.ErrClosedPipe
	}

	data, exists := m.objects[objectId]
	if !exists {
		return nil, nil, io.ErrUnexpectedEOF
	}

	// Create a buffer writer
	writer := &mockWriter{
		store:  m,
		objId:  objectId,
		buffer: bytes.NewBuffer(data[:0]),
	}

	finisher := func() error { return nil }
	return writer, finisher, nil
}

// GetObjectInfo returns the ObjectInfo for a given ObjectId.
// This is used by store composition helpers.
func (m *MockStore) GetObjectInfo(objId ObjectId) (ObjectInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, exists := m.objects[objId]
	if !exists {
		return ObjectInfo{}, false
	}

	// MockStore doesn't have real file offsets, so we use objId as fake offset
	return ObjectInfo{
		Offset: FileOffset(objId),
		Size:   len(data),
	}, true
}

// WriteBatchedObjs writes multiple objects in a batch for efficiency.
// For MockStore, this extracts each object's data from the combined data slice using sizes.
func (m *MockStore) WriteBatchedObjs(objIds []ObjectId, data []byte, sizes []int) error {
	if len(objIds) != len(sizes) {
		return errors.New("objIds and sizes slices must have the same length")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	offset := 0
	for i, objId := range objIds {
		if _, exists := m.objects[objId]; !exists {
			return errors.New("invalid object ID")
		}

		size := sizes[i]
		if offset+size > len(data) {
			return errors.New("data slice too short for given sizes")
		}

		m.objects[objId] = make([]byte, size)
		copy(m.objects[objId], data[offset:offset+size])
		offset += size
	}

	return nil
}

// mockWriter implements io.Writer for MockStore objects.
type mockWriter struct {
	store  *MockStore
	objId  ObjectId
	buffer *bytes.Buffer
}

func (w *mockWriter) Write(p []byte) (n int, err error) {
	// Write to buffer
	n, err = w.buffer.Write(p)
	if err != nil {
		return n, err
	}

	// Update the store's copy
	w.store.mu.Lock()
	w.store.objects[w.objId] = w.buffer.Bytes()
	w.store.mu.Unlock()

	return n, nil
}
