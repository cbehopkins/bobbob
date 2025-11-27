package store

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

var errStoreNotInitialized = errors.New("store is not initialized")

type baseStore struct {
	mu        sync.RWMutex // Protects file operations and store state
	filePath  string
	file      *os.File
	objectMap *ObjectMap
	allocator Allocator
	closed    bool // Track if store is closed
}

// NewBasicStore creates a new baseStore at the given file path.
// If the file already exists, it loads the existing store.
// Otherwise, it creates a new file and initializes an empty store.
func NewBasicStore(filePath string) (*baseStore, error) {
	if _, err := os.Stat(filePath); err == nil {
		return LoadBaseStore(filePath)
	}
	pointerSize := 8
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	allocator, err := NewBasicAllocator(file)
	if err != nil {
		return nil, err
	}
	allocator.end = int64(pointerSize)
	// Initialize the Store
	store := &baseStore{
		filePath:  filePath,
		file:      file,
		objectMap: NewObjectMap(),
		allocator: allocator,
	}
	store.objectMap.Set(0, ObjectInfo{Offset: 0, Size: pointerSize})

	// Write the initial offset (zero) to the store
	_, err = file.Write(make([]byte, pointerSize))
	if err != nil {
		return nil, err
	}

	return store, nil
}

// LoadBaseStore loads an existing baseStore from the given file path.
// It reads the object map from the file and initializes the allocator.
func LoadBaseStore(filePath string) (*baseStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	// Read the initial offset
	var initialOffset int64
	err = binary.Read(file, binary.LittleEndian, &initialOffset)
	if err != nil {
		return nil, err
	}

	// Seek to the initial offset
	_, err = file.Seek(initialOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Deserialize the ObjectMap directly from the file
	objectMap := NewObjectMap()
	err = objectMap.Deserialize(file)
	if err != nil {
		return nil, err
	}
	allocator := &BasicAllocator{
		end: initialOffset,
	}
	// Initialize the Store
	store := &baseStore{
		filePath:  filePath,
		file:      file,
		objectMap: objectMap,
		allocator: allocator,
	}

	// FIXME this should be done in its own go routine
	err = allocator.RefreshFreeList(store)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// Helper function to check if the file is initialized
func (s *baseStore) checkFileInitialized() error {
	// Caller should hold lock
	if s.file == nil {
		return errStoreNotInitialized
	}
	if s.closed {
		return errors.New("store is closed")
	}
	return nil
}

// Helper function to update the initial offset in the file
// Caller must hold the write lock
func (s *baseStore) updateInitialOffset(fileOffset FileOffset) error {
	objOffset := int64(fileOffset)
	offsetBytes := make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		offsetBytes[i] = byte(objOffset >> (i * 8))
	}

	if _, err := s.file.WriteAt(offsetBytes, 0); err != nil {
		return err
	}

	return nil
}

// NewObj is a convenience wrapper around LateWriteNewObj that allocates an object
// without returning a writer. Use this when you want to allocate an object but
// will write to it later via WriteToObj.
func (s *baseStore) NewObj(size int) (ObjectId, error) {
	objId, _, finisher, err := s.LateWriteNewObj(size)
	if err != nil {
		return 0, err
	}
	if finisher != nil {
		finisher() // Close immediately since we're not using the writer
	}
	return objId, nil
}

// LateWriteNewObj is the fundamental allocation method that allocates an object
// and returns a writer for immediate use. This is the primitive operation;
// NewObj is a convenience wrapper around this method.
func (s *baseStore) LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkFileInitialized(); err != nil {
		return ObjNotAllocated, nil, nil, err
	}

	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return ObjNotAllocated, nil, nil, err
	}
	// Create a section writer that writes to the correct offset in the file
	writer := NewSectionWriter(s.file, int64(fileOffset), int64(size))

	s.objectMap.Set(objId, ObjectInfo{Offset: fileOffset, Size: size})

	return objId, writer, nil, nil
}

// WriteToObj is a Late method that returns a writer for an existing object.
// This should be done with extreme caution and avoided where possible.
// Prefer creating a new object, writing to it, then deleting the old one.
// Only one writer should be allowed to write to an object at a time.
// If multiple writers try to write, then they (FIXME will be one implemented) queued.
func (s *baseStore) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.checkFileInitialized(); err != nil {
		return nil, nil, err
	}
	// Grabbing a full lock because (unless errors) we are modifying
	obj, found := s.objectMap.Get(objectId)
	if !found {
		return nil, nil, errors.New("object not found")
	}
	writer := NewSectionWriter(s.file, int64(obj.Offset), int64(obj.Size))
	return writer, s.createCloser(objectId), nil
}

func (s *baseStore) createCloser(objectId ObjectId) func() error {
	return func() error {
		_, found := s.objectMap.Get(objectId)
		if !found { // This should never happen
			return errors.New("object not found in the closer. This is very bad!")
		}
		return nil
	}
}

// LateReadObj is the fundamental read method that returns a reader for streaming access.
// This is the primitive operation; ReadBytesFromObj is a convenience wrapper.
func (s *baseStore) LateReadObj(objId ObjectId) (io.Reader, Finisher, error) {
	if !IsValidObjectId(objId) {
		return nil, nil, errors.New("invalid objectId")
	}
	return s.lateReadObj(objId)
}

func (s *baseStore) lateReadObj(objId ObjectId) (io.Reader, Finisher, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.checkFileInitialized(); err != nil {
		return nil, nil, err
	}
	obj, found := s.objectMap.Get(objId)
	if !found {
		return nil, nil, errors.New("object not found")
	}
	// For now they are always the same but we will implement a mapping in the future
	fileOffset := FileOffset(obj.Offset)
	reader := io.NewSectionReader(s.file, int64(fileOffset), int64(obj.Size))
	return reader, s.createCloser(objId), nil
}

func (s *baseStore) DeleteObj(objId ObjectId) error {
	// FIXME Complex types have one object that points to others
	// We need a method that lists all the objects that are part of a complex object
	// And then we recursively delete them
	// However we canly do that on the Unmarshalled type - so we do not have that information here
	// Therefore complex Objects need to implement a delete method
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	_, found := s.objectMap.GetAndDelete(objId, func(obj ObjectInfo) {
		s.allocator.Free(obj.Offset, obj.Size)
	})
	if !found {
		return errors.New("object not found")
	}

	return nil
}

// Sync flushes the file to disk
func (s *baseStore) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	return s.file.Sync()
}

// Close closes the store and writes the ObjectMap to disk
func (s *baseStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	// Mark as closed to prevent further operations
	s.closed = true

	data, err := s.objectMap.Marshal()
	if err != nil {
		return err
	}
	// We need a special method here to allocate LAST item in the file
	// This is because we need to write the object map to the end of the file
	_, fileOffset, err := s.allocator.Allocate(len(data))
	if err != nil {
		return err
	}

	n, err := s.file.WriteAt(data, int64(fileOffset))
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("did not write all the data")
	}

	// Update the first object in the store with the offset of the serialized ObjectMap
	if err := s.updateInitialOffset(FileOffset(fileOffset)); err != nil {
		return err
	}

	err = s.file.Sync()
	if err != nil {
		return err
	}
	return s.file.Close()
}
