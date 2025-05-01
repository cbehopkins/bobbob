package store

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

var errStoreNotInitialized = errors.New("store is not initialized")

type baseStore struct {
	filePath  string
	file      *os.File
	objectMap *ObjectMap
	allocator *BasicAllocator
}
// NewBasicStore creates a new Store and initializes it with a basic ObjectMap
func NewBasicStore(filePath string) (*baseStore, error) {
	if _, err := os.Stat(filePath); err == nil {
		return LoadBaseStore(filePath)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Initialize the Store
	store := &baseStore{
		filePath:  filePath,
		file:      file,
		objectMap: NewObjectMap(),
		allocator: &BasicAllocator{},
	}

	// Write the initial offset (zero) to the store
	_, err = file.Write(make([]byte, 8)) // 8 bytes for int64
	if err != nil {
		return nil, err
	}
	store.objectMap.Set(0, ObjectInfo{Offset: 0, Size: 8})
	store.allocator.end = 8
	return store, nil
}

func LoadBaseStore(filePath string) (*baseStore, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
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

	// Read the serialized ObjectMap
	// FIXME this assumption on the size is deeply flawed
	data := make([]byte, 4096) // Assuming the ObjectMap is not larger than 1024 bytes
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// Deserialize the ObjectMap
	objectMap := NewObjectMap()
	err = objectMap.Unmarshal(data[:n])
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
	if s.file == nil {
		return errStoreNotInitialized
	}
	return nil
}

// Helper function to update the initial offset in the file
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

// NewObj is the externally visible interface when you want to just allocate an object
// But you don't want to write to it immediately
func (s *baseStore) NewObj(size int) (ObjectId, error) {
	if err := s.checkFileInitialized(); err != nil {
		return 0, err
	}
	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return 0, err
	}
	objInfo := &ObjectInfo{Offset: fileOffset, Size: size}
	s.objectMap.Set(objId, *objInfo)

	return objId, nil
}

// LateWriteNewObj allows you to allocate an object and write to it later
// This is useful when you want to write a large object and you don't want to keep it in memory
func (s *baseStore) LateWriteNewObj(size int) (ObjectId, io.Writer, Finisher, error) {
	if err := s.checkFileInitialized(); err != nil {
		return ObjNotAllocated, nil, nil, err
	}

	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return ObjNotAllocated, nil, nil, err
	}
	// Create a writer that writes to the file
	writer := io.Writer(s.file)

	s.objectMap.Set(objId, ObjectInfo{Offset: fileOffset, Size: size})

	return objId, writer, nil, nil
}

// WriteToObj writes to an existing object in the store
// This is something that should be done with extreme caution and avoided where possible
// Only one writer should be allowed to write to an object at a time
// If multiple writers try to write, then they (FIXME will be one implemented) queued
func (s *baseStore) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
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

// LateReadObj reads an object from the store
// Returns a reader so as to not force large objects into memory
func (s *baseStore) LateReadObj(offset ObjectId) (io.Reader, Finisher, error) {
	if !IsValidObjectId(offset) {
		return nil, nil, errors.New("invalid objectId")
	}
	return s.lateReadObj(offset)
}

func (s *baseStore) lateReadObj(offset ObjectId) (io.Reader, Finisher, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, nil, err
	}
	obj, found := s.objectMap.Get(offset)
	if !found {
		return nil, nil, errors.New("object not found")
	}
	// For now they are always the same but we will implement a mapping in the future
	fileOffset := FileOffset(obj.Offset)
	reader := io.NewSectionReader(s.file, int64(fileOffset), int64(obj.Size))
	return reader, s.createCloser(offset), nil
}
func (s *baseStore) DeleteObj(objId ObjectId) error {
	// FIXME Complex types have one object that points to others
	// We need a method that lists all the objects that are part of a complex object
	// And then we recursively delete them
	// However we canly do that on the Unmarshalled type - so we do not have that information here
	// Therefore complex Objects need to implement a delete method
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
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	return s.file.Sync()
}

// Close closes the store and writes the ObjectMap to disk
func (s *baseStore) Close() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
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