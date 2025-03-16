package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)
var ObjNotWritten = ObjectId(-1)
var ObjNotAllocated = ObjectId(-2)
var objNotPreAllocated = ObjectId(-3)
// Finisher is a callback that must be called when you have finished
// With the request in question
type Finisher func() error
// ObjReader is an interface for getting an io.Reader for an object
type ObjReader interface {
	LateReadObj(id ObjectId) (io.Reader, error)
}
type ObjWriter interface {
	LateWriteNewObj(size int) (ObjectId, io.Writer, error)
	WriteToObj(objectId ObjectId) (io.Writer, Finisher, error)
	WriteBytesToObj(data []byte, objectId ObjectId) error
	WriteNewObjFromBytes(data []byte) (ObjectId, error)
}
type Storer interface {
	NewObj(size int) (ObjectId, error)
	DeleteObj(objId ObjectId) error
	Close() error
	ObjReader
	ObjWriter
}

type store struct {
	filePath  string
	file      *os.File
	objectMap *ObjectMap
	allocator *BasicAllocator
}

type ObjectInfo struct {
	Offset    FileOffset
	Size      int
}

// Helper function to check if the file is initialized
func (s *store) checkFileInitialized() error {
	if s.file == nil {
		return errors.New("store is not initialized")
	}
	return nil
}

// Helper function to update the initial offset in the file
func (s *store) updateInitialOffset(fileOffset FileOffset) error {
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

// NewStore creates a new Store and initializes it with a basic ObjectMap
func NewStore(filePath string) (*store, error) {
	if _, err := os.Stat(filePath); err == nil {
		return LoadStore(filePath)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Initialize the Store
	store := &store{
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

func LoadStore(filePath string) (*store, error) {
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
	data := make([]byte, 1024) // Assuming the ObjectMap is not larger than 1024 bytes
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
	store := &store{
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


// NewObj is the externally visible interface when you want to just allocate an object
// But you don't want to write to it immediately
func (s *store) NewObj(size int) (ObjectId, error) {
	if s.file == nil {
		return 0, errors.New("store is not initialized")
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
func (s *store) LateWriteNewObj(size int) (ObjectId, io.Writer, error) {
	if s.file == nil {
		return 0, nil, errors.New("store is not initialized")
	}

	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return 0, nil, err
	}
	fmt.Println("Writing new object at offset", fileOffset)

	// Create a writer that writes to the file
	writer := io.Writer(s.file)

	s.objectMap.Set(objId, ObjectInfo{Offset: fileOffset, Size: size})

	return objId, writer, nil
}
// WriteNewObjFromBytes writes a new object to the store from a byte slice
// This is useful when you have the data in memory and you want to write it to the store
func (s *store) WriteNewObjFromBytes(data []byte) (ObjectId, error) {
	size := len(data)
	if s.file == nil {
		return 0, errors.New("store is not initialized")
	}

	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return 0, err
	}

	// Write the data to the file
	_, err = s.file.WriteAt(data, int64(fileOffset))
	if err != nil {
		return 0, err
	}

	s.objectMap.Set(objId, ObjectInfo{Offset: fileOffset, Size: size})

	return objId, nil
}
// WriteToObj writes to an existing object in the store
// This is something that should be done with extreme caution and avoided where possible
// Only one writer should be allowed to write to an object at a time
// If multiple writers try to write, then they (FIXME will be one implemented) queued
func (s *store) WriteToObj(objectId ObjectId) (io.Writer, Finisher, error) {
	if s.file == nil {
		return nil, nil, errors.New("store is not initialized")
	}
	// Grabbing a full lock because (unless errors) we are modifying
	obj, found := s.objectMap.Get(objectId)
	if !found {
		return nil, nil, errors.New("object not found")
	}
	writer := NewSectionWriter(s.file, int64(obj.Offset), int64(obj.Size))
	return writer, s.createCloser(objectId), nil
} 
// WriteBytesToObj writes a byte slice to an existing object in the store
// It is preferred to create a new object, write to it and then delete the old object
// This allows for less risk of file corruption
func (s *store) WriteBytesToObj(data []byte, objectId ObjectId) error {
	writer, closer, err := s.WriteToObj(objectId)
	if err != nil {
		return err
	}
	defer closer()
	
	n, err := writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("did not write all the data")
	}
	return nil
}
func (s *store) createCloser(objectId ObjectId) func() error {
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
func (s *store) LateReadObj(offset ObjectId) (io.Reader, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, err
	}
	obj, found := s.objectMap.Get(offset)
	if !found {
		return nil, errors.New("object not found")
	}
	// For now they are always the same but we will implement a mapping in the future
	fileOffset := FileOffset(obj.Offset)
	reader := io.NewSectionReader(s.file, int64(fileOffset), int64(obj.Size))
	return reader, nil
}
func (s *store) DeleteObj(objId ObjectId) error {
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
func (s *store) Sync() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	return s.file.Sync()
}

// Close closes the store and writes the ObjectMap to disk
func (s *store) Close() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	data, err := s.objectMap.Marshal()
	if err != nil {
		return err
	}

	objId, err := s.WriteNewObjFromBytes(data)
	if err != nil {
		return err
	}

	// Update the first object in the store with the offset of the serialized ObjectMap
	if err := s.updateInitialOffset(FileOffset(objId)); err != nil {
		return err
	}

	return s.file.Close()
}
