package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
)

// ObjReader is an interface for getting an io.Reader for an object
type ObjReader interface {
	ReadObj(id ObjectId) (io.Reader, error)
}
type Store struct {
	filePath      string
	file          *os.File
	objectMap     ObjectMap
	objectMapLock sync.RWMutex
	allocator     *Allocator
}

// Helper function to check if the file is initialized
func (s *Store) checkFileInitialized() error {
	if s.file == nil {
		return errors.New("store is not initialized")
	}
	return nil
}

// Helper function to write data to the file
func (s *Store) writeData(data []byte) (ObjectId, io.Writer, error) {
	offset, writer, err := s.WriteNewObj(len(data))
	if err != nil {
		return 0, nil, err
	}

	if _, err := writer.Write(data); err != nil {
		return 0, nil, err
	}

	return offset, writer, nil
}

// Helper function to update the initial offset in the file
func (s *Store) updateInitialOffset(fileOffset FileOffset) error {
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
func NewStore(filePath string) (*Store, error) {
	if _, err := os.Stat(filePath); err == nil {
		return LoadStore(filePath)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Initialize the Store
	store := &Store{
		filePath:  filePath,
		file:      file,
		objectMap: make(ObjectMap),
		allocator: &Allocator{},
	}

	// Write the initial offset (zero) to the store
	_, err = file.Write(make([]byte, 8)) // 8 bytes for int64
	if err != nil {
		return nil, err
	}
	store.objectMap[0] = ObjectInfo{Offset: 0, Size: 8}
	store.allocator.end = 8
	return store, nil
}

func LoadStore(filePath string) (*Store, error) {
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
	objectMap := make(ObjectMap)
	err = Unmarshal(data[:n], &objectMap)
	if err != nil {
		return nil, err
	}
	allocator := &Allocator{
		end: initialOffset,
	}
	// Initialize the Store
	store := &Store{
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

func (s *Store) WriteNewObj(size int) (ObjectId, io.Writer, error) {
	if s.file == nil {
		return 0, nil, errors.New("store is not initialized")
	}

	// offset, err := s.file.Seek(0, io.SeekEnd)
	objId, fileOffset, err := s.allocator.Allocate(size)
	if err != nil {
		return 0, nil, err
	}
	fmt.Println("Writing new object at offset", fileOffset)

	// Create a writer that writes to the file
	writer := io.Writer(s.file)

	// A New Object is always allocated end of the current file
	// FIXME we should actually maintain a free list and use that but...
	s.objectMapLock.Lock()
	s.objectMap[objId] = ObjectInfo{Offset: fileOffset, Size: size}
	s.objectMapLock.Unlock()

	return objId, writer, nil
}

// WriteToObj writes to an existing object in the store
// This is something that should be done with extreme caution and avoided where possible
// Only one writer should be allowed to write to an object at a time
// If multiple writers try to write, then they (will be one implemented) queued
func (s *Store) WriteToObj(objectId ObjectId) (io.Writer, func() error, error) {
	if s.file == nil {
		return nil, nil, errors.New("store is not initialized")
	}
	// Grabbing a full lock because (unless errors) we are modifying
	s.objectMapLock.Lock()
	obj, found := s.objectMap[objectId]
	s.objectMapLock.Unlock()
	if !found {
		return nil, nil, errors.New("object not found")
	}
	writer := NewSectionWriter(s.file, int64(obj.Offset), int64(obj.Size))
	return writer, s.createCloser(objectId), nil
}

func (s *Store) createCloser(objectId ObjectId) func() error {
	return func() error {
		defer func() {
			err := s.file.Sync()
			if err != nil {
				panic("Error syncing file: " + err.Error())
			}
		}()
		s.objectMapLock.RLock()
		defer s.objectMapLock.RUnlock()
		_, found := s.objectMap[objectId]
		if !found { // This should never happen
			return errors.New("object not found in the closer. This is very bad!")
		}
		return nil
	}
}
func (s *Store) ReadObj(offset ObjectId) (io.Reader, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, err
	}
	s.objectMapLock.RLock()
	obj, found := s.objectMap[offset]
	s.objectMapLock.RUnlock()
	if !found {
		return nil, errors.New("object not found")
	}
	// For now they are always the same but we will implement a mapping in the future
	fileOffset := FileOffset(obj.Offset)
	reader := io.NewSectionReader(s.file, int64(fileOffset), int64(obj.Size))
	return reader, nil
}
func (s *Store) DeleteObj(objId ObjectId) error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	s.objectMapLock.Lock()
	defer s.objectMapLock.Unlock()
	obj, found := s.objectMap[objId]
	if !found {
		return errors.New("object not found")
	}

	s.allocator.Free(obj.Offset, obj.Size)
	delete(s.objectMap, objId)
	return nil
}

// Sync flushes the file to disk
func (s *Store) Sync() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	return s.file.Sync()
}

// Close closes the store and writes the ObjectMap to disk
func (s *Store) Close() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}
	s.objectMapLock.RLock()

	data, err := Marshal(s.objectMap)
	s.objectMapLock.RUnlock()
	if err != nil {
		return err
	}

	objId, _, err := s.writeData(data)
	if err != nil {
		return err
	}

	// Update the first object in the store with the offset of the serialized ObjectMap
	if err := s.updateInitialOffset(FileOffset(objId)); err != nil {
		return err
	}

	return s.file.Close()
}

// FindGaps returns a channel that yields gaps in the objectMap
func (s *Store) FindGaps() <-chan Gap {
	gapChan := make(chan Gap)

	go func() {
		defer close(gapChan)

		s.objectMapLock.RLock()
		defer s.objectMapLock.RUnlock()

		// Collect all object offsets and sizes
		var offsets []int64
		for _, obj := range s.objectMap {
			offsets = append(offsets, int64(obj.Offset))
		}

		// Sort the offsets
		slices.Sort(offsets)

		// Find gaps between objects
		for i := 1; i < len(offsets); i++ {
			prevObj := s.objectMap[ObjectId(offsets[i-1])]
			currOffset := offsets[i]

			prevEnd := int64(prevObj.Offset) + int64(prevObj.Size)
			if currOffset > prevEnd {
				gapChan <- Gap{Start: prevEnd, End: currOffset}
			}
		}
	}()

	return gapChan
}
