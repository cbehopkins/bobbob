package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"os"
)

type ObjectInfo struct {
	Offset int64
	Size   int
}

type ObjectMap map[int64]ObjectInfo

func (om ObjectMap) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(om)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (om *ObjectMap) Deserialize(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(om)
}

type Store struct {
	filePath  string
	file      *os.File
	objectMap ObjectMap
}

// Helper function to check if the file is initialized
func (s *Store) checkFileInitialized() error {
	if s.file == nil {
		return errors.New("store is not initialized")
	}
	return nil
}

// Helper function to handle seeking to a specific offset
func (s *Store) seekToOffset(offset int64) error {
	_, err := s.file.Seek(offset, io.SeekStart)
	return err
}

// Helper function to write data to the file
func (s *Store) writeData(data []byte) (int64, io.Writer, error) {
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
func (s *Store) updateInitialOffset(offset int64) error {
	if err := s.seekToOffset(0); err != nil {
		return err
	}

	offsetBytes := make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		offsetBytes[i] = byte(offset >> (i * 8))
	}

	if _, err := s.file.Write(offsetBytes); err != nil {
		return err
	}

	return nil
}

func NewBob(filePath string) (*Store, error) {
	if _, err := os.Stat(filePath); err == nil {
		return LoadStore(filePath)
	}

	file, err := os.Create(filePath)
	if (err != nil) {
		return nil, err
	}

	// Initialize the Store
	store := &Store{
		filePath:  filePath,
		file:      file,
		objectMap: make(ObjectMap),
	}

	// Write the initial offset (zero) to the store
	_, err = file.Write(make([]byte, 8)) // 8 bytes for int64
	if err != nil {
		return nil, err
	}

	// Ensure the initial offset is written to the disk
	err = file.Sync()
	if err != nil {
		return nil, err
	}
	store.objectMap[0] = ObjectInfo{Offset: 0, Size: 8}	

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
	err = objectMap.Deserialize(data[:n])
	if err != nil {
		return nil, err
	}

	// Initialize the Store
	store := &Store{
		filePath:  filePath,
		file:      file,
		objectMap: objectMap,
	}

	return store, nil
}

func (s *Store) WriteNewObj(size int) (int64, io.Writer, error) {
	if s.file == nil {
		return 0, nil, errors.New("store is not initialized")
	}

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nil, err
	}

	// Create a writer that writes to the file
	writer := io.Writer(s.file)

	// Add the object info to the map
	s.objectMap[offset] = ObjectInfo{Offset: offset, Size: size}

	return offset, writer, nil
}

func (s *Store) WriteToObj(offset int64) (io.Writer, error) {
    if s.file == nil {
        return nil, errors.New("store is not initialized")
    }

    _, found := s.objectMap[offset]
    if !found {
        return nil, errors.New("object not found")
    }

    // Create an OffsetWriter that writes to the specified offset
    writer := io.NewOffsetWriter(s.file, offset)

    return writer, nil
}

func (s *Store) ReadObj(offset int64) (io.Reader, error) {
	if err := s.checkFileInitialized(); err != nil {
		return nil, err
	}

	obj, found := s.objectMap[offset]
	if !found {
		return nil, errors.New("object not found")
	}

	if err := s.seekToOffset(offset); err != nil {
		return nil, err
	}

	// Create a limited reader that reads up to the size of the object
	reader := io.LimitReader(s.file, int64(obj.Size))

	return reader, nil
}

func (s *Store) Close() error {
	if err := s.checkFileInitialized(); err != nil {
		return err
	}

	// Serialize the ObjectMap
	data, err := s.objectMap.Serialize()
	if err != nil {
		return err
	}

	// Write the serialized ObjectMap to the store
	offset, _, err := s.writeData(data)
	if err != nil {
		return err
	}

	// Update the first object in the store with the offset of the serialized ObjectMap
	if err := s.updateInitialOffset(offset); err != nil {
		return err
	}

	return s.file.Close()
}