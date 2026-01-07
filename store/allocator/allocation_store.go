package allocator

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// AllocationRecord represents a single allocation entry persisted in the store.
type AllocationRecord struct {
	ID     ObjectId   `json:"id"`
	Offset FileOffset `json:"offset"`
	Size   int        `json:"size"`
}

// AllocationStore abstracts persistence of allocation metadata.
// Implementations may be backed by memory or disk.
type AllocationStore interface {
	Add(id ObjectId, offset FileOffset, size int) error
	Delete(id ObjectId) error
	Get(id ObjectId) (FileOffset, int, bool, error)
	// IterateAll calls fn for each record; iteration stops if fn returns false.
	IterateAll(fn func(id ObjectId, offset FileOffset, size int) bool) error
	// ReplaceAll atomically replaces all records with the provided set.
	ReplaceAll(records []AllocationRecord) error
}

// InMemoryAllocationStore is a simple in-memory implementation.
type InMemoryAllocationStore struct {
	mu    sync.RWMutex
	table map[ObjectId]allocatedRegion
}

func NewInMemoryAllocationStore() *InMemoryAllocationStore {
	return &InMemoryAllocationStore{table: make(map[ObjectId]allocatedRegion)}
}

func (s *InMemoryAllocationStore) Add(id ObjectId, offset FileOffset, size int) error {
	s.mu.Lock()
	s.table[id] = allocatedRegion{fileOffset: offset, size: size}
	s.mu.Unlock()
	return nil
}

func (s *InMemoryAllocationStore) Delete(id ObjectId) error {
	s.mu.Lock()
	delete(s.table, id)
	s.mu.Unlock()
	return nil
}

func (s *InMemoryAllocationStore) Get(id ObjectId) (FileOffset, int, bool, error) {
	s.mu.RLock()
	r, ok := s.table[id]
	s.mu.RUnlock()
	if !ok {
		return 0, 0, false, nil
	}
	return r.fileOffset, r.size, true, nil
}

func (s *InMemoryAllocationStore) IterateAll(fn func(id ObjectId, offset FileOffset, size int) bool) error {
	s.mu.RLock()
	for id, r := range s.table {
		if !fn(id, r.fileOffset, r.size) {
			break
		}
	}
	s.mu.RUnlock()
	return nil
}

func (s *InMemoryAllocationStore) ReplaceAll(records []AllocationRecord) error {
	s.mu.Lock()
	s.table = make(map[ObjectId]allocatedRegion, len(records))
	for _, rec := range records {
		s.table[rec.ID] = allocatedRegion{fileOffset: rec.Offset, size: rec.Size}
	}
	s.mu.Unlock()
	return nil
}

// FileAllocationStore persists allocation metadata as a JSON file.
// Format: map[ObjectId]AllocationRecord
type FileAllocationStore struct {
	mu     sync.Mutex
	path   string
	table  map[ObjectId]allocatedRegion
	loaded bool
	dirty  bool // true if table has changes not yet persisted
}

func NewFileAllocationStore(path string) *FileAllocationStore {
	return &FileAllocationStore{path: path}
}

const allocFileMagic = "ALST1" // simple magic header for binary format

func (s *FileAllocationStore) ensureLoaded() error {
	if s.loaded {
		return nil
	}
	tbl, err := s.readAllFromDisk()
	if err != nil {
		return err
	}
	s.table = tbl
	s.loaded = true
	return nil
}

// readAllFromDisk loads the table from disk, supporting both the new binary
// format and a legacy JSON map format for compatibility.
func (s *FileAllocationStore) readAllFromDisk() (map[ObjectId]allocatedRegion, error) {
	data := make(map[ObjectId]allocatedRegion)
	// If file doesn't exist, treat as empty
	if _, err := os.Stat(s.path); errors.Is(err, os.ErrNotExist) {
		return data, nil
	}
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	header := make([]byte, len(allocFileMagic))
	if _, err := br.Read(header); err != nil {
		return nil, err
	}
	if string(header) == allocFileMagic {
		// Binary format: [magic][count:uint32][records...]
		var count uint32
		if err := binary.Read(br, binary.LittleEndian, &count); err != nil {
			return nil, err
		}
		for i := uint32(0); i < count; i++ {
			var id int64
			var offset int64
			var size int32
			if err := binary.Read(br, binary.LittleEndian, &id); err != nil {
				return nil, err
			}
			if err := binary.Read(br, binary.LittleEndian, &offset); err != nil {
				return nil, err
			}
			if err := binary.Read(br, binary.LittleEndian, &size); err != nil {
				return nil, err
			}
			data[ObjectId(id)] = allocatedRegion{fileOffset: FileOffset(offset), size: int(size)}
		}
		return data, nil
	}

	// Legacy JSON fallback: map[string]AllocationRecord
	// Reset reader to start after failed magic check
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(f)
	var raw map[string]AllocationRecord
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	for k, rec := range raw {
		// Keys are decimal ObjectId values
		// Parse manually without fmt to avoid overhead
		var id int64
		// simple parse: iterate runes
		sign := int64(1)
		for i, r := range k {
			if i == 0 && r == '-' {
				sign = -1
				continue
			}
			if r < '0' || r > '9' {
				// skip malformed
				id = 0
				sign = 0
				break
			}
			id = id*10 + int64(r-'0')
		}
		if sign == 0 {
			continue
		}
		id *= sign
		data[ObjectId(id)] = allocatedRegion{fileOffset: rec.Offset, size: rec.Size}
	}
	return data, nil
}

func (s *FileAllocationStore) writeAllToDisk(data map[ObjectId]allocatedRegion) error {
	// Ensure directory exists
	if dir := filepath.Dir(s.path); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	// Write magic
	if _, err := bw.Write([]byte(allocFileMagic)); err != nil {
		_ = f.Close()
		return err
	}
	// Write count
	var count uint32 = uint32(len(data))
	if err := binary.Write(bw, binary.LittleEndian, count); err != nil {
		_ = f.Close()
		return err
	}
	// Write records
	for id, r := range data {
		if err := binary.Write(bw, binary.LittleEndian, int64(id)); err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, int64(r.fileOffset)); err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(bw, binary.LittleEndian, int32(r.size)); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (s *FileAllocationStore) Add(id ObjectId, offset FileOffset, size int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return err
	}
	s.table[id] = allocatedRegion{fileOffset: offset, size: size}
	s.dirty = true
	return nil
}

func (s *FileAllocationStore) Delete(id ObjectId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return err
	}
	delete(s.table, id)
	s.dirty = true
	return nil
}

// BatchAdd adds multiple entries efficiently without persisting after each one.
func (s *FileAllocationStore) BatchAdd(entries map[ObjectId]allocatedRegion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return err
	}
	for id, region := range entries {
		s.table[id] = region
	}
	if len(entries) > 0 {
		s.dirty = true
	}
	return nil
}

// Flush writes any pending changes to disk.
func (s *FileAllocationStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	if err := s.ensureLoaded(); err != nil {
		return err
	}
	err := s.writeAllToDisk(s.table)
	if err == nil {
		s.dirty = false
	}
	return err
}

func (s *FileAllocationStore) Get(id ObjectId) (FileOffset, int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return 0, 0, false, err
	}
	r, ok := s.table[id]
	if !ok {
		return 0, 0, false, nil
	}
	return r.fileOffset, r.size, true, nil
}

func (s *FileAllocationStore) IterateAll(fn func(id ObjectId, offset FileOffset, size int) bool) error {
	s.mu.Lock()
	if err := s.ensureLoaded(); err != nil {
		s.mu.Unlock()
		return err
	}
	for id, r := range s.table {
		if !fn(id, r.fileOffset, r.size) {
			break
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *FileAllocationStore) ReplaceAll(records []AllocationRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.table = make(map[ObjectId]allocatedRegion, len(records))
	for _, rec := range records {
		s.table[rec.ID] = allocatedRegion{fileOffset: rec.Offset, size: rec.Size}
	}
	s.loaded = true
	return s.writeAllToDisk(s.table)
}
