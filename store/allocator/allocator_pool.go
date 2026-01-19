package allocator

import (
	"errors"
	"io"
	"os"

	"github.com/cbehopkins/bobbob/internal"
)

// allocatorRef wraps a blockAllocator with its persistent storage management.
type allocatorRef struct {
	allocator *blockAllocator
	// File Offset that this allocator is stored at in the store
	// Offset of <1 means not yet persisted
	fileOff FileOffset
	synced  bool
	// File handle for direct I/O operations
	file *os.File
	// Metadata for lazy loading (set during UnmarshalMultiple)
	blockSize  int // blockSize of the allocator (needed for lazy load)
	blockCount int // blockCount of the allocator (needed for lazy load)
}

// ensureLoaded lazily loads the allocator from disk if it hasn't been loaded yet.
// This is called transparently when accessing the allocator.
func (r *allocatorRef) ensureLoaded() error {
	if r.allocator != nil {
		return nil // Already loaded
	}
	if r.fileOff < 1 {
		return errors.New("allocatorRef not persisted")
	}
	if r.file == nil {
		return errors.New("no file handle for lazy load")
	}

	// Calculate size and read from disk
	bitCount := (r.blockCount + 7) / 8
	allocatorDataSize := 8 + bitCount + 2*r.blockCount
	allocData := make([]byte, allocatorDataSize)

	n, err := r.file.ReadAt(allocData, int64(r.fileOff))
	if err != nil {
		return err
	}
	if n != allocatorDataSize {
		return errors.New("incomplete read from file")
	}

	// Unmarshal the block allocator
	allocator := &blockAllocator{
		blockSize:      r.blockSize,
		blockCount:     r.blockCount,
		allocatedList:  make([]bool, r.blockCount),
		requestedSizes: make([]int, r.blockCount),
	}
	if err := allocator.Unmarshal(allocData); err != nil {
		return err
	}

	r.allocator = allocator
	return nil
}

// sizeInBytes returns the number of bytes required to marshal this allocatorRef.
// Not our normal signature for this - but that's fine for now.
func (r *allocatorRef) sizeInBytes(blockCount int) int {
	bitCount := (blockCount + 7) / 8
	return 8 + bitCount + 2*blockCount
}

// Delegation methods for allocatorRef to blockAllocator methods
func (r *allocatorRef) Allocate(size int) (ObjectId, FileOffset, error) {
	if err := r.ensureLoaded(); err != nil {
		return 0, 0, err
	}
	return r.allocator.Allocate(size)
}

func (r *allocatorRef) AllocateRun(size int, count int) ([]ObjectId, []FileOffset, error) {
	if err := r.ensureLoaded(); err != nil {
		return nil, nil, err
	}
	return r.allocator.AllocateRun(size, count)
}

func (r *allocatorRef) Free(fileOffset FileOffset, size int) error {
	if err := r.ensureLoaded(); err != nil {
		return err
	}
	return r.allocator.Free(fileOffset, size)
}

func (r *allocatorRef) ContainsObjectId(objId ObjectId) bool {
	if r == nil {
		return false
	}
	if err := r.ensureLoaded(); err != nil {
		return false
	}
	return r.allocator.ContainsObjectId(objId)
}

func (r *allocatorRef) GetFileOffset(objId ObjectId) (FileOffset, error) {
	if err := r.ensureLoaded(); err != nil {
		return 0, err
	}
	return r.allocator.GetFileOffset(objId)
}

func (r *allocatorRef) Marshal() ([]byte, error) {
	if err := r.ensureLoaded(); err != nil {
		return nil, err
	}
	return r.allocator.Marshal()
}

// GetObjectInfo checks if this allocator contains the ObjectId and returns its metadata.
// Returns FileOffset and size if found, or an error if not found or invalid.
func (r *allocatorRef) GetObjectInfo(objId ObjectId, blockSize int) (FileOffset, int, error) {
	if r == nil {
		return 0, 0, errors.New("allocatorRef is nil")
	}
	if !r.ContainsObjectId(objId) {
		return 0, 0, errors.New("ObjectId not in allocator range")
	}

	// Ensure allocator is loaded before accessing it
	if err := r.ensureLoaded(); err != nil {
		return 0, 0, err
	}

	realOff, allocErr := r.allocator.GetFileOffset(objId)
	if allocErr != nil {
		return 0, 0, allocErr
	}

	sz := blockSize
	if requested, ok := r.allocator.requestedSize(objId); ok {
		sz = requested
	}

	return realOff, sz, nil
}

type allocatorSlice []*allocatorRef

// Marshal serializes the allocator slice.
// Returns: [count:4][allocatorData...]
func (s allocatorSlice) Marshal() ([]byte, error) {
	data := make([]byte, 0)

	// Serialize slice count
	count := len(s)
	data = append(data,
		byte(count>>24), byte(count>>16),
		byte(count>>8), byte(count))

	// Serialize allocator state data (includes startingFileOffset via blockAllocator.Marshal)
	for _, allocator := range s {
		allocatorData, err := allocator.Marshal()
		if err != nil {
			return nil, err
		}
		data = append(data, allocatorData...)
	}

	return data, nil
}

// GetObjectInfo searches for an ObjectId within this slice's allocators.
// Returns the FileOffset and size if found, or an error if not found.
func (s allocatorSlice) GetObjectInfo(objId ObjectId, blockSize int) (FileOffset, int, error) {
	for _, ref := range s {
		if !ref.ContainsObjectId(objId) {
			continue
		}
		realOff, sz, err := ref.GetObjectInfo(objId, blockSize)
		if err == nil {
			return realOff, sz, nil
		}
	}
	return 0, 0, errors.New("ObjectId not found in slice")
}

// Unmarshal deserializes the allocator slice.
// Reconstructs allocator state data (ObjectIds are derived from startingFileOffset in blockAllocator).
// Returns the number of bytes consumed, or an error.
func (s *allocatorSlice) Unmarshal(data []byte, blockCount int) (int, error) {
	if len(data) < 4 {
		return 0, errors.New("invalid slice data length")
	}

	offset := 0

	// Deserialize slice count
	count := int(data[offset])<<24 | int(data[offset+1])<<16 |
		int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	// Deserialize allocator state data (includes startingFileOffset via blockAllocator.Unmarshal)
	bitCount := (blockCount + 7) / 8
	allocatorDataSize := 8 + bitCount + 2*blockCount

	*s = make(allocatorSlice, count)
	for i := range count {
		if len(data) < offset+allocatorDataSize {
			return 0, errors.New("invalid allocator state data")
		}
		allocator := &blockAllocator{
			blockSize:      0, // Will be set by caller
			blockCount:     blockCount,
			allocatedList:  make([]bool, blockCount),
			requestedSizes: make([]int, blockCount),
		}
		if err := allocator.Unmarshal(data[offset : offset+allocatorDataSize]); err != nil {
			return 0, err
		}
		offset += allocatorDataSize
		(*s)[i] = &allocatorRef{
			allocator: allocator,
		}
	}

	return offset, nil
}
func (p *allocatorPool) PreMarshal() ([]int, error) {
	// Count unpersisted allocatorRefs
	count := 0
	for _, ref := range p.available {
		if ref.fileOff < 1 {
			count++
		}
	}
	for _, ref := range p.full {
		if ref.fileOff < 1 {
			count++
		}
	}

	// First size is for the LUT: [availCount:4][fullCount:4][fileOff1:8][fileOff2:8]...
	lutSize := 8 + (len(p.available)+len(p.full))*8
	arr := make([]int, 0, count+1)
	arr = append(arr, lutSize)

	// Then add sizes for each unpersisted allocatorRef
	for _, ref := range p.available {
		if ref.fileOff < 1 {
			arr = append(arr, ref.sizeInBytes(p.blockCount))
		}
	}
	for _, ref := range p.full {
		if ref.fileOff < 1 {
			arr = append(arr, ref.sizeInBytes(p.blockCount))
		}
	}
	return arr, nil
}

func (p *allocatorPool) populateObjIds(objIds []ObjectId) error {
	index := 0
	for _, ref := range p.available {
		if ref.fileOff < 1 {
			if index >= len(objIds) {
				return internal.ErrRePreAllocate
			}
			ref.fileOff = FileOffset(objIds[index])
			index++
		}
	}
	for _, ref := range p.full {
		if ref.fileOff < 1 {
			if index >= len(objIds) {
				return internal.ErrRePreAllocate
			}
			ref.fileOff = FileOffset(objIds[index])
			index++
		}
	}
	if index != len(objIds) {
		return errors.New("allocated more than needed")
	}
	return nil
}

// buildLookupTable constructs the serialized lookup table containing counts and fileOffsets
// for all allocators. Returns the LUT bytes and a slice of all allocatorRefs.
// Assumes all refs have been populated with valid fileOff values.
func (p *allocatorPool) buildLookupTable() ([]byte, []*allocatorRef, error) {
	// Collect all allocatorRefs
	allRefs := make([]*allocatorRef, 0, len(p.available)+len(p.full))
	allRefs = append(allRefs, p.available...)
	allRefs = append(allRefs, p.full...)

	// Build the lookup table: [availCount:4][fullCount:4][fileOff1:8][fileOff2:8]...
	lutSize := 8 + len(allRefs)*8
	lut := make([]byte, lutSize)
	offset := 0

	// Write counts
	availCount := int32(len(p.available))
	fullCount := int32(len(p.full))
	lut[offset] = byte(availCount >> 24)
	lut[offset+1] = byte(availCount >> 16)
	lut[offset+2] = byte(availCount >> 8)
	lut[offset+3] = byte(availCount)
	offset += 4

	lut[offset] = byte(fullCount >> 24)
	lut[offset+1] = byte(fullCount >> 16)
	lut[offset+2] = byte(fullCount >> 8)
	lut[offset+3] = byte(fullCount)
	offset += 4

	// Write fileOffsets for all allocatorRefs
	for _, ref := range allRefs {
		if ref.fileOff < 1 {
			return nil, nil, internal.ErrRePreAllocate
		}
		// Write fileOff to LUT (8 bytes, big-endian)
		fileOff := int64(ref.fileOff)
		lut[offset] = byte(fileOff >> 56)
		lut[offset+1] = byte(fileOff >> 48)
		lut[offset+2] = byte(fileOff >> 40)
		lut[offset+3] = byte(fileOff >> 32)
		lut[offset+4] = byte(fileOff >> 24)
		lut[offset+5] = byte(fileOff >> 16)
		lut[offset+6] = byte(fileOff >> 8)
		lut[offset+7] = byte(fileOff)
		offset += 8
	}

	return lut, allRefs, nil
}

// prepareAllocatorWrites creates a list of ObjectAndByteFunc for writing unsynced allocators.
// This list is ready to be passed to the store's batch write mechanism or a background worker.
// Allocators are marked as synced after their marshal closure executes successfully.
func (p *allocatorPool) prepareAllocatorWrites(allRefs []*allocatorRef) []ObjectAndByteFunc {
	objectAndByteFuncs := make([]ObjectAndByteFunc, 0, len(allRefs))

	for _, ref := range allRefs {
		if ref.synced {
			// Skip any entries that have already been written
			continue
		}
		// Capture ref in local variable for closure
		localRef := ref
		objectAndByteFuncs = append(objectAndByteFuncs, ObjectAndByteFunc{
			ObjectId: ObjectId(localRef.fileOff),
			ByteFunc: func() ([]byte, error) {
				data, err := localRef.Marshal()
				if err == nil {
					// Mark as synced after successful marshal
					localRef.synced = true
				}
				return data, err
			},
		})
	}

	return objectAndByteFuncs
}

// MarshalMultiple serializes all allocatorRefs in the pool for persistence.
// Each allocatorRef is written to its assigned fileOff location.
// The identity object contains a lookup table: [availCount:4][fullCount:4][fileOff1:8][fileOff2:8]...
// Returns an identity function (returning the first objId for the LUT),
// the list of objects to write, and any error.

func (p *allocatorPool) MarshalMultiple(objIds []ObjectId) (func() ObjectId, []ObjectAndByteFunc, error) {
	if len(objIds) == 0 {
		return nil, nil, errors.New("no ObjectIds allocated")
	}

	// First ObjectId is reserved for LUT; remaining for allocatorRefs
	allocIds := objIds[1:]
	if err := p.populateObjIds(allocIds); err != nil {
		return nil, nil, err
	}

	// Build lookup table
	lut, allRefs, err := p.buildLookupTable()
	if err != nil {
		return nil, nil, err
	}

	// The first objId is for the LUT itself
	lutObjId := objIds[0]

	// Build the list of objects to write
	objectAndByteFuncs := make([]ObjectAndByteFunc, 0, len(allRefs)+1)

	// First, write the LUT
	lutCopy := make([]byte, len(lut))
	copy(lutCopy, lut)
	objectAndByteFuncs = append(objectAndByteFuncs, ObjectAndByteFunc{
		ObjectId: lutObjId,
		ByteFunc: func() ([]byte, error) { return lutCopy, nil },
	})

	// Then prepare writes for unsynced allocators
	allocatorWrites := p.prepareAllocatorWrites(allRefs)
	objectAndByteFuncs = append(objectAndByteFuncs, allocatorWrites...)

	// Identity function returns the LUT ObjectId
	return func() ObjectId { return lutObjId }, objectAndByteFuncs, nil
}

// Delete frees all allocated fileOffsets from the parent allocator.
func (p *allocatorPool) Delete() error {
	// Free all available allocator storage
	for _, ref := range p.available {
		if ref.fileOff >= 1 {
			size := ref.sizeInBytes(p.blockCount)
			if err := p.parent.Free(ref.fileOff, size); err != nil {
				return err
			}
		}
	}
	// Free all full allocator storage
	for _, ref := range p.full {
		if ref.fileOff >= 1 {
			size := ref.sizeInBytes(p.blockCount)
			if err := p.parent.Free(ref.fileOff, size); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalMultiple deserializes allocatorRefs from the store and populates the pool lazily.
// The objData reader contains a lookup table: [availCount:4][fullCount:4][fileOff1:8][fileOff2:8]...
// Individual block allocators are NOT loaded into memory. Instead, metadata is stored and
// allocators are loaded on-demand when first accessed via ensureLoaded().
func (p *allocatorPool) UnmarshalMultiple(objData io.Reader, objReader any) error {
	// Read the LUT header: [availCount:4][fullCount:4]
	header := make([]byte, 8)
	if _, err := io.ReadFull(objData, header); err != nil {
		return err
	}

	availCount := int(header[0])<<24 | int(header[1])<<16 | int(header[2])<<8 | int(header[3])
	fullCount := int(header[4])<<24 | int(header[5])<<16 | int(header[6])<<8 | int(header[7])
	totalCount := availCount + fullCount

	// Read all fileOffsets from LUT
	fileOffsets := make([]FileOffset, totalCount)
	for i := range totalCount {
		offsetBytes := make([]byte, 8)
		if _, err := io.ReadFull(objData, offsetBytes); err != nil {
			return err
		}
		fileOff := int64(offsetBytes[0])<<56 | int64(offsetBytes[1])<<48 |
			int64(offsetBytes[2])<<40 | int64(offsetBytes[3])<<32 |
			int64(offsetBytes[4])<<24 | int64(offsetBytes[5])<<16 |
			int64(offsetBytes[6])<<8 | int64(offsetBytes[7])
		fileOffsets[i] = FileOffset(fileOff)
	}

	// Create available allocatorRefs with lazy-load metadata (don't load allocators yet)
	p.available = make(allocatorSlice, availCount)
	for i := range availCount {
		fileOff := fileOffsets[i]
		p.available[i] = &allocatorRef{
			allocator:  nil, // Lazy load on first access
			fileOff:    fileOff,
			file:       p.file,
			synced:     true,
			blockSize:  p.blockSize,
			blockCount: p.blockCount,
		}
	}

	// Create full allocatorRefs with lazy-load metadata (don't load allocators yet)
	p.full = make(allocatorSlice, fullCount)
	for i := range fullCount {
		fileOff := fileOffsets[availCount+i]
		p.full[i] = &allocatorRef{
			allocator:  nil, // Lazy load on first access
			fileOff:    fileOff,
			file:       p.file,
			synced:     true,
			blockSize:  p.blockSize,
			blockCount: p.blockCount,
		}
	}

	return nil
}

// allocatorPool keeps available and full block allocators for a given size.
// The pool manages both the allocators and their persistence metadata (ObjectIds).
type allocatorPool struct {
	available allocatorSlice
	full      allocatorSlice
	// File handle for direct I/O operations
	file *os.File
	// Parent allocator for provisioning new block allocators
	parent Allocator
	// Block size for this pool
	blockSize int
	// Number of blocks per allocator
	blockCount int
}

// ContainsObjectId checks if any allocator in this pool owns the given ObjectId.
func (p *allocatorPool) ContainsObjectId(objId ObjectId) bool {
	for _, ref := range p.available {
		if ref != nil && ref.ContainsObjectId(objId) {
			return true
		}
	}
	for _, ref := range p.full {
		if ref != nil && ref.ContainsObjectId(objId) {
			return true
		}
	}
	return false
}

// NewAllocatorPool creates a new allocator pool for a specific block size.
func NewAllocatorPool(blockSize, blockCount int, parent Allocator, file *os.File) *allocatorPool {
	return &allocatorPool{
		available:  make(allocatorSlice, 0),
		full:       make(allocatorSlice, 0),
		file:       file,
		parent:     parent,
		blockSize:  blockSize,
		blockCount: blockCount,
	}
}

// Marshal serializes the complete allocator pool state.
// Uses the allocatorSlice Marshal methods for each list.
func (p *allocatorPool) Marshal() ([]byte, error) {
	// Serialize available allocators
	availData, err := p.available.Marshal()
	if err != nil {
		return nil, err
	}

	// Serialize full allocators
	fullData, err := p.full.Marshal()
	if err != nil {
		return nil, err
	}

	// Combine both
	return append(availData, fullData...), nil
}

// Unmarshal deserializes the complete allocator pool state.
// Uses the allocatorSlice Unmarshal methods for each list.
// Returns the number of bytes consumed, or an error.
func (p *allocatorPool) Unmarshal(data []byte, blockCount int) (int, error) {
	offset := 0

	// Deserialize available allocators
	availBytes, err := p.available.Unmarshal(data[offset:], blockCount)
	if err != nil {
		return 0, err
	}
	offset += availBytes

	// Deserialize full allocators
	fullBytes, err := p.full.Unmarshal(data[offset:], blockCount)
	if err != nil {
		return 0, err
	}
	offset += fullBytes

	// No need to set blockSize on unmarshaled allocators - they'll have it when lazy-loaded
	// But if any were already loaded (shouldn't happen), ensure blockSize is set
	for _, ref := range p.available {
		if ref != nil && ref.allocator != nil {
			ref.allocator.blockSize = p.blockSize
		}
	}
	for _, ref := range p.full {
		if ref != nil && ref.allocator != nil {
			ref.allocator.blockSize = p.blockSize
		}
	}

	return offset, nil
}

// freeFromPool attempts to free a block from this pool's allocators.
// Checks both available and full allocators. If the block is in a full allocator,
// moves that allocator back to available after freeing.
// Returns nil if successfully freed, error if not found or if free fails.
func (p *allocatorPool) freeFromPool(fileOffset FileOffset, blockSize int, postFree func(FileOffset, int) error) error {
	// Check available allocators first
	for _, ref := range p.available {
		if ref == nil {
			continue
		}
		if err := ref.ensureLoaded(); err != nil {
			continue // Skip allocators that can't be loaded
		}
		allocator := ref.allocator
		if fileOffset < allocator.startingFileOffset || fileOffset >= allocator.startingFileOffset+FileOffset(allocator.blockCount*allocator.blockSize) {
			continue
		}
		if objId, ok := allocator.objectIdForOffset(fileOffset); ok {
			allocator.setRequestedSize(objId, 0)
		}
		err := allocator.Free(fileOffset, blockSize)
		if err == nil {
			// Mark as unsynced since allocator state changed
			ref.synced = false
			if postFree != nil {
				if postErr := postFree(fileOffset, blockSize); postErr != nil {
					return postErr
				}
			}
			return nil
		}
	}

	// Then check allocators previously marked full; once a slot is freed, move it back to available
	for i, ref := range p.full {
		if ref == nil {
			continue
		}
		if err := ref.ensureLoaded(); err != nil {
			continue // Skip allocators that can't be loaded
		}
		allocator := ref.allocator
		if fileOffset < allocator.startingFileOffset || fileOffset >= allocator.startingFileOffset+FileOffset(allocator.blockCount*allocator.blockSize) {
			continue
		}
		if objId, ok := allocator.objectIdForOffset(fileOffset); ok {
			allocator.setRequestedSize(objId, 0)
		}
		err := allocator.Free(fileOffset, blockSize)
		if err == nil {
			// Mark as unsynced since allocator state changed
			ref.synced = false
			p.available = append(p.available, ref)
			p.full = append(p.full[:i], p.full[i+1:]...)
			if postFree != nil {
				if postErr := postFree(fileOffset, blockSize); postErr != nil {
					return postErr
				}
			}
			return nil
		}
	}

	return errors.New("block not found in pool")
}

// GetObjectInfo searches for an ObjectId within this pool's allocators.
// Returns the FileOffset and size if found, or an error if not found.
// Checks both available and full allocators.
func (p *allocatorPool) GetObjectInfo(objId ObjectId, blockSize int) (FileOffset, int, error) {
	// Check available allocators first
	if realOff, sz, err := p.available.GetObjectInfo(objId, blockSize); err == nil {
		return realOff, sz, nil
	}

	// Check full allocators
	return p.full.GetObjectInfo(objId, blockSize)
}

// Allocate attempts to allocate a block from this pool.
// It tries available allocators first, moving them to full if they become exhausted.
// If no available allocators have space, it provisions a new allocator from the parent.
// Returns the ObjectId, FileOffset, whether a new allocator was created, and any error.
//
// CODE SMELL: Returning *allocatorRef couples the pool to caller's needs (omni uses this
// to register new allocators in rangeCache). Consider observer pattern or callback instead.
func (p *allocatorPool) Allocate() (ObjectId, FileOffset, *allocatorRef, error) {
	// Try available allocators first
	for len(p.available) > 0 {
		ref := p.available[0]
		if err := ref.ensureLoaded(); err != nil {
			return 0, 0, nil, err
		}
		id, offset, err := ref.allocator.Allocate(p.blockSize)
		if errors.Is(err, AllAllocated) {
			// Move to full list
			p.full = append(p.full, ref)
			p.available = p.available[1:]
			continue
		}
		if err != nil {
			return 0, 0, nil, err
		}
		// Mark as unsynced since allocator state changed
		ref.synced = false
		return id, offset, nil, nil
	}

	// No available allocators or all were full: provision a new one from parent
	baseObjId, fileOffset, err := p.parent.Allocate(p.blockSize * p.blockCount)
	if err != nil {
		return 0, 0, nil, err
	}

	newAllocator := NewBlockAllocator(p.blockSize, p.blockCount, fileOffset, baseObjId, p.file)
	newRef := &allocatorRef{
		allocator: newAllocator,
		file:      p.file,
	}
	p.available = append(p.available, newRef)

	// Allocate from the new allocator
	id, offset, err := newRef.allocator.Allocate(p.blockSize)
	if err != nil {
		return 0, 0, nil, err
	}
	return id, offset, newRef, nil
}

// SetRequestedSize finds the allocator containing objId and sets its requested size.
// Searches both available and full allocator lists.
func (p *allocatorPool) SetRequestedSize(objId ObjectId, size int) {
	// Check available allocators
	for _, ref := range p.available {
		if ref.ContainsObjectId(objId) {
			// Ensure it's loaded and then set the requested size
			if err := ref.ensureLoaded(); err == nil {
				ref.allocator.setRequestedSize(objId, size)
				// Mark as unsynced since allocator state changed
				ref.synced = false
			}
			return
		}
	}

	// Check full allocators
	for _, ref := range p.full {
		if ref.ContainsObjectId(objId) {
			// Ensure it's loaded and then set the requested size
			if err := ref.ensureLoaded(); err == nil {
				ref.allocator.setRequestedSize(objId, size)
				// Mark as unsynced since allocator state changed
				ref.synced = false
			}
			return
		}
	}
}

// AllocateRun attempts to allocate a contiguous run of blocks from this pool.
// It tries available allocators first, moving them to full if they become exhausted.
// If no available allocators have space, it provisions a new allocator from the parent.
// Returns partial results if fewer than requested blocks are available, along with the allocator ref if a new one was created.
//
// CODE SMELL: Returning *allocatorRef couples the pool to caller's needs (omni uses this
// to register new allocators in rangeCache). Consider observer pattern or callback instead.
func (p *allocatorPool) AllocateRun(requestCount int) ([]ObjectId, []FileOffset, *allocatorRef, error) {
	// Try available allocators - they now return partial results
	for i := 0; i < len(p.available); {
		ref := p.available[i]
		if err := ref.ensureLoaded(); err != nil {
			return nil, nil, nil, err
		}
		allocator := ref.allocator
		objIds, offsets, err := allocator.AllocateRun(p.blockSize, requestCount)
		if err != nil && err.Error() != "size must match block size" && err.Error() != "count must be positive" {
			return nil, nil, nil, err
		}
		// If no slots available, move to full list
		if len(objIds) == 0 {
			p.full = append(p.full, ref)
			p.available = append(p.available[:i], p.available[i+1:]...)
			continue
		}
		// Accept partial allocation - mark as unsynced since allocator state changed
		ref.synced = false
		return objIds, offsets, nil, nil
	}

	// Need a new allocator segment
	baseObjId, fileOffset, err := p.parent.Allocate(p.blockSize * p.blockCount)
	if err != nil {
		return []ObjectId{}, []FileOffset{}, nil, nil
	}

	newAllocator := NewBlockAllocator(p.blockSize, p.blockCount, fileOffset, baseObjId, p.file)
	newRef := &allocatorRef{
		allocator: newAllocator,
		file:      p.file,
	}
	p.available = append(p.available, newRef)

	objIds, offsets, err := newRef.allocator.AllocateRun(p.blockSize, requestCount)
	return objIds, offsets, newRef, err
}
