package stringstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

type stringLoc struct {
	fileOffset bobbob.FileOffset
	length     int
}

type writeJob struct {
	objectId bobbob.ObjectId
	data     []byte
	isDelete bool          // True for delete operations
	doneChan chan struct{} // For flush notifications; nil for normal writes
}

type stringStoreShard struct {
	file *os.File
	cfg  Config

	useMu              sync.RWMutex
	unloaded           bool
	unloadedHasFreeSlot bool
	strdataSize        int64
	lastAccess         atomic.Int64

	// Allocator state - simple counter with freelist
	allocMu        sync.Mutex
	nextId         bobbob.ObjectId
	freeList       map[bobbob.ObjectId]struct{}
	maxAllocated   bobbob.ObjectId
	allocatedSizes map[bobbob.ObjectId]int // Track sizes for allocated-but-not-yet-written objects

	// Offset lookup - populated by write worker
	offsetMu     sync.RWMutex
	offsetLookup map[bobbob.ObjectId]stringLoc
	nextOffset   bobbob.FileOffset

	// Per-object locks for fine-grained concurrency
	objectLocks *store.ObjectLockMap

	// Write worker
	ioMu         sync.RWMutex
	writeQueue   chan *writeJob
	closeCh      chan struct{}
	workerDone   chan struct{}
	compactMu    sync.RWMutex
	diskWritePool *diskWritePool // Pool of workers for concurrent disk I/O

	// Marshalling fields
	modified          atomic.Bool // Atomic to prevent race conditions on concurrent writes
	stateObjId        bobbob.ObjectId
	strdataObjId      bobbob.ObjectId
	strdataObjIdStale bobbob.ObjectId
	primeObjId        bobbob.ObjectId
}

func newStringStoreShard(cfg Config) (*stringStoreShard, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(normalized.FilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}

	store := &stringStoreShard{
		file:           file,
		cfg:            normalized,
		nextId:         normalized.StartingObjectId,
		freeList:       make(map[bobbob.ObjectId]struct{}),
		maxAllocated:   normalized.StartingObjectId,
		allocatedSizes: make(map[bobbob.ObjectId]int),
		offsetLookup:   make(map[bobbob.ObjectId]stringLoc),
		objectLocks:    store.NewObjectLockMap(),
		nextOffset:     0,
		primeObjId:     bobbob.ObjNotAllocated,
		writeQueue:     make(chan *writeJob, 4096),
		closeCh:        make(chan struct{}),
		workerDone:     make(chan struct{}),
	}
	store.lastAccess.Store(time.Now().UnixNano())

	// Initialize disk write pool
	store.diskWritePool = newDiskWritePool(store, normalized.DiskWritePoolSize)

	go store.writeWorker()

	return store, nil
}

func newUnloadedStringStoreShard(cfg Config, stateObjId, strdataObjId bobbob.ObjectId, strdataSize int64, hasFreeSlot bool) *stringStoreShard {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		// Caller should have validated; fall back to provided config.
		normalized = cfg
	}
	shard := &stringStoreShard{
		cfg:                normalized,
		unloaded:           true,
		unloadedHasFreeSlot: hasFreeSlot,
		stateObjId:         stateObjId,
		strdataObjId:       strdataObjId,
		strdataSize:        strdataSize,
	}
	shard.lastAccess.Store(time.Now().UnixNano())
	return shard
}

func (s *stringStoreShard) touch() {
	s.lastAccess.Store(time.Now().UnixNano())
}

func (s *stringStoreShard) lastAccessTime() time.Time {
	nanos := s.lastAccess.Load()
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

type shardStateSummary struct {
	maxStrings    uint32
	startObjId    bobbob.ObjectId
	objInterval   bobbob.ObjectId
	nextId         bobbob.ObjectId
	maxAllocated   bobbob.ObjectId
	freeListCount  uint32
}

func parseShardStateSummary(data []byte) (shardStateSummary, error) {
	buf := bytes.NewReader(data)

	var version uint8
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return shardStateSummary{}, fmt.Errorf("read version: %w", err)
	}
	if version != 2 {
		return shardStateSummary{}, fmt.Errorf("unsupported version: %d", version)
	}

	var maxStrings uint32
	var startObjId, objInterval bobbob.ObjectId
	if err := binary.Read(buf, binary.LittleEndian, &maxStrings); err != nil {
		return shardStateSummary{}, fmt.Errorf("read MaxNumberOfStrings: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &startObjId); err != nil {
		return shardStateSummary{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &objInterval); err != nil {
		return shardStateSummary{}, err
	}

	var nextId, maxAllocated bobbob.ObjectId
	var nextOff int64
	var strdataObjId bobbob.ObjectId
	if err := binary.Read(buf, binary.LittleEndian, &nextId); err != nil {
		return shardStateSummary{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &maxAllocated); err != nil {
		return shardStateSummary{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &nextOff); err != nil {
		return shardStateSummary{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &strdataObjId); err != nil {
		return shardStateSummary{}, err
	}

	var freeCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &freeCount); err != nil {
		return shardStateSummary{}, err
	}
	for i := 0; i < int(freeCount); i++ {
		var objId bobbob.ObjectId
		if err := binary.Read(buf, binary.LittleEndian, &objId); err != nil {
			return shardStateSummary{}, err
		}
	}

	var lookupCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &lookupCount); err != nil {
		return shardStateSummary{}, err
	}
	for i := 0; i < int(lookupCount); i++ {
		var objId bobbob.ObjectId
		var fileOffset, length int64
		if err := binary.Read(buf, binary.LittleEndian, &objId); err != nil {
			return shardStateSummary{}, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &fileOffset); err != nil {
			return shardStateSummary{}, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return shardStateSummary{}, err
		}
	}

	return shardStateSummary{
		maxStrings:   maxStrings,
		startObjId:   startObjId,
		objInterval:  objInterval,
		nextId:       nextId,
		maxAllocated: maxAllocated,
		freeListCount: freeCount,
	}, nil
}

// NewObj allocates a new ObjectId (allocator only).
func (s *stringStoreShard) NewObj(size int) (bobbob.ObjectId, error) {
	if s.unloaded {
		return bobbob.ObjNotAllocated, errShardUnloaded
	}
	if size < 0 {
		return bobbob.ObjNotAllocated, errors.New("size must be >= 0")
	}

	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	// Try freelist first
	for objId := range s.freeList {
		delete(s.freeList, objId)
		s.allocatedSizes[objId] = size
		s.modified.Store(true)
		return objId, nil
	}

	// Allocate new ID
	maxObjects := bobbob.ObjectId(s.cfg.MaxNumberOfStrings) * s.cfg.ObjectIdInterval
	maxId := s.cfg.StartingObjectId + maxObjects
	if s.nextId >= maxId {
		return bobbob.ObjNotAllocated, errStoreCapacity
	}

	objId := s.nextId
	s.nextId += s.cfg.ObjectIdInterval
	if s.nextId > s.maxAllocated {
		s.maxAllocated = s.nextId
	}
	s.allocatedSizes[objId] = size
	s.modified.Store(true)

	return objId, nil
}

// acquireObjectLock is a convenience wrapper for ObjectLockMap.AcquireObjectLock.
func (s *stringStoreShard) acquireObjectLock(objectId bobbob.ObjectId) *store.ObjectLock {
	return s.objectLocks.AcquireObjectLock(store.ObjectId(objectId))
}

// releaseObjectLock is a convenience wrapper for ObjectLockMap.ReleaseObjectLock.
func (s *stringStoreShard) releaseObjectLock(objectId bobbob.ObjectId, objLock *store.ObjectLock) {
	s.objectLocks.ReleaseObjectLock(store.ObjectId(objectId), objLock)
}

// LateWriteNewObj allocates ID and returns writer+finisher.
func (s *stringStoreShard) LateWriteNewObj(size int) (bobbob.ObjectId, io.Writer, bobbob.Finisher, error) {
	if s.unloaded {
		return bobbob.ObjNotAllocated, nil, nil, errShardUnloaded
	}
	objId, err := s.NewObj(size)
	if err != nil {
		return bobbob.ObjNotAllocated, nil, nil, err
	}

	writer := newFixedSizeBufferWriter(size)
	finisher := func() error {
		data, err := writer.finalize()
		if err != nil {
			return err
		}
		return s.writeToObj(objId, data)
	}

	return objId, writer, finisher, nil
}

// WriteToObj returns writer+finisher for object (works for allocated or existing).
func (s *stringStoreShard) WriteToObj(objectId bobbob.ObjectId) (io.Writer, bobbob.Finisher, error) {
	if s.unloaded {
		return nil, nil, errShardUnloaded
	}
	// Check offsetLookup first (for existing/written objects)
	s.offsetMu.RLock()
	loc, existsInOffset := s.offsetLookup[objectId]
	s.offsetMu.RUnlock()

	var size int
	if existsInOffset {
		// Object already written - use its recorded size
		size = loc.length
	} else {
		// Check if allocated but not yet written
		s.allocMu.Lock()
		allocatedSize, existsInAlloc := s.allocatedSizes[objectId]
		s.allocMu.Unlock()

		if !existsInAlloc {
			return nil, nil, errNotYetWritten
		}
		size = allocatedSize
	}

	writer := newFixedSizeBufferWriter(size)
	finisher := func() error {
		data, err := writer.finalize()
		if err != nil {
			return err
		}
		return s.writeToObj(objectId, data)
	}

	return writer, finisher, nil
}

// writeToObj enqueues data to be written (non-blocking).
func (s *stringStoreShard) writeToObj(objectId bobbob.ObjectId, data []byte) error {
	if s.unloaded {
		return errShardUnloaded
	}
	// Copy data since caller may reuse buffer
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	job := &writeJob{
		objectId: objectId,
		data:     dataCopy,
		isDelete: false,
	}

	select {
	case s.writeQueue <- job:
		s.modified.Store(true)
		return nil
	case <-s.closeCh:
		return errStoreClosed
	}
}

// WriteBatchedObjs writes multiple objects.
func (s *stringStoreShard) WriteBatchedObjs(objIds []bobbob.ObjectId, data []byte, sizes []int) error {
	if len(objIds) != len(sizes) {
		return fmt.Errorf("objIds length %d does not match sizes length %d", len(objIds), len(sizes))
	}
	if len(objIds) == 0 {
		return nil
	}

	total := 0
	for _, size := range sizes {
		if size < 0 {
			return errors.New("sizes must be >= 0")
		}
		total += size
	}
	if total != len(data) {
		return fmt.Errorf("sum of sizes (%d) does not match data length (%d)", total, len(data))
	}

	// Write each object individually
	offset := 0
	for i, objId := range objIds {
		size := sizes[i]
		if err := s.writeToObj(objId, data[offset:offset+size]); err != nil {
			return err
		}
		offset += size
	}

	return nil
}

// LateReadObj reads from object, waiting for pending writes if necessary.
// Uses per-object locks for fine-grained concurrency - only waits for writes to the specific object being read.
func (s *stringStoreShard) LateReadObj(id bobbob.ObjectId) (io.Reader, bobbob.Finisher, error) {
	if s.unloaded {
		return nil, nil, errShardUnloaded
	}
	// Quick check without acquiring lock
	s.offsetMu.RLock()
	loc, exists := s.offsetLookup[id]
	s.offsetMu.RUnlock()

	if exists {
		// Common case: object is written, acquire read lock and return
		objLock := s.acquireObjectLock(id)
		objLock.Mu.RLock()

		// Double-check after acquiring lock (object might have been deleted)
		s.offsetMu.RLock()
		loc, exists = s.offsetLookup[id]
		s.offsetMu.RUnlock()

		if !exists {
			objLock.Mu.RUnlock()
			s.releaseObjectLock(id, objLock)
			return nil, nil, errNotYetWritten
		}

		s.ioMu.RLock()
		reader := io.NewSectionReader(s.file, int64(loc.fileOffset), int64(loc.length))
		s.ioMu.RUnlock()

		finisher := func() error {
			objLock.Mu.RUnlock()
			s.releaseObjectLock(id, objLock)
			return nil
		}
		return reader, finisher, nil
	}

	// Object not in offsetLookup - check if there's a pending write
	s.allocMu.Lock()
	_, pendingWrite := s.allocatedSizes[id]
	s.allocMu.Unlock()

	if !pendingWrite {
		// Object was never allocated or already deleted
		return nil, nil, errNotYetWritten
	}

	// There's a pending write - flush writes FIRST (without holding object lock to avoid deadlock)
	if err := s.flushWrites(); err != nil {
		return nil, nil, err
	}

	// Now acquire read lock and return the object
	objLock := s.acquireObjectLock(id)
	objLock.Mu.RLock()

	// Check offsetLookup after flush and lock acquisition
	s.offsetMu.RLock()
	loc, exists = s.offsetLookup[id]
	s.offsetMu.RUnlock()

	if !exists {
		// Write must have failed or object was deleted
		objLock.Mu.RUnlock()
		s.releaseObjectLock(id, objLock)
		return nil, nil, errNotYetWritten
	}

	s.ioMu.RLock()
	reader := io.NewSectionReader(s.file, int64(loc.fileOffset), int64(loc.length))
	s.ioMu.RUnlock()

	finisher := func() error {
		objLock.Mu.RUnlock()
		s.releaseObjectLock(id, objLock)
		return nil
	}
	return reader, finisher, nil
}

// DeleteObj enqueues deletion to write worker (serialized with writes).
func (s *stringStoreShard) DeleteObj(objId bobbob.ObjectId) error {
	if s.unloaded {
		return errShardUnloaded
	}
	// Don't modify freelist here - let write worker handle it for proper serialization
	job := &writeJob{
		objectId: objId,
		data:     nil,
		isDelete: true,
	}

	select {
	case s.writeQueue <- job:
		s.modified.Store(true)
		return nil
	case <-s.closeCh:
		return errStoreClosed
	}
}

func (s *stringStoreShard) flushWrites() error {
	if s.unloaded {
		return errShardUnloaded
	}
	flushCh := make(chan struct{})
	flushJob := &writeJob{
		objectId: 0,
		data:     nil,
		doneChan: flushCh,
	}

	select {
	case s.writeQueue <- flushJob:
		<-flushCh
		return nil
	case <-s.closeCh:
		return errStoreClosed
	}
}

// Close shuts down write worker using channel close.
func (s *stringStoreShard) Close() error {
	if s.unloaded {
		return nil
	}
	close(s.closeCh)
	<-s.workerDone

	// Wait for all pending disk I/O to complete
	s.diskWritePool.waitForPending()
	
	// Close the disk write pool (waits for all workers to finish)
	s.diskWritePool.close()

	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	if err := s.file.Sync(); err != nil {
		_ = s.file.Close()
		return err
	}
	return s.file.Close()
}

// PrimeObject - keep existing implementation for now
func (s *stringStoreShard) PrimeObject(size int) (bobbob.ObjectId, error) {
	if s.unloaded {
		return bobbob.ObjNotAllocated, errShardUnloaded
	}
	if s.primeObjId != bobbob.ObjNotAllocated {
		return s.primeObjId, nil
	}

	objId, writer, finisher, err := s.LateWriteNewObj(size)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}
	if _, err := writer.Write(make([]byte, size)); err != nil {
		return bobbob.ObjNotAllocated, err
	}
	if finisher != nil {
		if err := finisher(); err != nil {
			return bobbob.ObjNotAllocated, err
		}
	}

	s.primeObjId = objId
	return objId, nil
}

// Compact - placeholder for now
func (s *stringStoreShard) Compact() error {
	if s.unloaded {
		return errShardUnloaded
	}
	// Flush any pending writes/deletes before compacting
	if err := s.flushWrites(); err != nil {
		return err
	}
	// Block new submissions and wait for in-flight disk I/O to complete
	s.diskWritePool.drain()

	s.compactMu.Lock()
	defer s.compactMu.Unlock()

	s.offsetMu.Lock()
	ids := make([]bobbob.ObjectId, 0, len(s.offsetLookup))
	for id := range s.offsetLookup {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	compactDir := filepath.Dir(s.cfg.FilePath)
	tempFile, err := os.CreateTemp(compactDir, "bobbob-stringstore-compact-*.blob")
	if err != nil {
		s.offsetMu.Unlock()
		return err
	}
	tempPath := tempFile.Name()

	newLookup := make(map[bobbob.ObjectId]stringLoc, len(ids))
	newOffset := bobbob.FileOffset(0)
	buf := make([]byte, 0)

	for _, id := range ids {
		loc := s.offsetLookup[id]
		if loc.length <= 0 {
			continue
		}
		if cap(buf) < loc.length {
			buf = make([]byte, loc.length)
		}
		buf = buf[:loc.length]
		readN, readErr := s.file.ReadAt(buf, int64(loc.fileOffset))
		if readErr != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			s.offsetMu.Unlock()
			return readErr
		}
		if readN != loc.length {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			s.offsetMu.Unlock()
			return io.ErrUnexpectedEOF
		}
		writeN, writeErr := tempFile.WriteAt(buf, int64(newOffset))
		if writeErr != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			s.offsetMu.Unlock()
			return writeErr
		}
		if writeN != loc.length {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			s.offsetMu.Unlock()
			return io.ErrShortWrite
		}

		newLookup[id] = stringLoc{
			fileOffset: newOffset,
			length:     loc.length,
		}
		newOffset += bobbob.FileOffset(loc.length)
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		s.offsetMu.Unlock()
		return err
	}
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		s.offsetMu.Unlock()
		return err
	}

	oldPath := s.cfg.FilePath
	backupPath := fmt.Sprintf("%s.bak.%d", oldPath, time.Now().UnixNano())

	if err := s.file.Close(); err != nil {
		_ = os.Remove(tempPath)
		s.offsetMu.Unlock()
		return err
	}
	if err := os.Rename(oldPath, backupPath); err != nil {
		_ = os.Remove(tempPath)
		s.offsetMu.Unlock()
		return err
	}
	if err := os.Rename(tempPath, oldPath); err != nil {
		_ = os.Rename(backupPath, oldPath)
		_ = os.Remove(tempPath)
		s.offsetMu.Unlock()
		return err
	}
	_ = os.Remove(backupPath)

	newFile, err := os.OpenFile(oldPath, os.O_RDWR, 0o600)
	if err != nil {
		s.offsetMu.Unlock()
		return err
	}

	s.file = newFile
	s.offsetLookup = newLookup
	s.nextOffset = newOffset
	s.modified.Store(true)

	s.offsetMu.Unlock()
	return nil
}

// writeWorker batches writes and updates offsetLookup.
func (s *stringStoreShard) writeWorker() {
	defer close(s.workerDone)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	const bufferSize = 16 * 1024
	buffer := make([]byte, 0, bufferSize)
	pendingJobs := make([]*writeJob, 0, 64)
	flushNotifications := make([]chan struct{}, 0, 8) // Channels to close after flush
	lockHeld := false

	lockIfNeeded := func() {
		if lockHeld {
			return
		}
		s.compactMu.RLock()
		lockHeld = true
	}

	unlockIfIdle := func() {
		if !lockHeld {
			return
		}
		if len(buffer) == 0 && len(pendingJobs) == 0 {
			s.compactMu.RUnlock()
			lockHeld = false
		}
	}

	flush := func() {
		if len(buffer) == 0 && len(flushNotifications) == 0 && len(pendingJobs) == 0 {
			return
		}
		lockIfNeeded()

		// Acquire per-object locks for all objects in pendingJobs
		// Deduplicate by ObjectId and sort to prevent deadlocks
		type lockPair struct {
			objectId bobbob.ObjectId
			lock     *store.ObjectLock
		}
		lockMap := make(map[bobbob.ObjectId]*store.ObjectLock)
		for _, job := range pendingJobs {
			if _, exists := lockMap[job.objectId]; !exists {
				lockMap[job.objectId] = s.acquireObjectLock(job.objectId)
			}
		}

		// Convert to slice and sort by ObjectId to prevent deadlocks
		locks := make([]lockPair, 0, len(lockMap))
		for objId, lock := range lockMap {
			locks = append(locks, lockPair{objectId: objId, lock: lock})
		}
		sort.Slice(locks, func(i, j int) bool {
			return locks[i].objectId < locks[j].objectId
		})

		// Acquire all write locks in sorted order
		for _, lp := range locks {
			lp.lock.Mu.Lock()
		}

		// Get current file end offset and update metadata
		s.offsetMu.Lock()
		s.allocMu.Lock()
		
		startOffset := s.nextOffset
		writeOffset := startOffset
		
		// Update offsetLookup for all write jobs
		for _, job := range pendingJobs {
			if job.isDelete {
				// Delete operation - remove from offsetLookup and add to freelist
				delete(s.offsetLookup, job.objectId)
				s.freeList[job.objectId] = struct{}{}
				delete(s.allocatedSizes, job.objectId)
			} else {
				// Write operation - add to offsetLookup
				s.offsetLookup[job.objectId] = stringLoc{
					fileOffset: writeOffset,
					length:     len(job.data),
				}
				delete(s.allocatedSizes, job.objectId)
				writeOffset += bobbob.FileOffset(len(job.data))
			}
		}
		s.nextOffset = writeOffset
		
		s.allocMu.Unlock()
		s.offsetMu.Unlock()

		// Notify any waiting readers that flush is complete (metadata updated)
		for _, ch := range flushNotifications {
			close(ch)
		}

		// Submit disk I/O to worker pool (non-blocking)
		if len(buffer) > 0 {
			// Copy buffer and locks for async processing
			bufferCopy := make([]byte, len(buffer))
			copy(bufferCopy, buffer)
			
			locksCopy := make([]*store.ObjectLock, len(locks))
			objectIdsCopy := make([]bobbob.ObjectId, len(locks))
			for i, lp := range locks {
				locksCopy[i] = lp.lock
				objectIdsCopy[i] = lp.objectId
			}
			
			task := &diskWriteTask{
				buffer:      bufferCopy,
				startOffset: startOffset,
				locks:       locksCopy,
				objectIds:   objectIdsCopy,
				errChan:     nil, // Errors will panic (fail-fast)
			}
			
			s.diskWritePool.submit(task)
		} else {
			// No I/O needed, release locks immediately
			for _, lp := range locks {
				lp.lock.Mu.Unlock()
				s.releaseObjectLock(lp.objectId, lp.lock)
			}
		}

		// Clear batch for reuse
		buffer = buffer[:0]
		pendingJobs = pendingJobs[:0]
		flushNotifications = flushNotifications[:0]
		unlockIfIdle()
	}

	for {
		select {
		case job := <-s.writeQueue:
			isFlushMarker := job.doneChan != nil && job.data == nil && !job.isDelete && job.objectId == 0
			if job.doneChan != nil {
				// Flush marker or delete completion - add to notification list
				flushNotifications = append(flushNotifications, job.doneChan)
			}
			if isFlushMarker {
				flush()
				continue
			}
			if job.isDelete {
				lockIfNeeded()
				// Delete operation - add to pending jobs for processing
				pendingJobs = append(pendingJobs, job)
				continue
			}
			lockIfNeeded()
			// Normal write - append to buffer
			buffer = append(buffer, job.data...)
			pendingJobs = append(pendingJobs, job)

			// Flush if buffer full
			if len(buffer) >= bufferSize {
				flush()
			}
			continue

		case <-ticker.C:
			flush()

		case <-s.closeCh:
			flush()
			return
		}
	}
}

// MarshalState serializes metadata.
func (s *stringStoreShard) MarshalState() ([]byte, error) {
	s.allocMu.Lock()
	s.offsetMu.RLock()
	defer s.allocMu.Unlock()
	defer s.offsetMu.RUnlock()

	var buf bytes.Buffer

	// Version
	if err := binary.Write(&buf, binary.LittleEndian, uint8(2)); err != nil {
		return nil, err
	}

	// Config
	if err := binary.Write(&buf, binary.LittleEndian, uint32(s.cfg.MaxNumberOfStrings)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.cfg.StartingObjectId); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.cfg.ObjectIdInterval); err != nil {
		return nil, err
	}

	// Allocator state
	if err := binary.Write(&buf, binary.LittleEndian, s.nextId); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.maxAllocated); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, int64(s.nextOffset)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.strdataObjId); err != nil {
		return nil, err
	}

	// Freelist
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(s.freeList))); err != nil {
		return nil, err
	}
	for objId := range s.freeList {
		if err := binary.Write(&buf, binary.LittleEndian, objId); err != nil {
			return nil, err
		}
	}

	// offsetLookup
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(s.offsetLookup))); err != nil {
		return nil, err
	}
	for objId, loc := range s.offsetLookup {
		if err := binary.Write(&buf, binary.LittleEndian, objId); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, int64(loc.fileOffset)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, int64(loc.length)); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// UnmarshalState deserializes metadata.
func (s *stringStoreShard) UnmarshalState(data []byte) error {
	s.allocMu.Lock()
	s.offsetMu.Lock()
	defer s.allocMu.Unlock()
	defer s.offsetMu.Unlock()

	buf := bytes.NewReader(data)

	// Version
	var version uint8
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("read version: %w", err)
	}
	if version != 2 {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Config validation
	var maxStrings uint32
	var startObjId, objInterval bobbob.ObjectId
	if err := binary.Read(buf, binary.LittleEndian, &maxStrings); err != nil {
		return fmt.Errorf("read MaxNumberOfStrings: %w", err)
	}
	if int(maxStrings) != s.cfg.MaxNumberOfStrings {
		return fmt.Errorf("config mismatch: MaxNumberOfStrings")
	}
	if err := binary.Read(buf, binary.LittleEndian, &startObjId); err != nil {
		return err
	}
	if startObjId != s.cfg.StartingObjectId {
		return fmt.Errorf("config mismatch: StartingObjectId")
	}
	if err := binary.Read(buf, binary.LittleEndian, &objInterval); err != nil {
		return err
	}
	if objInterval != s.cfg.ObjectIdInterval {
		return fmt.Errorf("config mismatch: ObjectIdInterval")
	}

	// Allocator state
	var nextId, maxAllocated bobbob.ObjectId
	var nextOff int64
	var strdataObjId bobbob.ObjectId
	if err := binary.Read(buf, binary.LittleEndian, &nextId); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &maxAllocated); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &nextOff); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &strdataObjId); err != nil {
		return err
	}

	// Freelist
	var freeCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &freeCount); err != nil {
		return err
	}
	s.freeList = make(map[bobbob.ObjectId]struct{}, freeCount)
	for i := 0; i < int(freeCount); i++ {
		var objId bobbob.ObjectId
		if err := binary.Read(buf, binary.LittleEndian, &objId); err != nil {
			return err
		}
		s.freeList[objId] = struct{}{}
	}

	// offsetLookup
	var lookupCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &lookupCount); err != nil {
		return err
	}
	s.offsetLookup = make(map[bobbob.ObjectId]stringLoc, lookupCount)
	for i := 0; i < int(lookupCount); i++ {
		var objId bobbob.ObjectId
		var fileOffset, length int64
		if err := binary.Read(buf, binary.LittleEndian, &objId); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &fileOffset); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return err
		}
		s.offsetLookup[objId] = stringLoc{
			fileOffset: bobbob.FileOffset(fileOffset),
			length:     int(length),
		}
	}

	// Update state
	s.nextId = nextId
	s.maxAllocated = maxAllocated
	s.nextOffset = bobbob.FileOffset(nextOff)
	s.strdataObjId = strdataObjId
	s.modified.Store(false)

	return nil
}

// MarshalData returns file contents.
func (s *stringStoreShard) MarshalData() ([]byte, error) {
	if s.unloaded {
		return nil, errShardUnloaded
	}
	// Ensure all pending writes are flushed to disk
	if err := s.flushWrites(); err != nil {
		return nil, err
	}
	
	// Wait for all disk I/O tasks to complete
	s.diskWritePool.waitForPending()
	
	s.ioMu.RLock()
	defer s.ioMu.RUnlock()

	if _, err := s.file.Seek(0, 0); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(s.file)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *stringStoreShard) snapshotToStore(st bobbob.Storer) (bobbob.ObjectId, bobbob.ObjectId, int64, error) {
	s.useMu.Lock()
	defer s.useMu.Unlock()

	if s.unloaded {
		return s.stateObjId, s.strdataObjId, s.strdataSize, nil
	}

	if err := s.flushWrites(); err != nil {
		return bobbob.ObjNotAllocated, bobbob.ObjNotAllocated, 0, err
	}

	stateBytes, err := s.MarshalState()
	if err != nil {
		return bobbob.ObjNotAllocated, bobbob.ObjNotAllocated, 0, fmt.Errorf("marshal shard state: %w", err)
	}
	dataBytes, err := s.MarshalData()
	if err != nil {
		return bobbob.ObjNotAllocated, bobbob.ObjNotAllocated, 0, fmt.Errorf("marshal shard data: %w", err)
	}

	stateObjId, err := writeNewBytesToStore(st, stateBytes)
	if err != nil {
		return bobbob.ObjNotAllocated, bobbob.ObjNotAllocated, 0, fmt.Errorf("persist shard state: %w", err)
	}
	strdataObjId, err := writeNewBytesToStore(st, dataBytes)
	if err != nil {
		return bobbob.ObjNotAllocated, bobbob.ObjNotAllocated, 0, fmt.Errorf("persist shard data: %w", err)
	}

	s.stateObjId = stateObjId
	s.strdataObjId = strdataObjId
	s.strdataSize = int64(len(dataBytes))
	return stateObjId, strdataObjId, s.strdataSize, nil
}

func (s *stringStoreShard) hasFreeSlot() bool {
	if s.unloaded {
		return s.unloadedHasFreeSlot
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	if len(s.freeList) > 0 {
		return true
	}

	maxObjects := bobbob.ObjectId(s.cfg.MaxNumberOfStrings) * s.cfg.ObjectIdInterval
	maxId := s.cfg.StartingObjectId + maxObjects
	return s.nextId < maxId
}
