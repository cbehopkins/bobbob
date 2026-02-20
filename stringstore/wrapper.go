package stringstore

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

type StringStore struct {
	cfg        Config
	mu         sync.RWMutex
	closed     bool
	compacting bool
	shards     []*stringStoreShard
	// wrapperStateObjId tracks the BaseStore ObjectId for wrapper state persistence.
	wrapperStateObjId bobbob.ObjectId
	parentStore       bobbob.Storer
	idleStop          chan struct{}
	idleDone          chan struct{}
}

func NewStringStore(cfg Config) (*StringStore, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &StringStore{cfg: normalized}, nil
}

// Config returns a copy of the normalized store configuration.
func (s *StringStore) Config() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// AttachParentStore provides the backing store used for unload/load operations.
func (s *StringStore) AttachParentStore(st bobbob.Storer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.parentStore = st
	if s.cfg.UnloadAfterIdle > 0 && s.idleStop == nil {
		s.idleStop = make(chan struct{})
		s.idleDone = make(chan struct{})
		go s.idleUnloadWorker(s.idleStop, s.idleDone)
	}
}

func (s *StringStore) idleUnloadWorker(stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)
	interval := s.cfg.UnloadScanInterval
	if interval <= 0 {
		interval = defaultUnloadScanInterval(s.cfg.UnloadAfterIdle)
	}
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			s.unloadIdleShards()
		}
	}
}

func (s *StringStore) NewObj(size int) (bobbob.ObjectId, error) {
	objId, _, finisher, err := s.LateWriteNewObj(size)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}
	if finisher == nil {
		return objId, nil
	}
	if err := finisher(); err != nil {
		return bobbob.ObjNotAllocated, err
	}
	return objId, nil
}

func (s *StringStore) PrimeObject(size int) (bobbob.ObjectId, error) {
	shard, err := s.ensureShard(0)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}
	shard.useMu.RLock()
	shard.touch()
	objId, err := shard.PrimeObject(size)
	shard.useMu.RUnlock()
	return objId, err
}

func (s *StringStore) LateWriteNewObj(size int) (bobbob.ObjectId, io.Writer, bobbob.Finisher, error) {
	if size < 0 {
		return bobbob.ObjNotAllocated, nil, nil, errors.New("size must be >= 0")
	}

	for {
		shard, err := s.selectShardForWrite()
		if err != nil {
			return bobbob.ObjNotAllocated, nil, nil, err
		}
		shard.useMu.RLock()
		shard.touch()
		objId, writer, finisher, err := shard.LateWriteNewObj(size)
		if err == nil {
			return objId, writer, wrapFinisherWithUnlock(shard, finisher), nil
		}
		shard.useMu.RUnlock()
		if err != errStoreCapacity {
			return bobbob.ObjNotAllocated, nil, nil, err
		}
		if err := s.createNextShard(); err != nil {
			return bobbob.ObjNotAllocated, nil, nil, err
		}
	}
}

func readBytesFromStore(st bobbob.Storer, objId bobbob.ObjectId) ([]byte, error) {
	reader, finisher, err := st.LateReadObj(objId)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(reader)
	if finisher != nil {
		finishErr := finisher()
		if err != nil {
			if finishErr != nil {
				return nil, fmt.Errorf("read failed: %w; finisher failed: %v", err, finishErr)
			}
			return nil, err
		}
		if finishErr != nil {
			return nil, fmt.Errorf("read finisher failed: %w", finishErr)
		}
	}
	return data, err
}

func writeNewBytesToStore(st bobbob.Storer, data []byte) (bobbob.ObjectId, error) {
	objId, writer, finisher, err := st.LateWriteNewObj(len(data))
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}
	if _, err := writer.Write(data); err != nil {
		if finisher != nil {
			_ = finisher()
		}
		return bobbob.ObjNotAllocated, err
	}
	if finisher != nil {
		if err := finisher(); err != nil {
			return bobbob.ObjNotAllocated, fmt.Errorf("write finisher failed: %w", err)
		}
	}
	return objId, nil
}

func writeBytesToExistingObj(st bobbob.Storer, objId bobbob.ObjectId, data []byte) error {
	writer, finisher, err := st.WriteToObj(objId)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	if err != nil {
		if finisher != nil {
			_ = finisher()
		}
		return err
	}
	if finisher != nil {
		if err := finisher(); err != nil {
			return fmt.Errorf("update finisher failed: %w", err)
		}
	}
	return nil
}

func wrapFinisherWithUnlock(shard *stringStoreShard, finisher bobbob.Finisher) bobbob.Finisher {
	return func() error {
		var err error
		if finisher != nil {
			err = finisher()
		}
		shard.useMu.RUnlock()
		return err
	}
}

// PersistToStore writes StringStore wrapper/shard state into the provided storer.
// Returns the wrapper state object ID to be persisted by the caller for reloading.
func (s *StringStore) PersistToStore(st bobbob.Storer, wrapperObjId bobbob.ObjectId) (bobbob.ObjectId, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return bobbob.ObjNotAllocated, errStoreClosed
	}
	shards := append([]*stringStoreShard(nil), s.shards...)
	s.mu.RUnlock()

	state := WrapperState{Version: 1, ShardRecords: make([]ShardRecord, 0, len(shards))}

	for _, shard := range shards {
		if shard.unloaded {
			if !store.IsValidObjectId(store.ObjectId(shard.stateObjId)) || !store.IsValidObjectId(store.ObjectId(shard.strdataObjId)) {
				return bobbob.ObjNotAllocated, fmt.Errorf("unloaded shard missing persisted state")
			}
			state.ShardRecords = append(state.ShardRecords, ShardRecord{
				StateObjId:  shard.stateObjId,
				StrdataObjId: shard.strdataObjId,
				StrdataSize:  shard.strdataSize,
			})
			continue
		}

		shard.useMu.RLock()
		shard.touch()
		if err := shard.flushWrites(); err != nil {
			shard.useMu.RUnlock()
			return bobbob.ObjNotAllocated, err
		}

		stateBytes, err := shard.MarshalState()
		if err != nil {
			shard.useMu.RUnlock()
			return bobbob.ObjNotAllocated, fmt.Errorf("marshal shard state: %w", err)
		}
		dataBytes, err := shard.MarshalData()
		if err != nil {
			shard.useMu.RUnlock()
			return bobbob.ObjNotAllocated, fmt.Errorf("marshal shard data: %w", err)
		}

		stateObjId, err := writeNewBytesToStore(st, stateBytes)
		if err != nil {
			shard.useMu.RUnlock()
			return bobbob.ObjNotAllocated, fmt.Errorf("persist shard state: %w", err)
		}
		strdataObjId, err := writeNewBytesToStore(st, dataBytes)
		if err != nil {
			shard.useMu.RUnlock()
			return bobbob.ObjNotAllocated, fmt.Errorf("persist shard data: %w", err)
		}

		shard.stateObjId = stateObjId
		shard.strdataObjId = strdataObjId
		shard.strdataSize = int64(len(dataBytes))
		state.ShardRecords = append(state.ShardRecords, ShardRecord{
			StateObjId:  stateObjId,
			StrdataObjId: strdataObjId,
			StrdataSize:  int64(len(dataBytes)),
		})
		shard.useMu.RUnlock()
	}

	wrapperData, err := marshalWrapperState(state)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}

	if wrapperObjId > bobbob.ObjNotAllocated {
		if err := writeBytesToExistingObj(st, wrapperObjId, wrapperData); err == nil {
			s.wrapperStateObjId = wrapperObjId
			return wrapperObjId, nil
		}
	}

	newWrapperObjId, err := writeNewBytesToStore(st, wrapperData)
	if err != nil {
		return bobbob.ObjNotAllocated, err
	}
	s.wrapperStateObjId = newWrapperObjId
	return newWrapperObjId, nil
}

// LoadStringStoreFromStore restores StringStore state from a wrapper state object.
func LoadStringStoreFromStore(cfg Config, st bobbob.Storer, wrapperObjId bobbob.ObjectId) (*StringStore, error) {
	ss, err := NewStringStore(cfg)
	if err != nil {
		return nil, err
	}

	wrapperData, err := readBytesFromStore(st, wrapperObjId)
	if err != nil {
		return nil, fmt.Errorf("read wrapper state: %w", err)
	}
	state, err := unmarshalWrapperState(wrapperData)
	if err != nil {
		return nil, fmt.Errorf("unmarshal wrapper state: %w", err)
	}

	for i, rec := range state.ShardRecords {
		shardCfg := ss.cfg
		shardCfg.FilePath = shardFilePath(ss.cfg.FilePath, i)
		shardCfg.StartingObjectId = ss.cfg.StartingObjectId + bobbob.ObjectId(i)*bobbob.ObjectId(ss.cfg.MaxNumberOfStrings)*ss.cfg.ObjectIdInterval

		if ss.cfg.LazyLoadShards {
			stateBytes, err := readBytesFromStore(st, rec.StateObjId)
			if err != nil {
				return nil, fmt.Errorf("read shard state: %w", err)
			}
			summary, err := parseShardStateSummary(stateBytes)
			if err != nil {
				return nil, fmt.Errorf("parse shard state: %w", err)
			}
			if int(summary.maxStrings) != shardCfg.MaxNumberOfStrings || summary.startObjId != shardCfg.StartingObjectId || summary.objInterval != shardCfg.ObjectIdInterval {
				return nil, fmt.Errorf("config mismatch loading shard %d", i)
			}
			maxObjects := bobbob.ObjectId(shardCfg.MaxNumberOfStrings) * shardCfg.ObjectIdInterval
			maxId := shardCfg.StartingObjectId + maxObjects
			hasFreeSlot := summary.freeListCount > 0 || summary.nextId < maxId
			unloadedShard := newUnloadedStringStoreShard(shardCfg, rec.StateObjId, rec.StrdataObjId, rec.StrdataSize, hasFreeSlot)
			ss.shards = append(ss.shards, unloadedShard)
			continue
		}

		shard, err := loadShardFromStore(shardCfg, st, rec)
		if err != nil {
			return nil, err
		}
		ss.shards = append(ss.shards, shard)
	}

	ss.wrapperStateObjId = wrapperObjId
	return ss, nil
}

func loadShardFromStore(cfg Config, st bobbob.Storer, rec ShardRecord) (*stringStoreShard, error) {
	shard, err := newStringStoreShard(cfg)
	if err != nil {
		return nil, err
	}

	stateBytes, err := readBytesFromStore(st, rec.StateObjId)
	if err != nil {
		_ = shard.Close()
		return nil, fmt.Errorf("read shard state: %w", err)
	}
	if err := shard.UnmarshalState(stateBytes); err != nil {
		_ = shard.Close()
		return nil, fmt.Errorf("unmarshal shard state: %w", err)
	}

	dataBytes, err := readBytesFromStore(st, rec.StrdataObjId)
	if err != nil {
		_ = shard.Close()
		return nil, fmt.Errorf("read shard data: %w", err)
	}

	shard.ioMu.Lock()
	if err := shard.file.Truncate(0); err != nil {
		shard.ioMu.Unlock()
		_ = shard.Close()
		return nil, err
	}
	if _, err := shard.file.Seek(0, 0); err != nil {
		shard.ioMu.Unlock()
		_ = shard.Close()
		return nil, err
	}
	if _, err := shard.file.Write(dataBytes); err != nil {
		shard.ioMu.Unlock()
		_ = shard.Close()
		return nil, err
	}
	shard.ioMu.Unlock()

	shard.stateObjId = rec.StateObjId
	shard.strdataObjId = rec.StrdataObjId
	shard.strdataSize = rec.StrdataSize
	return shard, nil
}

func (s *StringStore) ensureShardLoaded(shardIndex int) (*stringStoreShard, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errStoreClosed
	}
	if shardIndex < 0 || shardIndex >= len(s.shards) {
		s.mu.RUnlock()
		return nil, io.ErrUnexpectedEOF
	}
	shard := s.shards[shardIndex]
	if !shard.unloaded {
		s.mu.RUnlock()
		return shard, nil
	}
	parent := s.parentStore
	s.mu.RUnlock()

	if parent == nil {
		return nil, errNoParentStore
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errStoreClosed
	}
	if shardIndex < 0 || shardIndex >= len(s.shards) {
		return nil, io.ErrUnexpectedEOF
	}
	shard = s.shards[shardIndex]
	if !shard.unloaded {
		return shard, nil
	}

	rec := ShardRecord{StateObjId: shard.stateObjId, StrdataObjId: shard.strdataObjId, StrdataSize: shard.strdataSize}
	loadedShard, err := loadShardFromStore(shard.cfg, parent, rec)
	if err != nil {
		return nil, err
	}
	s.shards[shardIndex] = loadedShard
	return loadedShard, nil
}

func (s *StringStore) unloadShardByIndex(shardIndex int) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errStoreClosed
	}
	if shardIndex < 0 || shardIndex >= len(s.shards) {
		s.mu.RUnlock()
		return io.ErrUnexpectedEOF
	}
	shard := s.shards[shardIndex]
	parent := s.parentStore
	s.mu.RUnlock()

	if parent == nil {
		return errNoParentStore
	}
	if shard.unloaded {
		return nil
	}

	stateObjId, strdataObjId, strdataSize, err := shard.snapshotToStore(parent)
	if err != nil {
		return err
	}

	hasFreeSlot := shard.hasFreeSlot()
	if closeErr := shard.Close(); closeErr != nil {
		return closeErr
	}

	unloadedShard := newUnloadedStringStoreShard(shard.cfg, stateObjId, strdataObjId, strdataSize, hasFreeSlot)

	s.mu.Lock()
	if shardIndex >= 0 && shardIndex < len(s.shards) {
		s.shards[shardIndex] = unloadedShard
	}
	s.mu.Unlock()

	return nil
}

func (s *StringStore) unloadIdleShards() {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return
	}
	if s.parentStore == nil || s.cfg.UnloadAfterIdle <= 0 {
		s.mu.RUnlock()
		return
	}
	shards := append([]*stringStoreShard(nil), s.shards...)
	s.mu.RUnlock()

	now := time.Now()
	for i, shard := range shards {
		if shard.unloaded {
			continue
		}
		last := shard.lastAccessTime()
		if last.IsZero() {
			continue
		}
		if now.Sub(last) >= s.cfg.UnloadAfterIdle {
			_ = s.unloadShardByIndex(i)
		}
	}
}

// UnloadAll persists all shards and releases their in-memory state.
func (s *StringStore) UnloadAll() error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errStoreClosed
	}
	count := len(s.shards)
	s.mu.RUnlock()

	for i := 0; i < count; i++ {
		if err := s.unloadShardByIndex(i); err != nil {
			return err
		}
	}
	return nil
}

func (s *StringStore) WriteToObj(objectId bobbob.ObjectId) (io.Writer, bobbob.Finisher, error) {
	shard, err := s.shardForObjectId(objectId)
	if err != nil {
		return nil, nil, err
	}
	shard.useMu.RLock()
	shard.touch()
	writer, finisher, err := shard.WriteToObj(objectId)
	if err != nil {
		shard.useMu.RUnlock()
		return nil, nil, err
	}
	return writer, wrapFinisherWithUnlock(shard, finisher), nil
}

func (s *StringStore) WriteBatchedObjs(objIds []bobbob.ObjectId, data []byte, sizes []int) error {
	if len(objIds) == 0 {
		return nil
	}
	shardIndex, err := s.shardIndexForObjectId(objIds[0])
	if err != nil {
		return err
	}
	for i := 1; i < len(objIds); i++ {
		idx, err := s.shardIndexForObjectId(objIds[i])
		if err != nil {
			return err
		}
		if idx != shardIndex {
			return errors.New("batched writes must target a single shard")
		}
	}
	shard, err := s.getShard(shardIndex)
	if err != nil {
		return err
	}
	shard.useMu.RLock()
	shard.touch()
	err = shard.WriteBatchedObjs(objIds, data, sizes)
	shard.useMu.RUnlock()
	return err
}

func (s *StringStore) LateReadObj(id bobbob.ObjectId) (io.Reader, bobbob.Finisher, error) {
	shard, err := s.shardForObjectId(id)
	if err != nil {
		return nil, nil, err
	}
	shard.useMu.RLock()
	shard.touch()
	reader, finisher, err := shard.LateReadObj(id)
	if err != nil {
		shard.useMu.RUnlock()
		return nil, nil, err
	}
	return reader, wrapFinisherWithUnlock(shard, finisher), nil
}

func (s *StringStore) DeleteObj(objId bobbob.ObjectId) error {
	shard, err := s.shardForObjectId(objId)
	if err != nil {
		return err
	}
	shard.useMu.RLock()
	shard.touch()
	err = shard.DeleteObj(objId)
	shard.useMu.RUnlock()
	return err
}

func (s *StringStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errStoreClosed
	}
	s.closed = true
	if s.idleStop != nil {
		close(s.idleStop)
		idleDone := s.idleDone
		s.idleStop = nil
		s.idleDone = nil
		s.mu.Unlock()
		if idleDone != nil {
			<-idleDone
		}
		s.mu.Lock()
	}
	shards := append([]*stringStoreShard(nil), s.shards...)
	s.mu.Unlock()

	var firstErr error
	for _, shard := range shards {
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *StringStore) Compact() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errStoreClosed
	}
	if s.compacting {
		s.mu.Unlock()
		return errStoreCompacting
	}
	s.compacting = true
	shards := append([]*stringStoreShard(nil), s.shards...)
	s.mu.Unlock()

	for _, shard := range shards {
		shard.useMu.RLock()
		shard.touch()
		if err := shard.Compact(); err != nil {
			shard.useMu.RUnlock()
			s.mu.Lock()
			s.compacting = false
			s.mu.Unlock()
			return err
		}
		shard.useMu.RUnlock()
	}

	s.mu.Lock()
	s.compacting = false
	s.mu.Unlock()

	return nil
}

func (s *StringStore) shardIndexForObjectId(id bobbob.ObjectId) (int, error) {
	s.mu.RLock()
	shards := append([]*stringStoreShard(nil), s.shards...)
	s.mu.RUnlock()
	if len(shards) == 0 {
		return -1, errors.New("invalid objectId")
	}

	for i, shard := range shards {
		start := shard.cfg.StartingObjectId
		if id < start {
			continue
		}
		if (id-start)%shard.cfg.ObjectIdInterval != 0 {
			continue
		}
		span := bobbob.ObjectId(shard.cfg.MaxNumberOfStrings) * shard.cfg.ObjectIdInterval
		if id < start+span {
			return i, nil
		}
	}

	return -1, errors.New("invalid objectId")
}

// OwnsObjectId returns true if the given ObjectId falls within any existing shard range.
// This checks alignment and bounds without verifying if the object currently exists.
func (s *StringStore) OwnsObjectId(id bobbob.ObjectId) bool {
	_, err := s.shardIndexForObjectId(id)
	return err == nil
}

func (s *StringStore) shardForObjectId(id bobbob.ObjectId) (*stringStoreShard, error) {
	shardIndex, err := s.shardIndexForObjectId(id)
	if err != nil {
		return nil, err
	}
	return s.getShard(shardIndex)
}

func (s *StringStore) getShard(shardIndex int) (*stringStoreShard, error) {
	return s.ensureShardLoaded(shardIndex)
}

func (s *StringStore) selectShardForWrite() (*stringStoreShard, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errStoreClosed
	}
	if s.compacting {
		s.mu.RUnlock()
		return nil, errStoreCompacting
	}
	shards := append([]*stringStoreShard(nil), s.shards...)
	s.mu.RUnlock()

	for i, shard := range shards {
		if shard.unloaded {
			if !shard.hasFreeSlot() {
				continue
			}
			loadedShard, err := s.ensureShardLoaded(i)
			if err != nil {
				return nil, err
			}
			if loadedShard.hasFreeSlot() {
				return loadedShard, nil
			}
			continue
		}
		if shard.hasFreeSlot() {
			return shard, nil
		}
	}

	if s.cfg.UnloadOnFull && s.parentStore != nil {
		for i, shard := range shards {
			if shard.unloaded {
				continue
			}
			if !shard.hasFreeSlot() {
				_ = s.unloadShardByIndex(i)
			}
		}
	}

	return s.ensureShard(len(shards))
}

func (s *StringStore) ensureShard(shardIndex int) (*stringStoreShard, error) {
	if shardIndex < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	s.mu.RLock()
	if shardIndex < len(s.shards) {
		s.mu.RUnlock()
		return s.ensureShardLoaded(shardIndex)
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errStoreClosed
	}
	if s.compacting {
		return nil, errStoreCompacting
	}
	if shardIndex < len(s.shards) {
		return s.shards[shardIndex], nil
	}
	if shardIndex != len(s.shards) {
		return nil, io.ErrUnexpectedEOF
	}

	shard, err := s.createShardLocked(shardIndex)
	if err != nil {
		return nil, err
	}
	s.shards = append(s.shards, shard)
	return shard, nil
}

func (s *StringStore) createNextShard() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	if s.compacting {
		return errStoreCompacting
	}
	shard, err := s.createShardLocked(len(s.shards))
	if err != nil {
		return err
	}
	s.shards = append(s.shards, shard)
	return nil
}

func (s *StringStore) createShardLocked(shardIndex int) (*stringStoreShard, error) {
	shardCfg := s.cfg
	shardCfg.FilePath = shardFilePath(s.cfg.FilePath, shardIndex)
	shardCfg.StartingObjectId = s.cfg.StartingObjectId + bobbob.ObjectId(shardIndex)*bobbob.ObjectId(s.cfg.MaxNumberOfStrings)*s.cfg.ObjectIdInterval
	return newStringStoreShard(shardCfg)
}

var _ bobbob.Storer = (*StringStore)(nil)
