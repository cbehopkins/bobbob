package stringstore

import (
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

// waitForWrites allows the async write worker to process pending writes.
// Default flush interval is 10ms, so 20ms ensures at least one flush cycle.
func waitForWrites() {
	time.Sleep(20 * time.Millisecond)
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		FilePath:           filepath.Join(t.TempDir(), "strings.blob"),
		MaxNumberOfStrings: 256,
		StartingObjectId:   100,
		ObjectIdInterval:   4,
	}
}

func TestObjectIdIntervalMapping(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id1, w1, f1, err := s.LateWriteNewObj(5)
	if err != nil {
		t.Fatalf("LateWriteNewObj 1 failed: %v", err)
	}
	if _, err := w1.Write([]byte("alpha")); err != nil {
		t.Fatalf("write 1 failed: %v", err)
	}
	if err := f1(); err != nil {
		t.Fatalf("finisher 1 failed: %v", err)
	}

	id2, w2, f2, err := s.LateWriteNewObj(4)
	if err != nil {
		t.Fatalf("LateWriteNewObj 2 failed: %v", err)
	}
	if _, err := w2.Write([]byte("beta")); err != nil {
		t.Fatalf("write 2 failed: %v", err)
	}
	if err := f2(); err != nil {
		t.Fatalf("finisher 2 failed: %v", err)
	}

	if id1 != 100 {
		t.Fatalf("id1 = %d, want 100", id1)
	}
	if id2 != 104 {
		t.Fatalf("id2 = %d, want 104", id2)
	}

	b1, err := store.ReadBytesFromObj(s, id1)
	if err != nil {
		t.Fatalf("read id1 failed: %v", err)
	}
	b2, err := store.ReadBytesFromObj(s, id2)
	if err != nil {
		t.Fatalf("read id2 failed: %v", err)
	}

	if string(b1) != "alpha" {
		t.Fatalf("id1 contents = %q, want alpha", string(b1))
	}
	if string(b2) != "beta" {
		t.Fatalf("id2 contents = %q, want beta", string(b2))
	}
}

func TestOwnsObjectIdSkipsMissingShardRanges(t *testing.T) {
	cfg := Config{
		FilePath:           filepath.Join(t.TempDir(), "strings.blob"),
		MaxNumberOfStrings: 2,
		StartingObjectId:   1000,
		ObjectIdInterval:   4,
	}

	s, err := NewStringStore(cfg)
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	ids := make([]bobbob.ObjectId, 3)
	for i := range ids {
		id, err := store.WriteNewObjFromBytes(s, []byte("v"))
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
		ids[i] = id
	}

	waitForWrites()

	if len(s.shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(s.shards))
	}
	if ids[0] != 1000 || ids[1] != 1004 || ids[2] != 1008 {
		t.Fatalf("unexpected ids: %v", ids)
	}

	shardSpan := bobbob.ObjectId(cfg.MaxNumberOfStrings) * cfg.ObjectIdInterval
	missingShardStart := cfg.StartingObjectId + 2*shardSpan
	if s.OwnsObjectId(missingShardStart) {
		t.Fatalf("expected OwnsObjectId false for missing shard range (id %d)", missingShardStart)
	}
}

func TestDeleteObjRemovesVisibility(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id, err := store.WriteNewObjFromBytes(s, []byte("hello"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if err := s.DeleteObj(id); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	if _, _, err := s.LateReadObj(id); err == nil {
		t.Fatalf("expected read error after delete")
	}
	if _, _, err := s.WriteToObj(id); err == nil {
		t.Fatalf("expected write error after delete")
	}
}

func TestPrimeObjectStable(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id1, err := s.PrimeObject(8)
	if err != nil {
		t.Fatalf("PrimeObject 1 failed: %v", err)
	}
	id2, err := s.PrimeObject(64)
	if err != nil {
		t.Fatalf("PrimeObject 2 failed: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("prime object IDs differ: %d vs %d", id1, id2)
	}
}

func TestWriteBatchedObjs(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id1, err := s.NewObj(3)
	if err != nil {
		t.Fatalf("NewObj 1 failed: %v", err)
	}
	id2, err := s.NewObj(2)
	if err != nil {
		t.Fatalf("NewObj 2 failed: %v", err)
	}

	data := []byte("abcde")
	if err := s.WriteBatchedObjs([]bobbob.ObjectId{id1, id2}, data, []int{3, 2}); err != nil {
		t.Fatalf("WriteBatchedObjs failed: %v", err)
	}

	b1, err := store.ReadBytesFromObj(s, id1)
	if err != nil {
		t.Fatalf("read id1 failed: %v", err)
	}
	b2, err := store.ReadBytesFromObj(s, id2)
	if err != nil {
		t.Fatalf("read id2 failed: %v", err)
	}

	if string(b1) != "abc" {
		t.Fatalf("id1 contents = %q, want abc", string(b1))
	}
	if string(b2) != "de" {
		t.Fatalf("id2 contents = %q, want de", string(b2))
	}
}

func TestConcurrentLateWriteNewObj(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	const n = 64
	ids := make([]bobbob.ObjectId, n)
	payloads := make([]string, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := fmt.Sprintf("value-%03d", i)
			id, writer, finisher, err := s.LateWriteNewObj(len(payload))
			if err != nil {
				t.Errorf("LateWriteNewObj(%d) failed: %v", i, err)
				return
			}
			if _, err := writer.Write([]byte(payload)); err != nil {
				t.Errorf("write(%d) failed: %v", i, err)
				return
			}
			if err := finisher(); err != nil {
				t.Errorf("finisher(%d) failed: %v", i, err)
				return
			}
			ids[i] = id
			payloads[i] = payload
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		data, err := store.ReadBytesFromObj(s, ids[i])
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if string(data) != payloads[i] {
			t.Fatalf("payload mismatch at %d: got %q want %q", i, string(data), payloads[i])
		}
	}
}

func TestWriteToObjReplacesData(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id, err := s.NewObj(5)
	if err != nil {
		t.Fatalf("NewObj failed: %v", err)
	}

	writer, finisher, err := s.WriteToObj(id)
	if err != nil {
		t.Fatalf("WriteToObj failed: %v", err)
	}
	if _, err := io.WriteString(writer, "hello"); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := finisher(); err != nil {
		t.Fatalf("finisher failed: %v", err)
	}

	data, err := store.ReadBytesFromObj(s, id)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("data = %q, want hello", string(data))
	}
}

func TestDeleteReuseObjectId(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id1, err := store.WriteNewObjFromBytes(s, []byte("first"))
	if err != nil {
		t.Fatalf("write first failed: %v", err)
	}
	if err := s.DeleteObj(id1); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	waitForWrites()

	id2, err := store.WriteNewObjFromBytes(s, []byte("second"))
	if err != nil {
		t.Fatalf("write second failed: %v", err)
	}
	waitForWrites()
	if id2 != id1 {
		t.Fatalf("expected reuse of %d, got %d", id1, id2)
	}

	data, err := store.ReadBytesFromObj(s, id2)
	if err != nil {
		t.Fatalf("read reused id failed: %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("data = %q, want second", string(data))
	}
}

func TestCompactShrinksFile(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id1, err := store.WriteNewObjFromBytes(s, []byte("first"))
	if err != nil {
		t.Fatalf("write first failed: %v", err)
	}
	id2, err := store.WriteNewObjFromBytes(s, []byte("second"))
	if err != nil {
		t.Fatalf("write second failed: %v", err)
	}
	if _, err := store.ReadBytesFromObj(s, id2); err != nil {
		t.Fatalf("read second before stat failed: %v", err)
	}
	waitForWrites()

	infoBefore, err := s.shards[0].file.Stat()
	if err != nil {
		t.Fatalf("stat before failed: %v", err)
	}

	if err := s.DeleteObj(id1); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	waitForWrites()
	if err := s.Compact(); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	infoAfter, err := s.shards[0].file.Stat()
	if err != nil {
		t.Fatalf("stat after failed: %v", err)
	}
	if infoAfter.Size() >= infoBefore.Size() {
		t.Fatalf("expected file to shrink: before=%d after=%d", infoBefore.Size(), infoAfter.Size())
	}

	data, err := store.ReadBytesFromObj(s, id2)
	if err != nil {
		t.Fatalf("read remaining id failed: %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("data = %q, want second", string(data))
	}

	data, err = store.ReadBytesFromObj(s, id1)
	if err == nil {
		t.Fatalf("expected deleted id to remain unreadable, got %q", string(data))
	}
}

func TestShardRolloverAllocatesNewRange(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxNumberOfStrings = 2
	s, err := NewStringStore(cfg)
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	id1, err := store.WriteNewObjFromBytes(s, []byte("a"))
	if err != nil {
		t.Fatalf("write 1 failed: %v", err)
	}
	id2, err := store.WriteNewObjFromBytes(s, []byte("b"))
	if err != nil {
		t.Fatalf("write 2 failed: %v", err)
	}
	id3, err := store.WriteNewObjFromBytes(s, []byte("c"))
	if err != nil {
		t.Fatalf("write 3 failed: %v", err)
	}

	if id1 != cfg.StartingObjectId {
		t.Fatalf("id1 = %d, want %d", id1, cfg.StartingObjectId)
	}
	if id2 != cfg.StartingObjectId+cfg.ObjectIdInterval {
		t.Fatalf("id2 = %d, want %d", id2, cfg.StartingObjectId+cfg.ObjectIdInterval)
	}
	if id3 != cfg.StartingObjectId+cfg.ObjectIdInterval*2 {
		t.Fatalf("id3 = %d, want %d", id3, cfg.StartingObjectId+cfg.ObjectIdInterval*2)
	}

	if len(s.shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(s.shards))
	}
}

func TestConcurrentMixedOps(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxNumberOfStrings = 32
	s, err := NewStringStore(cfg)
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	var (
		mu     sync.Mutex
		ids    []bobbob.ObjectId
		active = make(map[bobbob.ObjectId]bool)
	)

	var writeCount atomic.Int64
	const workers = 8
	const perWorker = 50

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < perWorker; i++ {
				payload := fmt.Sprintf("val-%d-%d", seed, i)
				id, err := store.WriteNewObjFromBytes(s, []byte(payload))
				if err != nil {
					t.Errorf("write failed: %v", err)
					return
				}
				writeCount.Add(1)

				mu.Lock()
				ids = append(ids, id)
				active[id] = true
				mu.Unlock()

				if rng.Intn(4) == 0 {
					if err := s.DeleteObj(id); err == nil {
						mu.Lock()
						active[id] = false
						mu.Unlock()
					}
				}
			}
		}(time.Now().UnixNano() + int64(w))
	}
	wg.Wait()

	mu.Lock()
	localIds := append([]bobbob.ObjectId(nil), ids...)
	localActive := make(map[bobbob.ObjectId]bool, len(active))
	for id, val := range active {
		localActive[id] = val
	}
	mu.Unlock()

	for _, id := range localIds {
		_, err := store.ReadBytesFromObj(s, id)
		if !localActive[id] {
			if err == nil {
				t.Fatalf("expected deleted id %d to be unreadable", id)
			}
			continue
		}
		if err != nil {
			t.Fatalf("read id %d failed: %v", id, err)
		}
	}
}

func TestCompactMultipleCycles(t *testing.T) {
	s, err := NewStringStore(testConfig(t))
	if err != nil {
		t.Fatalf("NewStringStore failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	const count = 40
	ids := make([]bobbob.ObjectId, 0, count)
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("payload-%03d", i)
		id, err := store.WriteNewObjFromBytes(s, []byte(payload))
		if err != nil {
			t.Fatalf("write failed: %v", err)
		}
		ids = append(ids, id)
	}

	for i, id := range ids {
		if i%2 == 0 {
			if err := s.DeleteObj(id); err != nil {
				t.Fatalf("delete failed: %v", err)
			}
		}
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("compact 1 failed: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact 2 failed: %v", err)
	}

	for i, id := range ids {
		data, err := store.ReadBytesFromObj(s, id)
		if i%2 == 0 {
			if err == nil {
				t.Fatalf("expected deleted id %d to be unreadable, got %q", id, string(data))
			}
			continue
		}
		if err != nil {
			t.Fatalf("read id %d failed: %v", id, err)
		}
		expected := fmt.Sprintf("payload-%03d", i)
		if string(data) != expected {
			t.Fatalf("data = %q, want %q", string(data), expected)
		}
	}
}
