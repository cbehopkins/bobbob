package vault_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/internal/testutil"
	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
	"github.com/cbehopkins/bobbob/yggdrasil/vault"
)

type fixedPayload struct {
	Value int64
}

func (p fixedPayload) Marshal() ([]byte, error) {
	buf := make([]byte, 64)
	binary.LittleEndian.PutUint64(buf[:8], uint64(p.Value))
	return buf, nil
}

func (p fixedPayload) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	if len(data) < 8 {
		return fixedPayload{}, fmt.Errorf("payload too short: %d", len(data))
	}
	v := int64(binary.LittleEndian.Uint64(data[:8]))
	return fixedPayload{Value: v}, nil
}

func (p fixedPayload) SizeInBytes() int {
	return 64
}

type stringPayload string

func (p stringPayload) Marshal() ([]byte, error) {
	return []byte(p), nil
}

func (p stringPayload) Unmarshal(data []byte) (types.UntypedPersistentPayload, error) {
	return stringPayload(data), nil
}

func (p stringPayload) SizeInBytes() int {
	return len(p)
}

// Soak test: with a stable number of items and fixed-size payload updates,
// the vault file size and allocator object count should stabilize over time.
func TestVaultFileSizeStableUnderPayloadChurn(t *testing.T) {
	t.Skip("Currently fails - reuse needs re-work")
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "fixedPayload",
			run: func(t *testing.T) {
				runPayloadChurnSoak[fixedPayload](t, fixedPayload{}, func(i int) fixedPayload {
					return fixedPayload{Value: int64(i)}
				})
			},
		},
		{
			name: "jsonPayloadString",
			run: func(t *testing.T) {
				runPayloadChurnSoak[types.JsonPayload[string]](t, types.JsonPayload[string]{}, func(i int) types.JsonPayload[string] {
					return types.JsonPayload[string]{Value: formatPayloadValue(i)}
				})
			},
		},
		{
			name: "stringPayload",
			run: func(t *testing.T) {
				runPayloadChurnSoak[stringPayload](t, stringPayload(""), func(i int) stringPayload {
					return stringPayload(formatPayloadValue(i))
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func runPayloadChurnSoak[P types.PersistentPayload[P]](
	t *testing.T,
	payloadTemplate P,
	payloadFunc func(int) P,
) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vault_soak.db")

	bs, err := store.NewBasicStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer bs.Close()

	// Optional allocation tracing for debugging (enable with BOBBOB_ALLOC_TRACE=1)
	if os.Getenv("BOBBOB_ALLOC_TRACE") != "" {
		bs.Allocator().SetOnAllocate(func(objId bobbob.ObjectId, fo bobbob.FileOffset, size int) {
			t.Logf("allocator.Allocate: obj=%d fo=%d size=%d", objId, fo, size)
		})
	}

	v, err := vault.LoadVault(bs)
	if err != nil {
		t.Fatalf("failed to load vault: %v", err)
	}
	defer v.Close()

	v.RegisterType((*types.MD5Key)(new(types.MD5Key)))
	v.RegisterType(payloadTemplate)

	collection, err := vault.GetOrCreateCollection[types.MD5Key, P](
		v, "test", types.MD5Less, (*types.MD5Key)(new(types.MD5Key)),
	)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	numItems, churnPerRound, warmupDuration, observeDuration := soakParams()

	for i := 0; i < numItems; i++ {
		key := makeMD5Key(i)
		collection.Insert(&key, payloadFunc(i))
	}

	if err := v.PersistCollection("test"); err != nil {
		t.Fatalf("initial persist failed: %v", err)
	}

	baseCount := bs.GetObjectCount()
	baseSize := int64(0)

	start := time.Now()
	warmupDeadline := start.Add(warmupDuration)
	observeDeadline := warmupDeadline.Add(observeDuration)

	round := 0
	for time.Now().Before(observeDeadline) {
		for i := 0; i < churnPerRound; i++ {
			idx := (round*churnPerRound + i) % numItems
			key := makeMD5Key(idx)

			// Delete and re-insert to force allocator reuse while keeping item count stable.
			collection.Delete(&key)
			collection.Insert(&key, payloadFunc(round*1000+i))
		}

		if err := v.PersistCollection("test"); err != nil {
			t.Fatalf("persist failed in round %d: %v", round, err)
		}

		if baseSize == 0 && time.Now().After(warmupDeadline) {
			baseCount = bs.GetObjectCount()
			baseSize = fileSize(t, dbPath)
		}

		round++
	}

	if baseSize == 0 {
		baseCount = bs.GetObjectCount()
		baseSize = fileSize(t, dbPath)
	}

	finalCount := bs.GetObjectCount()
	finalSize := fileSize(t, dbPath)

	maxCountGrowth := baseCount/10 + 1000 // 10% or 1000 objects, whichever is larger
	if finalCount > baseCount+maxCountGrowth {
		t.Fatalf("object count grew unexpectedly: base=%d final=%d", baseCount, finalCount)
	}

	maxSizeGrowth := baseSize/10 + 1*1024*1024 // 10% or 1MB, whichever is larger
	if finalSize > baseSize+maxSizeGrowth {
		t.Fatalf("file size grew unexpectedly: base=%d final=%d", baseSize, finalSize)
	}
}

func makeMD5Key(i int) types.MD5Key {
	var key types.MD5Key
	copy(key[:], fmt.Sprintf("key-%08d", i))
	return key
}

func formatPayloadValue(i int) string {
	return fmt.Sprintf("v-%08d", i)
}

func soakParams() (numItems int, churnPerRound int, warmupDuration time.Duration, observeDuration time.Duration) {
	if os.Getenv("BOBBOB_LONG_SOAK") != "" {
		return 3000, 300, 5 * time.Second, 15 * time.Second
	}
	return 800, 80, 1 * time.Second, 3 * time.Second
}

func fileSize(t *testing.T, path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat failed for %s: %v", path, err)
	}
	return info.Size()
}

func TestStringPayloadReloadAfterPersist(t *testing.T) {
	dbPath, cleanup := testutil.CreateTempFile(t, "string_payload_reload.db")
	defer cleanup()

	bs, err := store.NewBasicStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	v, err := vault.LoadVault(bs)
	if err != nil {
		t.Fatalf("failed to load vault: %v", err)
	}

	v.RegisterType((*types.MD5Key)(new(types.MD5Key)))
	v.RegisterType(types.StringPayload(""))

	collection, err := vault.GetOrCreateCollection[types.MD5Key, types.StringPayload](
		v, "string_payloads", types.MD5Less, (*types.MD5Key)(new(types.MD5Key)),
	)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	expected := map[int]string{
		1: "alpha",
		2: "bravo",
		3: "charlie",
	}

	for i, val := range expected {
		key := makeMD5Key(i)
		collection.Insert(&key, types.StringPayload(val))
	}

	if err := v.PersistCollection("string_payloads"); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	if err := v.Close(); err != nil {
		t.Fatalf("vault close failed: %v", err)
	}

	bs2, err := store.NewBasicStore(filepath.Clean(dbPath))
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer bs2.Close()

	v2, err := vault.LoadVault(bs2)
	if err != nil {
		t.Fatalf("failed to reload vault: %v", err)
	}
	defer v2.Close()

	v2.RegisterType((*types.MD5Key)(new(types.MD5Key)))
	v2.RegisterType(types.StringPayload(""))

	collection2, err := vault.GetOrCreateCollection[types.MD5Key, types.StringPayload](
		v2, "string_payloads", types.MD5Less, (*types.MD5Key)(new(types.MD5Key)),
	)
	if err != nil {
		t.Fatalf("failed to reload collection: %v", err)
	}

	for i, want := range expected {
		key := makeMD5Key(i)
		got := collection2.Search(&key)
		if got == nil || got.IsNil() {
			t.Fatalf("expected to find key %d after reload", i)
		}

		payload := string(got.GetPayload())
		if payload != want {
			t.Fatalf("payload mismatch for key %d: got %q want %q", i, payload, want)
		}
	}
}
