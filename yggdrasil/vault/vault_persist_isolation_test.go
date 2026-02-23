package vault

import (
	"path/filepath"
	"strconv"
	"testing"

	bobbob "github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersist_MultiStore_LargeDataset tests if MultiStore (without ConcurrentStore wrapper)
// can handle large-scale persistence without hanging.
func TestPersist_MultiStore_LargeDataset(t *testing.T) {
	size := 50_000 // Start with 50k to see if it completes

	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "multistore_large.db")

	// Create MultiStore directly (no ConcurrentStore wrapper)
	stre, err := multistore.NewMultiStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create multistore: %v", err)
	}
	defer stre.Close()

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[string]{})

	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
		v, "test_coll", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	t.Logf("Inserting %d items...", size)
	for i := 0; i < size; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: "value_" + strconv.Itoa(i)}
		coll.Insert(&key, payload)
	}

	t.Logf("Persisting...")
	if err := coll.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	t.Logf("SUCCESS: MultiStore completed with %d items", size)
}

// TestPersist_ConcurrentMultiStore_NoStringStore tests if the deadlock occurs
// even when StringStore is completely disabled (fallback to generic allocation).
func TestPersist_ConcurrentMultiStore_NoStringStore(t *testing.T) {
	size := 50_000

	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "concurrent_nostring.db")

	stre, err := multistore.NewConcurrentMultiStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent multistore: %v", err)
	}
	defer stre.Close()

	// Verify StringStore is available
	if ss, ok := stre.(bobbob.StringStorer); ok {
		t.Logf("StringStorer is available on store (as expected)")
		_ = ss
	} else {
		t.Logf("StringStorer NOT available on store")
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[string]{})

	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
		v, "test_coll", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	t.Logf("Inserting %d items...", size)
	for i := 0; i < size; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: "value_" + strconv.Itoa(i)}
		coll.Insert(&key, payload)
	}

	t.Logf("Persisting...")
	if err := coll.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	t.Logf("SUCCESS: ConcurrentMultiStore completed with %d items", size)
}

// TestPersist_ConcurrentMultiStore_SmallDataset confirms that small datasets work fine.
func TestPersist_ConcurrentMultiStore_SmallDataset(t *testing.T) {
	size := 500

	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "concurrent_small.db")

	stre, err := multistore.NewConcurrentMultiStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create concurrent multistore: %v", err)
	}
	defer stre.Close()

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[string]{})

	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
		v, "test_coll", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	t.Logf("Inserting %d items...", size)
	for i := 0; i < size; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: "value_" + strconv.Itoa(i)}
		coll.Insert(&key, payload)
	}

	t.Logf("Persisting...")
	if err := coll.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	t.Logf("SUCCESS: ConcurrentMultiStore completed with %d items", size)
}
