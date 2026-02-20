package vault

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestPersist_BasicStore_WithJsonPayload tests BasicStore with JsonPayload (no StringStore).
// BasicStore does not have StringStorer interface, so JsonPayload will use generic allocation.
func TestPersist_BasicStore_WithJsonPayload(t *testing.T) {
	size := 5_000

	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "basic_jsonpayload.db")
	
	stre, err := store.NewBasicStore(storePath)
	if err != nil {
		t.Fatalf("Failed to create basic store: %v", err)
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

	t.Logf("SUCCESS: BasicStore + JsonPayload completed with %d items", size)
}
