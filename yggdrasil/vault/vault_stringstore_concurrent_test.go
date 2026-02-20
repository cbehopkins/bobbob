package vault

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestConcurrentMultiStore_StringStorer_Simple verifies StringStorer works
// with ConcurrentMultiStore in a simple single-threaded scenario.
func TestConcurrentMultiStore_StringStorer_Simple(t *testing.T) {
	tempDir := t.TempDir()
	storePath := tempDir + "/simple_stringstore_test.db"

	// Create ConcurrentMultiStore
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
		v, "test_col", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert a small dataset without background monitoring
	size := 100
	for i := range size {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(i)}
		coll.Insert(&key, payload)
	}

	// Persist
	if err := coll.Persist(); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Verify we can read back
	for i := range size {
		key := types.IntKey(i)
		node := coll.Search(&key)
		if node == nil {
			t.Errorf("Key %d not found", i)
			continue
		}
		value := node.GetPayload()
		expectedValue := "v_" + strconv.Itoa(i)
		if value.Value != expectedValue {
			t.Errorf("Key %d: got %q, expected %q", i, value.Value, expectedValue)
		}
	}

	t.Log("SUCCESS: ConcurrentMultiStore with StringStorer works correctly")
}

// BenchmarkConcurrentMultiStore_StringStorer_Small benchmarks a small dataset
// to verify StringStorer is being used without hanging.
func BenchmarkConcurrentMultiStore_StringStorer_Small(b *testing.B) {
	tempDir := b.TempDir()
	storePath := tempDir + "/bench_stringstore_concurrent.db"

	// Create ConcurrentMultiStore
	stre, err := multistore.NewConcurrentMultiStore(storePath, 0)
	if err != nil {
		b.Fatalf("Failed to create concurrent multistore: %v", err)
	}
	defer stre.Close()

	v, err := LoadVault(stre)
	if err != nil {
		b.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[string]{})

	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
		v, "bench_col", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		b.Fatalf("Failed to create collection: %v", err)
	}

	// Smaller dataset: 1000 items instead of 200,000
	size := 1000
	batchSize := 100

	// Initial load
	for i := range size {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(i)}
		coll.Insert(&key, payload)
	}

	if err := coll.Persist(); err != nil {
		b.Fatalf("Initial Persist failed: %v", err)
	}

	nextKey := size
	b.ResetTimer()

	// Run benchmark without background monitoring
	for i := 0; i < b.N; i++ {
		for range batchSize {
			key := types.IntKey(nextKey)
			payload := types.JsonPayload[string]{Value: "v_" + strconv.Itoa(nextKey)}
			coll.Insert(&key, payload)
			nextKey++
		}

		if err := coll.Persist(); err != nil {
			b.Fatalf("Persist failed at iteration %d: %v", i, err)
		}
	}

	b.StopTimer()
	fmt.Printf("Inserted and persisted %d items in batches of %d\n", nextKey-size, batchSize)
}
