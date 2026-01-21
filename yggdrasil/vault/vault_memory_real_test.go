package vault

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/store"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestVaultMemoryStatsReal mirrors TestVaultMemoryStats but uses a real disk-backed store.
func TestVaultMemoryStatsReal(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "memory_stats_real.db")
	stre, err := store.NewBasicStore(storePath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	// Register types
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})

	// Create collection
	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Initially should have few/no nodes in memory
	stats := v.GetMemoryStats()
	if stats.TotalInMemoryNodes > 1 {
		t.Errorf("Expected 0-1 nodes initially, got %d", stats.TotalInMemoryNodes)
	}

	// Insert some data
	for i := range 10 {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
	}

	// Should now have nodes in memory
	stats = v.GetMemoryStats()
	if stats.TotalInMemoryNodes == 0 {
		t.Error("Expected nodes in memory after insertion")
	}
	if stats.CollectionNodes["users"] == 0 {
		t.Error("Expected users collection to have nodes in memory")
	}
}

// TestVaultFlushOlderThanReal mirrors TestVaultFlushOlderThan but uses real disk-backed store.
func TestVaultFlushOlderThanReal(t *testing.T) {
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "flush_older_real.db")
	stre, err := store.NewBasicStore(storePath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}
	defer v.Close()

	// Register types
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Insert data
	for i := range 20 {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
	}

	initialStats := v.GetMemoryStats()
	if initialStats.TotalInMemoryNodes == 0 {
		t.Fatal("Expected nodes in memory after insertion")
	}

	// Flush nodes older than 1 second from now (should flush all)
	time.Sleep(10 * time.Millisecond)
	cutoff := time.Now().Unix() + 1
	flushed, err := v.FlushOlderThan(cutoff)
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	if flushed == 0 {
		t.Error("Expected to flush some nodes")
	}

	afterStats := v.GetMemoryStats()
	if afterStats.TotalInMemoryNodes >= initialStats.TotalInMemoryNodes {
		t.Errorf("Expected fewer nodes after flush, had %d, now %d",
			initialStats.TotalInMemoryNodes, afterStats.TotalInMemoryNodes)
	}
}
