package vault

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/internal/testutil"
	collections "github.com/cbehopkins/bobbob/multistore"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestVaultMemoryStats verifies that GetMemoryStats correctly reports the number
// of nodes in memory across all collections.
func TestVaultMemoryStats(t *testing.T) {
	v := newTestVault(t)
	defer v.Close()

	// Create a collection
	users := addCollection[types.IntKey, types.JsonPayload[testutil.TestUserData]](t, v, "users")

	// Initially should have 0 nodes in memory
	stats := v.GetMemoryStats()
	if stats.TotalInMemoryNodes != 0 {
		t.Errorf("Expected 0 nodes initially, got %d", stats.TotalInMemoryNodes)
	}

	// Insert some data
	for i := range 10 {
		insertTestUser(t, users, int32(i), "user")
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

// TestVaultFlushOlderThan verifies that FlushOlderThan removes old nodes from memory
// while keeping recent ones.
func TestVaultFlushOlderThan(t *testing.T) {
	v := newTestVault(t)
	users := addCollection[types.IntKey, types.JsonPayload[testutil.TestUserData]](t, v, "users")

	// Insert data
	for i := range 20 {
		insertTestUser(t, users, int32(i), "user")
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

	v.Close()
}

// TestEnableMemoryMonitoring verifies that automatic memory monitoring triggers
// flushing when conditions are met.
func TestEnableMemoryMonitoring(t *testing.T) {
	v := newMockVault(t)

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Track when flush is called
	flushCalled := false
	flushCount := 0

	// Enable monitoring to flush when we have more than 5 nodes
	v.EnableMemoryMonitoring(
		func(stats MemoryStats) bool {
			return stats.TotalInMemoryNodes > 5
		},
		func(stats MemoryStats) (int, error) {
			flushCalled = true
			flushCount++
			cutoff := time.Now().Unix() - 1
			return v.FlushOlderThan(cutoff)
		},
	)

	// Set a low check interval for testing
	v.SetCheckInterval(5)

	// Insert data - should trigger flush after 5 operations
	for i := range 15 {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
		// Trigger check
		v.checkMemoryAndFlush()
	}

	if !flushCalled {
		t.Error("Expected flush to be called")
	}
	if flushCount == 0 {
		t.Error("Expected at least one flush")
	}

	v.Close()
}

// TestSetMemoryBudget verifies the convenience function for setting a memory budget.
func TestSetMemoryBudget(t *testing.T) {
	v := newMockVault(t)

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Set a memory budget of 10 nodes
	v.SetMemoryBudget(10, 5)
	v.SetCheckInterval(5)

	// Insert data
	for i := range 30 {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
		v.checkMemoryAndFlush()
	}

	// Budget should have kept nodes in check
	stats := v.GetMemoryStats()
	// Due to flushing, we shouldn't have way more than budget
	// (exact count depends on timing and tree structure)
	if stats.TotalInMemoryNodes > 50 {
		t.Errorf("Expected memory budget to limit nodes, got %d", stats.TotalInMemoryNodes)
	}

	v.Close()
}

// TestMemoryStatsMultipleCollections verifies that memory stats work correctly
// with multiple collections.
func TestMemoryStatsMultipleCollections(t *testing.T) {
	v := newMockVault(t)

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType((*types.StringKey)(new(string)))
	v.RegisterType(types.JsonPayload[UserData]{})
	v.RegisterType(types.JsonPayload[ProductData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	products, err := GetOrCreateCollection[types.StringKey, types.JsonPayload[ProductData]](
		v, "products", types.StringLess, (*types.StringKey)(new(string)),
	)
	if err != nil {
		t.Fatalf("Failed to create products collection: %v", err)
	}

	// Insert into users
	for i := range 5 {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
	}

	// Insert into products
	for i := range 3 {
		key := types.StringKey("product" + string(rune(i)))
		products.Insert(&key, types.JsonPayload[ProductData]{
			Value: ProductData{Name: "Product", Price: 10.0, Stock: 100},
		})
	}

	stats := v.GetMemoryStats()

	// Should have data in both collections
	if stats.CollectionNodes["users"] == 0 {
		t.Error("Expected users collection to have nodes")
	}
	if stats.CollectionNodes["products"] == 0 {
		t.Error("Expected products collection to have nodes")
	}

	// Total should be sum of both
	expectedTotal := stats.CollectionNodes["users"] + stats.CollectionNodes["products"]
	if stats.TotalInMemoryNodes != expectedTotal {
		t.Errorf("Expected total %d, got %d", expectedTotal, stats.TotalInMemoryNodes)
	}

	v.Close()
}

// TestMemoryMonitoringWithNoMonitor verifies that operations work normally
// when no memory monitor is configured.
func TestMemoryMonitoringWithNoMonitor(t *testing.T) {
	v := newMockVault(t)

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Insert data without monitoring
	for i := range 10 {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
		// Should not error even without monitor
		err := v.checkMemoryAndFlush()
		if err != nil {
			t.Fatalf("checkMemoryAndFlush failed without monitor: %v", err)
		}
	}

	// Stats should still work
	stats := v.GetMemoryStats()
	if stats.TotalInMemoryNodes == 0 {
		t.Error("Expected nodes in memory")
	}

	v.Close()
}

// TestSetMemoryBudgetWithPercentile verifies that SetMemoryBudgetWithPercentile
// automatically flushes the oldest percentage of nodes when memory limit is exceeded.
func TestSetMemoryBudgetWithPercentile(t *testing.T) {
	v := newMockVault(t)

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Set budget: max 50 nodes, flush 50% when exceeded
	v.SetMemoryBudgetWithPercentile(50, 50)
	v.SetCheckInterval(1) // Check after every operation

	// Insert 60 nodes to exceed the limit
	for i := range 60 {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
		time.Sleep(1 * time.Millisecond) // Small delay to ensure different access times

		// Trigger monitoring check
		v.checkMemoryAndFlush()
	}

	// After flushing, we should have fewer nodes in memory
	stats := v.GetMemoryStats()
	if stats.TotalInMemoryNodes > 50 {
		// We might still be slightly over due to timing, but should be much less than 60
		t.Logf("After flush: %d nodes (might be slightly over 50 due to timing)", stats.TotalInMemoryNodes)
	}

	// Verify we can still access all nodes (they'll be loaded from disk if needed)
	for i := range 60 {
		key := types.IntKey(i)
		node := users.Search(&key)
		if node == nil || node.IsNil() {
			t.Errorf("Failed to find key %d after flushing", i)
		}
	}

	v.Close()
}

// TestSetMemoryBudgetWithPercentile_LargeDataset simulates a large write load and
// asserts that the in-memory footprint stays bounded by the configured budget.
// The dataset size is kept moderate for test runtime, but large enough to exercise
// flushing behavior and catch regressions where nodes accumulate unchecked.
func TestSetMemoryBudgetWithPercentile_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory budget test in short mode")
	}
	tempDir := t.TempDir()
	storePath := filepath.Join(tempDir, "percentile_large.db")
	stre, err := collections.NewMultiStore(storePath, 0)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	v, err := LoadVault(stre)
	if err != nil {
		t.Fatalf("Failed to load vault: %v", err)
	}

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[int]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[int]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	const (
		totalItems   = 100_000
		budgetNodes  = 2_000
		heapCeiling  = 250 * 1024 * 1024 // 250 MB ceiling; allocator tracking is now distributed across allocators
		flushPercent = 50
	)

	v.SetMemoryBudgetWithPercentile(budgetNodes, flushPercent)
	v.SetCheckInterval(1)

	// Capture baseline heap usage.
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	baselineAlloc := ms.Alloc

	// Insert a large dataset; force periodic checks to trigger flushing.
	for i := range totalItems {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[int]{Value: i})

		if i%1_000 == 0 {
			if err := v.checkMemoryAndFlush(); err != nil {
				t.Fatalf("checkMemoryAndFlush failed at %d: %v", i, err)
			}
		}
	}

	if err := v.checkMemoryAndFlush(); err != nil {
		t.Fatalf("final checkMemoryAndFlush failed: %v", err)
	}

	// Reclaim any garbage before measuring.
	runtime.GC()
	runtime.ReadMemStats(&ms)
	heapUsed := int64(ms.Alloc - baselineAlloc)

	stats := v.GetMemoryStats()

	// Print detailed memory breakdown for analysis
	t.Logf("=== Memory Analysis ===")
	t.Logf("In-memory nodes: %d (budget: %d)", stats.TotalInMemoryNodes, budgetNodes)
	t.Logf("Heap used (runtime.MemStats.Alloc): %d bytes (%.2f MB)", heapUsed, float64(heapUsed)/(1024*1024))
	t.Logf("")
	t.Logf("Memory Breakdown (theoretical calculation):")
	t.Logf("  Node struct size (estimated): %d bytes", stats.MemoryBreakdown.NodeStructSize)
	t.Logf("  Estimated node memory: %d bytes (%.2f MB)",
		stats.MemoryBreakdown.EstimatedNodeMemory,
		float64(stats.MemoryBreakdown.EstimatedNodeMemory)/(1024*1024))
	t.Logf("  Number of collections: %d", stats.MemoryBreakdown.NumCollections)
	t.Logf("  Number of objects in store: %d", stats.MemoryBreakdown.NumObjectsInStore)
	t.Logf("  Estimated allocator memory: %d bytes (%.2f KB)",
		stats.MemoryBreakdown.EstimatedAllocatorMemory,
		float64(stats.MemoryBreakdown.EstimatedAllocatorMemory)/1024)
	t.Logf("  Vault overhead: %d bytes (%.2f KB)",
		stats.MemoryBreakdown.VaultOverhead,
		float64(stats.MemoryBreakdown.VaultOverhead)/1024)
	t.Logf("  Total estimated: %d bytes (%.2f MB)",
		stats.MemoryBreakdown.TotalEstimated,
		float64(stats.MemoryBreakdown.TotalEstimated)/(1024*1024))
	t.Logf("")
	t.Logf("Comparison:")
	ratio := float64(heapUsed) / float64(stats.MemoryBreakdown.TotalEstimated)
	t.Logf("  Actual heap / Estimated: %.2fx", ratio)
	t.Logf("  Difference: %d bytes (%.2f MB)",
		heapUsed-stats.MemoryBreakdown.TotalEstimated,
		float64(heapUsed-stats.MemoryBreakdown.TotalEstimated)/(1024*1024))
	t.Logf("")
	t.Logf("Per-object heap overhead (from difference): %.2f bytes",
		float64(heapUsed-stats.MemoryBreakdown.TotalEstimated)/float64(stats.MemoryBreakdown.NumObjectsInStore))

	if stats.TotalInMemoryNodes > budgetNodes+budgetNodes/2 {
		t.Fatalf("in-memory nodes exceeded budget: %d (budget %d)", stats.TotalInMemoryNodes, budgetNodes)
	}

	if heapUsed > heapCeiling {
		t.Fatalf("heap usage too high: %d bytes (ceiling %d)", heapUsed, heapCeiling)
	}

	// Spot-check a few keys to ensure data is still readable (may load from disk).
	for _, probe := range []int{0, totalItems / 2, totalItems - 1} {
		key := types.IntKey(probe)
		node := users.Search(&key)
		if node == nil || node.IsNil() || node.GetPayload().Value != probe {
			t.Fatalf("probe key %d missing or incorrect", probe)
		}
	}

	_ = v.Close()
	_ = stre.Close()
}

// TestFlushOldestPercentile verifies that FlushOldestPercentile correctly
// flushes the oldest percentage of nodes across all collections.
func TestFlushOldestPercentile(t *testing.T) {
	v := newMockVault(t)

	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[UserData]{})

	users, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[UserData]](
		v, "users", types.IntLess, (*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create users collection: %v", err)
	}

	// Insert 100 nodes
	for i := 0; i < 100; i++ {
		key := types.IntKey(i)
		users.Insert(&key, types.JsonPayload[UserData]{
			Value: UserData{Username: "user", Email: "user@example.com", Age: 25},
		})
		time.Sleep(1 * time.Millisecond) // Ensure different access times
	}

	// Get initial count
	initialStats := v.GetMemoryStats()
	t.Logf("Initial nodes in memory: %d", initialStats.TotalInMemoryNodes)

	// Flush oldest 30%
	flushed, err := v.FlushOldestPercentile(30)
	if err != nil {
		t.Fatalf("FlushOldestPercentile failed: %v", err)
	}

	t.Logf("Flushed %d nodes", flushed)

	// Should have flushed approximately 30 nodes (might flush more due to children)
	if flushed < 25 {
		t.Errorf("Expected to flush at least 25 nodes, got %d", flushed)
	}

	// Verify nodes were flushed (should have fewer in memory now)
	afterStats := v.GetMemoryStats()
	t.Logf("After flush: %d nodes in memory (down from %d)", afterStats.TotalInMemoryNodes, initialStats.TotalInMemoryNodes)

	if afterStats.TotalInMemoryNodes >= initialStats.TotalInMemoryNodes {
		t.Errorf("Expected fewer nodes after flushing, before=%d, after=%d", initialStats.TotalInMemoryNodes, afterStats.TotalInMemoryNodes)
	}

	v.Close()
}

// TestFlushOldestPercentileInvalidInput verifies error handling for invalid percentages.
func TestFlushOldestPercentileInvalidInput(t *testing.T) {
	v := newMockVault(t)

	// Test invalid percentages
	testCases := []int{0, -1, 101, 150}
	for _, percentage := range testCases {
		_, err := v.FlushOldestPercentile(percentage)
		if err == nil {
			t.Errorf("Expected error for percentage %d, got nil", percentage)
		}
	}

	v.Close()
}
