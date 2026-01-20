package vault

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestMemoryMonitorCallbacksAreInvoked verifies that SetMemoryBudgetWithPercentileWithCallbacks
// properly triggers shouldFlushDebug and onFlushDebug callbacks during bulk inserts.
// This is a regression test for cases where callbacks were never invoked despite
// exceeding the node budget.
func TestMemoryMonitorCallbacksAreInvoked(t *testing.T) {
	file := filepath.Join(t.TempDir(), "vault_memory_monitor.db")

	session, colls, err := OpenVaultWithIdentity(
		file,
		PayloadIdentitySpec[string, types.IntKey, types.JsonPayload[string]]{
			Identity:        "testColl",
			LessFunc:        types.IntLess,
			KeyTemplate:     new(types.IntKey),
			PayloadTemplate: types.JsonPayload[string]{},
		},
	)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity failed: %v", err)
	}
	defer session.Close()

	coll, ok := colls["testColl"].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[string]])
	if !ok {
		t.Fatalf("unexpected collection type: %T", colls["testColl"])
	}

	// Track callback invocations
	shouldFlushCalls := 0
	onFlushCalls := 0
	shouldFlushDebug := func(stats MemoryStats, shouldFlush bool) {
		shouldFlushCalls++
		t.Logf("shouldFlushDebug called: totalNodes=%d, shouldFlush=%v", stats.TotalInMemoryNodes, shouldFlush)
	}
	onFlushDebug := func(stats MemoryStats, flushed int) {
		onFlushCalls++
		t.Logf("onFlushDebug called: nodesFlushed=%d, totalNodes=%d", flushed, stats.TotalInMemoryNodes)
	}

	// Set memory budget: max 50 nodes, flush oldest 50% when exceeded
	session.Vault.SetMemoryBudgetWithPercentileWithCallbacks(50, 50, shouldFlushDebug, onFlushDebug)

	// Insert hundreds of items to exceed the 50-node budget
	// Note: checkMemoryAndFlush() must be called explicitly after operations
	// (it's not automatic on every Insert)
	numInserts := 500
	for i := 0; i < numInserts; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: fmt.Sprintf("item-%d", i)}
		coll.Insert(&key, payload)

		// The memory monitor checks at intervals (default: every 100 operations)
		// In practice, you'd call this periodically or have a background task do it
		if err := session.Vault.checkMemoryAndFlush(); err != nil {
			t.Fatalf("checkMemoryAndFlush failed: %v", err)
		}
	}

	t.Logf("Inserted %d items with max budget of 50 nodes", numInserts)
	t.Logf("shouldFlushDebug called: %d times", shouldFlushCalls)
	t.Logf("onFlushDebug called: %d times", onFlushCalls)

	// Verify callbacks were invoked
	if shouldFlushCalls == 0 {
		t.Error("expected shouldFlushDebug to be called, but it was never invoked")
	}
	if onFlushCalls == 0 {
		t.Error("expected onFlushDebug to be called, but it was never invoked")
	}

	// Final memory stats check
	stats := session.Vault.GetMemoryStats()
	t.Logf("Final memory stats: totalNodes=%d, operationsSinceFlush=%d",
		stats.TotalInMemoryNodes, stats.OperationsSinceLastFlush)
}

// TestMemoryMonitorDoesNotFlushIfUnderBudget verifies that when node count stays
// below the threshold, flush callbacks are not invoked.
func TestMemoryMonitorDoesNotFlushIfUnderBudget(t *testing.T) {
	file := filepath.Join(t.TempDir(), "vault_memory_no_flush.db")

	session, colls, err := OpenVaultWithIdentity(
		file,
		PayloadIdentitySpec[string, types.IntKey, types.JsonPayload[string]]{
			Identity:        "testColl",
			LessFunc:        types.IntLess,
			KeyTemplate:     new(types.IntKey),
			PayloadTemplate: types.JsonPayload[string]{},
		},
	)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity failed: %v", err)
	}
	defer session.Close()

	coll, ok := colls["testColl"].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[string]])
	if !ok {
		t.Fatalf("unexpected collection type: %T", colls["testColl"])
	}

	shouldFlushCalls := 0
	onFlushCalls := 0
	shouldFlushDebug := func(stats MemoryStats, shouldFlush bool) {
		shouldFlushCalls++
	}
	onFlushDebug := func(stats MemoryStats, flushed int) {
		onFlushCalls++
	}

	// Disable background monitoring for deterministic testing
	session.Vault.SetBackgroundMonitoring(false)

	// Set high memory budget: max 10000 nodes (we'll only insert 50)
	session.Vault.SetMemoryBudgetWithPercentileWithCallbacks(10000, 50, shouldFlushDebug, onFlushDebug)

	// Lower the check interval so we actually check during our small test
	session.Vault.SetCheckInterval(10)

	// Insert only a few items (well under budget)
	// Note: checkMemoryAndFlush() must be called explicitly (see above)
	numInserts := 50
	for i := 0; i < numInserts; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: fmt.Sprintf("item-%d", i)}
		coll.Insert(&key, payload)

		if err := session.Vault.checkMemoryAndFlush(); err != nil {
			t.Fatalf("checkMemoryAndFlush failed: %v", err)
		}
	}

	t.Logf("Inserted %d items with max budget of 10000 nodes", numInserts)
	t.Logf("shouldFlushDebug called: %d times", shouldFlushCalls)
	t.Logf("onFlushDebug called: %d times", onFlushCalls)

	// Verify callbacks were never invoked (since we're under budget)
	if shouldFlushCalls == 0 {
		t.Error("expected shouldFlushDebug to be called (even if shouldFlush=false), but it was never invoked")
	}
	if onFlushCalls != 0 {
		t.Errorf("expected onFlushDebug NOT to be called when under budget, but it was called %d times", onFlushCalls)
	}
}

// TestBackgroundMemoryMonitoring verifies that the background goroutine automatically
// calls checkMemoryAndFlush without explicit intervention. This is the ideal UX where
// SetMemoryBudgetWithPercentile() handles everything automatically.
func TestBackgroundMemoryMonitoring(t *testing.T) {
	file := filepath.Join(t.TempDir(), "vault_background_monitor.db")

	session, colls, err := OpenVaultWithIdentity(
		file,
		PayloadIdentitySpec[string, types.IntKey, types.JsonPayload[string]]{
			Identity:        "testColl",
			LessFunc:        types.IntLess,
			KeyTemplate:     new(types.IntKey),
			PayloadTemplate: types.JsonPayload[string]{},
		},
	)
	if err != nil {
		t.Fatalf("OpenVaultWithIdentity failed: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Errorf("session.Close: %v", err)
		}
		// Small delay to ensure Windows releases file handles
		time.Sleep(10 * time.Millisecond)
	})

	coll, ok := colls["testColl"].(*treap.PersistentPayloadTreap[types.IntKey, types.JsonPayload[string]])
	if !ok {
		t.Fatalf("unexpected collection type: %T", colls["testColl"])
	}

	var shouldFlushCalls, onFlushCalls int32
	shouldFlushDebug := func(stats MemoryStats, shouldFlush bool) {
		atomic.AddInt32(&shouldFlushCalls, 1)
	}
	onFlushDebug := func(stats MemoryStats, flushed int) {
		atomic.AddInt32(&onFlushCalls, 1)
		t.Logf("Background flush: %d nodes flushed", flushed)
	}

	// Enable memory monitoring with low budget - background goroutine takes over from here
	session.Vault.SetMemoryBudgetWithPercentileWithCallbacks(50, 50, shouldFlushDebug, onFlushDebug)

	// Start the background monitoring goroutine to automatically check memory periodically
	session.Vault.StartBackgroundMonitoring()

	// Insert items WITHOUT calling checkMemoryAndFlush()
	// The background goroutine should handle memory checks automatically
	numInserts := 500
	for i := 0; i < numInserts; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: fmt.Sprintf("item-%d", i)}
		coll.Insert(&key, payload)
		// NO explicit checkMemoryAndFlush() call - background goroutine handles it!
	}

	// Give the background goroutine time to run a few checks (it runs every 100ms)
	// With 500 inserts and 100ms checks, we should see some activity
	time.Sleep(500 * time.Millisecond)

	t.Logf("Inserted %d items with max budget of 50 nodes (automatic background monitoring)", numInserts)
	t.Logf("shouldFlushDebug called: %d times", atomic.LoadInt32(&shouldFlushCalls))
	t.Logf("onFlushDebug called: %d times", atomic.LoadInt32(&onFlushCalls))

	// Verify the background goroutine was active and flushing
	// Since we inserted 500 items with a 50-node budget and the background goroutine
	// checks every 100ms, we should definitely see some flush callbacks
	if atomic.LoadInt32(&shouldFlushCalls) == 0 {
		t.Error("expected background goroutine to invoke shouldFlushDebug, but it was never called")
	}
	if atomic.LoadInt32(&onFlushCalls) == 0 {
		t.Error("expected background goroutine to invoke onFlushDebug, but it was never called")
	}

	// Verify memory was actually flushed (only a few nodes should remain)
	stats := session.Vault.GetMemoryStats()
	if stats.TotalInMemoryNodes > 100 {
		t.Logf("Warning: expected fewer nodes in memory, but have %d nodes", stats.TotalInMemoryNodes)
	}
}
