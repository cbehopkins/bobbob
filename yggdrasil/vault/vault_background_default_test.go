package vault

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestAutoBackgroundMonitoringEnabledByDefault verifies that setting a memory budget
// automatically starts background monitoring without calling StartBackgroundMonitoring().
func TestAutoBackgroundMonitoringEnabledByDefault(t *testing.T) {
	// Use MockStore for faster, logic-focused testing of monitoring behavior
	v := newMockVault(t)
	defer v.Close()

	// Register types needed for collection
	v.RegisterType((*types.StringKey)(new(string)))
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[string]{})

	// Create collection
	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
		v,
		"testColl",
		types.IntLess,
		(*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	var shouldFlushCalls, onFlushCalls int32
	shouldFlushDebug := func(stats MemoryStats, shouldFlush bool) { atomic.AddInt32(&shouldFlushCalls, 1) }
	onFlushDebug := func(stats MemoryStats, flushed int) { atomic.AddInt32(&onFlushCalls, 1) }

	// Set memory budget; background monitoring should auto-start by default
	v.SetMemoryBudgetWithPercentileWithCallbacks(50, 50, shouldFlushDebug, onFlushDebug)

	// Insert items WITHOUT calling StartBackgroundMonitoring() or checkMemoryAndFlush()
	numInserts := 500
	for i := 0; i < numInserts; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: fmt.Sprintf("item-%d", i)}
		coll.Insert(&key, payload)
	}

	// Allow background checks to run
	time.Sleep(500 * time.Millisecond)

	if atomic.LoadInt32(&shouldFlushCalls) == 0 {
		t.Error("expected auto background monitoring to invoke shouldFlushDebug")
	}
	if atomic.LoadInt32(&onFlushCalls) == 0 {
		t.Error("expected auto background monitoring to invoke onFlushDebug")
	}
}

// TestDisableBackgroundMonitoringForDeterministic ensures we can disable background monitoring
// for deterministic tests and still trigger flushes manually.
func TestDisableBackgroundMonitoringForDeterministic(t *testing.T) {
	// Use MockStore for faster, logic-focused testing of monitoring behavior
	v := newMockVault(t)
	defer v.Close()

	// Register types needed for collection
	v.RegisterType((*types.StringKey)(new(string)))
	v.RegisterType((*types.IntKey)(new(int32)))
	v.RegisterType(types.JsonPayload[string]{})

	// Create collection
	coll, err := GetOrCreateCollection[types.IntKey, types.JsonPayload[string]](
		v,
		"testColl",
		types.IntLess,
		(*types.IntKey)(new(int32)),
	)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Disable background monitoring before setting budget
	v.SetBackgroundMonitoring(false)

	shouldFlushCalls := 0
	onFlushCalls := 0
	shouldFlushDebug := func(stats MemoryStats, shouldFlush bool) { shouldFlushCalls++ }
	onFlushDebug := func(stats MemoryStats, flushed int) { onFlushCalls++ }

	v.SetMemoryBudgetWithPercentileWithCallbacks(50, 50, shouldFlushDebug, onFlushDebug)

	// Insert items; with monitoring disabled, callbacks should not be invoked automatically
	numInserts := 250
	for i := 0; i < numInserts; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: fmt.Sprintf("item-%d", i)}
		coll.Insert(&key, payload)
	}

	// Give time; should still be zero because background is disabled
	time.Sleep(300 * time.Millisecond)
	if onFlushCalls != 0 {
		t.Fatalf("expected no onFlush callbacks with background disabled, got %d", onFlushCalls)
	}

	// Now trigger manual checks
	v.SetCheckInterval(1)
	if err := v.checkMemoryAndFlush(); err != nil {
		t.Fatalf("manual checkMemoryAndFlush failed: %v", err)
	}
	if onFlushCalls == 0 {
		t.Errorf("expected manual flush to invoke onFlushDebug, got %d", onFlushCalls)
	}
}
