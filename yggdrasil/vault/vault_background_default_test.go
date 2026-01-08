package vault

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cbehopkins/bobbob/yggdrasil/treap"
	"github.com/cbehopkins/bobbob/yggdrasil/types"
)

// TestAutoBackgroundMonitoringEnabledByDefault verifies that setting a memory budget
// automatically starts background monitoring without calling StartBackgroundMonitoring().
func TestAutoBackgroundMonitoringEnabledByDefault(t *testing.T) {
	file := filepath.Join(t.TempDir(), "vault_auto_background.db")

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
	shouldFlushDebug := func(stats MemoryStats, shouldFlush bool) { shouldFlushCalls++ }
	onFlushDebug := func(stats MemoryStats, flushed int) { onFlushCalls++ }

	// Set memory budget; background monitoring should auto-start by default
	session.Vault.SetMemoryBudgetWithPercentileWithCallbacks(50, 50, shouldFlushDebug, onFlushDebug)

	// Insert items WITHOUT calling StartBackgroundMonitoring() or checkMemoryAndFlush()
	numInserts := 500
	for i := 0; i < numInserts; i++ {
		key := types.IntKey(i)
		payload := types.JsonPayload[string]{Value: fmt.Sprintf("item-%d", i)}
		coll.Insert(&key, payload)
	}

	// Allow background checks to run
	time.Sleep(500 * time.Millisecond)

	if shouldFlushCalls == 0 {
		t.Error("expected auto background monitoring to invoke shouldFlushDebug")
	}
	if onFlushCalls == 0 {
		t.Error("expected auto background monitoring to invoke onFlushDebug")
	}
}

// TestDisableBackgroundMonitoringForDeterministic ensures we can disable background monitoring
// for deterministic tests and still trigger flushes manually.
func TestDisableBackgroundMonitoringForDeterministic(t *testing.T) {
	file := filepath.Join(t.TempDir(), "vault_disable_background.db")

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

	// Disable background monitoring before setting budget
	session.Vault.SetBackgroundMonitoring(false)

	shouldFlushCalls := 0
	onFlushCalls := 0
	shouldFlushDebug := func(stats MemoryStats, shouldFlush bool) { shouldFlushCalls++ }
	onFlushDebug := func(stats MemoryStats, flushed int) { onFlushCalls++ }

	session.Vault.SetMemoryBudgetWithPercentileWithCallbacks(50, 50, shouldFlushDebug, onFlushDebug)

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
	session.Vault.SetCheckInterval(1)
	if err := session.Vault.checkMemoryAndFlush(); err != nil {
		t.Fatalf("manual checkMemoryAndFlush failed: %v", err)
	}
	if onFlushCalls == 0 {
		t.Errorf("expected manual flush to invoke onFlushDebug, got %d", onFlushCalls)
	}
}
