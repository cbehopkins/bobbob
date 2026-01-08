# Vault Memory Monitoring

This document explains how memory monitoring works in the Vault layer: configuring budgets, background monitoring, and best practices for tests and stress runs.

## Overview

- **Goal:** Keep in-memory treap nodes bounded while working with large datasets.
- **Mechanism:** A `MemoryMonitor` evaluates `MemoryStats` and triggers flushes via callbacks.
- **Default:** Background monitoring auto-starts when a memory budget is configured.

## Quick Start

```go
// Set a node-count budget and time-based flush policy
vault.SetMemoryBudget(1000, 10) // Keep max 1000 nodes; flush >10s old
// Background monitoring runs automatically every ~100ms
```

```go
// Flush oldest percentile when budget is exceeded
vault.SetMemoryBudgetWithPercentile(1000, 25) // Flush oldest 25%
// Background monitoring runs automatically
```

## Disabling Background Monitoring (Deterministic Tests)

For tests that need deterministic control over when flushes happen:

```go
// Turn off background monitoring
vault.SetBackgroundMonitoring(false)

// Configure budget as usual
vault.SetMemoryBudgetWithPercentile(50, 50)

// Manually trigger checks
vault.SetCheckInterval(1)
_ = vault.checkMemoryAndFlush()
```

## Custom Policies via EnableMemoryMonitoring

```go
vault.EnableMemoryMonitoring(
    func(stats vault.MemoryStats) bool {
        // Trigger when node count exceeds threshold
        return stats.TotalInMemoryNodes > 1000
    },
    func(stats vault.MemoryStats) (int, error) {
        // Flush nodes older than 10 seconds
        cutoff := time.Now().Unix() - 10
        return vault.FlushOlderThan(cutoff)
    },
)
// Background monitoring auto-starts by default
```

## Safety & Robustness

- **Panic recovery:** Both the background monitor and manual checks recover from panics in flush logic to prevent test crashes.
- **Stack overflow mitigation:** Persistent payload treap persistence uses an **iterative post-order traversal** to avoid deep recursion.

## Running Stress Tests

A heavy stress test (`TestSetMemoryBudgetWithPercentile_LargeDataset`) is **skipped by default** to keep CI fast. Enable explicitly:

Windows (PowerShell):

```powershell
$env:VAULT_RUN_LARGE_DATASET = "1"
go test ./yggdrasil/vault -run TestSetMemoryBudgetWithPercentile_LargeDataset -v
```

## API Summary

- `SetMemoryBudget(maxNodes, flushAgeSeconds)`
- `SetMemoryBudgetWithPercentile(maxNodes, flushPercent)`
- `EnableMemoryMonitoring(shouldFlush, onFlush)`
- `SetBackgroundMonitoring(enabled)`
- `StartBackgroundMonitoring()` / `StopBackgroundMonitoring()`
- `SetCheckInterval(interval)`
- `GetMemoryStats()`
- `FlushOlderThan(cutoffTime)` / `FlushOldestPercentile(percentage)`

## Tips

- For production, rely on default background monitoring.
- For test determinism, disable background monitoring and call manual checks.
- Prefer percentile-based flushing for aggressive memory reduction when limits are hit.
