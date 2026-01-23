package allocator

func countUnsynced(pool *allocatorPool) int {
	count := 0
	for _, ref := range pool.available {
		if ref != nil && !ref.synced {
			count++
		}
	}
	for _, ref := range pool.full {
		if ref != nil && !ref.synced {
			count++
		}
	}
	return count
}

// TestOmniBlockAllocatorBackgroundPersist has been removed as part of pre-refactor cleanup.
// Background persistence (StartBackgroundPersist/StopBackgroundPersist) is being removed
// in favor of explicit Marshal/Unmarshal-only persistence.
// The test is preserved as a comment to document the previous behavior:
//
// Previously tested:
// - Creating allocations and marking allocators as "unsynced"
// - Starting a background persist loop that periodically calls persistUnsyncedOnce()
// - Verifying that unsynced allocators become synced after background persist runs
// - Ensuring MarshalMultiple only writes LUT after background persist (no allocator rewrites)
//
// This functionality is intentionally removed. New design: persistence is explicit via Marshal/Unmarshal.
