package treap

import "time"

// StartBackgroundPersistOldestPercentile starts a scheduled persister that persists
// the oldest percentage of in-memory nodes on a fixed interval.
func (t *PersistentTreap[T]) StartBackgroundPersistOldestPercentile(
	percentage int,
	interval time.Duration,
	opts ...BackgroundPersistOption,
) BackgroundPersister {
	settings := applyBackgroundPersistOptions(opts)
	p := NewScheduledBackgroundPersister[T](t)
	p.startOldestPercentilePersisterTicker(percentage, interval, settings.autoFlush)
	return backgroundPersisterAdapter[T]{p}
}

// StartBackgroundPersistOldNodes starts a scheduled persister that persists nodes
// older than maxAge on a fixed interval.
func (t *PersistentTreap[T]) StartBackgroundPersistOldNodes(
	maxAge time.Duration,
	interval time.Duration,
	opts ...BackgroundPersistOption,
) BackgroundPersister {
	settings := applyBackgroundPersistOptions(opts)
	p := NewScheduledBackgroundPersister[T](t)
	p.startOldNodePersisterTicker(maxAge, interval, settings.autoFlush)
	return backgroundPersisterAdapter[T]{p}
}

// StartBackgroundPersistOldestPercentile starts a scheduled persister for payload treaps.
func (t *PersistentPayloadTreap[K, P]) StartBackgroundPersistOldestPercentile(
	percentage int,
	interval time.Duration,
	opts ...BackgroundPersistOption,
) BackgroundPersister {
	return t.PersistentTreap.StartBackgroundPersistOldestPercentile(percentage, interval, opts...)
}

// StartBackgroundPersistOldNodes starts a scheduled persister for payload treaps.
func (t *PersistentPayloadTreap[K, P]) StartBackgroundPersistOldNodes(
	maxAge time.Duration,
	interval time.Duration,
	opts ...BackgroundPersistOption,
) BackgroundPersister {
	return t.PersistentTreap.StartBackgroundPersistOldNodes(maxAge, interval, opts...)
}
