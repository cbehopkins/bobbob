package treap

import (
	"fmt"
	"sync"
	"time"
)

// BackgroundPersistWorker[T] provides alternative persist strategies that don't require
// walking the entire tree. These are designed to reduce persist overhead by only
// persisting "cold" (rarely accessed) nodes that can safely be flushed later.
type BackgroundPersistWorker[T any] struct {
	mu sync.Mutex
}

// NewBackgroundPersistWorker creates a new strategy for selective persistence.
func NewBackgroundPersistWorker[T any]() *BackgroundPersistWorker[T] {
	return &BackgroundPersistWorker[T]{}
}

// PersistOldNodesOlderThan persists all in-memory nodes that haven't been accessed
// since the given timestamp. This allows Flush to safely remove them from memory
// without losing data.
//
// This is useful for periodic background maintenance:
// - Call every N seconds to persist stale nodes
// - Then Flush can safely evict them without disk I/O
// - Only persists "cold" data, leaving hot nodes in memory
//
// Returns the number of nodes persisted and any error.
//
// Thread-safe and does not acquire the main tree write lock.
func (w *BackgroundPersistWorker[T]) PersistOldNodesOlderThan(
	treap *PersistentTreap[T],
	cutoffTimestamp int64,
) (int, error) {
	if treap == nil {
		return 0, fmt.Errorf("treap is nil")
	}

	// Collect all in-memory nodes that meet the criteria
	// This is read-only, so we can use the read lock
	treap.mu.RLock()
	var nodesToPersist []*PersistentTreapNode[T]
	w.collectOldNodes(treap.root, &nodesToPersist, cutoffTimestamp)
	treap.mu.RUnlock()

	if len(nodesToPersist) == 0 {
		return 0, nil
	}

	// Persist each node individually
	// Each node's persist operation is independent and safe
	persistedCount := 0
	for _, node := range nodesToPersist {
		if node == nil || !node.IsObjectIdInvalid() {
			// Node is already persisted (has valid objectId)
			persistedCount++
			continue
		}

		// Persist this individual node
		if err := node.persist(); err != nil {
			// Log but continue - one node failure shouldn't stop others
			continue
		}
		persistedCount++
	}

	return persistedCount, nil
}

// collectOldNodes recursively collects nodes that haven't been accessed since cutoffTimestamp.
// Only looks at in-memory pointers, doesn't load from disk.
func (w *BackgroundPersistWorker[T]) collectOldNodes(
	node TreapNodeInterface[T],
	collected *[]*PersistentTreapNode[T],
	cutoffTimestamp int64,
) {
	if node == nil || node.IsNil() {
		return
	}

	pNode, ok := node.(*PersistentTreapNode[T])
	if !ok {
		return
	}

	// Check if this node is old enough
	if pNode.GetLastAccessTime() < cutoffTimestamp && pNode.GetLastAccessTime() > 0 {
		*collected = append(*collected, pNode)
	}

	// Recurse to children (only follow in-memory pointers)
	if pNode.TreapNode.left != nil {
		w.collectOldNodes(pNode.TreapNode.left, collected, cutoffTimestamp)
	}
	if pNode.TreapNode.right != nil {
		w.collectOldNodes(pNode.TreapNode.right, collected, cutoffTimestamp)
	}
}

// PersistOldestNodesPercentile persists the oldest N% of in-memory nodes
// (by last access time). This is a simpler heuristic than absolute timestamps.
//
// Useful for memory management:
// - Periodically call PersistOldestNodesPercentile(25) to persist cold data
// - Then Flush can evict those same nodes
// - Keeps working set in memory, cold data on disk
//
// Returns the number of nodes persisted and any error.
//
// Thread-safe and does not acquire the main tree write lock.
func (w *BackgroundPersistWorker[T]) PersistOldestNodesPercentile(
	treap *PersistentTreap[T],
	percentage int,
) (int, error) {
	if treap == nil {
		return 0, fmt.Errorf("treap is nil")
	}

	if percentage <= 0 || percentage > 100 {
		return 0, fmt.Errorf("percentage must be between 1 and 100, got %d", percentage)
	}

	// Collect all in-memory nodes with their timestamps
	treap.mu.RLock()
	var nodeInfos []struct {
		node      *PersistentTreapNode[T]
		timestamp int64
	}
	w.collectAllInMemoryNodesWithTimestamps(treap.root, &nodeInfos)
	treap.mu.RUnlock()

	if len(nodeInfos) == 0 {
		return 0, nil
	}

	// Sort by timestamp (ascending) so oldest are first
	// In practice, we don't need full sort - just partition
	w.sortNodesByTimestamp(nodeInfos)

	// Calculate how many to persist
	numToPersist := (len(nodeInfos) * percentage) / 100
	if numToPersist == 0 {
		numToPersist = 1
	}

	// Persist the oldest nodes
	persistedCount := 0
	for i := 0; i < numToPersist && i < len(nodeInfos); i++ {
		node := nodeInfos[i].node
		if node == nil || !node.IsObjectIdInvalid() {
			// Already persisted
			persistedCount++
			continue
		}

		// Persist this node
		if err := node.persist(); err != nil {
			// Continue on error
			continue
		}
		persistedCount++
	}

	return persistedCount, nil
}

// collectAllInMemoryNodesWithTimestamps collects nodes with their access timestamps.
func (w *BackgroundPersistWorker[T]) collectAllInMemoryNodesWithTimestamps(
	node TreapNodeInterface[T],
	collected *[]struct {
		node      *PersistentTreapNode[T]
		timestamp int64
	},
) {
	if node == nil || node.IsNil() {
		return
	}

	pNode, ok := node.(*PersistentTreapNode[T])
	if !ok {
		return
	}

	*collected = append(*collected, struct {
		node      *PersistentTreapNode[T]
		timestamp int64
	}{pNode, pNode.GetLastAccessTime()})

	if pNode.TreapNode.left != nil {
		w.collectAllInMemoryNodesWithTimestamps(pNode.TreapNode.left, collected)
	}
	if pNode.TreapNode.right != nil {
		w.collectAllInMemoryNodesWithTimestamps(pNode.TreapNode.right, collected)
	}
}

// sortNodesByTimestamp sorts nodes by timestamp (oldest first).
// Uses a simple insertion-sort-like approach suitable for moderate sizes.
func (w *BackgroundPersistWorker[T]) sortNodesByTimestamp(
	nodes []struct {
		node      *PersistentTreapNode[T]
		timestamp int64
	},
) {
	for i := 1; i < len(nodes); i++ {
		key := nodes[i]
		j := i - 1
		for j >= 0 && nodes[j].timestamp > key.timestamp {
			nodes[j+1] = nodes[j]
			j--
		}
		nodes[j+1] = key
	}
}

// ScheduledBackgroundPersister[T] wraps BackgroundPersistWorker with timing logic.
// This allows you to schedule periodic persistence of old nodes.
type ScheduledBackgroundPersister[T any] struct {
	worker               *BackgroundPersistWorker[T]
	treap                *PersistentTreap[T]
	stopChan             chan struct{}
	wg                   sync.WaitGroup
	statsmu              sync.Mutex
	totalNodesPersisted  int64
	totalRuns            int64
	lastError            error
	lastRunTime          time.Time
}

// NewScheduledBackgroundPersister creates a persister that can run on a schedule.
func NewScheduledBackgroundPersister[T any](treap *PersistentTreap[T]) *ScheduledBackgroundPersister[T] {
	return &ScheduledBackgroundPersister[T]{
		worker:   NewBackgroundPersistWorker[T](),
		treap:    treap,
		stopChan: make(chan struct{}),
	}
}

// StartOldNodePersisterTicker starts a background goroutine that periodically persists
// nodes older than the cutoff time.
//
// Example:
//  persister.StartOldNodePersisterTicker(30*time.Second, 10*time.Second)
//  // Every 10 seconds, persist nodes not accessed in 30 seconds
//
// Call Stop() to shut down the goroutine.
func (p *ScheduledBackgroundPersister[T]) StartOldNodePersisterTicker(
	maxAge time.Duration,
	tickInterval time.Duration,
) {
	p.startOldNodePersisterTicker(maxAge, tickInterval, false)
}

func (p *ScheduledBackgroundPersister[T]) startOldNodePersisterTicker(
	maxAge time.Duration,
	tickInterval time.Duration,
	autoFlush bool,
) {
	if tickInterval == 0 {
		tickInterval = 5 * time.Second
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-p.stopChan:
				return
			case <-ticker.C:
				cutoffTime := time.Now().Unix() - int64(maxAge.Seconds())
				count, err := p.worker.PersistOldNodesOlderThan(p.treap, cutoffTime)
				if autoFlush {
					_, flushErr := p.treap.FlushOlderThan(cutoffTime)
					if flushErr != nil {
						if err == nil {
							err = flushErr
						} else {
							err = fmt.Errorf("persist error: %v; flush error: %w", err, flushErr)
						}
					}
				}

				p.statsmu.Lock()
				p.lastRunTime = time.Now()
				p.totalNodesPersisted += int64(count)
				p.totalRuns++
				p.lastError = err
				p.statsmu.Unlock()
			}
		}
	}()
}

// StartOldestPercentilePersisterTicker starts a background goroutine that periodically
// persists the oldest N% of nodes.
//
// Example:
//  persister.StartOldestPercentilePersisterTicker(25, 10*time.Second)
//  // Every 10 seconds, persist the oldest 25% of nodes
//
// Call Stop() to shut down the goroutine.
func (p *ScheduledBackgroundPersister[T]) StartOldestPercentilePersisterTicker(
	percentage int,
	tickInterval time.Duration,
) {
	p.startOldestPercentilePersisterTicker(percentage, tickInterval, false)
}

func (p *ScheduledBackgroundPersister[T]) startOldestPercentilePersisterTicker(
	percentage int,
	tickInterval time.Duration,
	autoFlush bool,
) {
	if tickInterval == 0 {
		tickInterval = 5 * time.Second
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-p.stopChan:
				return
			case <-ticker.C:
				count, err := p.worker.PersistOldestNodesPercentile(p.treap, percentage)
				if autoFlush {
					_, flushErr := p.treap.FlushOldestPercentile(percentage)
					if flushErr != nil {
						if err == nil {
							err = flushErr
						} else {
							err = fmt.Errorf("persist error: %v; flush error: %w", err, flushErr)
						}
					}
				}

				p.statsmu.Lock()
				p.lastRunTime = time.Now()
				p.totalNodesPersisted += int64(count)
				p.totalRuns++
				p.lastError = err
				p.statsmu.Unlock()
			}
		}
	}()
}

// Stop shuts down the background goroutine and waits for it to finish.
func (p *ScheduledBackgroundPersister[T]) Stop() {
	close(p.stopChan)
	p.wg.Wait()
}

// Stats returns statistics about the background persister's activity.
func (p *ScheduledBackgroundPersister[T]) Stats() (totalPersisted int64, totalRuns int64, lastError error, lastRunTime time.Time) {
	p.statsmu.Lock()
	defer p.statsmu.Unlock()
	return p.totalNodesPersisted, p.totalRuns, p.lastError, p.lastRunTime
}

// BackgroundPersister provides a non-generic handle for scheduled persisters.
type BackgroundPersister interface {
	Stop()
	Stats() (int64, int64, error, time.Time)
}

// BackgroundPersistable is implemented by treaps that can start background persisters.
type BackgroundPersistable interface {
	StartBackgroundPersistOldestPercentile(percentage int, interval time.Duration, opts ...BackgroundPersistOption) BackgroundPersister
	StartBackgroundPersistOldNodes(maxAge, interval time.Duration, opts ...BackgroundPersistOption) BackgroundPersister
}

// BackgroundPersistOption customizes background persistence behavior.
type BackgroundPersistOption func(*backgroundPersistOptions)

type backgroundPersistOptions struct {
	autoFlush bool
}

// WithAutoFlush flushes the same nodes that were just persisted.
func WithAutoFlush() BackgroundPersistOption {
	return func(o *backgroundPersistOptions) {
		o.autoFlush = true
	}
}

func applyBackgroundPersistOptions(opts []BackgroundPersistOption) backgroundPersistOptions {
	settings := backgroundPersistOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&settings)
		}
	}
	return settings
}

type backgroundPersisterAdapter[T any] struct {
	*ScheduledBackgroundPersister[T]
}

func (a backgroundPersisterAdapter[T]) Stop() {
	a.ScheduledBackgroundPersister.Stop()
}

func (a backgroundPersisterAdapter[T]) Stats() (int64, int64, error, time.Time) {
	return a.ScheduledBackgroundPersister.Stats()
}
