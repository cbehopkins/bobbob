package stringstore

import (
	"errors"
	"time"
)

var (
	errStoreClosed     = errors.New("string store is closed")
	errStoreCompacting = errors.New("string store is compacting")
	errStoreCapacity   = errors.New("string store capacity exceeded")
	errNotYetWritten   = errors.New("object not yet written (write pending)")
	errNoParentStore   = errors.New("string store has no parent store")
	errShardUnloaded   = errors.New("string store shard is unloaded")
)

const (
	defaultFlushInterval = 2 * time.Millisecond
	defaultMaxBatchBytes = 1 << 20 // 1MB
)
