package stringstore

import (
	"errors"
	"os"
	"sync"

	"github.com/cbehopkins/bobbob"
	"github.com/cbehopkins/bobbob/store"
)

type fixedSizeBufferWriter struct {
	mu       sync.Mutex
	expected int
	buffer   []byte
	closed   bool
}

func newFixedSizeBufferWriter(expected int) *fixedSizeBufferWriter {
	return &fixedSizeBufferWriter{
		expected: expected,
		buffer:   make([]byte, 0, expected),
	}
}

func (w *fixedSizeBufferWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, errors.New("writer is closed")
	}
	if len(w.buffer)+len(p) > w.expected {
		return 0, errors.New("write limit exceeded")
	}
	w.buffer = append(w.buffer, p...)
	return len(p), nil
}

func (w *fixedSizeBufferWriter) finalize() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, errors.New("writer already finalized")
	}
	w.closed = true

	out := make([]byte, w.expected)
	copy(out, w.buffer)
	return out, nil
}

// diskWriteTask represents a single disk I/O operation
type diskWriteTask struct {
	buffer      []byte
	startOffset bobbob.FileOffset
	locks       []*store.ObjectLock
	objectIds   []bobbob.ObjectId // Corresponding object IDs for lock release
	errChan     chan error        // Receives write error (or nil on success)
}

// diskWritePool manages concurrent disk I/O workers
type diskWritePool struct {
	file     *os.File
	shard    *stringStoreShard
	taskCh   chan *diskWriteTask
	stopCh   chan struct{}
	wg       sync.WaitGroup
	activeWg sync.WaitGroup // Tracks active tasks for waitForPending
	submitMu sync.RWMutex   // Blocks submissions during drain
}

func newDiskWritePool(shard *stringStoreShard, poolSize int) *diskWritePool {
	pool := &diskWritePool{
		file:   shard.file,
		shard:  shard,
		taskCh: make(chan *diskWriteTask, poolSize*2), // Buffer to allow some queueing
		stopCh: make(chan struct{}),
	}

	// Start worker goroutines
	for i := 0; i < poolSize; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}

	return pool
}

func (p *diskWritePool) worker() {
	defer p.wg.Done()

	for {
		select {
		case task := <-p.taskCh:
			p.processTask(task)
			p.activeWg.Done() // Signal task completion

		case <-p.stopCh:
			return
		}
	}
}

func (p *diskWritePool) processTask(task *diskWriteTask) {
	// Defer release of all locks and send result
	defer func() {
		for i, lock := range task.locks {
			lock.Mu.Unlock()
			p.shard.releaseObjectLock(task.objectIds[i], lock)
		}
	}()

	var err error
	if len(task.buffer) > 0 {
		// Write entire buffer to file in one shot
		p.shard.ioMu.Lock()
		n, writeErr := p.file.WriteAt(task.buffer, int64(task.startOffset))
		p.shard.ioMu.Unlock()

		if writeErr != nil {
			err = writeErr
		} else if n != len(task.buffer) {
			err = errors.New("short write")
		}
	}

	// Send result to error channel or panic if nil
	if task.errChan != nil {
		select {
		case task.errChan <- err:
		default:
			// Channel closed or full - panic on error
			if err != nil {
				panic("disk write failed (channel closed): " + err.Error())
			}
		}
	} else if err != nil {
		// No error channel provided - fail fast
		panic("disk write failed: " + err.Error())
	}
}

func (p *diskWritePool) submit(task *diskWriteTask) {
	p.submitMu.RLock()
	defer p.submitMu.RUnlock()

	p.activeWg.Add(1) // Track this task
	select {
	case p.taskCh <- task:
		// Non-blocking submit, worker will process async
	case <-p.stopCh:
		// Pool shutting down, release locks immediately
		for i, lock := range task.locks {
			lock.Mu.Unlock()
			p.shard.releaseObjectLock(task.objectIds[i], lock)
		}
		p.activeWg.Done() // Task won't be processed
	}
}

func (p *diskWritePool) drain() {
	p.submitMu.Lock()
	defer p.submitMu.Unlock()
	p.activeWg.Wait()
}

func (p *diskWritePool) close() {
	close(p.stopCh)
	p.wg.Wait()
}

// waitForPending waits for all currently pending tasks to complete
func (p *diskWritePool) waitForPending() {
	p.activeWg.Wait()
}
