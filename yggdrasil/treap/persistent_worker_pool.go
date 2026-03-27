package treap

import (
	"sync"
)

type persistWorker struct {
	pool      *persistWorkerPool
	workloads <-chan func() error
}

type persistWorkerPool struct {
	workers   []persistWorker
	wg        sync.WaitGroup
	workloads chan func() error
	done      chan struct{}
	errOnce   sync.Once
	firstErr  error
}

func (p *persistWorkerPool) fail(err error) {
	if err == nil {
		return
	}
	p.errOnce.Do(func() {
		p.firstErr = err
		close(p.done)
	})
}

func (p *persistWorkerPool) getFailure() error {
	return p.firstErr
}

func (p *persistWorkerPool) Submit(workload func() error) error {
	// If the pool is nil, run the workload synchronously.
	// Calling a method with a nil pointer receiver is valid in Go;
	// just avoid dereferencing receiver fields on this path.
	if workload == nil {
		return nil
	}
	// return workload()
	if p == nil {
		// return errors.New("persist worker pool is nil")
		return workload()
	}

	// Reject new work as soon as any worker reports a failure.
	select {
	case <-p.done:
		return p.getFailure()
	case p.workloads <- workload:
		return nil
	}
}

func (p *persistWorkerPool) Close() error {
	close(p.workloads)
	p.wg.Wait()
	return p.getFailure()
}

func newPersistWorkerPool(workerCount int) *persistWorkerPool {
	workloads := make(chan func() error, workerCount*2)

	pool := &persistWorkerPool{
		workers:   make([]persistWorker, workerCount),
		workloads: workloads,
		done:      make(chan struct{}),
	}
	for i := range workerCount {
		worker := persistWorker{
			pool:      pool,
			workloads: workloads,
		}
		pool.workers[i] = worker
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for {
				select {
				case <-worker.pool.done:
					return
				case workload, ok := <-worker.workloads:
					if !ok {
						return
					}
					if err := workload(); err != nil {
						worker.pool.fail(err)
						return
					}
				}
			}
		}()
	}
	return pool
}
