package treap

import (
	"context"
	"sync"
)

type persistWorker struct {
	workloads <-chan func() error
	errorChan chan<- error
}

type persistWorkerPool struct {
	workers   []persistWorker
	wg        sync.WaitGroup
	workloads chan<- func() error
	errorChan <-chan error
	ctx       context.Context	// FIXME This is a mistake - we should just close the  workloads channel to signal shutdown, and workers should select on that instead of a separate context. This is more complex than it needs to be and can lead to subtle bugs if not used carefully.
	cancel    context.CancelFunc
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
	
	// Check if workers have aborted due to errors
	select {
	case <-p.ctx.Done():
		// Workers aborted, drain error channel and return first error
		err, ok := <-p.errorChan
		if ok {
			return err
		}
		return p.ctx.Err()
	case p.workloads <- workload:
		return nil
	}
}

func (p *persistWorkerPool) Close() error {
	close(p.workloads)
	p.wg.Wait()
	err, ok := <-p.errorChan
	if !ok {
		return nil
	}
	return err
}

func newPersistWorkerPool(workerCount int) *persistWorkerPool {
	workloads := make(chan func() error, workerCount*2)
	errorChan := make(chan error, workerCount*2)
	ctx, cancel := context.WithCancel(context.Background())
	
	pool := &persistWorkerPool{
		workers:   make([]persistWorker, workerCount),
		workloads: workloads,
		errorChan: errorChan,
		ctx:       ctx,
		cancel:    cancel,
	}
	for i := range workerCount {
		worker := persistWorker{
			workloads: workloads,
			errorChan: errorChan,
		}
		pool.workers[i] = worker
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			for workload := range worker.workloads {
				if err := workload(); err != nil {
					worker.errorChan <- err
					cancel() // Signal all workers to abort
					return   // Exit worker on first error
				}
			}
		}()
	}
	go func() {
		pool.wg.Wait()
		close(errorChan)
	}()
	return pool
}
