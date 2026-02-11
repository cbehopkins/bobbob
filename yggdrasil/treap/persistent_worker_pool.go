package treap

import (
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
	p.workloads <- workload
	return nil
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
	pool := &persistWorkerPool{
		workers:   make([]persistWorker, workerCount),
		workloads: workloads,
		errorChan: errorChan,
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
