package minerva

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type ProcessPool struct {
	processes []*Process
	mutex     sync.RWMutex
	logger    *zerolog.Logger
	queue     ProcessPQ
}

// NewProcessPool creates a new process pool.
func NewProcessPool(name string, timeout int, size int, logger *zerolog.Logger, cwd string, cmd string, cmdArgs []string) *ProcessPool {
	pool := &ProcessPool{
		processes: make([]*Process, size),
		logger:    logger,
		mutex:     sync.RWMutex{},
	}
	pool.queue = ProcessPQ{processes: make([]*ProcessWithPrio, 0), mutex: sync.Mutex{}, pool: pool}
	for i := 0; i < size; i++ {
		pool.newProcess(name, i, cmd, cmdArgs, logger, timeout, cwd)
	}
	return pool
}

// newProcess creates a new process in the process pool.
func (pool *ProcessPool) newProcess(name string, i int, cmd string, cmdArgs []string, logger *zerolog.Logger, timeout int, cwd string) {

	pool.mutex.Lock()

	pool.processes[i] = &Process{
		isReady:         0,
		latency:         0,
		logger:          logger,
		name:            fmt.Sprintf("%s#%d", name, i),
		cmdStr:          cmd,
		cmdArgs:         cmdArgs,
		timeout:         timeout,
		requestsHandled: 0,
		restarts:        0,
		id:              i,
		cwd:             cwd,
	}
	pool.mutex.Unlock()
	pool.processes[i].Start()

}

// ExportAll exports all the processes in the process pool as a slice of ProcessExport.
func (pool *ProcessPool) ExportAll() []ProcessExport {
	pool.mutex.RLock()
	var exports []ProcessExport
	for _, process := range pool.processes {

		if process != nil {
			process.mutex.Lock()
			exports = append(exports, ProcessExport{
				IsReady:         atomic.LoadInt32(&process.isReady) == 1,
				Latency:         process.latency,
				InputQueue:      len(process.inputQueue),
				OutputQueue:     len(process.outputQueue),
				Name:            process.name,
				Restarts:        process.restarts,
				RequestsHandled: process.requestsHandled,
			})
			process.mutex.Unlock()
		}
	}
	pool.mutex.RUnlock()
	return exports
}

// GetWorker returns a worker process from the process pool.
func (pool *ProcessPool) GetWorker() (*Process, error) {
	pool.queue.Update()

	if pool.queue.Len() == 0 {
		return nil, fmt.Errorf("no available workers")
	}

	processWithPrio := heap.Pop(&pool.queue).(*ProcessWithPrio)
	return pool.processes[processWithPrio.processId], nil
}

// Should wait for X seconds until at least one worker is ready
func (pool *ProcessPool) WaitForReady(maxTime time.Duration) error {
	start := time.Now()
	for {
		pool.mutex.RLock()
		ready := true
		for _, process := range pool.processes {
			if atomic.LoadInt32(&process.isReady) == 0 {
				ready = false
				break
			}
		}
		pool.mutex.RUnlock()
		if ready {
			return nil
		}
		if time.Since(start) > maxTime {
			return fmt.Errorf("timeout waiting for workers to be ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// SendCommand sends a command to a worker in the process pool.
func (pool *ProcessPool) SendCommand(cmd map[string]interface{}) (map[string]interface{}, error) {
	worker, err := pool.GetWorker()
	if err != nil {

		return nil, err
	}

	return worker.SendCommand(cmd)
}

// StopAll stops all the processes in the process pool.
func (pool *ProcessPool) StopAll() {
	for _, process := range pool.processes {
		process.Stop()
	}
}

type ProcessWithPrio struct {
	processId   int
	queueLength int
	handled     int
}

type ProcessPQ struct {
	processes []*ProcessWithPrio
	mutex     sync.Mutex
	pool      *ProcessPool
}

func (pq *ProcessPQ) Len() int {
	return len(pq.processes)
}

func (pq *ProcessPQ) Less(i, j int) bool {
	if pq.processes[i].queueLength == pq.processes[j].queueLength {
		return pq.processes[i].handled < pq.processes[j].handled
	}
	return pq.processes[i].queueLength < pq.processes[j].queueLength
}

func (pq *ProcessPQ) Swap(i, j int) {
	pq.processes[i], pq.processes[j] = pq.processes[j], pq.processes[i]
}

func (pq *ProcessPQ) Push(x interface{}) {
	item := x.(*ProcessWithPrio)
	pq.processes = append(pq.processes, item)
}

func (pq *ProcessPQ) Pop() interface{} {
	old := pq.processes
	n := len(old)
	item := old[n-1]
	pq.processes = old[:n-1]
	return item
}

func (pq *ProcessPQ) Update() {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	pq.processes = nil

	pq.pool.mutex.RLock()
	defer pq.pool.mutex.RUnlock()

	for _, process := range pq.pool.processes {
		if atomic.LoadInt32(&process.isReady) == 1 {
			pq.Push(&ProcessWithPrio{
				processId:   process.id,
				queueLength: len(process.inputQueue),
				handled:     process.requestsHandled,
			})
		}
	}

	heap.Init(pq)
}
