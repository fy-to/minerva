package minerva

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

type ProcessPool struct {
	processes []*Process
	mutex     sync.RWMutex
	logger    *zerolog.Logger
	queue     ProcessPQ
}

// NewProcessPool creates a new process pool with the specified name, size, logger, command, and command arguments.
// It initializes the pool with the specified number of processes and returns the created process pool.
func NewProcessPool(name string, timeout int, size int, logger *zerolog.Logger, cmd string, cmdArgs []string) *ProcessPool {
	pool := &ProcessPool{
		processes: make([]*Process, size),
		logger:    logger,
		mutex:     sync.RWMutex{},
	}
	pool.queue = ProcessPQ{processes: make([]*ProcessWithPrio, 0), mutex: sync.Mutex{}, pool: pool}
	for i := 0; i < size; i++ {
		pool.newProcess(name, i, cmd, cmdArgs, logger, timeout)
	}
	return pool
}

// newProcess creates a new process in the process pool.
// It takes the following parameters:
// - name: the name of the process
// - i: the index of the process in the process pool
// - cmd: the command to execute
// - cmdArgs: the arguments for the command
// - logger: the logger for logging errors and messages
//
// It initializes the process with the given parameters, sets up the stdin and stdout pipes,
// and starts the process.
func (pool *ProcessPool) newProcess(name string, i int, cmd string, cmdArgs []string, logger *zerolog.Logger, timeout int) {

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
	}
	pool.mutex.Unlock()
	pool.processes[i].Start()

}

// ExportAll exports all the processes in the process pool as a slice of ProcessExport.
// It acquires a lock on the process pool, iterates over each process, and creates a ProcessExport object for each non-nil process.
// The ProcessExport object contains information such as the process's readiness, last heartbeat, latency, input queue length, output queue length, and name.
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
// It selects the worker with the minimum length of the input queue.
// If there are no available workers, it returns an error.
func (pool *ProcessPool) GetWorker() (*Process, error) {
	pool.queue.Update()

	if pool.queue.Len() == 0 {
		return nil, fmt.Errorf("no available workers")
	}

	processWithPrio := heap.Pop(&pool.queue).(*ProcessWithPrio)
	return pool.processes[processWithPrio.processId], nil
}

// SendCommand sends a command to a worker in the process pool.
// It takes a map of command parameters as input and returns the response from the worker.
// If an error occurs during the process, it returns nil and the error.
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

// ProcessPQ implements a priority queue for processes.
type ProcessPQ struct {
	processes []*ProcessWithPrio
	mutex     sync.Mutex
	pool      *ProcessPool
}

// Implementing the heap.Interface for ProcessPQ.
func (pq *ProcessPQ) Len() int {
	return len(pq.processes)
}

func (pq *ProcessPQ) Less(i, j int) bool {
	// First, compare based on queue length.
	if pq.processes[i].queueLength == pq.processes[j].queueLength {
		// If equal, compare based on the number of handled requests.
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

// Update rebuilds the priority queue with current process states.
func (pq *ProcessPQ) Update() {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	// Clear current queue
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

	// Reinitialize the heap to ensure the queue is correctly sorted.
	heap.Init(pq)
}
