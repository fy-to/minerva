package minerva

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type ProcessPool struct {
	processes []*Process
	mutex     sync.Mutex
	logger    *zerolog.Logger
}

// NewProcessPool creates a new process pool with the specified name, size, logger, command, and command arguments.
// It initializes the pool with the specified number of processes and returns the created process pool.
func NewProcessPool(name string, size int, logger *zerolog.Logger, cmd string, cmdArgs []string) *ProcessPool {
	pool := &ProcessPool{
		processes: make([]*Process, size),
		logger:    logger,
		mutex:     sync.Mutex{},
	}
	for i := 0; i < size; i++ {
		pool.newProcess(name, i, cmd, cmdArgs, logger)
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
func (pool *ProcessPool) newProcess(name string, i int, cmd string, cmdArgs []string, logger *zerolog.Logger) {

	_cmd := exec.Command(cmd, cmdArgs...)
	stdin, err := _cmd.StdinPipe()
	if err != nil {
		logger.Error().Err(err).Msg("[minerva] Failed to get stdin pipe for process")
		return
	}
	stdout, err := _cmd.StdoutPipe()
	if err != nil {
		logger.Error().Err(err).Msg("[minerva] Failed to get stdout pipe for process")
		return
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	process := &Process{
		cmd:           _cmd,
		inputQueue:    make(chan map[string]interface{}, 25),
		outputQueue:   make(chan map[string]interface{}, 100),
		isReady:       false,
		lastHeartbeat: time.Now(),
		latency:       0,
		logger:        logger,
		stdin:         json.NewEncoder(stdin),
		stdout:        bufio.NewScanner(stdout),
		name:          fmt.Sprintf("%s#%d", name, i),
	}
	pool.processes[i] = process
	pool.processes[i].Start()

}

// ExportAll exports all the processes in the process pool as a slice of ProcessExport.
// It acquires a lock on the process pool, iterates over each process, and creates a ProcessExport object for each non-nil process.
// The ProcessExport object contains information such as the process's readiness, last heartbeat, latency, input queue length, output queue length, and name.
func (pool *ProcessPool) ExportAll() []ProcessExport {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	var exports []ProcessExport
	for _, process := range pool.processes {

		if process != nil {
			exports = append(exports, ProcessExport{
				IsReady:       process.isReady,
				LastHeartbeat: process.lastHeartbeat.UnixMilli(),
				Latency:       process.latency,
				InputQueue:    len(process.inputQueue),
				OutputQueue:   len(process.outputQueue),
				Name:          process.name,
			})
		}
	}
	return exports
}

// GetWorker returns a worker process from the process pool.
// It selects the worker with the minimum length of the input queue.
// If there are no available workers, it returns an error.
func (pool *ProcessPool) GetWorker() (*Process, error) {
	pool.mutex.Lock()
	minQueueLength := math.MaxInt32
	var minQueueProcess *Process
	for _, process := range pool.processes {

		process.mutex.RLock()
		if process.isReady {
			if len(process.inputQueue) < minQueueLength {
				minQueueLength = len(process.inputQueue)
				minQueueProcess = process
			}
		}
		process.mutex.RUnlock()
	}
	pool.mutex.Unlock()
	if minQueueProcess != nil {
		return minQueueProcess, nil
	}

	return nil, errors.New("no free worker available")
}

// SendCommand sends a command to a worker in the process pool.
// It takes a map of command parameters as input and returns the response from the worker.
// If an error occurs during the process, it returns nil and the error.
func (pool *ProcessPool) SendCommand(cmd map[string]interface{}) (map[string]interface{}, error) {
	worker, err := pool.GetWorker()
	if err != nil {

		return nil, err
	}

	if _, ok := cmd["id"]; !ok {
		cmd["id"] = uuid.New().String()
	}
	if _, ok := cmd["type"]; !ok {
		cmd["type"] = "main"
	}
	return worker.SendCommand(cmd)
}

// StopAll stops all the processes in the process pool.
func (pool *ProcessPool) StopAll() {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	for _, process := range pool.processes {
		process.Stop()
	}
}
