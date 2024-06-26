package minerva

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type ITask interface {
	MarkAsSuccess()
	MarkAsFailed(err error)
	GetPriority() int
	GetID() string
	GetMaxRetries() int
	GetRetries() int
	GetCreatedAt() time.Time
	GetTaskGroup() ITaskGroup
	GetProvider() IProvider
	UpdateRetries(int) error
	GetTimeout() time.Duration
	UpdateLastError(string) error
	OnComplete()
	OnStart()
}

type ITaskGroup interface {
	MarkComplete() error
	GetTaskCount() int
	GetTaskCompletedCount() int
	UpdateTaskCompletedCount(int) error
}

type IProvider interface {
	Handle(task ITask, server string) error
	Name() string
}

type TaskPriority struct {
	task     ITask
	priority int
	index    int
}

type PriorityQueue []*TaskPriority

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].priority == pq[j].priority {
		return pq[i].task.GetCreatedAt().Before(pq[j].task.GetCreatedAt())
	}
	return pq[i].priority < pq[j].priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TaskPriority)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[:n-1]
	return item
}

type TaskQueueManager struct {
	queues               map[string]map[string]*PriorityQueue
	lock                 map[string]*sync.Mutex
	cond                 map[string]*sync.Cond
	queueSizes           map[string]map[string]int
	logger               *zerolog.Logger
	providers            *[]IProvider
	shutdownCh           chan struct{}
	wg                   sync.WaitGroup
	inProgressTasks      map[string]bool
	inProgressTasksMutex sync.Mutex
	queueCond            map[string]*sync.Cond
	queueLock            map[string]*sync.Mutex
	shutdownMutex        sync.Mutex
	shouldRestart        bool
	maxTimeForTask       time.Duration
	selfLock             sync.Mutex
	initServerMap        map[string][]string
	handleTimeout        bool
}

func NewTaskQueueManager(logger *zerolog.Logger, providers *[]IProvider, servers map[string][]string, maxTime time.Duration, handleTimeout bool) *TaskQueueManager {
	tm := &TaskQueueManager{
		queues:               make(map[string]map[string]*PriorityQueue),
		lock:                 make(map[string]*sync.Mutex),
		cond:                 make(map[string]*sync.Cond),
		queueSizes:           make(map[string]map[string]int),
		logger:               logger,
		providers:            providers,
		shutdownCh:           make(chan struct{}),
		queueCond:            make(map[string]*sync.Cond),
		queueLock:            make(map[string]*sync.Mutex),
		inProgressTasksMutex: sync.Mutex{},
		inProgressTasks:      make(map[string]bool),
		shutdownMutex:        sync.Mutex{},
		maxTimeForTask:       maxTime,
		selfLock:             sync.Mutex{},
		shouldRestart:        false,
		initServerMap:        servers,
		handleTimeout:        handleTimeout,
	}
	tm.selfLock.Lock()
	defer tm.selfLock.Unlock()
	tm.shutdownMutex.Lock()
	defer tm.shutdownMutex.Unlock()
	for _, provider := range *providers {
		tm.queues[provider.Name()] = make(map[string]*PriorityQueue)
		tm.queueSizes[provider.Name()] = make(map[string]int)
		for _, server := range servers[provider.Name()] {
			pq := make(PriorityQueue, 0)
			heap.Init(&pq)
			tm.queues[provider.Name()][server] = &pq
			tm.queueSizes[provider.Name()][server] = 0
			mutex := &sync.Mutex{}
			tm.lock[provider.Name()] = mutex
			tm.cond[provider.Name()] = sync.NewCond(mutex)
			tm.queueLock[server] = &sync.Mutex{}
			tm.queueCond[server] = sync.NewCond(tm.queueLock[server])
		}
	}

	return tm
}
func (m *TaskQueueManager) GetShouldRestart() bool {
	m.selfLock.Lock()
	defer m.selfLock.Unlock()
	return m.shouldRestart
}
func (m *TaskQueueManager) SetShouldRestart(shouldRestart bool) {
	m.selfLock.Lock()
	defer m.selfLock.Unlock()
	m.shouldRestart = shouldRestart
}
func (m *TaskQueueManager) HasTaskInQueue(task ITask) bool {
	taskID := task.GetID()

	// Check in-progress tasks first
	if m.inProgressTasks[taskID] {
		return true
	}

	// Then check queues as before
	providerName := task.GetProvider().Name()
	for _, serverQueue := range m.queues[providerName] {
		for _, taskPriority := range *serverQueue {
			if taskPriority.task.GetID() == taskID {
				return true
			}
		}
	}
	return false
}

func (m *TaskQueueManager) Start(tasks []ITask) {
	for providerName, serverMap := range m.queues {
		for server := range serverMap {
			m.wg.Add(1)
			go m.processQueue(providerName, server)
		}
	}

	// Add tasks after goroutines have started
	for _, task := range tasks {
		m.AddTask(task)
	}
}
func (m *TaskQueueManager) GetTotalTasks() int {
	totalTasks := 0
	for _, serverMap := range m.queueSizes {
		for _, queueSize := range serverMap {
			totalTasks += queueSize
		}
	}
	return totalTasks
}
func (m *TaskQueueManager) AddTask(task ITask) error {

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {

		providerName := task.GetProvider().Name()
		m.lock[providerName].Lock()
		defer m.lock[providerName].Unlock()

		if _, ok := m.queues[providerName]; !ok {
			m.logger.Error().Msgf("[minerva|%s] Invalid provider", providerName)
			task.MarkAsFailed(errors.New("invalid provider"))
			done <- errors.New("invalid provider")
			return
		}

		// Lock for inProgressTasks operations
		m.inProgressTasksMutex.Lock()
		if m.HasTaskInQueue(task) {
			m.logger.Warn().Msgf("[minerva|%s] Task already in queue or being processed", providerName)
			m.inProgressTasksMutex.Unlock() // Unlock before returning
			done <- errors.New("task already in queue or being processed")
			return
		}
		m.inProgressTasks[task.GetID()] = true
		m.inProgressTasksMutex.Unlock()

		server := m.selectServerWithLowestQueue(providerName)
		m.queueLock[server].Lock()
		heap.Push(m.queues[providerName][server], &TaskPriority{task: task, priority: task.GetPriority()})
		queueSize := m.queueSizes[providerName][server]
		m.queueSizes[providerName][server]++
		m.queueLock[server].Unlock()
		m.cond[providerName].Broadcast()
		m.logger.Info().Msgf("[minerva|%s|%s] Task added to queue (queue size: %d)", providerName, server, queueSize)
		done <- nil
		return
	}()

	select {
	case <-ctx.Done():
		cancel()
		m.logger.Error().Msgf("[minerva|%s|%s] Timed out while adding task to queue", task.GetProvider().Name(), task.GetID())
		m.SetShouldRestart(true)
		return ctx.Err()
	case err := <-done:
		cancel()
		return err
	}
}

func (m *TaskQueueManager) processQueue(providerName, server string) {
	m.logger.Info().Msgf("[minerva|%s|%s] Starting queue processor", providerName, server)
	defer m.wg.Done()
	for {
		select {
		case <-m.shutdownCh:
			return
		default:
			m.lock[providerName].Lock()
			for m.queueSizes[providerName][server] == 0 {
				m.cond[providerName].Wait()
				select {
				case <-m.shutdownCh:
					m.lock[providerName].Unlock()
					return
				default:
					// Continue processing if not shutting down
				}
			}

			taskPriority := heap.Pop(m.queues[providerName][server]).(*TaskPriority)
			task := taskPriority.task
			m.queueSizes[providerName][server]--

			m.lock[providerName].Unlock()

			// Ensure handleTask is called within a separate goroutine to prevent blocking.
			if m.handleTimeout {
				if err := m.handleTaskWithTimeout(task, providerName, server); err != nil {
					m.logger.Error().Err(err).Msgf("[minerva|%s|%s] Failed to handle task", providerName, server)
				}
			} else {
				if err := m.handleTask(task, providerName, server); err != nil {
					m.logger.Error().Err(err).Msgf("[minerva|%s|%s] Failed to handle task", providerName, server)
				}
			}
		}
	}
}

func (m *TaskQueueManager) Restart(tasks []ITask) {
	m.SetShouldRestart(false)
	m.logger.Warn().Msg("Restarting task queue manager")
	m.selfLock.Lock()
	defer m.selfLock.Unlock()
	m.Shutdown()
	m = NewTaskQueueManager(m.logger, m.providers, m.initServerMap, m.maxTimeForTask, m.handleTimeout)
	m.Start(tasks)
}

func (m *TaskQueueManager) handleTaskWithTimeout(task ITask, providerName, server string) error {
	timeout := task.GetTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	// Create a channel to signal completion (or error)
	done := make(chan error, 1)
	go func() {
		startTime := time.Now()
		result := m.handleTask(task, providerName, server)
		elapsed := time.Since(startTime)
		if elapsed > m.maxTimeForTask {
			m.logger.Warn().Msgf("[minerva|%s|%s] Task took too long to process: %s", providerName, server, elapsed)
			m.SetShouldRestart(true)
		}
		done <- result
	}()

	// Wait for either the task to complete or the context to expire
	select {
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	case err := <-done:
		cancel()
		return err

	}

}

func (m *TaskQueueManager) handleTask(task ITask, providerName, server string) error {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error().Msgf("[minerva|%s|%s] Recovered from panic: %v", providerName, server, r)
		}
	}()

	m.logger.Info().Msgf("[minerva|%s|%s] Handling task", providerName, server)
	task.OnStart()

	err := task.GetProvider().Handle(task, server)
	if err != nil {
		task.UpdateLastError(err.Error())
		retries := task.GetRetries()
		maxRetries := task.GetMaxRetries()
		if retries >= maxRetries {
			task.MarkAsFailed(err)
			task.OnComplete()
			return nil
		}
		task.UpdateRetries(retries + 1)
		m.inProgressTasksMutex.Lock()
		delete(m.inProgressTasks, task.GetID())
		m.inProgressTasksMutex.Unlock()
		m.AddTask(task)
		return err
	} else {
		m.inProgressTasksMutex.Lock()
		delete(m.inProgressTasks, task.GetID())
		m.inProgressTasksMutex.Unlock()
	}

	m.logger.Info().Msgf("[minerva|%s|%s] Task handled successfully", providerName, server)
	task.MarkAsSuccess()
	task.OnComplete()
	return nil
}

func (m *TaskQueueManager) selectServerWithLowestQueue(providerName string) string {
	minQueue := int(^uint(0) >> 1)
	var selectedServer string
	for server, queueSize := range m.queueSizes[providerName] {
		if queueSize < minQueue {
			minQueue = queueSize
			selectedServer = server
		}
	}
	return selectedServer
}

func (m *TaskQueueManager) Shutdown() {
	m.shutdownMutex.Lock() // Lock to prevent concurrent Shutdown calls
	defer m.shutdownMutex.Unlock()

	// Check if already shutting down
	select {
	case <-m.shutdownCh:
		return // Already shutting down
	default:
	}

	close(m.shutdownCh) // Signal shutdown to worker goroutines

	// Broadcast to all queue conditions to wake up any waiting goroutines
	for _, cond := range m.cond {
		cond.Broadcast()
	}

	for server := range m.queueCond {
		m.queueCond[server].Broadcast() // Wake goroutines waiting on queue conditions
	}

	m.wg.Wait() // Wait for all worker goroutines to finish
}
