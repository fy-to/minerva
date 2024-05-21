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
	GetMutex() *sync.Mutex
}

type ITaskGroup interface {
	MarkComplete() error
	GetTaskCount() int
	GetTaskCompletedCount() int
	UpdateTaskCompletedCount(int) error
	GetMutex() *sync.Mutex
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
}

func NewTaskQueueManager(logger *zerolog.Logger, providers *[]IProvider, servers map[string][]string) *TaskQueueManager {
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
	}

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
func (m *TaskQueueManager) AddTask(task ITask) {
	providerName := task.GetProvider().Name()
	m.lock[providerName].Lock()
	defer m.lock[providerName].Unlock()

	if _, ok := m.queues[providerName]; !ok {
		m.logger.Error().Msgf("[minerva|%s] Invalid provider", providerName)
		task.MarkAsFailed(errors.New("invalid provider"))
		return
	}

	// Lock for inProgressTasks operations
	m.inProgressTasksMutex.Lock()
	if m.HasTaskInQueue(task) {
		m.logger.Info().Msgf("[minerva|%s] Task already in queue or being processed", providerName)
		m.inProgressTasksMutex.Unlock() // Unlock before returning
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
			m.inProgressTasksMutex.Lock()
			delete(m.inProgressTasks, task.GetID())
			m.inProgressTasksMutex.Unlock()
			m.lock[providerName].Unlock()

			// Ensure handleTask is called within a separate goroutine to prevent blocking.
			if err := m.handleTaskWithTimeout(task, providerName, server); err != nil {
				m.logger.Error().Err(err).Msgf("[minerva|%s|%s] Failed to handle task", providerName, server)
			}
		}
	}
}

func (m *TaskQueueManager) handleTaskWithTimeout(task ITask, providerName, server string) error {
	timeout := task.GetTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	// Create a channel to signal completion (or error)
	done := make(chan error, 1)
	go func() {
		done <- m.handleTask(task, providerName, server)
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
		m.AddTask(task)
		return err
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
	close(m.shutdownCh)
	m.wg.Wait()
}
