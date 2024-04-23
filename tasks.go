package minerva

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Interfaces definition for tasks, task groups, and providers
type ITask interface {
	MarkAsSuccess()
	MarkAsFailed(err error)
	GetPriority() int
	GetMaxRetries() int
	GetRetries() int
	GetCreatedAt() time.Time
	GetTaskGroup() ITaskGroup
	GetProvider() IProvider
	UpdateRetries(int) error
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

// TaskPriority wraps a task with its priority and position in the queue
type TaskPriority struct {
	task     ITask
	priority int
	index    int
}

// PriorityQueue for managing tasks based on priority
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
	item.index = -1 // for safety
	*pq = old[:n-1]
	return item
}

// TaskQueueManager manages task queues for different providers and servers
type TaskQueueManager struct {
	queues     map[string]map[string]*PriorityQueue
	lock       map[string]*sync.Mutex
	cond       map[string]*sync.Cond
	queueSizes map[string]map[string]int
	logger     *zerolog.Logger
	providers  *[]IProvider
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

// NewTaskQueueManager creates a new TaskQueueManager
func NewTaskQueueManager(logger *zerolog.Logger, providers *[]IProvider, servers map[string][]string) *TaskQueueManager {
	tm := &TaskQueueManager{
		queues:     make(map[string]map[string]*PriorityQueue),
		lock:       make(map[string]*sync.Mutex),
		cond:       make(map[string]*sync.Cond),
		queueSizes: make(map[string]map[string]int),
		logger:     logger,
		providers:  providers,
		shutdownCh: make(chan struct{}),
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
		}
	}

	return tm
}

// Start begins processing tasks in all queues
func (m *TaskQueueManager) Start(tasks []ITask) {

	for providerName, serverMap := range m.queues {
		for server := range serverMap {
			m.wg.Add(1)
			go m.processQueue(providerName, server)
		}
	}

	for _, task := range tasks {
		m.AddTask(task)
	}
}

// AddTask adds a task to the appropriate queue based on its provider and server
func (m *TaskQueueManager) AddTask(task ITask) {
	providerName := task.GetProvider().Name()
	if _, ok := m.queues[providerName]; !ok {
		m.logger.Error().Msgf("[minerva|%s] Invalid provider", providerName)
		task.MarkAsFailed(errors.New("invalid provider"))
		return
	}

	server := m.selectServerWithLowestQueue(providerName)
	m.lock[providerName].Lock()
	heap.Push(m.queues[providerName][server], &TaskPriority{task: task, priority: task.GetPriority()})
	m.queueSizes[providerName][server]++
	m.cond[providerName].Signal()
	m.lock[providerName].Unlock()
}

// processQueue processes tasks from a specific queue for a provider and server
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
			}

			taskPriority := heap.Pop(m.queues[providerName][server]).(*TaskPriority)
			task := taskPriority.task
			m.queueSizes[providerName][server]--
			m.lock[providerName].Unlock()

			if err := m.handleTask(task, providerName, server); err != nil {
				m.logger.Error().Err(err).Msgf("[minerva|%s|%s] Failed to handle task", providerName, server)
				m.AddTask(task) // Requeue task if handling fails and not exceeded retries
			}
		}
	}
}

// handleTask tries to handle a task and manages retries and errors
func (m *TaskQueueManager) handleTask(task ITask, providerName, server string) error {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error().Msgf("[minerva|%s|%s] Recovered from panic: %v", providerName, server, r)
		}
	}()

	m.logger.Info().Msgf("[minerva|%s|%s] Handling task", providerName, server)

	if err := task.GetProvider().Handle(task, server); err != nil {
		task.UpdateLastError(err.Error())
		retries := task.GetRetries()
		maxRetries := task.GetMaxRetries()
		if retries >= maxRetries && maxRetries != 0 {
			task.MarkAsFailed(err)
			return nil
		}
		task.UpdateRetries(retries + 1)
		return err
	}
	task.MarkAsSuccess()
	return nil
}

// selectServerWithLowestQueue selects the server with the lowest number of queued tasks
func (m *TaskQueueManager) selectServerWithLowestQueue(providerName string) string {
	minQueue := int(^uint(0) >> 1) // Max int
	var selectedServer string
	for server, queueSize := range m.queueSizes[providerName] {
		if queueSize < minQueue {
			minQueue = queueSize
			selectedServer = server
		}
	}
	return selectedServer
}

// Shutdown signals all processing to stop and waits for completion
func (m *TaskQueueManager) Shutdown() {
	close(m.shutdownCh)
	m.wg.Wait()
}
