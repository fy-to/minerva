package minerva

import (
	"container/heap"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type ITask interface {
	MarkAsSuccess()
	MarkAsFailed(error)
	Save() error
	Priority() int
	Options() map[string]interface{}
	MaxRetries() int
	Retries() int
	LastError() string
	Failed() bool
	CreatedAt() time.Time
	CompletedAt() time.Time
	UUID() string
	TaskGroup() ITaskGroup
	Provider() IProvider
	UpdateRetries(int) error
}

type ITaskGroup interface {
	AddTask(task ITask) error
	MarkComplete() error
	Save() error
	CompletedAt() time.Time
	TaskCount() int
	TaskCompletedCount() int
	Command() string
}

type IProvider interface {
	Handle(ITask, string) error
	Name() string
}

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

func NewTaskQueueManager(logger *zerolog.Logger, providers *[]IProvider) *TaskQueueManager {
	return &TaskQueueManager{
		queues:     make(map[string]map[string]*PriorityQueue),
		lock:       make(map[string]*sync.Mutex),
		cond:       make(map[string]*sync.Cond),
		providers:  providers,
		logger:     logger,
		queueSizes: make(map[string]map[string]int),
		shutdownCh: make(chan struct{}),
		wg:         sync.WaitGroup{},
	}
}

func (m *TaskQueueManager) Start(tasks []ITask) {
	for _, provider := range *m.providers {
		providerName := provider.Name()
		m.lock[providerName] = &sync.Mutex{}
		m.cond[providerName] = sync.NewCond(m.lock[providerName])
		m.queueSizes[providerName] = make(map[string]int)
		m.queues[providerName] = make(map[string]*PriorityQueue)
	}

	for _, task := range tasks {
		m.AddTask(task)
	}

	for provider, servers := range m.queues {
		for server := range servers {
			m.logger.Debug().Msgf("[minerva|%s|%s] Starting queue processor", provider, server)
			go m.processQueue(provider, server)
		}
	}
}
func (m *TaskQueueManager) selectServerWithLowestQueue(provider string) string {
	lowestSize := math.MaxInt
	lowestServer := ""

	for server, size := range m.queueSizes[provider] {
		if size == 0 {
			return server
		}
		if size < lowestSize {
			lowestSize = size
			lowestServer = server
		}
	}

	return lowestServer
}
func (m *TaskQueueManager) AddTask(task ITask) {
	provider := task.Provider().Name()
	server := m.selectServerWithLowestQueue(provider)
	m.lock[provider].Lock()
	defer m.lock[provider].Unlock()
	if _, ok := m.queues[provider][server]; !ok {
		m.queues[provider][server] = &PriorityQueue{}
	}

	heap.Push(m.queues[provider][server], &TaskPriority{
		task:     task,
		priority: task.Priority(),
	})
	m.logger.Debug().Msgf("[minerva|%s|%s] Added task %d/%d", provider, server, task.Priority(), task.Retries())
	m.queueSizes[provider][server]++
	m.cond[provider].Signal()
}

func (m *TaskQueueManager) processQueue(provider, server string) {
	m.wg.Add(1)
	defer m.wg.Done()
	m.lock[provider].Lock()
	defer m.lock[provider].Unlock()
	for {
		for m.queueSizes[provider][server] == 0 {
			select {
			case <-m.shutdownCh:
				m.lock[provider].Unlock()
				return
			default:
				m.cond[provider].Wait()
			}
		}
		taskPriority := heap.Pop(m.queues[provider][server]).(*TaskPriority)
		m.queueSizes[provider][server]--
		m.cond[provider].L.Unlock()

		var err error
		err = taskPriority.task.Provider().Handle(taskPriority.task, server)
		if err != nil {
			m.logger.Error().Err(err).Msgf("[minerva|%s|%s] Failed to handle task %d/%d", provider, server, taskPriority.task.Priority(), taskPriority.task.Retries())
			taskPriority.task.UpdateRetries(taskPriority.task.Retries() + 1)

			if taskPriority.task.Retries() < taskPriority.task.MaxRetries() {
				m.AddTask(taskPriority.task)
			} else {
				taskPriority.task.MarkAsFailed(err)
			}
		} else {
			taskPriority.task.MarkAsSuccess()
		}
		m.cond[provider].L.Lock()
	}
}

func (m *TaskQueueManager) Shutdown() {
	close(m.shutdownCh)
	m.wg.Wait()

	m.logger.Info().Msg("TaskQueueManager shutdown complete.")
}
