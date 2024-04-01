package minerva

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type ITask interface {
	MarkAsSuccess()
	MarkAsFailed(error)
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

func (m *TaskQueueManager) Start(tasks []ITask) {

	for _, provider := range *m.providers {
		for server := range m.queues[provider.Name()] {
			m.logger.Info().Msgf("[minerva|%s|%s] Starting queue processor", provider.Name(), server)
			go m.processQueue(provider.Name(), server)
		}
	}

	for _, task := range tasks {
		m.AddTask(task)
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
	panicked := false
	var provider string
	var server string

	func() {
		defer func() {
			if r := recover(); r != nil {
				errMsg := fmt.Sprintf("Recovered from panic: %v", r)
				m.logger.Error().Msgf("[minerva] recovered from panic: %s", errMsg)
				panicked = true
			}
		}()

		provider = task.GetProvider().Name()
		server = m.selectServerWithLowestQueue(provider)
	}()

	if panicked {
		return
	}
	// check if provider is not nil and is in the list of providers by checking m.queues for exemple
	if _, ok := m.queues[provider]; !ok {
		m.logger.Error().Msgf("[minerva|%s|%s] Provider not found", provider, server)
		return
	}
	m.lock[provider].Lock()
	defer m.lock[provider].Unlock()
	if _, ok := m.queues[provider][server]; !ok {
		m.queues[provider][server] = &PriorityQueue{}
	}

	heap.Push(m.queues[provider][server], &TaskPriority{
		task:     task,
		priority: task.GetPriority(),
	})
	m.logger.Info().Msgf("[minerva|%s|%s] Added task with priority %d to queue (%d/%d)", provider, server, task.GetPriority(), task.GetRetries(), task.GetMaxRetries())
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
		t := taskPriority.task
		m.queueSizes[provider][server]--

		m.cond[provider].L.Unlock()
		hasPanic := false

		func() {
			defer func() {
				if r := recover(); r != nil {
					errMsg := fmt.Sprintf("Recovered from panic: %v", r)
					m.logger.Error().Msgf("[minerva|%s|%s] recovered from panic: %s", provider, server, errMsg)

					t.UpdateLastError(errMsg)
					t.MarkAsFailed(errors.New(errMsg))
					t.OnComplete()
					if t.GetTaskGroup() != nil {
						taskGroup := t.GetTaskGroup()
						taskGroup.UpdateTaskCompletedCount(taskGroup.GetTaskCompletedCount() + 1)
						if taskGroup.GetTaskCount() == taskGroup.GetTaskCompletedCount() {
							taskGroup.MarkComplete()
						}
					}
					hasPanic = true
				}

				if hasPanic {
					return
				}

				t.OnStart()
				err := t.GetProvider().Handle(t, server)
				if err != nil {
					m.logger.Warn().Msgf("[minerva|%s|%s] Failed to handle task %d/%d: %s", provider, server, t.GetRetries(), t.GetMaxRetries(), err.Error())
					t.UpdateRetries(t.GetRetries() + 1)
					t.UpdateLastError(err.Error())
					if t.GetRetries() <= t.GetMaxRetries() {
						m.logger.Info().Msgf("[minerva|%s|%s] Retrying task %d/%d", provider, server, t.GetRetries(), t.GetMaxRetries())
						//m.AddTask(t)
						return
					} else {
						t.MarkAsFailed(err)
						t.OnComplete()
						m.logger.Error().Msgf("[minerva|%s|%s] Task failed after %d retries", provider, server, t.GetMaxRetries())
						if t.GetTaskGroup() != nil {
							taskGroup := t.GetTaskGroup()
							taskGroup.UpdateTaskCompletedCount(taskGroup.GetTaskCompletedCount() + 1)
							if taskGroup.GetTaskCount() == taskGroup.GetTaskCompletedCount() {
								taskGroup.MarkComplete()
							}
						}
						return
					}
				} else {
					t.MarkAsSuccess()
					t.OnComplete()
					if t.GetTaskGroup() != nil {
						taskGroup := t.GetTaskGroup()
						taskGroup.UpdateTaskCompletedCount(taskGroup.GetTaskCompletedCount() + 1)
						if taskGroup.GetTaskCount() == taskGroup.GetTaskCompletedCount() {
							taskGroup.MarkComplete()
						}
					}
				}
			}()
		}()
		m.cond[provider].L.Lock()
	}
}

func (m *TaskQueueManager) Shutdown() {
	close(m.shutdownCh)
	m.wg.Wait()

	m.logger.Info().Msg("TaskQueueManager shutdown complete.")
}
