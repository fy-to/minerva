package minerva

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Define a simple Task implementation for testing
type MockTask struct {
	ID           string        `json:"id"`
	Priority     int           `json:"priority"`
	Retries      int           `json:"retries"`
	MaxRetries   int           `json:"max_retries"`
	CreatedAt    time.Time     `json:"created_at"`
	LastError    string        `json:"last_error"`
	Success      bool          `json:"success"`
	MockProvider *MockProvider `json:"provider"`
	WillFail     bool          `json:"will_fail"`
	Failed       bool          `json:"failed"`
	Completed    bool          `json:"completed"`
	HandleCalls  int           `json:"handle_calls"`
	WillFailOnce bool          `json:"will_fail_once"`
	HandleServer string        `json:"handle_server"`
	TestCase     string        `json:"test_case"`
	Mutex        *sync.Mutex
}

func (m *MockTask) MarkAsSuccess() {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.Success = true
	m.Completed = true
}
func (m *MockTask) MarkAsFailed(err error) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.LastError = err.Error()
	m.Failed = true
	m.Completed = true
}
func (m *MockTask) GetPriority() int                   { return m.Priority }
func (m *MockTask) GetMutex() *sync.Mutex              { return m.Mutex }
func (m *MockTask) GetID() string                      { return m.ID }
func (m *MockTask) GetMaxRetries() int                 { return m.MaxRetries }
func (m *MockTask) GetRetries() int                    { return m.Retries }
func (m *MockTask) GetCreatedAt() time.Time            { return m.CreatedAt }
func (m *MockTask) GetTaskGroup() ITaskGroup           { return nil }
func (m *MockTask) GetProvider() IProvider             { return m.MockProvider }
func (m *MockTask) UpdateRetries(newRetries int) error { m.Retries = newRetries; return nil }
func (m *MockTask) GetTimeout() time.Duration          { return 5 * time.Second }
func (m *MockTask) UpdateLastError(err string) error   { m.LastError = err; return nil }
func (m *MockTask) OnComplete()                        {}
func (m *MockTask) OnStart()                           {}

// Define a simple Provider implementation for testing
type MockProvider struct {
	name string
}

func (mp *MockProvider) Handle(task ITask, server string) error {
	t := task.(*MockTask)
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.HandleServer = server
	if t.WillFail && !t.WillFailOnce || (t.WillFailOnce && t.HandleCalls == 0) {
		t.HandleCalls++
		//log.Printf("Task %s fails, HandleCalls: %d\n", t.ID, t.HandleCalls)
		return errors.New("intentional failure")
	}
	t.HandleCalls++
	//log.Printf("Task %s succeeds, HandleCalls: %d\n", t.ID, t.HandleCalls)
	return nil
}
func (mp *MockProvider) Name() string { return mp.name }

// Setting up the test environment
func TestTaskQueueManager(t *testing.T) {
	logger := zerolog.New(nil).Level(zerolog.Disabled)

	mockProvider1 := &MockProvider{name: "Provider1"}
	mockProvider2 := &MockProvider{name: "Provider2"}
	servers := map[string][]string{
		"Provider1": {"Server1", "Server2"},
		"Provider2": {"Server3", "Server4", "Server5"},
	}

	manager := NewTaskQueueManager(&logger, &[]IProvider{
		mockProvider1, mockProvider2}, servers)

	var tasks []ITask

	for i := 0; i < 10000; i++ {
		provider := mockProvider1
		if i%2 == 0 {
			provider = mockProvider2
		}

		maxRetries := 1
		willFail := false
		willFailOnce := false
		testCase := ""

		switch i % 4 {
		case 0: // Case 1: 1 retry, succeed after retry
			maxRetries = 1
			willFailOnce = true
			testCase = "Case 1: 1 retry, succeed after retry"
		case 1: // Case 2: 3 retries, always fail
			maxRetries = 3
			willFail = true
			testCase = "Case 2: 3 retries, always fail"
		case 2: // Case 3: 3 retries, fail once then succeed
			maxRetries = 3
			willFailOnce = true
			testCase = "Case 3: 3 retries, fail once then succeed"
		case 3: // Case 4: 1 retry, fail always
			maxRetries = 1
			willFail = true
			testCase = "Case 4: 1 retry, fail always"
		}

		task := &MockTask{
			ID:           "task" + strconv.Itoa(i+1),
			Priority:     1,
			CreatedAt:    time.Now(),
			MaxRetries:   maxRetries,
			MockProvider: provider,
			WillFail:     willFail,
			WillFailOnce: willFailOnce,
			TestCase:     testCase,
			Mutex:        &sync.Mutex{},
		}
		tasks = append(tasks, task)
	}

	// Running the manager and tasks
	go manager.Start(tasks)

	time.Sleep(6 * time.Second)

	// Test scenarios
	// Evaluate results
	for i, item := range tasks {
		task := item.(*MockTask)

		task.Mutex.Lock()

		var expectedHandleCalls int
		if task.WillFail && !task.WillFailOnce {
			expectedHandleCalls = task.MaxRetries + 1 // Continues to fail through all retries
		} else if task.WillFailOnce {
			expectedHandleCalls = 2 // Fails once, then should succeed the next time
		} else {
			expectedHandleCalls = 1 // No failure, succeeds on the first attempt
		}

		if task.HandleCalls != expectedHandleCalls {
			t.Errorf("Task %d [%s] expected %d handle calls, got %d", i+1, task.TestCase, expectedHandleCalls, task.HandleCalls)
		}

		if (!task.WillFail && !task.WillFailOnce && !task.Success) || (task.WillFailOnce && task.HandleCalls == 2 && !task.Success) {
			t.Errorf("Task %d [%s] expected to succeed but did not", i+1, task.TestCase)
		}

		if task.WillFail && !task.WillFailOnce && task.HandleCalls > task.MaxRetries && !task.Failed {
			t.Errorf("Task %d [%s] expected to fail but did not", i+1, task.TestCase)
		}

		task.Mutex.Unlock()
	}

	// check if GetTotalTasks returns 0
	if manager.GetTotalTasks() != 0 {
		t.Errorf("Expected GetTotalTasks to return 0, got %d", manager.GetTotalTasks())
	}
	t.Logf("All tests passed (10000 tasks with 2 providers)")

}
