package minerva

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type JSManager struct {
	Mu     sync.Mutex
	Nodes  map[string]map[int]*JSProcess
	Logger *zerolog.Logger
}

// NewNodeManager creates a new instance of JSManager, which manages the nodes and processes.
// It takes a logger of type zerolog.Logger as a parameter.
// Returns a pointer to the newly created JSManager.
func NewNodeManager(logger zerolog.Logger) *JSManager {
	return &JSManager{
		Nodes:  make(map[string]map[int]*JSProcess),
		Logger: &logger,
	}
}

type SetupFunction func([]interface{}) (string, error)

// StartProcess starts a new process in the JSManager.
// It takes the following parameters:
// - id: the ID of the process
// - processName: the name of the process (websiteUUID, for example)
// - nodeFile: the file path of the node file to be executed
// - nodeCmd: the command to execute the node file (node, bun or any JS runtime)
// - separator: the separator used in the node file to separate the output (e.g '\n')
// - timeout: the timeout duration in seconds for one execution of the process
// - forceSetup: a flag indicating whether to force the setup function to be executed (should call the setup function, to update the node file if needed)
// - setupFunction: the setup function to be executed if the node file does not exist or forceSetup is true (should return the content of the node file)
// - args: the arguments to be passed to the setup function (can be nil, used for setup function)
// It returns an error if any error occurs during the setup or execution of the process.
func (jsm *JSManager) StartProcess(
	id int,
	processName string,
	nodeFile string,
	nodeCmd string,
	separator string,
	timeout int,
	forceSetup bool,
	setupFunction SetupFunction,
	args *[]interface{},
) error {
	if _, err := os.Stat(nodeFile); os.IsNotExist(err) || forceSetup {
		fileContent, err := setupFunction(*args)
		if err != nil {
			return err
		}
		err = os.WriteFile(nodeFile, []byte(fileContent), 0644)
		if err != nil {
			return err
		}
	}

	jsProcess := &JSProcess{
		Name:      processName,
		NodeCmd:   nodeCmd,
		CreatedAt: time.Now().Unix(),
		Id:        id,
		IsBusy:    true,
		Args:      args,
		SetupFunc: &setupFunction,
		NodeFile:  nodeFile,
		Separator: separator,
		Timeout:   timeout,
		Manager:   jsm,
	}

	jsm.Mu.Lock()
	if _, ok := jsm.Nodes[processName]; !ok {
		jsm.Nodes[processName] = make(map[int]*JSProcess)
	}
	jsm.Nodes[processName][id] = jsProcess
	jsm.Mu.Unlock()
	jsm.Nodes[processName][id].Start()

	return nil
}

// CountAllNodes compte le nombre total de nœuds dans le gestionnaire JSManager.
func (jsm *JSManager) CountAllNodes() int {
	jsm.Mu.Lock()
	defer jsm.Mu.Unlock()
	count := 0
	for _, process := range jsm.Nodes {
		count += len(process)
	}
	return count
}

// StopAllNodes arrête tous les nœuds gérés par le JSManager.
func (jsm *JSManager) StopAllNodes() {
	for _, process := range jsm.Nodes {
		for _, jsProcess := range process {
			jsProcess.Stop()
		}
	}
}

// GetProcessFromPool retrieves an available JSProcess from the pool for a given process name.
// It checks each process in the pool and returns the first process that is not busy and running.
// If no available process is found within the specified timeout, it returns an error.
func (jsm *JSManager) GetProcessFromPool(processName string) (*JSProcess, error) {
	var keepChecking int32 = 1 // Use an int32 for atomic operations
	jsm.Mu.Lock()
	defer jsm.Mu.Unlock()

	processChan := make(chan *JSProcess, 1)
	quitChan := make(chan bool)

	go func() {
		for atomic.LoadInt32(&keepChecking) != 0 {
			select {
			case <-quitChan:
				return
			default:
				for _, process := range jsm.Nodes[processName] {
					if !process.IsBusy && process.Cmd != nil {
						process.IsBusy = true
						processChan <- process
						return
					}
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case process := <-processChan:
		atomic.StoreInt32(&keepChecking, 0)
		close(quitChan)
		return process, nil
	case <-time.After(250 * time.Millisecond):
		atomic.StoreInt32(&keepChecking, 0)
		close(quitChan)
		return nil, fmt.Errorf("[minerva] no available process")
	}
}

// ExecuteJSCode executes the given JavaScript code on a specific process.
// It acquires a process from the process pool, marks it as busy, and then executes the code.
// If the execution times out, the process is restarted.
// It returns the result of the execution as a map[string]interface{} and any error encountered.
func (jsm *JSManager) ExecuteJSCode(jsCode string, processName string) (map[string]interface{}, error) {
	process, err := jsm.GetProcessFromPool(processName)
	if err != nil {
		return nil, err
	}

	resultChan := make(chan JSResponse, 1)
	go process.executeJSCodeInternal(jsCode, resultChan)
	select {
	case res := <-resultChan:
		jsm.Mu.Lock()
		defer jsm.Mu.Unlock()
		process.IsBusy = false
		return res.Data, res.GoError
	case <-time.After(time.Duration(process.Timeout) * time.Second):
		go process.Restart()
		jsm.Logger.Info().Msgf("[minerva|%s] execution timed out", processName)
		return nil, fmt.Errorf("[minerva] execution timed out")
	}
}
