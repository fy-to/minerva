package minerva

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type JSManager struct {
	Mu     sync.Mutex
	Nodes  map[string]map[int]*JSProcess
	Logger *zerolog.Logger
}

func NewNodeManager(logger zerolog.Logger) *JSManager {
	return &JSManager{
		Nodes:  make(map[string]map[int]*JSProcess),
		Logger: &logger,
	}
}

type SetupFunction func([]interface{}) (string, error)

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

func (jsm *JSManager) RestartAllNodesByName(processName string) {
	jsm.Mu.Lock()
	if _, ok := jsm.Nodes[processName]; ok {
		for _, jsProcess := range jsm.Nodes[processName] {
			jsProcess.Stop()
		}
	}
	jsm.Mu.Unlock()

	for _, jsProcess := range jsm.Nodes[processName] {
		jsProcess.Start()
	}
}
func (jsm *JSManager) CountAllNodes() int {
	jsm.Mu.Lock()
	defer jsm.Mu.Unlock()
	count := 0
	for _, process := range jsm.Nodes {
		count += len(process)
	}
	return count
}
func (jsm *JSManager) StopAllNodes() {
	for _, process := range jsm.Nodes {
		for _, jsProcess := range process {
			jsProcess.Stop()
		}
	}
}

func (jsm *JSManager) GetProcessFromPool(processName string) (*JSProcess, error) {
	processResult := make(chan *JSProcess, 1)

	go func() {
		for _, process := range jsm.Nodes[processName] {
			jsm.Logger.Debug().Msgf("[minerva] [%s] checking process %d - %t", processName, process.Id, process.IsBusy)
			if !process.IsBusy && process.Cmd != nil {
				processResult <- process
				return
			}
		}
	}()
	select {
	case process := <-processResult:
		return process, nil
	case <-time.After(500 * time.Millisecond):
		jsm.Logger.Info().Msgf("[minerva] [%s] no available process", processName)
		return nil, fmt.Errorf("[minerva] no available process")
	}
}
func (jsm *JSManager) ExecuteJSCode(jsCode string, processName string) (map[string]interface{}, error) {
	jsm.Mu.Lock()
	process, err := jsm.GetProcessFromPool(processName)
	if err != nil {
		jsm.Mu.Unlock()
		return nil, err
	}
	process.IsBusy = true
	jsm.Mu.Unlock()

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
		jsm.Logger.Info().Msgf("[minerva] [%s] execution timed out", processName)
		return nil, fmt.Errorf("[minerva] execution timed out")
	}
}
