package minerva

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"
)

type JSManager struct {
	mu    sync.Mutex
	nodes map[string]map[int]*JSProcess
}

func NewNodeManager() *JSManager {
	return &JSManager{
		nodes: make(map[string]map[int]*JSProcess),
	}
}

type SetupFunction func(...interface{}) (string, error)

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
		fileContent, err := setupFunction(*args...)
		if err != nil {
			return err
		}
		err = os.WriteFile(nodeFile, []byte(fileContent), 0644)
		if err != nil {
			return err
		}
	}
	cmd := exec.Command(nodeCmd, "--experimental-default-type=module", nodeFile)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	jsProcess := &JSProcess{
		cmd:       cmd,
		stdin:     bufio.NewWriter(stdin),
		stdout:    bufio.NewReader(stdout),
		name:      processName,
		nodeCmd:   nodeCmd,
		createdAt: time.Now().Unix(),
		id:        id,
		busy:      false,
		args:      args,
		setupFunc: &setupFunction,
		nodeFile:  nodeFile,
		separator: separator,
		timeout:   timeout,
	}

	jsm.mu.Lock()
	if _, ok := jsm.nodes[processName]; !ok {
		jsm.nodes[processName] = make(map[int]*JSProcess)
	}
	jsm.nodes[processName][id] = jsProcess
	jsm.nodes[processName][id].Start()
	jsm.mu.Unlock()

	return nil
}

func (jsm *JSManager) RestartAllNodesByName(processName string) {
	jsm.mu.Lock()
	if _, ok := jsm.nodes[processName]; ok {
		for _, jsProcess := range jsm.nodes[processName] {
			jsProcess.Stop()
		}
	}
	jsm.mu.Unlock()

	for _, jsProcess := range jsm.nodes[processName] {
		jsProcess.Start()
	}
}
func (jsm *JSManager) CountAllNodes() int {
	jsm.mu.Lock()
	defer jsm.mu.Unlock()
	count := 0
	for _, process := range jsm.nodes {
		count += len(process)
	}
	return count
}
func (jsm *JSManager) StopAllNodes() {
	jsm.mu.Lock()
	defer jsm.mu.Unlock()
	for _, process := range jsm.nodes {
		for _, jsProcess := range process {
			jsProcess.Stop()
		}
	}
}

func (jsm *JSManager) GetProcessFromPool(processName string) (*JSProcess, error) {
	processResult := make(chan *JSProcess, 1)

	go func() {
		jsm.mu.Lock()
		defer jsm.mu.Unlock()
		for _, process := range jsm.nodes[processName] {
			if !process.busy && process.cmd != nil {
				processResult <- process
				return
			}
		}
	}()
	select {
	case process := <-processResult:
		return process, nil
	case <-time.After(500 * time.Millisecond):
		return nil, fmt.Errorf("[minerva] no available process")
	}
}
func (jsm *JSManager) ExecuteJSCode(ctx *interface{}, jsCode, websiteUUID string) (map[string]interface{}, error) {
	jsm.mu.Lock()
	process, err := jsm.GetProcessFromPool(websiteUUID)
	if err != nil {
		return nil, err
	}
	process.busy = true
	jsm.mu.Unlock()

	resultChan := make(chan JSResponse, 1)
	go process.executeJSCodeInternal(jsCode, resultChan)
	select {
	case res := <-resultChan:
		jsm.mu.Lock()
		defer jsm.mu.Unlock()
		process.busy = false
		return res.Data, res.ActualError
	case <-time.After(time.Duration(process.timeout) * time.Second):
		jsm.mu.Lock()
		defer jsm.mu.Unlock()
		go process.Restart()
		return nil, fmt.Errorf("[minerva] execution timed out")
	}
}
