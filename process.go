package minerva

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

type JSProcess struct {
	Cmd       *exec.Cmd
	StdIn     *bufio.Writer
	StdOut    *bufio.Reader
	Name      string
	NodeCmd   string
	CreatedAt int64
	IsBusy    bool
	Id        int
	Args      *[]interface{}
	SetupFunc *SetupFunction
	NodeFile  string
	Separator string
	Timeout   int
	Manager   *JSManager
}
type JSResponse struct {
	Success bool                   `json:"success"`
	Data    map[string]interface{} `json:"data"`
	Error   string                 `json:"error"`
	GoError error                  `json:"-"`
}

func (jsProcess *JSProcess) Stop() error {
	jsProcess.Manager.Mu.Lock()
	defer jsProcess.Manager.Mu.Unlock()
	if jsProcess.Cmd != nil && jsProcess.Cmd.ProcessState != nil && !jsProcess.Cmd.ProcessState.Exited() {
		jsProcess.StdIn = nil
		jsProcess.StdOut = nil
		jsProcess.Cmd.Process.Signal(os.Interrupt)
		jsProcess.Cmd.Process.Kill()
		jsProcess.Cmd.Wait()

		jsProcess.Manager.Logger.Info().Msgf("[minerva] Stopped process %s with id %d (pid: %d)", jsProcess.Name, jsProcess.Id, jsProcess.Cmd.Process.Pid)
		jsProcess.Cmd.Process.Release()
		jsProcess.Cmd = nil
		jsProcess.IsBusy = true
	}

	return nil
}

func (jsProcess *JSProcess) Start() error {
	jsProcess.Manager.Mu.Lock()
	defer jsProcess.Manager.Mu.Unlock()
	if jsProcess.Cmd == nil {
		cmd := exec.Command(jsProcess.NodeCmd, "--experimental-default-type=module", jsProcess.NodeFile)
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		jsProcess.Cmd = cmd
		jsProcess.StdIn = bufio.NewWriter(stdin)
		jsProcess.StdOut = bufio.NewReader(stdout)
		jsProcess.IsBusy = false
	}

	if err := jsProcess.Cmd.Start(); err != nil {
		return err
	}
	jsProcess.Manager.Logger.Info().Msgf("[minerva] Started process %s with id %d (pid: %d)", jsProcess.Name, jsProcess.Id, jsProcess.Cmd.Process.Pid)

	return nil
}

func (jsProcess *JSProcess) Restart() error {
	if err := jsProcess.Stop(); err != nil {
		return err
	}

	if err := jsProcess.Start(); err != nil {
		return err
	}

	return nil
}

func (jsProcess *JSProcess) executeJSCodeInternal(jsCode string, resultChan chan JSResponse) {
	_, err := jsProcess.StdIn.WriteString(jsCode + "\n")
	if err != nil {
		jsProcess.Restart()

		resultChan <- JSResponse{
			Success: false,
			Data:    nil,
			Error:   "error writing to stdin",
			GoError: fmt.Errorf("[minerva] [%s] error writing to stdin: %s", jsProcess.Name, err.Error()),
		}
		return
	}
	jsProcess.StdIn.Flush()
	var buffer bytes.Buffer
	var tempBuffer bytes.Buffer

	for {
		chunk := make([]byte, 4096)
		n, err := jsProcess.StdOut.Read(chunk)
		if err != nil && err != io.EOF {
			resultChan <- JSResponse{
				Success: false,
				Data:    nil,
				Error:   "error reading from stdout",
				GoError: fmt.Errorf("[minerva] [%s] error reading from stdout: %s", jsProcess.Name, err.Error()),
			}
			return
		}
		tempBuffer.Write(chunk[:n])
		if strings.Contains(tempBuffer.String(), jsProcess.Separator) {
			parts := strings.Split(tempBuffer.String(), jsProcess.Separator)
			buffer.WriteString(parts[0])
			tempBuffer.Reset()
			if len(parts) > 1 {
				tempBuffer.WriteString(parts[1])
			}
			break
		} else if err == io.EOF {
			buffer.Write(tempBuffer.Bytes())
			break
		}
	}
	var response JSResponse
	err = json.Unmarshal(buffer.Bytes(), &response)
	if err != nil {
		resultChan <- JSResponse{
			Success: false,
			Data:    nil,
			Error:   "error unmarshalling response",
			GoError: fmt.Errorf("[minerva] [%s] error unmarshalling response: %s", jsProcess.Name, err.Error()),
		}
		return
	}

	resultChan <- response
}
