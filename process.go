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
	cmd       *exec.Cmd
	stdin     *bufio.Writer
	stdout    *bufio.Reader
	name      string
	nodeCmd   string
	createdAt int64
	busy      bool
	id        int
	args      *[]interface{}
	setupFunc *SetupFunction
	nodeFile  string
	separator string
	timeout   int
}
type JSResponse struct {
	Success     bool                   `json:"success"`
	Data        map[string]interface{} `json:"data"`
	Error       string                 `json:"error"`
	ActualError error                  `json:"-"`
}

func (jsProcess *JSProcess) Stop() error {
	if jsProcess.cmd != nil && jsProcess.cmd.ProcessState != nil && !jsProcess.cmd.ProcessState.Exited() {
		jsProcess.stdin = nil
		jsProcess.stdout = nil
		jsProcess.cmd.Process.Signal(os.Interrupt)
		jsProcess.cmd.Process.Kill()
		jsProcess.cmd.Wait()
		jsProcess.cmd.Process.Release()
		jsProcess.cmd = nil
		jsProcess.busy = true
	}

	return nil
}

func (jsProcess *JSProcess) Start() error {
	if jsProcess.cmd == nil {
		cmd := exec.Command(jsProcess.nodeCmd, "--experimental-default-type=module", jsProcess.nodeFile)
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		jsProcess.cmd = cmd
		jsProcess.stdin = bufio.NewWriter(stdin)
		jsProcess.stdout = bufio.NewReader(stdout)
		jsProcess.busy = false
	}

	if err := jsProcess.cmd.Start(); err != nil {
		return err
	}
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
	_, err := jsProcess.stdin.WriteString(jsCode + "\n")
	if err != nil {
		jsProcess.Restart()

		resultChan <- JSResponse{
			Success:     false,
			Data:        nil,
			Error:       "error writing to stdin",
			ActualError: fmt.Errorf("[minerva] error writing to stdin: %s", err.Error()),
		}
		return
	}
	jsProcess.stdin.Flush()
	var buffer bytes.Buffer
	var tempBuffer bytes.Buffer

	for {
		chunk := make([]byte, 4096)
		n, err := jsProcess.stdout.Read(chunk)
		if err != nil && err != io.EOF {
			resultChan <- JSResponse{
				Success:     false,
				Data:        nil,
				Error:       "error reading from stdout",
				ActualError: fmt.Errorf("[minerva] error reading from stdout: %s", err.Error()),
			}
			return
		}
		tempBuffer.Write(chunk[:n])
		if strings.Contains(tempBuffer.String(), jsProcess.separator) {
			parts := strings.Split(tempBuffer.String(), jsProcess.separator)
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
			Success:     false,
			Data:        nil,
			Error:       "error unmarshalling response",
			ActualError: fmt.Errorf("[minerva] error unmarshalling response: %s", err.Error()),
		}
		return
	}

	resultChan <- response
}
