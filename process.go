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

// Stop stops the JSProcess by sending an interrupt signal and killing the process.
// It releases the process resources and sets the Cmd to nil.
// If the process is already stopped or has exited, it does nothing.
// It returns an error if there was an issue stopping the process.
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
		jsProcess.Cmd = nil
		jsProcess.IsBusy = true
	}

	return nil
}

// Start starts the JSProcess by executing the specified Node.js command with the given file.
// It acquires a lock on the JSProcess manager, sets up the command, pipes for stdin and stdout,
// and starts the command. If an error occurs during the process startup, it logs the error and returns it.
// Otherwise, it returns nil.
func (jsProcess *JSProcess) Start() error {
	jsProcess.Manager.Mu.Lock()
	defer jsProcess.Manager.Mu.Unlock()
	if jsProcess.Cmd == nil || jsProcess.Cmd.ProcessState == nil || jsProcess.Cmd.ProcessState.Exited() {
		cmd := exec.Command(jsProcess.NodeCmd, "--experimental-default-type=module", jsProcess.NodeFile)
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		jsProcess.Cmd = cmd
		jsProcess.StdIn = bufio.NewWriter(stdin)
		jsProcess.StdOut = bufio.NewReader(stdout)
		jsProcess.IsBusy = false

		if err := jsProcess.Cmd.Start(); err != nil {
			jsProcess.Manager.Logger.Error().Msgf("[minerva|%s] Error starting process with id %d: %s", jsProcess.Name, jsProcess.Id, err.Error())
			return err
		}
		jsProcess.Manager.Logger.Info().Msgf("[minerva|%s] Started process with id %d (pid: %d)", jsProcess.Name, jsProcess.Id, jsProcess.Cmd.Process.Pid)
	} else {
		jsProcess.Manager.Logger.Info().Msgf("[minerva|%s] Process with id %d is already running", jsProcess.Name, jsProcess.Id)
	}

	return nil
}

// Restart restarts the JSProcess by first stopping it and then starting it again.
// If an error occurs during the stop or start process, it will be returned.
// Returns nil if the restart is successful.
func (jsProcess *JSProcess) Restart() error {
	jsProcess.Manager.Logger.Info().Msgf("[minerva|%s] Restarting process with id %d", jsProcess.Name, jsProcess.Id)
	jsProcess.Stop()

	if err := jsProcess.Start(); err != nil {
		return err
	}

	return nil
}

// executeJSCodeInternal executes the given JavaScript code in the JSProcess instance and sends the result through the resultChan channel.
// If there is an error writing to stdin or reading from stdout, the JSResponse with the corresponding error message is sent.
// The function reads the output from stdout until it finds the separator defined in the JSProcess instance.
// It then splits the output into two parts, appends the first part to the buffer, and stores the second part in the tempBuffer.
// If the end of the output is reached (EOF), the tempBuffer is appended to the buffer.
// The buffer is then unmarshalled into a JSResponse struct and sent through the resultChan channel.
func (jsProcess *JSProcess) executeJSCodeInternal(jsCode string, resultChan chan JSResponse) {
	_, err := jsProcess.StdIn.WriteString(jsCode + "\n")
	if err != nil {
		jsProcess.Restart()

		resultChan <- JSResponse{
			Success: false,
			Data:    nil,
			Error:   "error writing to stdin",
			GoError: fmt.Errorf("[minerva|%s] error writing to stdin: %s", jsProcess.Name, err.Error()),
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
				GoError: fmt.Errorf("[minerva|%s] error reading from stdout: %s", jsProcess.Name, err.Error()),
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
		go jsProcess.Restart()
		resultChan <- JSResponse{
			Success: false,
			Data:    nil,
			Error:   "error unmarshalling response",
			GoError: fmt.Errorf("[minerva|%s] error unmarshalling response: %s", jsProcess.Name, err.Error()),
		}
		return
	}

	resultChan <- response
}
