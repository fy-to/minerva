package minerva

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Process struct {
	cmd             *exec.Cmd
	isReady         int32
	isBusy          int32
	latency         int64
	mutex           sync.RWMutex
	logger          *zerolog.Logger
	stdin           *json.Encoder
	stdout          *bufio.Reader
	name            string
	cmdStr          string
	cmdArgs         []string
	timeout         int
	requestsHandled int
	restarts        int
	id              int
	cwd             string
	pool            *ProcessPool
}

type ProcessExport struct {
	IsReady         bool   `json:"IsReady"`
	Latency         int64  `json:"Latency"`
	InputQueue      int    `json:"InputQueue"`
	OutputQueue     int    `json:"OutputQueue"`
	Name            string `json:"Name"`
	Restarts        int    `json:"Restarts"`
	RequestsHandled int    `json:"RequestsHandled"`
}

// Start starts the process by creating a new exec.Cmd, setting up the stdin and stdout pipes, and starting the process.
func (p *Process) Start() {
	p.SetReady(0)
	_cmd := exec.Command(p.cmdStr, p.cmdArgs...)
	stdin, err := _cmd.StdinPipe()
	if err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to get stdin pipe for process", p.name)
		return
	}
	stdout, err := _cmd.StdoutPipe()
	if err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to get stdout pipe for process", p.name)
		return
	}

	p.mutex.Lock()
	p.cmd = _cmd
	p.stdin = json.NewEncoder(stdin)
	p.stdout = bufio.NewReader(stdout)
	p.mutex.Unlock()
	p.cmd.Dir = p.cwd
	go p.WaitForReadyScan()
	if err := p.cmd.Start(); err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to start process", p.name)
		return
	}
}

// Stop stops the process by sending a kill signal to the process and cleaning up the resources.
func (p *Process) Stop() {
	p.SetReady(0)
	p.mutex.Lock()
	p.mutex.Unlock()
	p.cmd.Process.Kill()
	p.cleanupChannelsAndResources()
	p.logger.Info().Msgf("[minerva|%s] Process stopped", p.name)
}

// cleanupChannelsAndResources closes the inputQueue and outputQueue channels and sets the cmd, stdin, stdout, ctx, cancel, and wg to nil.
func (p *Process) cleanupChannelsAndResources() {
	p.mutex.Lock()
	p.cmd = nil
	p.stdin = nil
	p.stdout = nil
	p.mutex.Unlock()
}

// Restart stops the process and starts it again.
func (p *Process) Restart() {
	p.logger.Info().Msgf("[minerva|%s] Restarting process", p.name)
	p.mutex.Lock()
	p.restarts = p.restarts + 1
	p.mutex.Unlock()
	p.Stop()
	if atomic.LoadInt32(&p.pool.shouldStop) == 0 {
		p.Start()
	}
}

// SetReady sets the readiness of the process.
// If ready is true, the process is marked as ready.
// If ready is false, the process is marked as not ready.
func (p *Process) SetReady(ready int32) {
	atomic.StoreInt32(&p.isReady, ready)
}

func (p *Process) IsReady() bool {
	return atomic.LoadInt32(&p.isReady) == 1
}

func (p *Process) IsBusy() bool {
	return atomic.LoadInt32(&p.isBusy) == 1
}

func (p *Process) SetBusy(busy int32) {
	atomic.StoreInt32(&p.isBusy, busy)
}

func (p *Process) WaitForReadyScan() {
	responseChan := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)
	go func() {
		for {
			line, err := p.stdout.ReadString('\n')
			if err != nil {
				errChan <- err
				return
			}
			if line == "" || line == "\n" {
				continue
			}

			var response map[string]interface{}
			if err := json.Unmarshal([]byte(line), &response); err != nil {
				p.logger.Warn().Msgf("[minerva|%s] Non JSON message received: '%s'", p.name, line)
				continue
			}
			switch response["type"] {
			case "ready":
				p.logger.Info().Msgf("[minerva|%s] Process is ready", p.name)
				responseChan <- response
				return
			}
		}
	}()
	select {
	case <-responseChan:
		p.SetReady(1)
		p.SetBusy(0)
		return
	case err := <-errChan:
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to read line", p.name)
		p.Restart()
	case <-time.After(20 * time.Second):
		p.logger.Error().Msgf("[minerva|%s] Communication timed out", p.name)
		p.Restart()
	}
}
func (p *Process) Communicate(cmd map[string]interface{}) (map[string]interface{}, error) {

	// Send command
	if err := p.stdin.Encode(cmd); err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to send command", p.name)
		p.Restart()
		return nil, err
	}

	// Log the command sent
	jsonCmd, _ := json.Marshal(cmd)
	p.logger.Debug().Msgf("[minerva|%s] Command sent: %v", p.name, string(jsonCmd))

	responseChan := make(chan map[string]interface{}, 1)
	errChan := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		defer func() {
			close(responseChan)
			close(errChan)
		}()
		p.logger.Debug().Msgf("[minerva|%s] Waiting for response", p.name)
		for {
			select {
			case <-ctx.Done():
				errChan <- errors.New("communication timed out")
				return
			default:
				line, err := p.stdout.ReadString('\n')
				if err != nil {
					p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to read line", p.name)
					errChan <- err
					return
				}
				if line == "" || line == "\n" {
					continue
				}

				var response map[string]interface{}
				if err := json.Unmarshal([]byte(line), &response); err != nil {
					p.logger.Warn().Msgf("[minerva|%s] Non JSON message received: '%s'", p.name, line)
					continue
				}

				// Check for matching response ID
				if response["type"] == "success" || response["type"] == "error" {
					id, ok := response["id"].(string)
					if ok && id == cmd["id"].(string) {
						responseChan <- response
						return
					}
				}
			}
		}
	}()

	select {
	case response := <-responseChan:
		return response, nil
	case err := <-errChan:
		p.logger.Error().Err(err).Msgf("[minerva|%s] Error during communication", p.name)
		p.Restart()
		return nil, err
	}
}

// SendCommand sends a command to the process and waits for the response.
func (p *Process) SendCommand(cmd map[string]interface{}) (map[string]interface{}, error) {
	p.SetBusy(1)
	defer p.SetBusy(0)

	if _, ok := cmd["id"]; !ok {
		cmd["id"] = uuid.New().String()
	}
	if _, ok := cmd["type"]; !ok {
		cmd["type"] = "main"
	}

	start := time.Now().UnixMilli()

	result, err := p.Communicate(cmd)
	if err != nil {
		return map[string]interface{}{"id": cmd["id"], "type": "error", "message": err.Error()}, nil
	}
	p.mutex.Lock()
	p.latency = time.Now().UnixMilli() - start
	p.requestsHandled = p.requestsHandled + 1
	p.mutex.Unlock()
	return result, nil
}
