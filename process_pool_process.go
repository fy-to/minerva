package minerva

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Process struct {
	cmd             *exec.Cmd
	inputQueue      chan map[string]interface{}
	outputQueue     chan map[string]interface{}
	isReady         int32
	latency         int64
	mutex           sync.RWMutex
	logger          *zerolog.Logger
	stdin           *json.Encoder
	stdout          *bufio.Reader
	name            string
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	cmdStr          string
	cmdArgs         []string
	timeout         int
	waitResponse    sync.Map
	requestsHandled int
	restarts        int
	id              int
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

// Start starts the process.
// If the process is already running, it logs a warning and returns.
// It sets the process as not ready, creates a new context with cancelation function,
// and starts the process command.
// If there is an error starting the process, it logs an error and returns.
// It logs an info message when the process is successfully started.
// It spawns three goroutines to run the reader, writer, and heartbeat functions.
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

	ctx, cancel := context.WithCancel(context.Background())
	p.mutex.Lock()
	p.ctx = ctx
	p.cancel = cancel
	p.cmd = _cmd
	p.stdin = json.NewEncoder(stdin)
	p.stdout = bufio.NewReader(stdout)
	p.inputQueue = make(chan map[string]interface{}, 100)
	p.outputQueue = make(chan map[string]interface{}, 100)
	p.wg = sync.WaitGroup{}
	p.waitResponse = sync.Map{}
	p.mutex.Unlock()

	if err := p.cmd.Start(); err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to start process", p.name)
		return
	}

	p.logger.Info().Msgf("[minerva|%s] Process started", p.name)
	p.wg.Add(2)
	go func() {
		p.runReader()
		p.wg.Done()
		p.logger.Debug().Msgf("[minerva|%s] Reader stopped", p.name)
	}()

	go func() {
		p.runWriter()
		p.wg.Done()
		p.logger.Debug().Msgf("[minerva|%s] Writer stopped", p.name)
	}()

}

func (p *Process) Stop() {
	// Signal the context to cancel any ongoing operations
	p.SetReady(0)
	p.mutex.Lock()
	p.cancel()
	p.mutex.Unlock()
	p.wg.Wait()
	p.cmd.Process.Kill()
	p.cleanupChannelsAndResources()
	p.logger.Info().Msgf("[minerva|%s] Process stopped", p.name)
}

func (p *Process) cleanupChannelsAndResources() {
	p.mutex.Lock()
	close(p.inputQueue)
	close(p.outputQueue)
	p.cmd = nil
	p.stdin = nil
	p.stdout = nil
	p.ctx = nil
	p.cancel = nil
	p.wg = sync.WaitGroup{}
	p.waitResponse = sync.Map{}
	p.mutex.Unlock()
}

// Restart restarts the process by stopping it and then starting it again.
func (p *Process) Restart() {
	p.logger.Info().Msgf("[minerva|%s] Restarting process", p.name)
	p.mutex.Lock()
	p.restarts = p.restarts + 1
	p.mutex.Unlock()
	p.Stop()
	p.Start()
}

// SetReady sets the readiness of the process.
// If ready is true, the process is marked as ready.
// If ready is false, the process is marked as not ready.
func (p *Process) SetReady(ready int32) {
	atomic.StoreInt32(&p.isReady, ready)
}

// runWriter is a goroutine that continuously reads commands from the inputQueue and sends them to the process's stdin.
// It runs until the process is stopped or an error occurs while sending a command.
func (p *Process) runWriter() {
	for {
		select {
		case <-p.ctx.Done():
			// Process stopped

			return
		case cmd := <-p.inputQueue:
			// Send command
			if err := p.stdin.Encode(cmd); err != nil {
				p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to send command", p.name)
				continue
			}
			id := cmd["id"].(string)
			ch, _ := p.waitResponse.Load(id)
			select {
			case <-ch.(chan bool):
				p.waitResponse.Delete(id)
			case <-time.After(time.Duration(p.timeout) * time.Second):
				p.logger.Error().Msgf("[minerva|%s] Command timed out", p.name)
				p.outputQueue <- map[string]interface{}{"id": id, "type": "error", "message": "command timeout"}
				p.waitResponse.Delete(id)
			}

		}
	}
}

func (p *Process) runReader() {
	outputChan := make(chan string)
	go func() {
		defer close(outputChan)
		for {
			line, err := p.stdout.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to read line", p.name)
				}
				return
			} else {
				outputChan <- line
			}
		}
	}()
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug().Msgf("[minerva|%s] Context done", p.name)
			return
		case line, ok := <-outputChan:
			if !ok {
				p.logger.Debug().Msgf("[minerva|%s] Output channel closed", p.name)
				return
			}
			if line == "" {
				continue
			}

			var msg map[string]interface{}
			if err := json.Unmarshal([]byte(line), &msg); err != nil {
				p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to parse message", p.name)
				continue
			}

			if msg["type"] == "ready" {
				p.logger.Info().Msgf("[minerva|%s] Process is ready", p.name)
				p.SetReady(1)
				continue
			}

			if msg["type"] == "success" || msg["type"] == "error" {
				id := msg["id"].(string)
				if ch, ok := p.waitResponse.Load(id); ok {
					ch.(chan bool) <- true
				}
				p.outputQueue <- msg
			}
		}
	}
}

// SendCommand sends a command to the process and waits for a response.
func (p *Process) SendCommand(cmd map[string]interface{}) (map[string]interface{}, error) {
	if _, ok := cmd["id"]; !ok {
		cmd["id"] = uuid.New().String()
	}
	if _, ok := cmd["type"]; !ok {
		cmd["type"] = "main"
	}

	ch := make(chan bool, 1)
	p.waitResponse.Store(cmd["id"].(string), ch)

	p.inputQueue <- cmd
	start := time.Now().UnixMilli()

	select {
	case <-p.ctx.Done():
		return map[string]interface{}{"id": cmd["id"], "type": "error", "message": "process stopped"}, nil
	case resp := <-p.outputQueue:
		if resp["id"] == cmd["id"] {
			p.mutex.Lock()
			p.latency = time.Now().UnixMilli() - start
			p.requestsHandled = p.requestsHandled + 1
			p.mutex.Unlock()

			if resp["type"] == "error" && resp["message"] == "command timeout" {
				p.Restart()
			}

			return resp, nil
		}
		return map[string]interface{}{"id": cmd["id"], "type": "error", "message": "invalid response"}, nil
	}
}
