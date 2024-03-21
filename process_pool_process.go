package minerva

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Process struct {
	cmd           *exec.Cmd
	inputQueue    chan map[string]interface{}
	outputQueue   chan map[string]interface{}
	isReady       bool
	lastHeartbeat time.Time
	latency       int64
	mutex         sync.RWMutex
	logger        *zerolog.Logger
	stdin         *json.Encoder
	stdout        *bufio.Scanner
	name          string
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

type ProcessExport struct {
	IsReady       bool  `json:"isReady"`
	LastHeartbeat int64 `json:"lastHeartbeat"`
	Latency       int64 `json:"latency"`
	InputQueue    int   `json:"inputQueue"`
	OutputQueue   int   `json:"outputQueue"`
	Name          string
}

// Start starts the process.
// If the process is already running, it logs a warning and returns.
// It sets the process as not ready, creates a new context with cancelation function,
// and starts the process command.
// If there is an error starting the process, it logs an error and returns.
// It logs an info message when the process is successfully started.
// It spawns three goroutines to run the reader, writer, and heartbeat functions.
func (p *Process) Start() {
	if p.cmd != nil && p.cmd.Process != nil {
		p.logger.Warn().Msgf("[minerva|%s] Process already running", p.name)
		return
	}
	p.SetReady(false)
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel
	p.wg = sync.WaitGroup{}

	if err := p.cmd.Start(); err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to start process", p.name)
		return
	}

	p.logger.Info().Msgf("[minerva|%s] Process started", p.name)
	p.wg.Add(3)
	go func() {
		p.runReader()
		p.wg.Done()
	}()
	go func() {
		p.runWriter()
		p.wg.Done()
	}()
	go func() {
		go p.runHeartbeat()
		p.wg.Done()
	}()
}

// Stop stops the process.
// It sets the process as not ready, acquires the mutex lock, cancels the context,
// waits for the wait group to complete, closes the input and output queues,
// sends a SIGINT signal to the process, and if it doesn't stop within 1 second,
// kills the process. Finally, it sets the cmd, stdin, and stdout to nil.
// It logs a message indicating that the process has stopped.
func (p *Process) Stop() {

	p.SetReady(false)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cancel()
	p.wg.Wait()
	close(p.inputQueue)
	close(p.outputQueue)
	if p.cmd.Process != nil {
		p.cmd.Process.Signal(syscall.SIGINT)
		select {
		case <-time.After(1 * time.Second):
			if p.cmd.Process != nil {
				p.cmd.Process.Kill()
			}
		case <-p.ctx.Done():
			// Process stopped
		}
	}
	p.cmd = nil
	p.stdin = nil
	p.stdout = nil
	p.logger.Info().Msgf("[minerva|%s] Process stopped", p.name)
}

// Restart restarts the process by stopping it and then starting it again.
func (p *Process) Restart() {
	p.Stop()
	p.Start()
}

// SetReady sets the readiness of the process.
// If ready is true, the process is marked as ready.
// If ready is false, the process is marked as not ready.
func (p *Process) SetReady(ready bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.isReady = ready
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
			if err := p.stdin.Encode(cmd); err != nil {
				p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to send command", p.name)
				p.Restart()
			}
		}
	}
}

// runReader is a method of the Process struct that reads the output from the process's stdout.
// It continuously scans the stdout for lines of text, parses them as JSON messages, and handles them accordingly.
// If a "ready" message is received, it sets the process as ready.
// If a "success" or "error" message is received, it sends the message to the outputQueue channel.
// The method stops running when the process is stopped or when there is no more output to read.
func (p *Process) runReader() {
	const maxBufferSize = 45 * 1024 * 1024
	buf := make([]byte, 4096)
	p.stdout.Buffer(buf, maxBufferSize)

	for {
		select {
		case <-p.ctx.Done():
			// Process stopped
			return
		default:
			if !p.stdout.Scan() {
				return
			}
			line := p.stdout.Text()
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
				p.SetReady(true)
			}

			if msg["type"] == "success" || msg["type"] == "error" {
				p.outputQueue <- msg
			}
		}
	}
}

// runHeartbeat is a method that runs the heartbeat functionality for a Process.
// It sends a heartbeat command to the inputQueue and waits for a response from the outputQueue.
// If a response is received within the timeout period, it updates the latency of the Process.
// If no response is received within the timeout period, it logs an error and restarts the Process.
func (p *Process) runHeartbeat() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			p.mutex.RLock()
			if p.isReady {
				p.lastHeartbeat = time.Now()
				startTime := time.Now().UnixMilli()
				cmd := map[string]interface{}{"type": "heartbeat", "id": uuid.New().String()}
				_, err := p.SendCommand(cmd)
				if err != nil {
					p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to send heartbeat", p.name)
					p.Restart()
				} else {
					endTime := time.Now().UnixMilli()
					p.latency = endTime - startTime
					p.logger.Debug().Msgf("[minerva|%s] Latency: %dms", p.name, p.latency)
				}

			}
			p.mutex.RUnlock()
			time.Sleep(15 * time.Second)
		}
	}
}

// SendCommand sends a command to the process and waits for a response.
// It takes a map of command parameters as input and returns the response as a map,
// along with an error if any.
// If the process is stopped before receiving a response, it returns an error with the message "process stopped".
// If the command times out after 3 seconds, it returns an error with the message "command timeout".
func (p *Process) SendCommand(cmd map[string]interface{}) (map[string]interface{}, error) {
	p.inputQueue <- cmd
	for {
		select {
		case <-p.ctx.Done():
			return nil, errors.New("process stopped")
		case resp := <-p.outputQueue:
			if resp["id"] == cmd["id"] {
				return resp, nil
			}
		case <-time.After(3 * time.Second):
			return nil, errors.New("command timeout")
		}
	}
}
