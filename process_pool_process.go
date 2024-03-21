package minerva

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Process struct {
	cmd          *exec.Cmd
	inputQueue   chan map[string]interface{}
	outputQueue  chan map[string]interface{}
	isReady      int32
	latency      int64
	mutex        sync.RWMutex
	logger       *zerolog.Logger
	stdin        *json.Encoder
	stdout       *bufio.Scanner
	name         string
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	cmdStr       string
	cmdArgs      []string
	timeout      int
	waitResponse sync.Map
}

type ProcessExport struct {
	IsReady     bool  `json:"isReady"`
	Latency     int64 `json:"latency"`
	InputQueue  int   `json:"inputQueue"`
	OutputQueue int   `json:"outputQueue"`
	Name        string
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
	p.cmd = _cmd
	p.stdin = json.NewEncoder(stdin)
	p.stdout = bufio.NewScanner(stdout)
	p.inputQueue = make(chan map[string]interface{}, 100)
	p.outputQueue = make(chan map[string]interface{}, 100)
	p.ctx = ctx
	p.cancel = cancel
	p.wg = sync.WaitGroup{}
	p.waitResponse = sync.Map{}
	p.wg.Add(2)
	p.mutex.Unlock()

	if err := p.cmd.Start(); err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to start process", p.name)
		return
	}

	p.logger.Info().Msgf("[minerva|%s] Process started", p.name)

	go func() {
		p.runReader()
		p.wg.Done()
	}()
	go func() {
		p.runWriter()
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

	p.SetReady(0)
	p.mutex.Lock()
	p.cancel()
	p.mutex.Unlock()
	p.wg.Wait()
	p.mutex.Lock()
	close(p.inputQueue)
	close(p.outputQueue)
	p.mutex.Unlock()
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
	p.cmd.Process.Release()

	p.mutex.Lock()
	p.stdin = nil
	p.stdout = nil
	p.cmd = nil
	p.mutex.Unlock()
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
				p.Restart()
				continue
			}
			id := cmd["id"].(string)
			ch, _ := p.waitResponse.Load(id)
			<-ch.(chan bool)
			p.waitResponse.Delete(id)
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
				p.SetReady(1)
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
// It takes a map of command parameters as input and returns the response as a map,
// along with an error if any.
// If the process is stopped before receiving a response, it returns an error with the message "process stopped".
// If the command times out after 3 seconds, it returns an error with the message "command timeout".
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
		return nil, errors.New("process stopped")
	case resp := <-p.outputQueue:
		if resp["id"] == cmd["id"] {
			p.mutex.Lock()
			p.latency = time.Now().UnixMilli() - start
			p.mutex.Unlock()
			return resp, nil
		}
		return nil, errors.New("invalid response")
	case <-time.After(time.Duration(p.timeout) * time.Second):
		return nil, errors.New("command timeout")
	}
}
