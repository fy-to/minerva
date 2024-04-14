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
	cwd             string
	pool            *ProcessPool
	restarting      int32
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
	if atomic.LoadInt32(&p.pool.shouldStop) == 1 {
		return
	}
	p.SetReady(0)
	p.SetRestarting(0)
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
	p.cmd.Dir = p.cwd
	if err := p.cmd.Start(); err != nil {
		p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to start process", p.name)
		return
	}

	p.logger.Info().Msgf("[minerva|%s] Process started", p.name)
	p.wg.Add(2)
	go func() {
		p.runReader()
		p.logger.Debug().Msgf("[minerva|%s] Reader stopped", p.name)
		p.wg.Done()
	}()

	go func() {
		p.runWriter()
		p.logger.Debug().Msgf("[minerva|%s] Writer stopped", p.name)

		p.wg.Done()
	}()
}

// Stop stops the process by sending a kill signal to the process and cleaning up the resources.
func (p *Process) Stop() {
	p.SetReady(0)

	p.cancel()

	done := make(chan bool)
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cleanupChannelsAndResources()
		p.logger.Info().Msgf("[minerva|%s] Process stopped", p.name)
	case <-time.After(1 * time.Second): // Set to whatever is reasonable
		p.logger.Error().Msgf("[minerva|%s] Timeout waiting for goroutines to finish", p.name)
		if p.cmd != nil && p.cmd.Process != nil {
			p.cmd.Process.Kill()
		}
		p.cleanupChannelsAndResources()
	}
}

func (p *Process) HardStop() {
	p.SetReady(0)
	p.cancel()
	p.cmd.Process.Kill()
	p.logger.Info().Msgf("[minerva|%s] Process stopped", p.name)
}

// cleanupChannelsAndResources closes the inputQueue and outputQueue channels and sets the cmd, stdin, stdout, ctx, cancel, and wg to nil.
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

// Restart stops the process and starts it again.
func (p *Process) Restart() {
	if atomic.LoadInt32(&p.restarting) == 1 {
		return
	}
	p.SetRestarting(1)
	p.mutex.Lock()
	p.restarts = p.restarts + 1
	p.mutex.Unlock()
	p.Stop()
	if atomic.LoadInt32(&p.pool.shouldStop) == 1 {
		return
	}
	p.logger.Info().Msgf("[minerva|%s] Restarting process", p.name)

	p.Start()
}

// SetReady sets the readiness of the process.
// If ready is true, the process is marked as ready.
// If ready is false, the process is marked as not ready.
func (p *Process) SetReady(ready int32) {
	atomic.StoreInt32(&p.isReady, ready)
}

func (p *Process) SetRestarting(restarting int32) {
	atomic.StoreInt32(&p.restarting, restarting)
}

// IsReady returns true if the process is ready, false otherwise.
func (p *Process) runWriter() {
	p.logger.Info().Msgf("[minerva|%s] Writer started", p.name)
	defer p.logger.Info().Msgf("[minerva|%s] Writer stopped", p.name)
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug().Msgf("[minerva|%s] Writer stopping: context done", p.name)
			return
		case <-p.pool.stop:
			p.logger.Debug().Msgf("[minerva|%s] Writer stopping: pool stopped (pool)", p.name)
			return
		case cmd, ok := <-p.inputQueue:
			if !ok {
				p.logger.Debug().Msgf("[minerva|%s] Input queue closed", p.name)
				p.Restart()
				return
			}
			if err := p.stdin.Encode(cmd); err != nil {
				p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to send command", p.name)
				select {
				case p.outputQueue <- map[string]interface{}{"id": cmd["id"], "type": "error", "message": "failed to send command"}:
				case <-p.ctx.Done():
					p.logger.Debug().Msgf("[minerva|%s] Context cancelled while handling error", p.name)
					return
				}
				continue
			}
			p.logger.Debug().Msgf("[minerva|%s] Command sent", p.name)
		}
	}
}

func (p *Process) runReader() {
	p.logger.Info().Msgf("[minerva|%s] Reader started", p.name)
	defer p.logger.Info().Msgf("[minerva|%s] Reader stopped", p.name)
	outputChan := make(chan string)
	go func() {
		defer close(outputChan)
		for {
			line, err := p.stdout.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					p.logger.Debug().Msgf("[minerva|%s] EOF reached", p.name)
				} else {
					p.logger.Error().Err(err).Msgf("[minerva|%s] Failed to read line", p.name)
					time.Sleep(time.Second)
					p.Restart()
				}
				return
			}
			outputChan <- line
		}
	}()
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug().Msgf("[minerva|%s] Context done, Reader stopping", p.name)
			return
		case line, ok := <-outputChan:
			if !ok {
				p.logger.Debug().Msgf("[minerva|%s] Output channel closed", p.name)
				p.Restart()
				return
			}
			if line == "" || line == "\n" {
				continue
			}
			var msg map[string]interface{}
			if err := json.Unmarshal([]byte(line), &msg); err != nil {
				p.logger.Warn().Msgf("[minerva|%s] Non JSON message received: '%s'", p.name, line)
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

// SendCommand sends a command to the process and waits for the response.
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
	}
	return map[string]interface{}{"id": cmd["id"], "type": "error", "message": "unknown error"}, nil
}
