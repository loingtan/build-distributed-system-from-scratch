// Supervisor: runs as the main process of a node's container.
//
// It starts the node process, relays its stdin/stdout/stderr as JSON lines,
// and SIGKILLs it on command, so the runner can crash and restart a node in
// milliseconds without recreating the container. It can also SIGSTOP and
// SIGCONT it, to model a process pause.
//
//	runner -> supervisor  {"op":"start","inc":1,"argv":[...],"env":{...}}
//	                      {"op":"in","inc":1,"line":"..."}
//	                      {"op":"kill","inc":1}
//	                      {"op":"pause","inc":1}   SIGSTOP the process group
//	                      {"op":"resume","inc":1}  SIGCONT it
//	supervisor -> runner  {"ev":"out"|"err","inc":1,"line":"..."}
//	                      {"ev":"exit","inc":1,"code":0,"signal":""}
//	                      {"ev":"killed","inc":1}
package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

type command struct {
	Op   string            `json:"op"`
	Inc  int               `json:"inc"`
	Argv []string          `json:"argv"`
	Env  map[string]string `json:"env"`
	Line string            `json:"line"`
}

type event struct {
	Ev     string `json:"ev"`
	Inc    int    `json:"inc"`
	Line   string `json:"line,omitempty"`
	Code   int    `json:"code"`
	Signal string `json:"signal,omitempty"`
}

type proc struct {
	inc   int
	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  chan struct{}

	// Lines for the process's stdin. A goroutine writes them, so a stopped
	// process whose pipe is full never blocks the command loop.
	mu    sync.Mutex
	cond  *sync.Cond
	lines []string
	shut  bool
}

func (p *proc) push(line string) {
	p.mu.Lock()
	p.lines = append(p.lines, line)
	p.mu.Unlock()
	p.cond.Signal()
}

func (p *proc) writer() {
	for {
		p.mu.Lock()
		for len(p.lines) == 0 && !p.shut {
			p.cond.Wait()
		}
		if p.shut {
			p.mu.Unlock()
			return
		}
		batch := p.lines
		p.lines = nil
		p.mu.Unlock()
		for _, l := range batch {
			if _, err := io.WriteString(p.stdin, l+"\n"); err != nil {
				break
			}
		}
	}
}

func (p *proc) stop() {
	p.mu.Lock()
	p.shut = true
	p.mu.Unlock()
	p.cond.Broadcast()
}

var (
	outMu sync.Mutex
	enc   = json.NewEncoder(os.Stdout)
)

func emit(e event) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = enc.Encode(e)
}

func relay(r io.Reader, kind string, inc int, wg *sync.WaitGroup) {
	defer wg.Done()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		emit(event{Ev: kind, Inc: inc, Line: sc.Text()})
	}
}

func start(c command) *proc {
	if len(c.Argv) == 0 {
		emit(event{Ev: "err", Inc: c.Inc, Line: "supervisor: empty argv"})
		return nil
	}
	cmd := exec.Command(c.Argv[0], c.Argv[1:]...)
	cmd.Env = os.Environ()
	for k, v := range c.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	p := &proc{inc: c.Inc, cmd: cmd, stdin: stdin, done: make(chan struct{})}
	p.cond = sync.NewCond(&p.mu)
	if err := cmd.Start(); err != nil {
		emit(event{Ev: "err", Inc: c.Inc, Line: "supervisor: " + err.Error()})
		emit(event{Ev: "exit", Inc: c.Inc, Code: 127})
		close(p.done)
		return p
	}
	go p.writer()
	var wg sync.WaitGroup
	wg.Add(2)
	go relay(stdout, "out", c.Inc, &wg)
	go relay(stderr, "err", c.Inc, &wg)
	go func() {
		wg.Wait()
		err := cmd.Wait()
		e := event{Ev: "exit", Inc: c.Inc}
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
					e.Signal = ws.Signal().String()
				} else {
					e.Code = ee.ExitCode()
				}
			} else {
				e.Code = 1
			}
		}
		emit(e)
		close(p.done)
	}()
	return p
}

func kill(p *proc) {
	if p == nil || p.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	<-p.done
	p.stop()
}

func signal(p *proc, sig syscall.Signal) {
	if p == nil || p.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-p.cmd.Process.Pid, sig)
}

func main() {
	var current *proc
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var c command
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			continue
		}
		switch c.Op {
		case "start":
			kill(current)
			current = start(c)
		case "in":
			if current != nil && current.inc == c.Inc {
				current.push(c.Line)
			}
		case "pause":
			if current != nil && current.inc == c.Inc {
				signal(current, syscall.SIGSTOP)
			}
		case "resume":
			if current != nil && current.inc == c.Inc {
				signal(current, syscall.SIGCONT)
			}
		case "kill":
			if current != nil && current.inc == c.Inc {
				kill(current)
				current = nil
			}
			emit(event{Ev: "killed", Inc: c.Inc})
		}
	}
	kill(current)
}
