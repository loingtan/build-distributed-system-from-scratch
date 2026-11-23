// Stragglers and backup tasks. Read the brief: npm run lab -- brief mr-stragglers
//
// MapReduce that survives worker crashes, from the previous task. A worker
// that is alive but slow keeps sending heartbeats, so its task is never
// re-run, and the whole job waits for it.
//
// TODO: backup tasks. When a worker asks for work and no task of the phase
// is idle, give it a copy of a task still running on exactly one other
// worker. Keep a list of workers per task; the first completion wins.
package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"quorumlab"
)

const (
	coord = "n1"
	limit = 25                     // fs accepts at most 25 lines per write or append
	poll  = 100 * time.Millisecond // an idle worker asks for work this often

	heartbeat = 100 * time.Millisecond // a busy worker says "still alive" this often
	silent    = 500 * time.Millisecond // a worker not heard from for this long is presumed dead
)

// ------------------------------------------------------- the job (given)

// hash is FNV-1a, 32 bits. A key belongs to reduce partition hash(key) % R.
func hash(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

type pair struct{ key, value string }

// Word count: map emits (word, "1") for every word; reduce adds them up.
func mapFn(line string) []pair {
	var out []pair
	for _, w := range strings.Fields(line) {
		out = append(out, pair{w, "1"})
	}
	return out
}

func reduceFn(key string, values []string) string {
	total := 0
	for _, v := range values {
		n, _ := strconv.Atoi(v)
		total += n
	}
	return key + " " + strconv.Itoa(total)
}

// ------------------------------------------------------ fs helpers (given)

func fsCall(ctx context.Context, n *quorumlab.Node, body map[string]any) (quorumlab.Message, error) {
	res, err := n.Call(ctx, "fs", body, 3*time.Second)
	if err != nil {
		return res, fmt.Errorf("fs %v: %w", body["type"], err)
	}
	if res.IsError() {
		return res, fmt.Errorf("fs %v failed: %s", body["type"], string(res.Body))
	}
	return res, nil
}

func readFile(ctx context.Context, n *quorumlab.Node, name string) ([]string, error) {
	res, err := fsCall(ctx, n, map[string]any{"type": "read", "name": name})
	if err != nil {
		return nil, err
	}
	var body struct {
		Data []string `json:"data"`
	}
	err = res.Decode(&body)
	return body.Data, err
}

// writeFile writes a file longer than limit lines as one write and then appends.
func writeFile(ctx context.Context, n *quorumlab.Node, name string, lines []string) error {
	first := lines[:min(limit, len(lines))]
	if _, err := fsCall(ctx, n, map[string]any{"type": "write", "name": name, "data": nonNil(first)}); err != nil {
		return err
	}
	for i := limit; i < len(lines); i += limit {
		chunk := lines[i:min(i+limit, len(lines))]
		if _, err := fsCall(ctx, n, map[string]any{"type": "append", "name": name, "data": chunk}); err != nil {
			return err
		}
	}
	return nil
}

// nonNil makes an empty file encode as [] rather than null.
func nonNil(lines []string) []string {
	if lines == nil {
		return []string{}
	}
	return lines
}

// commitFile writes the whole file under a name no other attempt uses, then
// renames it onto name. Readers see either nothing or the complete file.
func commitFile(ctx context.Context, n *quorumlab.Node, name string, lines []string) error {
	tmp := fmt.Sprintf("tmp-%s-%d", n.ID(), int64(n.Rand()*1e12))
	if err := writeFile(ctx, n, tmp, lines); err != nil {
		return err
	}
	_, err := fsCall(ctx, n, map[string]any{"type": "rename", "from": tmp, "to": name})
	return err
}

// task is what the coordinator hands to a worker.
type task struct {
	Kind    string `json:"kind"` // "map" or "reduce"
	ID      int    `json:"id"`
	Input   string `json:"input,omitempty"`   // map: the split to read
	Reduces int    `json:"reduces,omitempty"` // map: number of reduce partitions
	Maps    int    `json:"maps,omitempty"`    // reduce: number of map tasks
}

// runAll runs fns concurrently and returns the first error.
func runAll(fns []func() error) error {
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, f := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = f()
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// runMap reads one input split and partitions its pairs by key into R
// intermediate files mr-<id>-<r>, one line "key value" per pair.
func runMap(ctx context.Context, n *quorumlab.Node, t task) error {
	lines, err := readFile(ctx, n, t.Input)
	if err != nil {
		return err
	}
	buckets := make([][]string, t.Reduces)
	for _, line := range lines {
		for _, p := range mapFn(line) {
			r := hash(p.key) % uint32(t.Reduces)
			buckets[r] = append(buckets[r], p.key+" "+p.value)
		}
	}
	var fns []func() error
	for r, b := range buckets {
		name := fmt.Sprintf("mr-%d-%d", t.ID, r)
		fns = append(fns, func() error { return commitFile(ctx, n, name, b) })
	}
	return runAll(fns)
}

// runReduce reads partition id of every map task's output, groups by key,
// and writes one line per key, sorted by key, to out-<id>.
func runReduce(ctx context.Context, n *quorumlab.Node, t task) error {
	parts := make([][]string, t.Maps)
	var fns []func() error
	for m := range t.Maps {
		fns = append(fns, func() error {
			lines, err := readFile(ctx, n, fmt.Sprintf("mr-%d-%d", m, t.ID))
			parts[m] = lines
			return err
		})
	}
	if err := runAll(fns); err != nil {
		return err
	}
	groups := map[string][]string{}
	for _, lines := range parts {
		for _, line := range lines {
			k, v, _ := strings.Cut(line, " ")
			groups[k] = append(groups[k], v)
		}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = reduceFn(k, groups[k])
	}
	return commitFile(ctx, n, fmt.Sprintf("out-%d", t.ID), out)
}

// ------------------------------------------------------------ coordinator

type taskState struct {
	task
	state  string // "idle", "running" or "done"
	worker string // who is running it
}

type jobState struct {
	msg     quorumlab.Message
	phase   string // "map", "reduce" or "done"
	maps    []*taskState
	reduces []*taskState
}

func (j *jobState) phaseTasks() []*taskState {
	if j.phase == "map" {
		return j.maps
	}
	return j.reduces
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	var job *jobState
	lastHeard := map[string]time.Time{} // worker -> when we last heard from it

	// release puts back to idle any task we thought worker was running: a
	// worker asking for work holds no task (it may have crashed and restarted).
	// Call with mu held.
	release := func(worker string) {
		if job == nil {
			return
		}
		for _, t := range append(append([]*taskState{}, job.maps...), job.reduces...) {
			if t.state == "running" && t.worker == worker {
				t.state = "idle"
			}
		}
	}

	n.Handle("job", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Inputs  []string `json:"inputs"`
			Reduces int      `json:"reduces"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		j := &jobState{msg: msg, phase: "map"}
		for id, in := range req.Inputs {
			j.maps = append(j.maps, &taskState{task: task{Kind: "map", ID: id, Input: in, Reduces: req.Reduces}, state: "idle"})
		}
		for id := range req.Reduces {
			j.reduces = append(j.reduces, &taskState{task: task{Kind: "reduce", ID: id, Maps: len(req.Inputs)}, state: "idle"})
		}
		mu.Lock()
		job = j
		mu.Unlock()
		return nil
	})

	n.Handle("get_task", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		lastHeard[msg.Src] = time.Now()
		release(msg.Src)
		// Reduce tasks are handed out only once every map task is done: a
		// reduce needs a piece of every map task's output.
		if job != nil && job.phase != "done" {
			for _, t := range job.phaseTasks() {
				if t.state == "idle" {
					t.state = "running"
					t.worker = msg.Src
					return n.Reply(msg, map[string]any{"type": "get_task_ok", "task": t.task})
				}
			}
		}
		return n.Reply(msg, map[string]any{"type": "get_task_ok", "task": nil})
	})

	n.Handle("heartbeat", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		lastHeard[msg.Src] = time.Now()
		mu.Unlock()
		return nil
	})

	n.Handle("task_done", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Kind string `json:"kind"`
			ID   int    `json:"id"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if err := n.Reply(msg, map[string]any{"type": "task_done_ok"}); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		lastHeard[msg.Src] = time.Now()
		if job == nil || job.phase == "done" || req.Kind != job.phase {
			return nil
		}
		tasks := job.phaseTasks()
		if req.ID < 0 || req.ID >= len(tasks) || tasks[req.ID].state == "done" {
			return nil
		}
		tasks[req.ID].state = "done"
		for _, t := range tasks {
			if t.state != "done" {
				return nil
			}
		}
		if job.phase == "map" {
			job.phase = "reduce"
			return nil
		}
		job.phase = "done"
		outputs := make([]string, len(job.reduces))
		for i := range job.reduces {
			outputs[i] = fmt.Sprintf("out-%d", i)
		}
		return n.Reply(job.msg, map[string]any{"type": "job_ok", "outputs": outputs})
	})

	// ------------------------------------------------------------- worker

	n.OnStart(func(ctx context.Context) {
		if n.ID() != coord {
			return
		}
		// Re-execution: the task of a worker that went silent is up for grabs again.
		n.Every(100*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			defer mu.Unlock()
			if job == nil || job.phase == "done" {
				return
			}
			for _, t := range job.phaseTasks() {
				if t.state == "running" && time.Since(lastHeard[t.worker]) > silent {
					t.state = "idle"
				}
			}
		})
	})

	n.OnStart(func(ctx context.Context) {
		if n.ID() == coord {
			return
		}
		var working atomic.Bool
		n.Every(heartbeat, func(ctx context.Context) {
			if working.Load() {
				n.Send(ctx, coord, map[string]any{"type": "heartbeat"})
			}
		})
		go func() {
			for {
				time.Sleep(poll)
				for {
					res, err := n.Call(ctx, coord, map[string]any{"type": "get_task"}, 500*time.Millisecond)
					if err != nil {
						break
					}
					var body struct {
						Task *task `json:"task"`
					}
					if err := res.Decode(&body); err != nil || body.Task == nil {
						break
					}
					t := *body.Task
					working.Store(true)
					if t.Kind == "map" {
						err = runMap(ctx, n, t)
					} else {
						err = runReduce(ctx, n, t)
					}
					working.Store(false)
					if err != nil {
						n.Log("task %s %d failed: %v", t.Kind, t.ID, err)
						break
					}
					n.Call(ctx, coord, map[string]any{"type": "task_done", "kind": t.Kind, "id": t.ID}, 500*time.Millisecond)
				}
			}
		}()
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
