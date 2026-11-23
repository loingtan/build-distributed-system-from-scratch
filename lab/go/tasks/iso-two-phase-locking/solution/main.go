// Serializable isolation with strict two-phase locking (2PL).
//
// A read takes a shared lock on its key, a write an exclusive one. Many
// transactions may share a key for reading; a writer has it alone. Locks are
// held until commit or abort, so once T has read a key, nobody can change it
// under T, and once T has written a key, nobody can read it until T is done.
//
// A request that cannot get its lock waits. Waiting can form a cycle (T1 waits
// for T2, which waits for T1): a deadlock. We look for a cycle every time a
// transaction starts to wait, and abort the transaction that would close it.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"quorumlab"
)

type request struct {
	Txn   string          `json:"txn"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type txn struct {
	id       string
	writes   map[string]json.RawMessage
	held     map[string]string // key -> "S" or "X"
	waiting  bool
	waitKey  string
	waitMode string
}

type db struct {
	mu    sync.Mutex
	cond  *sync.Cond // signalled whenever locks are released
	data  map[string]json.RawMessage
	txns  map[string]*txn
	locks map[string]map[string]string // key -> txn id -> "S" or "X"
}

// blockers lists the transactions t would wait for to get mode on key.
func (d *db) blockers(t *txn, key, mode string) []string {
	var out []string
	for id, m := range d.locks[key] {
		if id != t.id && (mode == "X" || m == "X") {
			out = append(out, id)
		}
	}
	return out
}

func (d *db) grant(t *txn, key, mode string) {
	if have := t.held[key]; have == "X" || have == mode {
		return
	}
	t.held[key] = mode
	if d.locks[key] == nil {
		d.locks[key] = map[string]string{}
	}
	d.locks[key][t.id] = mode
}

// deadlocks reports whether t waiting for key would close a cycle in the waits-for graph.
func (d *db) deadlocks(t *txn, key, mode string) bool {
	seen := map[string]bool{}
	stack := d.blockers(t, key, mode)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if id == t.id {
			return true
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		if o := d.txns[id]; o != nil && o.waiting {
			stack = append(stack, d.blockers(o, o.waitKey, o.waitMode)...)
		}
	}
	return false
}

// finish releases everything t holds and wakes the waiters.
func (d *db) finish(t *txn) {
	for key := range t.held {
		delete(d.locks[key], t.id)
	}
	delete(d.txns, t.id)
	d.cond.Broadcast()
}

// lock takes mode on key for t, waiting while it is busy. It returns false if
// waiting would deadlock; t has then been aborted. Call with d.mu held.
func (d *db) lock(t *txn, key, mode string) bool {
	for {
		if len(d.blockers(t, key, mode)) == 0 {
			t.waiting = false
			d.grant(t, key, mode)
			return true
		}
		if !t.waiting {
			if d.deadlocks(t, key, mode) {
				t.waiting = false
				d.finish(t)
				return false
			}
			t.waiting, t.waitKey, t.waitMode = true, key, mode
		}
		d.cond.Wait() // releases d.mu while asleep
	}
}

func main() {
	n := quorumlab.NewNode()
	d := &db{data: map[string]json.RawMessage{}, txns: map[string]*txn{}, locks: map[string]map[string]string{}}
	d.cond = sync.NewCond(&d.mu)
	aborted := map[string]any{"type": "aborted"}

	decode := func(msg quorumlab.Message) (request, error) {
		var req request
		err := msg.Decode(&req)
		return req, err
	}

	n.Handle("begin", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.txns[req.Txn] = &txn{id: req.Txn, writes: map[string]json.RawMessage{}, held: map[string]string{}}
		d.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "begin_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		d.mu.Lock()
		t := d.txns[req.Txn]
		if t == nil || !d.lock(t, req.Key, "S") {
			d.mu.Unlock()
			return n.Reply(msg, aborted)
		}
		value, mine := t.writes[req.Key]
		if !mine {
			value = d.data[req.Key]
		}
		d.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		d.mu.Lock()
		t := d.txns[req.Txn]
		if t == nil || !d.lock(t, req.Key, "X") {
			d.mu.Unlock()
			return n.Reply(msg, aborted)
		}
		t.writes[req.Key] = req.Value
		d.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	n.Handle("commit", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		d.mu.Lock()
		t := d.txns[req.Txn]
		if t != nil {
			for k, v := range t.writes {
				d.data[k] = v
			}
			d.finish(t)
		}
		d.mu.Unlock()
		if t == nil {
			return n.Reply(msg, aborted)
		}
		return n.Reply(msg, map[string]any{"type": "commit_ok"})
	})

	n.Handle("abort", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		d.mu.Lock()
		if t := d.txns[req.Txn]; t != nil {
			d.finish(t)
		}
		d.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "abort_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
