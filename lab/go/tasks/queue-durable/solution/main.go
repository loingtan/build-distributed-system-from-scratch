// A durable broker: everything it must not forget lives on disk, as plain
// JSON:
//
//	jobs   id -> { job, state, deliveries, deadline }
//	ready  ids waiting, oldest first (may hold stale ids)
//	dead   jobs that used up their attempts
//
// A handler changes the disk first and replies afterwards, so anything a
// client has been told is already safe.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const (
	visibility    = 500 * time.Millisecond
	maxDeliveries = 3
)

// job is what consumers are handed: { id, payload }.
type job struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// record is one job's entry in disk.jobs. Deadline is when the current
// hand-out expires, in milliseconds since the test started.
type record struct {
	Job        job    `json:"job"`
	State      string `json:"state"` // "ready", "inflight", "done" or "dead"
	Deliveries int    `json:"deliveries"`
	Deadline   int64  `json:"deadline"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	jobs := map[string]*record{}
	ready := []string{}
	dead := []job{}

	// save writes the whole state in one atomic, fsynced update. The caller
	// holds mu, and replies only after save returns.
	save := func() error {
		return n.Disk().Update(func(tx *quorumlab.Tx) error {
			if err := tx.Set("jobs", jobs); err != nil {
				return err
			}
			if err := tx.Set("ready", ready); err != nil {
				return err
			}
			return tx.Set("dead", dead)
		})
	}

	now := func() int64 { return n.Elapsed().Milliseconds() }

	// Timers do not survive a crash, so the disk holds the moment each
	// hand-out expires and the timer is only a reminder to go and look.
	watch := func(id string, deadline int64) {
		n.After(time.Duration(max(0, deadline-now()))*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			defer mu.Unlock()
			rec := jobs[id]
			// Stale reminder: the job was acknowledged, or handed out again since.
			if rec == nil || rec.State != "inflight" || rec.Deadline != deadline {
				return
			}
			if rec.Deliveries >= maxDeliveries {
				rec.State = "dead"
				dead = append(dead, rec.Job)
			} else {
				rec.State = "ready"
				ready = append([]string{id}, ready...)
			}
			if err := save(); err != nil {
				n.Log("saving the queue: %v", err)
			}
		})
	}

	// After a restart: read the state back and pick up the hand-outs that
	// were running when we died.
	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		for key, v := range map[string]any{"jobs": &jobs, "ready": &ready, "dead": &dead} {
			if _, err := n.Disk().Get(key, v); err != nil {
				log.Fatalf("reading %s from disk: %v", key, err)
			}
		}
		for id, rec := range jobs {
			if rec.State == "inflight" {
				watch(id, rec.Deadline)
			}
		}
	})

	n.Handle("enqueue", func(ctx context.Context, msg quorumlab.Message) error {
		var j job
		if err := msg.Decode(&j); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if _, ok := jobs[j.ID]; !ok {
			jobs[j.ID] = &record{Job: j, State: "ready"}
			ready = append(ready, j.ID)
			if err := save(); err != nil {
				return err // no reply: the producer will retry
			}
		}
		return n.Reply(msg, map[string]any{"type": "enqueue_ok"})
	})

	n.Handle("dequeue", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var rec *record
		for len(ready) > 0 && rec == nil {
			candidate := jobs[ready[0]]
			ready = ready[1:]
			if candidate != nil && candidate.State == "ready" {
				rec = candidate
			}
		}
		if rec == nil {
			// Dropping stale ids from ready needs no write: after a crash
			// dequeue would skip them again.
			return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": nil})
		}
		rec.State = "inflight"
		rec.Deliveries++
		rec.Deadline = now() + visibility.Milliseconds()
		if err := save(); err != nil {
			return err
		}
		watch(rec.Job.ID, rec.Deadline)
		return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": rec.Job})
	})

	n.Handle("ack", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			ID string `json:"id"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if rec, ok := jobs[req.ID]; ok && (rec.State == "ready" || rec.State == "inflight") {
			rec.State = "done"
			if err := save(); err != nil {
				return err
			}
		}
		return n.Reply(msg, map[string]any{"type": "ack_ok"})
	})

	n.Handle("dead_letters", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "dead_letters_ok", "jobs": dead})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
