// Dead letters. Job states: "ready" -> "inflight" -> "done", or back to
// "ready" after a timeout. New here: a job whose timeout expires for the
// maxDeliveries-th time becomes "dead" and is parked where a human can look
// at it.
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

type record struct {
	job        job
	state      string
	timer      *quorumlab.Timer
	deliveries int
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	jobs := map[string]*record{} // id -> record
	var ready []string           // ids waiting, oldest first (may hold stale ids)
	dead := []job{}              // jobs that used up their attempts

	n.Handle("enqueue", func(ctx context.Context, msg quorumlab.Message) error {
		var j job
		if err := msg.Decode(&j); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if _, ok := jobs[j.ID]; !ok {
			jobs[j.ID] = &record{job: j, state: "ready"}
			ready = append(ready, j.ID)
		}
		// A retry gets the same answer: the producer is still waiting for one.
		return n.Reply(msg, map[string]any{"type": "enqueue_ok"})
	})

	n.Handle("dequeue", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// Skip ids that were acknowledged while they sat in the queue.
		var rec *record
		for len(ready) > 0 && rec == nil {
			candidate := jobs[ready[0]]
			ready = ready[1:]
			if candidate.state == "ready" {
				rec = candidate
			}
		}
		if rec == nil {
			return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": nil})
		}
		rec.state = "inflight"
		rec.deliveries++
		rec.timer = n.After(visibility, func(ctx context.Context) {
			mu.Lock()
			defer mu.Unlock()
			// An ack may have finished the job while this timer was firing.
			if rec.state != "inflight" {
				return
			}
			if rec.deliveries >= maxDeliveries {
				// Out of attempts. Stop retrying, but do not throw the job away.
				rec.state = "dead"
				dead = append(dead, rec.job)
			} else {
				rec.state = "ready"
				ready = append([]string{rec.job.ID}, ready...)
			}
		})
		return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": rec.job})
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
		// Wherever the job is now (with this consumer, with another one, or
		// back in the queue), it is finished. A second ack finds it done; a
		// dead job stays dead.
		if rec, ok := jobs[req.ID]; ok && (rec.state == "ready" || rec.state == "inflight") {
			if rec.timer != nil {
				rec.timer.Stop()
			}
			rec.state = "done"
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
