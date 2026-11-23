// Acknowledge and redeliver: a job is in exactly one place, waiting in ready
// or out with a consumer in inFlight. It is only forgotten when the consumer
// acknowledges it.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const visibility = 500 * time.Millisecond

// job is what consumers are handed: { id, payload }.
type job struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// handout is a job out with a consumer, and the timer that takes it back.
type handout struct {
	job   job
	timer *quorumlab.Timer
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	var ready []job                   // jobs waiting, oldest first
	inFlight := map[string]*handout{} // id -> hand-out

	n.Handle("enqueue", func(ctx context.Context, msg quorumlab.Message) error {
		var j job
		if err := msg.Decode(&j); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		ready = append(ready, j)
		return n.Reply(msg, map[string]any{"type": "enqueue_ok"})
	})

	n.Handle("dequeue", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if len(ready) == 0 {
			return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": nil})
		}
		j := ready[0]
		ready = ready[1:]
		// If the consumer stays silent for visibility, assume it died and put
		// the job back at the front: it is the oldest work there is.
		h := &handout{job: j}
		h.timer = n.After(visibility, func(ctx context.Context) {
			mu.Lock()
			defer mu.Unlock()
			// The timer may already be running when an ack stops it: only
			// act if this hand-out is still the one in flight.
			if inFlight[j.ID] != h {
				return
			}
			delete(inFlight, j.ID)
			ready = append([]job{j}, ready...)
		})
		inFlight[j.ID] = h
		return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": j})
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
		if h, ok := inFlight[req.ID]; ok {
			h.timer.Stop()
			delete(inFlight, req.ID)
		}
		return n.Reply(msg, map[string]any{"type": "ack_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
