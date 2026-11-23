// Acknowledge and redeliver. Read the brief: npm run lab -- brief queue-ack
//
// The FIFO queue from the last task. A dequeued job is forgotten at once,
// so a consumer that crashes halfway through takes the job with it.
//
// TODO: keep a handed-out job until its consumer sends { type: "ack", id }.
// If no ack arrives within visibility, make the job available again.
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

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	var queue []job

	n.Handle("enqueue", func(ctx context.Context, msg quorumlab.Message) error {
		var j job
		if err := msg.Decode(&j); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		queue = append(queue, j)
		return n.Reply(msg, map[string]any{"type": "enqueue_ok"})
	})

	n.Handle("dequeue", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var next *job
		if len(queue) > 0 {
			next = &queue[0]
			queue = queue[1:]
		}
		// TODO: remember the job as "in flight" and start a timer with
		// n.After(visibility, ...).
		return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": next})
	})

	// TODO: n.Handle("ack", ...) -> forget the job for good, reply { type: "ack_ok" }

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
