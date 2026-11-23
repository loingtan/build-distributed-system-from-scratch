// A FIFO queue: one broker node, oldest job at the front, newest at the back.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"quorumlab"
)

// job is what consumers are handed: { id, payload }.
type job struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

func main() {
	n := quorumlab.NewNode()

	// The queue: oldest job at the front, newest at the back.
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

	// Handlers run concurrently, so taking the front job happens under the
	// mutex: two consumers asking at the same moment can never be given the
	// same job. Every handler also replies before it lets go of the mutex, so
	// the replies leave in the same order as the changes to the queue.
	n.Handle("dequeue", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var next *job
		if len(queue) > 0 {
			next = &queue[0]
			queue = queue[1:]
		}
		return n.Reply(msg, map[string]any{"type": "dequeue_ok", "job": next})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
