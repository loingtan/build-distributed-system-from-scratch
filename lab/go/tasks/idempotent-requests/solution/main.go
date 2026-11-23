// Idempotent requests: apply each request_id once, acknowledge every copy.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// Remember which requests have been applied. A retry is acknowledged again
	// (the client is still waiting for an answer) but changes nothing.
	var mu sync.Mutex
	total := 0
	applied := map[string]bool{}

	n.Handle("add", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Delta     int    `json:"delta"`
			RequestID string `json:"request_id"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// Check and apply under one lock: two copies of a request can be
		// handled at the same moment.
		mu.Lock()
		if !applied[req.RequestID] {
			applied[req.RequestID] = true
			total += req.Delta
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": total})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
