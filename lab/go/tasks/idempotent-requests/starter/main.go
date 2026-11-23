// Idempotent requests. Read the brief: npm run lab -- brief idempotent-requests
//
// Clients retry when a reply is slow, so the same request can arrive twice.
// Every message carries a "request_id", which is the same on a retry.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	total := 0

	n.Handle("add", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Delta     int    `json:"delta"`
			RequestID string `json:"request_id"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: apply each request_id only once, but still answer every copy.
		mu.Lock()
		total += req.Delta
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
