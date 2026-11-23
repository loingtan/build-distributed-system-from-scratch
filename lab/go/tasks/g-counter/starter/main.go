// Grow-only counter. Read the brief: npm run lab -- brief g-counter
//
// A first attempt: keep a total and tell peers about each delta.
// It passes on a healthy network. Run the other tests to see it break.
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
			Delta int `json:"delta"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		total += req.Delta
		mu.Unlock()
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "added", "delta": req.Delta})
		}
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("added", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Delta int `json:"delta"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		total += req.Delta
		mu.Unlock()
		return nil
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
