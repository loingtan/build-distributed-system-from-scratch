// Lamport clocks: one counter per node, bumped past every stamp it receives.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// Each handler holds mu from start to finish, Send and Reply included, so
	// two concurrent events cannot report their stamps out of order.
	var mu sync.Mutex
	clock := 0

	n.Handle("event", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		clock++
		return n.Reply(msg, map[string]any{"type": "event_ok", "ts": clock})
	})

	n.Handle("notify", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			To string `json:"to"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		clock++
		if err := n.Send(ctx, req.To, map[string]any{"type": "note", "ts": clock}); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "notify_ok", "ts": clock})
	})

	// The receive rule: jump past the sender's stamp, so that anything we do
	// from now on is stamped later than the event that caused it.
	n.Handle("note", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			TS int `json:"ts"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		clock = max(clock, req.TS) + 1
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
