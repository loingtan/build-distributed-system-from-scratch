// Multi-node broadcast: flood each value to every peer, the first time only.
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
	seen := map[int]bool{}

	// Forward a value only the first time we see it. Without this check the
	// nodes would pass the same value back and forth forever.
	learn := func(ctx context.Context, value int) {
		mu.Lock()
		if seen[value] {
			mu.Unlock()
			return
		}
		seen[value] = true
		mu.Unlock()
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "gossip", "message": value})
		}
	}

	n.Handle("broadcast", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Message int `json:"message"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		learn(ctx, req.Message)
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Message int `json:"message"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		learn(ctx, req.Message)
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		values := make([]int, 0, len(seen))
		for v := range seen {
			values = append(values, v)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": values})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
