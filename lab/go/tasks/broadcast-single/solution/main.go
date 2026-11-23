// Single-node broadcast: remember every value, return them all on read.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// A set of values. A map is not a JSON list, so read turns it into one.
	var mu sync.Mutex
	seen := map[int]bool{}

	n.Handle("broadcast", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Message int `json:"message"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		seen[req.Message] = true
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
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
