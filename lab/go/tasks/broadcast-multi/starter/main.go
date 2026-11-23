// Multi-node broadcast. Read the brief: npm run lab -- brief broadcast-multi
//
// Your single-node solution. Each node only knows what it was told directly.
// n.Peers() lists the other nodes, e.g. [n2 n3 n4 n5].
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
		// TODO: tell the other nodes. n.Send(ctx, peer, map[string]any{"type": "gossip", ...})
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// TODO: handle the messages nodes send each other.

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
