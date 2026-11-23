// Fault-tolerant broadcast: retry each gossip until the peer acknowledges it.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	seen := map[int]bool{}

	// Keep sending until the peer says it has the value. Call returns
	// ErrTimeout when no reply arrives in time: that is our cue to try again.
	deliver := func(peer string, value int) {
		for {
			_, err := n.Call(context.Background(), peer, map[string]any{"type": "gossip", "message": value}, 300*time.Millisecond)
			if err == nil {
				return
			}
		}
	}

	learn := func(value int, from string) {
		mu.Lock()
		if seen[value] {
			mu.Unlock()
			return
		}
		seen[value] = true
		mu.Unlock()
		for _, peer := range n.Peers() {
			if peer != from {
				go deliver(peer, value)
			}
		}
	}

	n.Handle("broadcast", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Message int `json:"message"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		learn(req.Message, "")
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	// Always acknowledge, even a value we already had: the sender may be
	// retrying because our previous acknowledgement was lost.
	n.Handle("gossip", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Message int `json:"message"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		learn(req.Message, msg.Src)
		return n.Reply(msg, map[string]any{"type": "gossip_ok"})
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
