// Multi-leader writes. Read the brief: npm run lab -- brief repl-multi-leader
//
// Every node is a leader now: it accepts writes to any key, answers at once
// and then sends the write to the others. Several nodes may write the same
// key at the same moment; all nodes must end up with the same value.
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
	store := map[string]any{} // key -> value

	// TODO: keep {value, ts} per key, with ts = [counter, node id]. The higher
	// counter wins; on a tie, the higher node id (compared as strings). Make the
	// counter a Lamport clock: +1 for each write accepted here, and never below
	// any counter this node has received.

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		// TODO: write_ok must carry the write's timestamp: {"type": "write_ok", "ts": [counter, node]}.
		if err := n.Reply(msg, map[string]any{"type": "write_ok"}); err != nil {
			return err
		}
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "replicate", "key": req.Key, "value": req.Value})
		}
		return nil
	})

	n.Handle("replicate", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: this keeps whichever write arrives last, and two nodes can see two
		// writes arrive in opposite orders. Keep the one with the higher timestamp.
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		return nil
	})

	// TODO: messages sent during a partition are lost for good. Have every node
	// send its whole store to its peers now and then (n.Every inside n.OnStart),
	// and merge what it receives with the same rule.

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key] // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
