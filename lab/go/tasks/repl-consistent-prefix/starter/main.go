// Consistent prefix. Read the brief: npm run lab -- brief repl-consistent-prefix
//
// There is no primary any more: every node accepts writes, applies them and
// sends them to the others. Clients still carry a token ("token", null at
// first) and send back the last one they were given.
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

	// TODO: a single number no longer describes a node's state, because writes
	// come from several origins. Name each write by {origin, n} (n counts the
	// writes accepted at that origin) and keep applied[origin] for every node.
	// That vector makes a good token.

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: first wait until this node has seen everything the client's token
		// says the client has seen.
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		// TODO: answer with a token.
		if err := n.Reply(msg, map[string]any{"type": "write_ok", "token": nil}); err != nil {
			return err
		}
		// TODO: the write depends on everything this node has applied so far.
		// Send that along (deps), with the write's origin and number.
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
		// TODO: this applies each write the moment it arrives. An answer can arrive
		// over a fast link before its question arrives over a slow one. Hold a
		// write back until its origin's earlier writes and its deps are applied.
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: wait until this node covers the request's token, then answer with a token.
		mu.Lock()
		value := store[req.Key] // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value, "token": nil})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
