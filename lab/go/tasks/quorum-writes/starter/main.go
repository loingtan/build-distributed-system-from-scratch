// Quorum writes. Read the brief: npm run lab -- brief quorum-writes
//
// Leaderless replication, step 1: quorum writes.
// Every node is a replica of every key, and any node can coordinate a put.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// Node ids are known once the node has started.
	var N, W int
	n.OnStart(func(ctx context.Context) {
		N = len(n.NodeIDs())
		W = N/2 + 1 // acknowledgements needed before put_ok
	})
	_ = W // Go rejects unused variables; delete this line once you use W

	var mu sync.Mutex
	store := map[string]any{} // key -> value: this replica's own copy

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// This only writes the local copy, so one crash loses the write.
		// TODO: send the write to every replica (this node counts as one of them):
		//   res, err := n.Call(ctx, peer, map[string]any{"type": "store", "key": req.Key, "value": req.Value}, 300*time.Millisecond)
		// Reply put_ok as soon as W replicas have stored it. Do not wait for all N:
		// start every Call in its own goroutine and count the answers as they
		// arrive on a channel. Never hold mu across n.Call.
		// If W cannot be reached in time, reply {"type": "error", "text": "..."}.
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("store", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: a write from a coordinator. Keep it and reply {"type": "store_ok"}.
		return nil
	})

	// Debug request used by the tests: this replica's own copy, no quorum.
	n.Handle("local_get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key] // nil (null) for a key this replica does not hold
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "local_get_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
