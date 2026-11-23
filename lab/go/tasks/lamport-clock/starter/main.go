// Lamport clocks. Read the brief: npm run lab -- brief lamport-clock
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// Handlers run concurrently. Hold mu for a whole handler, including its
	// Send and Reply, so the stamps leave this node in the order they were made.
	var mu sync.Mutex
	clock := 0

	// A local event: stamp it and report the stamp.
	n.Handle("event", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// TODO: advance the clock
		return n.Reply(msg, map[string]any{"type": "event_ok", "ts": clock})
	})

	// Send a note to another node: the body's "to" field is its id.
	n.Handle("notify", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: advance the clock, send {"type": "note", "ts": ts} to "to"
		// with n.Send(ctx, ...), and reply {"type": "notify_ok", "ts": ts}
		// with the same ts.
		return nil
	})

	// A note from another node arrives.
	n.Handle("note", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: move the clock past the note's "ts"
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
