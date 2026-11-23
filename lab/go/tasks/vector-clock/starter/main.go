// Vector clocks. Read the brief: npm run lab -- brief vector-clock
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// One counter per node, e.g. {"n1": 0, "n2": 0, "n3": 0}. Handlers run
	// concurrently: hold mu for a whole handler, Send and Reply included.
	var mu sync.Mutex
	clock := map[string]int{}
	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range n.NodeIDs() {
			clock[id] = 0
		}
	})

	n.Handle("event", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// TODO: count this event in our own entry
		return n.Reply(msg, map[string]any{"type": "event_ok", "clock": clock})
	})

	n.Handle("notify", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: count the event, send {"type": "note", "clock": clock} to the
		// body's "to" with n.Send(ctx, ...), and reply
		// {"type": "notify_ok", "clock": clock} with the same clock.
		return nil
	})

	n.Handle("note", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: merge the note's "clock" into ours (entry by entry, keep the
		// larger), then count the receive as an event of our own.
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
