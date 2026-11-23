// Vector clocks: one counter per node, merged entry by entry on receive.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	// Each handler holds mu from start to finish, Send and Reply included:
	// the clock it reports is exactly the clock after its own event. (Reply
	// and Send encode the map before returning, so passing it is safe.)
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
		clock[n.ID()]++
		return n.Reply(msg, map[string]any{"type": "event_ok", "clock": clock})
	})

	// One event: tick once, and use that clock for both the note and the
	// reply. Passing ctx to Send tags the note with this handler run.
	n.Handle("notify", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			To string `json:"to"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		clock[n.ID()]++
		if err := n.Send(ctx, req.To, map[string]any{"type": "note", "clock": clock}); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "notify_ok", "clock": clock})
	})

	// Merge: for every node, keep the larger count. Afterwards our clock covers
	// everything the sender knew about. Then count the receive itself.
	n.Handle("note", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Clock map[string]int `json:"clock"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for id, v := range req.Clock {
			clock[id] = max(clock[id], v)
		}
		clock[n.ID()]++
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
