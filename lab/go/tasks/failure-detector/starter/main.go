// Failure detector. Read the brief: npm run lab -- brief failure-detector
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

	// When did we last hear from each peer? (n.Elapsed() at that moment.)
	// Handlers run concurrently: hold mu while touching lastHeard.
	var mu sync.Mutex
	lastHeard := map[string]time.Duration{}

	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		for _, peer := range n.Peers() {
			lastHeard[peer] = 0
		}
		mu.Unlock()

		// TODO: send {"type": "heartbeat"} to every peer on a timer (n.Every).
	})

	n.Handle("heartbeat", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: note that msg.Src is alive right now (n.Elapsed()).
		return nil
	})

	n.Handle("status", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// TODO: a peer is suspected when it has been silent for too long.
		suspected := []string{}
		return n.Reply(msg, map[string]any{"type": "status_ok", "suspected": suspected})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
