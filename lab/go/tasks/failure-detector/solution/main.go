// Failure detector: heartbeats every 100ms; a peer is suspected after 500ms
// of silence. That tolerates four lost or late heartbeats in a row before
// raising an alarm.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const (
	heartbeatEvery = 100 * time.Millisecond
	suspectAfter   = 500 * time.Millisecond
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	lastHeard := map[string]time.Duration{} // peer -> n.Elapsed() when we last heard from it

	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		for _, peer := range n.Peers() {
			lastHeard[peer] = 0
		}
		mu.Unlock()

		n.Every(heartbeatEvery, func(ctx context.Context) {
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, map[string]any{"type": "heartbeat"})
			}
		})
	})

	n.Handle("heartbeat", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		lastHeard[msg.Src] = n.Elapsed()
		mu.Unlock()
		return nil
	})

	// Suspicion is computed when asked, never stored. A peer that starts
	// talking again stops being suspected with no extra code.
	n.Handle("status", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		now := n.Elapsed()
		suspected := []string{}
		for _, peer := range n.Peers() {
			if now-lastHeard[peer] > suspectAfter {
				suspected = append(suspected, peer)
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "status_ok", "suspected": suspected})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
