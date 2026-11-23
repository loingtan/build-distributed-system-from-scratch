// Probe one member at a time. Read the brief: npm run lab -- brief swim-probe
//
// The heartbeat failure detector from earlier, now reporting a membership
// view. It works, but every node tells every other node "I am alive" ten
// times a second: (N - 1) * 10 messages per node per second. That grows with
// the cluster and blows the message budget.
//
// TODO: replace the heartbeats with SWIM probing. Every protocol period
// (200ms), ping ONE member and wait briefly for its ack. No ack: mark it
// "suspect". An ack: mark it "alive" again. Go round a shuffled list of
// peers so every member is probed regularly.
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

	// The view: every node id, this one included, mapped to "alive" or "suspect".
	n.Handle("members", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		now := n.Elapsed()
		members := map[string]string{n.ID(): "alive"}
		for _, peer := range n.Peers() {
			if now-lastHeard[peer] > suspectAfter {
				members[peer] = "suspect"
			} else {
				members[peer] = "alive"
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "members_ok", "members": members})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
