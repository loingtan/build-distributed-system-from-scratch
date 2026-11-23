// SWIM, part 1: probe one member per protocol period.
//
// Each node sends one ping per period and answers the pings it receives, so
// it sends about two messages per period whatever the size of the cluster.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const (
	period     = 200 * time.Millisecond
	ackTimeout = 80 * time.Millisecond
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	status := map[string]string{} // node id -> "alive" | "suspect"
	var order []string            // peers still to probe in this pass

	// Round robin over a shuffled list: random enough that no two nodes probe
	// in lockstep, yet every member is probed at least once per pass.
	nextTarget := func() string {
		if len(order) == 0 {
			order = n.Peers()
			for i := len(order) - 1; i > 0; i-- {
				k := int(n.Rand() * float64(i+1))
				order[i], order[k] = order[k], order[i]
			}
		}
		target := order[len(order)-1]
		order = order[:len(order)-1]
		return target
	}

	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		for _, id := range n.NodeIDs() {
			status[id] = "alive"
		}
		mu.Unlock()

		n.Every(period, func(ctx context.Context) {
			mu.Lock()
			target := nextTarget()
			mu.Unlock()

			_, err := n.Call(ctx, target, map[string]any{"type": "ping"}, ackTimeout)

			// A later successful probe clears the suspicion: nothing is final yet.
			mu.Lock()
			if err != nil {
				status[target] = "suspect"
			} else {
				status[target] = "alive"
			}
			mu.Unlock()
		})
	})

	n.Handle("ping", func(ctx context.Context, msg quorumlab.Message) error {
		return n.Reply(msg, map[string]any{"type": "ack"})
	})

	n.Handle("members", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		members := map[string]string{}
		for id, st := range status {
			members[id] = st
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "members_ok", "members": members})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
