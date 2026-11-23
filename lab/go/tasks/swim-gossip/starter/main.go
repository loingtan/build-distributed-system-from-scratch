// Spread the news by piggybacking. Read the brief: npm run lab -- brief swim-gossip
//
// Direct and indirect probing, from the previous task. Each node finds out
// about a crash only when it probes the crashed node itself, which in a big
// cluster can take many periods, and it only ever says "suspect".
//
// TODO: when a probe fails (direct and indirect), mark the target "dead"
// and stop probing it. Then spread the news: keep a buffer of recent
// updates {id, status} and attach a few of them ("updates": [...]) to
// every ping, ack, ping_req and ping_req_ok you send. When you receive an
// update you did not know, apply it and buffer it too. Drop an entry after
// it has been sent about 3 * log2(N + 1) times.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

const (
	period          = 200 * time.Millisecond
	ackTimeout      = 60 * time.Millisecond
	indirectK       = 3
	indirectTimeout = 120 * time.Millisecond // > ackTimeout plus two network hops
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	status := map[string]string{} // node id -> "alive" | "suspect"
	var order []string

	shuffled := func(list []string) []string {
		out := append([]string(nil), list...)
		for i := len(out) - 1; i > 0; i-- {
			k := int(n.Rand() * float64(i+1))
			out[i], out[k] = out[k], out[i]
		}
		return out
	}

	nextTarget := func() string {
		if len(order) == 0 {
			order = shuffled(n.Peers())
		}
		target := order[len(order)-1]
		order = order[:len(order)-1]
		return target
	}

	ping := func(ctx context.Context, target string) bool {
		_, err := n.Call(ctx, target, map[string]any{"type": "ping"}, ackTimeout)
		return err == nil
	}

	probe := func(ctx context.Context, target string) bool {
		if ping(ctx, target) {
			return true
		}
		var helpers []string
		for _, p := range shuffled(n.Peers()) {
			if p != target && len(helpers) < indirectK {
				helpers = append(helpers, p)
			}
		}
		// Ask all helpers at once; any ok is enough.
		results := make(chan bool, len(helpers))
		for _, h := range helpers {
			go func(h string) {
				reply, err := n.Call(ctx, h, map[string]any{"type": "ping_req", "target": target}, indirectTimeout)
				var body struct {
					OK bool `json:"ok"`
				}
				results <- err == nil && reply.Decode(&body) == nil && body.OK
			}(h)
		}
		ok := false
		for range helpers {
			if <-results {
				ok = true
			}
		}
		return ok
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
			alive := probe(ctx, target)
			mu.Lock()
			if alive {
				status[target] = "alive"
			} else {
				status[target] = "suspect"
			}
			mu.Unlock()
		})
	})

	n.Handle("ping", func(ctx context.Context, msg quorumlab.Message) error {
		return n.Reply(msg, map[string]any{"type": "ack"})
	})

	// Ping the target for someone else and tell them how it went.
	n.Handle("ping_req", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Target string `json:"target"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "ping_req_ok", "ok": ping(ctx, req.Target)})
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
