// Counter with decrements: two grow-only tables, one for everything ever
// added and one for everything ever subtracted.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// table is a grow-only counter: one entry per node, how much that node has
// counted. A node only ever increases its own entry.
type table map[string]int

func (t table) sum() int {
	total := 0
	for _, v := range t {
		total += v
	}
	return total
}

// mergeFrom takes the larger value for each entry. Both tables only grow, so
// the larger value is always the newer one.
func (t table) mergeFrom(other table) {
	for id, v := range other {
		if v > t[id] {
			t[id] = v
		}
	}
}

func (t table) clone() table {
	c := make(table, len(t))
	for k, v := range t {
		c[k] = v
	}
	return c
}

func main() {
	n := quorumlab.NewNode()

	// Each table only grows, so each merges with max. The value is the
	// difference.
	var mu sync.Mutex
	up := table{}
	down := table{}

	n.OnStart(func(ctx context.Context) {
		n.Every(150*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			body := map[string]any{"type": "merge", "up": up.clone(), "down": down.clone()}
			mu.Unlock()
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, body)
			}
		})
	})

	n.Handle("add", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Delta int `json:"delta"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		if req.Delta >= 0 {
			up[n.ID()] += req.Delta
		} else {
			down[n.ID()] += -req.Delta
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		value := up.sum() - down.sum()
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("merge", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Up   table `json:"up"`
			Down table `json:"down"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		up.mergeFrom(req.Up)
		down.mergeFrom(req.Down)
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
