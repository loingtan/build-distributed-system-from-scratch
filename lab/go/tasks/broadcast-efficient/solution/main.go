// Efficient broadcast: batch values per neighbour and gossip over a star.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// Two ideas, each of which cuts the message count:
//
//  1. Batching. Each node keeps, per neighbour, the values that neighbour has
//     not acknowledged yet, and flushes them in one message on a timer.
//  2. Topology. Nodes are arranged as a star: everyone talks to one hub, and
//     the hub talks to everyone. A value needs at most two hops.
//
// The price: values wait for the next flush, and the hub is a single point
// of failure for propagation (values still arrive once it is reachable).
const (
	flushEvery   = 250 * time.Millisecond
	batchTimeout = 240 * time.Millisecond // shorter than flushEvery: one batch per tick
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	seen := map[int]bool{}
	unacked := map[string]map[int]bool{} // neighbour -> values it has not acknowledged
	inFlight := map[string]bool{}        // neighbour -> a batch is waiting for its ack

	learn := func(value int, from string) {
		mu.Lock()
		defer mu.Unlock()
		if seen[value] {
			return
		}
		seen[value] = true
		for peer, pending := range unacked {
			if peer != from {
				pending[value] = true
			}
		}
	}

	// sendBatch delivers one batch and, if it is acknowledged, forgets only
	// what this batch carried; newer values stay queued. A lost batch or ack
	// is simply sent again (with anything new) on the next flush.
	sendBatch := func(ctx context.Context, peer string, batch []int) {
		_, err := n.Call(ctx, peer, map[string]any{"type": "gossip", "messages": batch}, batchTimeout)
		mu.Lock()
		defer mu.Unlock()
		inFlight[peer] = false
		if err != nil {
			return
		}
		for _, v := range batch {
			delete(unacked[peer], v)
		}
	}

	// flush sends each neighbour everything it has not acknowledged, at most
	// one batch in flight per neighbour so a slow ack never doubles the cost.
	flush := func(ctx context.Context) {
		mu.Lock()
		batches := map[string][]int{}
		for peer, pending := range unacked {
			if len(pending) == 0 || inFlight[peer] {
				continue
			}
			batch := make([]int, 0, len(pending))
			for v := range pending {
				batch = append(batch, v)
			}
			batches[peer] = batch
			inFlight[peer] = true
		}
		mu.Unlock()
		for peer, batch := range batches {
			go sendBatch(ctx, peer, batch)
		}
	}

	n.OnStart(func(ctx context.Context) {
		hub := n.NodeIDs()[0]
		neighbours := []string{hub}
		if n.ID() == hub {
			neighbours = n.Peers()
		}
		mu.Lock()
		for _, peer := range neighbours {
			unacked[peer] = map[int]bool{}
		}
		mu.Unlock()
		n.Every(flushEvery, flush)
	})

	n.Handle("broadcast", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Message int `json:"message"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		learn(req.Message, "")
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Messages []int `json:"messages"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		for _, v := range req.Messages {
			learn(v, msg.Src)
		}
		return n.Reply(msg, map[string]any{"type": "gossip_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		values := make([]int, 0, len(seen))
		for v := range seen {
			values = append(values, v)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": values})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
