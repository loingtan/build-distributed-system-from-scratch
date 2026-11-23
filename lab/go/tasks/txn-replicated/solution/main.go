// Totally-available transactions: apply locally and reply at once, then
// replicate in the background with last-writer-wins on Lamport timestamps.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// entry is one key's value plus a tag saying when and where it was written:
// TS is a Lamport timestamp, By is the writer's node id.
type entry struct {
	Key   any    `json:"key"`
	Value any    `json:"value"`
	TS    int    `json:"ts"`
	By    string `json:"by"`
}

// newer is last writer wins. Timestamps can tie, so the node id breaks ties.
// Every node applies the same rule, so every node picks the same winner.
func newer(a, b entry) bool {
	return a.TS > b.TS || (a.TS == b.TS && a.By > b.By)
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	store := map[any]entry{}
	clock := 0

	// Anti-entropy: ship the whole store to every peer, over and over. A lost
	// message is repaired by the next round.
	n.OnStart(func(ctx context.Context) {
		n.Every(200*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			entries := make([]entry, 0, len(store))
			for _, e := range store {
				entries = append(entries, e)
			}
			mu.Unlock()
			if len(entries) == 0 {
				return
			}
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, map[string]any{"type": "sync", "entries": entries})
			}
		})
	})

	// The mutex is held for the whole transaction, so transactions on one
	// node still run one at a time.
	n.Handle("txn", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Txn [][3]any `json:"txn"` // [["r", key, null], ["w", key, value], ...]
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		result := make([][3]any, len(req.Txn))
		mu.Lock()
		clock++
		for i, op := range req.Txn {
			f, key, value := op[0], op[1], op[2]
			if f == "r" {
				var current any // a missing key reads as nil: null
				if e, ok := store[key]; ok {
					current = e.Value
				}
				result[i] = [3]any{"r", key, current}
				continue
			}
			store[key] = entry{Key: key, Value: value, TS: clock, By: n.ID()}
			result[i] = [3]any{"w", key, value}
		}
		mu.Unlock()
		// Reply immediately. Never wait for peers: they may be unreachable.
		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": result})
	})

	n.Handle("sync", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Entries []entry `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, incoming := range req.Entries {
			clock = max(clock, incoming.TS)
			if current, ok := store[incoming.Key]; !ok || newer(incoming, current) {
				store[incoming.Key] = incoming
			}
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
