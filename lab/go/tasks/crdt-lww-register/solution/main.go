// Last-writer-wins registers, one per key. Each stored value carries a tag
// (ts, node). The tag with the higher ts wins; on equal ts the higher node
// id wins, so every replica orders any two tags the same way.
//
// ts comes from a Lamport clock: a write uses clock + 1, and every tag
// received pushes the clock forward. A write is therefore stamped later than
// every write its node had already seen.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"quorumlab"
)

type entry struct {
	Value any    `json:"value"`
	Ts    int    `json:"ts"`
	Node  string `json:"node"`
}

// newer reports whether x beats y. Comparing node ids on equal timestamps
// makes the order total.
func newer(x entry, y *entry) bool {
	if y == nil {
		return true
	}
	if x.Ts != y.Ts {
		return x.Ts > y.Ts
	}
	return x.Node > y.Node
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	registers := map[string]*entry{}
	clock := 0

	n.OnStart(func(ctx context.Context) {
		n.Every(100*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			body, _ := json.Marshal(map[string]any{"type": "merge", "registers": registers})
			mu.Unlock()
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, json.RawMessage(body))
			}
		})
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		clock++
		registers[req.Key] = &entry{Value: req.Value, Ts: clock, Node: n.ID()}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		var value any
		if r := registers[req.Key]; r != nil {
			value = r.Value
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("merge", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Registers map[string]entry `json:"registers"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		for key, e := range req.Registers {
			clock = max(clock, e.Ts)
			if newer(e, registers[key]) {
				registers[key] = &e
			}
		}
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
