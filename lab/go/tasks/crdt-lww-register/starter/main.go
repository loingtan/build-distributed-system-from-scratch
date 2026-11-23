// Last-writer-wins register. Read the brief: npm run lab -- brief crdt-lww-register
//
// Last-writer-wins registers, one per key, stamped with a Lamport clock.
// Run the tests: two writes on opposite sides of a partition can get the
// same timestamp, and then each replica keeps the one it had.
// TODO: make the order of tags total, so every replica picks the same winner.
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

// newer reports whether x beats y.
func newer(x entry, y *entry) bool {
	if y == nil {
		return true
	}
	return x.Ts > y.Ts
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
