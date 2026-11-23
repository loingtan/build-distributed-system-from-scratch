// Multi-value registers, one per key. Each key holds a list of versions
// { value, vv }. vv is a version vector: for each node, how many of that
// node's writes to this key the version has seen, itself included.
//
// A write replaces every version this replica holds: its vector is their
// merge plus one for this node, so it dominates all of them. Merging two
// replicas keeps every version that no other version dominates. Versions
// written without seeing each other dominate neither way and stay as
// siblings until a later write has seen them all.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"quorumlab"
)

type vclock map[string]int

type version struct {
	Value any    `json:"value"`
	VV    vclock `json:"vv"`
}

// dominates reports whether x has seen everything y has, and something more.
func dominates(x, y vclock) bool {
	more := false
	for id, b := range y {
		if x[id] < b {
			return false
		}
	}
	for id, a := range x {
		if a > y[id] {
			more = true
		}
	}
	return more
}

func equal(x, y vclock) bool {
	return !dominates(x, y) && !dominates(y, x) && sameEntries(x, y)
}

func sameEntries(x, y vclock) bool {
	for id := range x {
		if x[id] != y[id] {
			return false
		}
	}
	for id := range y {
		if x[id] != y[id] {
			return false
		}
	}
	return true
}

// merge keeps every version not dominated by another, once each.
func merge(mine, theirs []version) []version {
	all := append(append([]version{}, mine...), theirs...)
	var out []version
	for _, v := range all {
		dominated := false
		for _, w := range all {
			if dominates(w.VV, v.VV) {
				dominated = true
				break
			}
		}
		if dominated {
			continue
		}
		dup := false
		for _, w := range out {
			if equal(w.VV, v.VV) {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, v)
		}
	}
	return out
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	registers := map[string][]version{}

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
		vv := vclock{}
		for _, v := range registers[req.Key] {
			for id, c := range v.VV {
				vv[id] = max(vv[id], c)
			}
		}
		vv[n.ID()]++
		registers[req.Key] = []version{{Value: req.Value, VV: vv}}
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
		values := []any{}
		for _, v := range registers[req.Key] {
			values = append(values, v.Value)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "values": values})
	})

	n.Handle("merge", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Registers map[string][]version `json:"registers"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		for key, versions := range req.Registers {
			registers[key] = merge(registers[key], versions)
		}
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
