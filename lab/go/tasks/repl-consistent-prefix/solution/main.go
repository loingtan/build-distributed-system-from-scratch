// Consistent prefix: every node accepts writes. A write is named by its origin
// (the node that accepted it) and its number there. applied[origin] counts the
// writes from each origin this node has applied; that vector is also the token.
package main

import (
	"context"
	"log"
	"maps"
	"sync"
	"time"

	"quorumlab"
)

type vec = map[string]int

type write struct {
	Origin string `json:"origin"`
	N      int    `json:"n"`
	Key    string `json:"key"`
	Value  any    `json:"value"`
	Deps   vec    `json:"deps"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	store := map[string]any{} // key -> value
	applied := vec{}          // origin -> writes applied from it
	var pending []write       // replicated writes that cannot be applied yet

	// covers: does this node have everything in v? The caller holds mu.
	covers := func(v vec) bool {
		for id, c := range v {
			if applied[id] < c {
				return false
			}
		}
		return true
	}

	// drain applies what is ready: the next write from its origin, whose
	// dependencies are all here. One write can unblock another, so loop.
	// The caller holds mu.
	drain := func() {
		for progress := true; progress; {
			progress = false
			rest := pending[:0]
			for _, e := range pending {
				switch {
				case e.N <= applied[e.Origin]:
					// already applied
				case e.N == applied[e.Origin]+1 && covers(e.Deps):
					store[e.Key] = e.Value
					applied[e.Origin] = e.N
					progress = true
				default:
					rest = append(rest, e)
				}
			}
			pending = rest
		}
	}

	// catchUp waits until this node has seen everything the client has seen.
	catchUp := func(token vec, limit time.Duration) bool {
		deadline := time.Now().Add(limit)
		for {
			mu.Lock()
			ok := covers(token)
			mu.Unlock()
			if ok {
				return true
			}
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	unavailable := map[string]any{"type": "error", "code": "unavailable"}

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
			Token vec    `json:"token"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if !catchUp(req.Token, 600*time.Millisecond) {
			return n.Reply(msg, unavailable)
		}
		mu.Lock()
		// Everything this node has applied (which now includes everything the
		// client has seen) happened before this write: its dependencies.
		w := write{Origin: n.ID(), N: applied[n.ID()] + 1, Key: req.Key, Value: req.Value, Deps: maps.Clone(applied)}
		applied[n.ID()] = w.N
		store[req.Key] = req.Value
		token := maps.Clone(applied)
		mu.Unlock()
		if err := n.Reply(msg, map[string]any{"type": "write_ok", "token": token}); err != nil {
			return err
		}
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "replicate", "origin": w.Origin, "n": w.N, "key": w.Key, "value": w.Value, "deps": w.Deps})
		}
		return nil
	})

	n.Handle("replicate", func(ctx context.Context, msg quorumlab.Message) error {
		var w write
		if err := msg.Decode(&w); err != nil {
			return err
		}
		mu.Lock()
		pending = append(pending, w)
		drain()
		mu.Unlock()
		return nil
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Token vec    `json:"token"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if !catchUp(req.Token, 600*time.Millisecond) {
			return n.Reply(msg, unavailable)
		}
		mu.Lock()
		value := store[req.Key] // a missing key reads as nil: null
		token := maps.Clone(applied)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value, "token": token})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
