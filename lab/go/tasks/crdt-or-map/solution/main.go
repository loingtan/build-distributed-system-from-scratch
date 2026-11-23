// A shopping cart: a map from item to quantity, with observed-remove keys.
// Every add is a tagged quantity change { tag: qty }. An item is in the cart
// while it has a change that no remove has deleted; its quantity is the sum
// of those changes. A remove deletes exactly the changes it has seen, so the
// quantity starts again from zero, and an add it could not see survives.
//
// This is the observed-remove set from before, with a number on each tag.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"quorumlab"
)

type state struct {
	Changes map[string]map[string]int `json:"changes"` // item -> tag -> qty
	Removed map[string]bool           `json:"removed"` // tombstoned tags
}

func (s *state) put(item, tag string, qty int) {
	if s.Changes[item] == nil {
		s.Changes[item] = map[string]int{}
	}
	s.Changes[item][tag] = qty
}

// mergeFrom is a union of both tables. A tag's qty never changes, so
// copying it is safe.
func (s *state) mergeFrom(o state) {
	for item, tags := range o.Changes {
		for t, q := range tags {
			s.put(item, t, q)
		}
	}
	for t := range o.Removed {
		s.Removed[t] = true
	}
}

func (s *state) view() map[string]int {
	value := map[string]int{}
	for item, tags := range s.Changes {
		live, sum := false, 0
		for t, q := range tags {
			if !s.Removed[t] {
				live = true
				sum += q
			}
		}
		if live {
			value[item] = sum
		}
	}
	return value
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	st := state{Changes: map[string]map[string]int{}, Removed: map[string]bool{}}
	counter := 0

	n.OnStart(func(ctx context.Context) {
		n.Every(100*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			body, _ := json.Marshal(map[string]any{"type": "merge", "changes": st.Changes, "removed": st.Removed})
			mu.Unlock()
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, json.RawMessage(body))
			}
		})
	})

	n.Handle("add", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Item string `json:"item"`
			Qty  int    `json:"qty"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		counter++
		st.put(req.Item, fmt.Sprintf("%s:%d", n.ID(), counter), req.Qty)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("remove", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Item string `json:"item"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		for t := range st.Changes[req.Item] {
			st.Removed[t] = true
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "remove_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		value := st.view()
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("merge", func(ctx context.Context, msg quorumlab.Message) error {
		var other state
		if err := msg.Decode(&other); err != nil {
			return err
		}
		mu.Lock()
		st.mergeFrom(other)
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
