// Shopping cart (OR-Map). Read the brief: npm run lab -- brief crdt-or-map
//
// A cart built by nesting what you have: an observed-remove set of item
// names, and a PN-counter of quantities per item.
// Run the tests: removing an item hides it, but its counter keeps its old
// value, so adding it again brings the removed quantity back.
// TODO: make a remove clear the quantity it has seen, and only that.
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

// counts is item -> node -> total; each per-node total only grows.
type counts map[string]map[string]int

func (c counts) addTo(item, node string, by int) {
	if c[item] == nil {
		c[item] = map[string]int{}
	}
	c[item][node] += by
}

func (c counts) maxFrom(o counts) {
	for item, perNode := range o {
		if c[item] == nil {
			c[item] = map[string]int{}
		}
		for id, v := range perNode {
			c[item][id] = max(c[item][id], v)
		}
	}
}

func (c counts) sum(item string) int {
	total := 0
	for _, v := range c[item] {
		total += v
	}
	return total
}

type state struct {
	Tags    map[string]map[string]bool `json:"tags"`    // item -> set of tags
	Removed map[string]bool            `json:"removed"` // tombstoned tags
	Up      counts                     `json:"up"`
	Down    counts                     `json:"down"`
}

func (s *state) present(item string) bool {
	for t := range s.Tags[item] {
		if !s.Removed[t] {
			return true
		}
	}
	return false
}

func (s *state) addTag(item, tag string) {
	if s.Tags[item] == nil {
		s.Tags[item] = map[string]bool{}
	}
	s.Tags[item][tag] = true
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	st := state{Tags: map[string]map[string]bool{}, Removed: map[string]bool{}, Up: counts{}, Down: counts{}}
	counter := 0

	n.OnStart(func(ctx context.Context) {
		n.Every(100*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			body, _ := json.Marshal(map[string]any{"type": "merge", "tags": st.Tags, "removed": st.Removed, "up": st.Up, "down": st.Down})
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
		st.addTag(req.Item, fmt.Sprintf("%s:%d", n.ID(), counter))
		if req.Qty >= 0 {
			st.Up.addTo(req.Item, n.ID(), req.Qty)
		} else {
			st.Down.addTo(req.Item, n.ID(), -req.Qty)
		}
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
		for t := range st.Tags[req.Item] {
			st.Removed[t] = true
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "remove_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		value := map[string]int{}
		for item := range st.Tags {
			if st.present(item) {
				value[item] = st.Up.sum(item) - st.Down.sum(item)
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("merge", func(ctx context.Context, msg quorumlab.Message) error {
		var other state
		if err := msg.Decode(&other); err != nil {
			return err
		}
		mu.Lock()
		for item, ts := range other.Tags {
			for t := range ts {
				st.addTag(item, t)
			}
		}
		for t := range other.Removed {
			st.Removed[t] = true
		}
		st.Up.maxFrom(other.Up)
		st.Down.maxFrom(other.Down)
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
