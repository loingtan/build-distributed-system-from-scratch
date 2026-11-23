// Observed-remove set. Every add creates a unique tag. A remove deletes the
// tags this replica has seen for that element, and only those. An element
// is present while at least one of its tags has not been removed.
//
// Both tables only grow (tags are added, tombstones are added), so merging
// is a union, exactly as in the grow-only set.
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
	Tags    map[string]map[string]bool `json:"tags"`    // element -> set of tags
	Removed map[string]bool            `json:"removed"` // tombstoned tags
}

func (s *state) present(element string) bool {
	for t := range s.Tags[element] {
		if !s.Removed[t] {
			return true
		}
	}
	return false
}

func (s *state) addTag(element, tag string) {
	if s.Tags[element] == nil {
		s.Tags[element] = map[string]bool{}
	}
	s.Tags[element][tag] = true
}

// mergeFrom is a union of both tables.
func (s *state) mergeFrom(o state) {
	for e, ts := range o.Tags {
		for t := range ts {
			s.addTag(e, t)
		}
	}
	for t := range o.Removed {
		s.Removed[t] = true
	}
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	st := state{Tags: map[string]map[string]bool{}, Removed: map[string]bool{}}
	counter := 0

	n.OnStart(func(ctx context.Context) {
		n.Every(100*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			body, _ := json.Marshal(map[string]any{"type": "merge", "tags": st.Tags, "removed": st.Removed})
			mu.Unlock()
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, json.RawMessage(body))
			}
		})
	})

	n.Handle("add", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Element string `json:"element"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		// A fresh tag even when the element is already present: this add
		// must survive a concurrent remove that could not have seen it.
		counter++
		st.addTag(req.Element, fmt.Sprintf("%s:%d", n.ID(), counter))
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("remove", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Element string `json:"element"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		for t := range st.Tags[req.Element] {
			st.Removed[t] = true
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "remove_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		value := []string{}
		for e := range st.Tags {
			if st.present(e) {
				value = append(value, e)
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
		st.mergeFrom(other)
		mu.Unlock()
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
