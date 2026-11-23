// Multi-leader writes: every node accepts writes to any key, answers at once
// and replicates afterwards. Concurrent writes to a key are settled by one
// rule every node applies: the higher timestamp [counter, nodeId] wins.
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

// ts is [counter, nodeId]. Compare counters first; on a tie the higher node
// id (as strings) wins. No two writes share a timestamp.
type ts struct {
	Counter int
	Node    string
}

func (t ts) MarshalJSON() ([]byte, error) { return json.Marshal([]any{t.Counter, t.Node}) }

func (t *ts) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if len(raw) != 2 {
		return fmt.Errorf("timestamp %s: want [counter, node]", b)
	}
	if err := json.Unmarshal(raw[0], &t.Counter); err != nil {
		return err
	}
	return json.Unmarshal(raw[1], &t.Node)
}

func (t ts) newer(o ts) bool {
	if t.Counter != o.Counter {
		return t.Counter > o.Counter
	}
	return t.Node > o.Node
}

type version struct {
	Value any
	TS    ts
}

// entry travels as [key, value, ts].
type entry struct {
	Key   string
	Value any
	TS    ts
}

func (e entry) MarshalJSON() ([]byte, error) { return json.Marshal([]any{e.Key, e.Value, e.TS}) }

func (e *entry) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if len(raw) != 3 {
		return fmt.Errorf("entry %s: want [key, value, ts]", b)
	}
	if err := json.Unmarshal(raw[0], &e.Key); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &e.Value); err != nil {
		return err
	}
	return json.Unmarshal(raw[2], &e.TS)
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	store := map[string]version{}
	clock := 0 // a Lamport clock

	// merge is the same everywhere and in any order, so nodes converge.
	// The caller holds mu.
	merge := func(e entry) {
		// Lamport rule: never fall behind a timestamp we have seen. Then a write
		// accepted here always beats every value this node already had.
		clock = max(clock, e.TS.Counter)
		if cur, ok := store[e.Key]; !ok || e.TS.newer(cur.TS) {
			store[e.Key] = version{e.Value, e.TS}
		}
	}

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
		e := entry{req.Key, req.Value, ts{clock, n.ID()}}
		merge(e)
		mu.Unlock()
		if err := n.Reply(msg, map[string]any{"type": "write_ok", "ts": e.TS}); err != nil {
			return err
		}
		for _, peer := range n.Peers() {
			n.Send(ctx, peer, map[string]any{"type": "replicate", "entries": []entry{e}})
		}
		return nil
	})

	n.Handle("replicate", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Entries []entry `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		for _, e := range req.Entries {
			merge(e)
		}
		mu.Unlock()
		return nil
	})

	// Anti-entropy: messages lost during a partition are never resent, so every
	// 100ms each node sends its whole (small) store to every peer.
	n.OnStart(func(ctx context.Context) {
		n.Every(100*time.Millisecond, func(ctx context.Context) {
			mu.Lock()
			all := make([]entry, 0, len(store))
			for k, v := range store {
				all = append(all, entry{k, v.Value, v.TS})
			}
			mu.Unlock()
			if len(all) == 0 {
				return
			}
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, map[string]any{"type": "replicate", "entries": all})
			}
		})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key].Value // a missing key reads as nil: null
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
