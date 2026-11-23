// Key-range partitioning. Read the brief: npm run lab -- brief shard-key-range
package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"quorumlab"
)

const maxKeys = 40 // a range holding more keys than this is split

// pair is one stored key and its value, sent as the JSON array [key, value].
type pair struct {
	Key   string
	Value json.RawMessage
}

func (p pair) MarshalJSON() ([]byte, error) { return json.Marshal([2]any{p.Key, p.Value}) }

func (p *pair) UnmarshalJSON(b []byte) error {
	var a [2]json.RawMessage
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	p.Value = a[1]
	return json.Unmarshal(a[0], &p.Key)
}

// node state, guarded by mu.
type state struct {
	mu    sync.Mutex
	store map[string]json.RawMessage // the keys this node owns
	// The range table, the same on every node once the splits have been
	// announced: [from, owner] sorted by from. A range runs from its from up
	// to the next entry's from; the first starts at "" and the last runs to
	// the end. At first one range holds everything, owned by the first node.
	table [][2]string
}

// rangeIndex is the range holding key: the last entry whose from <= key.
func (s *state) rangeIndex(key string) int {
	return sort.Search(len(s.table), func(i int) bool { return s.table[i][0] > key }) - 1
}

func (s *state) ownerOf(key string) string { return s.table[s.rangeIndex(key)][1] }

// addBoundary: from `from` up to the next boundary now belongs to owner.
// Boundaries are only ever added, never moved, so it does not matter in
// which order a node hears about two splits.
func (s *state) addBoundary(from, owner string) {
	i := s.rangeIndex(from)
	if s.table[i][0] == from {
		return
	}
	s.table = slices.Insert(s.table, i+1, [2]string{from, owner})
}

// localScan is what this node itself stores in [from, to).
func (s *state) localScan(from, to string) []pair {
	out := []pair{}
	for k, v := range s.store {
		if k >= from && k < to {
			out = append(out, pair{k, v})
		}
	}
	return out
}

func sortPairs(ps []pair) {
	slices.SortFunc(ps, func(a, b pair) int { return strings.Compare(a.Key, b.Key) })
}

func main() {
	n := quorumlab.NewNode()
	s := &state{store: map[string]json.RawMessage{}}
	n.OnStart(func(ctx context.Context) {
		s.mu.Lock()
		s.table = [][2]string{{"", n.NodeIDs()[0]}}
		s.mu.Unlock()
	})

	// maybeSplit is called by the owner after a put.
	// TODO: if the range holding key now has more than maxKeys keys, cut it at
	// its middle key, give the upper half (boundary and keys) to another node,
	// and announce the new boundary to every node. Until then one node owns
	// everything.
	maybeSplit := func(ctx context.Context, key string) {}

	n.Handle("split", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			From  string `json:"from"`
			Owner string `json:"owner"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		s.addBoundary(req.From, req.Owner)
		s.mu.Unlock()
		return nil
	})

	// The new owner learns of the split from the data itself. A key already
	// here came from a put after the split, so it is newer.
	n.Handle("take", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			From    string `json:"from"`
			Entries []pair `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		s.addBoundary(req.From, n.ID())
		for _, e := range req.Entries {
			if _, ok := s.store[e.Key]; !ok {
				s.store[e.Key] = e.Value
			}
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "take_ok"})
	})

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		owner := s.ownerOf(req.Key)
		if owner == n.ID() {
			s.store[req.Key] = req.Value
		}
		s.mu.Unlock()
		if owner != n.ID() {
			if _, err := n.Call(ctx, owner, map[string]any{"type": "put", "key": req.Key, "value": req.Value}, time.Second); err != nil {
				return nil
			}
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		}
		maybeSplit(ctx, req.Key)
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		owner := s.ownerOf(req.Key)
		v, ok := s.store[req.Key]
		s.mu.Unlock()
		if owner == n.ID() {
			if !ok {
				v = json.RawMessage("null")
			}
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": v})
		}
		res, err := n.Call(ctx, owner, map[string]any{"type": "get", "key": req.Key}, time.Second)
		if err != nil {
			return nil
		}
		var body struct {
			Value json.RawMessage `json:"value"`
		}
		if err := res.Decode(&body); err != nil {
			return err
		}
		if body.Value == nil {
			body.Value = json.RawMessage("null")
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": body.Value})
	})

	type scanReq struct {
		From string `json:"from"`
		To   string `json:"to"`
	}

	n.Handle("scan_local", func(ctx context.Context, msg quorumlab.Message) error {
		var req scanReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		entries := s.localScan(req.From, req.To)
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "scan_local_ok", "entries": entries})
	})

	// For now a scan asks every node for what it has in the range.
	// TODO: ask only the owners of the ranges that overlap [from, to).
	n.Handle("scan", func(ctx context.Context, msg quorumlab.Message) error {
		var req scanReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		owners := n.NodeIDs()
		s.mu.Unlock()
		parts := make([][]pair, len(owners))
		failed := false
		var wg sync.WaitGroup
		var fmu sync.Mutex
		for i, owner := range owners {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if owner == n.ID() {
					s.mu.Lock()
					parts[i] = s.localScan(req.From, req.To)
					s.mu.Unlock()
					return
				}
				res, err := n.Call(ctx, owner, map[string]any{"type": "scan_local", "from": req.From, "to": req.To}, time.Second)
				var body struct {
					Entries []pair `json:"entries"`
				}
				if err == nil {
					err = res.Decode(&body)
				}
				fmu.Lock()
				defer fmu.Unlock()
				if err != nil {
					failed = true
					return
				}
				parts[i] = body.Entries
			}()
		}
		wg.Wait()
		if failed {
			return nil
		}
		entries := slices.Concat(parts...)
		if entries == nil {
			entries = []pair{}
		}
		sortPairs(entries)
		return n.Reply(msg, map[string]any{"type": "scan_ok", "entries": entries})
	})

	n.Handle("ranges", func(ctx context.Context, msg quorumlab.Message) error {
		s.mu.Lock()
		table := slices.Clone(s.table)
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "ranges_ok", "ranges": table})
	})

	n.Handle("local_keys", func(ctx context.Context, msg quorumlab.Message) error {
		s.mu.Lock()
		keys := make([]string, 0, len(s.store))
		for k := range s.store {
			keys = append(keys, k)
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "local_keys_ok", "keys": keys})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
