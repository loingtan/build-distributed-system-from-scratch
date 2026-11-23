// Serializable snapshot isolation (SSI).
//
// Snapshot isolation with first-committer-wins, plus one more check. A
// transaction R that reads a key which a concurrent transaction W writes has
// an rw-antidependency R -> W: R must come before W in any serial order,
// because it did not see W's write. Every cycle that snapshot isolation lets
// through contains a "pivot": a transaction with an rw edge in and an rw edge
// out, both to concurrent transactions. So we never let a pivot commit.
//
// Edges are recorded when the second of the two transactions commits. When a
// commit would give some committed transaction (or itself) both an incoming
// and an outgoing rw edge, the committing transaction is aborted instead.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"quorumlab"
)

type request struct {
	Txn   string          `json:"txn"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type version struct {
	ts    int
	value json.RawMessage
}

type txn struct {
	snap   int
	writes map[string]json.RawMessage
	reads  map[string]bool
}

// done is a committed transaction kept while an open one may overlap it.
type done struct {
	commitTs    int
	reads       map[string]bool
	writes      map[string]bool
	rwIn, rwOut bool
}

// store is the whole database; one mutex guards it.
type store struct {
	mu       sync.Mutex
	versions map[string][]version // key -> versions, oldest first
	clock    int                  // timestamp of the newest commit
	txns     map[string]*txn
	recent   []*done
}

func (s *store) visible(key string, snap int) json.RawMessage {
	list := s.versions[key]
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].ts <= snap {
			return list[i].value
		}
	}
	return nil
}

// collect drops every version that no open snapshot can see: for each key it
// keeps the newest version at or below the oldest snapshot, and everything newer.
func (s *store) collect() {
	oldest := s.clock
	for _, t := range s.txns {
		oldest = min(oldest, t.snap)
	}
	for key, list := range s.versions {
		keep := 0
		for i, v := range list {
			if v.ts <= oldest {
				keep = i
			}
		}
		if keep > 0 {
			s.versions[key] = append([]version(nil), list[keep:]...)
		}
	}
	// A committed transaction matters only while some open one began before it committed.
	kept := s.recent[:0]
	for _, c := range s.recent {
		if c.commitTs > oldest {
			kept = append(kept, c)
		}
	}
	s.recent = kept
}

func overlaps[V any](set map[string]bool, keys map[string]V) bool {
	for k := range keys {
		if set[k] {
			return true
		}
	}
	return false
}

// check decides whether t may commit. If so it returns the concurrent
// committed transactions t gets an rw edge to (outTo) and from (inFrom).
func (s *store) check(t *txn) (ok bool, outTo, inFrom []*done) {
	for _, c := range s.recent {
		if c.commitTs <= t.snap {
			continue // committed before t began: not concurrent
		}
		if overlaps(c.writes, t.writes) {
			return false, nil, nil // first committer wins
		}
		if overlaps(c.writes, t.reads) {
			outTo = append(outTo, c) // t read what c wrote: t -> c
		}
		if overlaps(c.reads, t.writes) {
			inFrom = append(inFrom, c) // c read what t writes: c -> t
		}
	}
	if len(outTo) > 0 && len(inFrom) > 0 {
		return false, nil, nil // t itself would be a pivot
	}
	for _, c := range outTo {
		if c.rwOut {
			return false, nil, nil // c would have t -> c in and c -> x out
		}
	}
	for _, c := range inFrom {
		if c.rwIn {
			return false, nil, nil // c would have x -> c in and c -> t out
		}
	}
	return true, outTo, inFrom
}

func main() {
	n := quorumlab.NewNode()
	s := &store{versions: map[string][]version{}, txns: map[string]*txn{}}
	aborted := map[string]any{"type": "aborted"}

	decode := func(msg quorumlab.Message) (request, error) {
		var req request
		err := msg.Decode(&req)
		return req, err
	}

	n.Handle("begin", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.txns[req.Txn] = &txn{snap: s.clock, writes: map[string]json.RawMessage{}, reads: map[string]bool{}}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "begin_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		s.mu.Lock()
		t, ok := s.txns[req.Txn]
		var value json.RawMessage
		if ok {
			t.reads[req.Key] = true
			if v, mine := t.writes[req.Key]; mine {
				value = v
			} else {
				value = s.visible(req.Key, t.snap)
			}
		}
		s.mu.Unlock()
		if !ok {
			return n.Reply(msg, aborted)
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		s.mu.Lock()
		t, ok := s.txns[req.Txn]
		if ok {
			t.writes[req.Key] = req.Value
		}
		s.mu.Unlock()
		if !ok {
			return n.Reply(msg, aborted)
		}
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	n.Handle("commit", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		s.mu.Lock()
		t, ok := s.txns[req.Txn]
		var outTo, inFrom []*done
		if ok {
			ok, outTo, inFrom = s.check(t)
			if !ok {
				delete(s.txns, req.Txn)
				s.collect()
			}
		}
		if ok {
			s.clock++
			writes := map[string]bool{}
			for k, v := range t.writes {
				s.versions[k] = append(s.versions[k], version{ts: s.clock, value: v})
				writes[k] = true
			}
			for _, c := range outTo {
				c.rwIn = true
			}
			for _, c := range inFrom {
				c.rwOut = true
			}
			s.recent = append(s.recent, &done{commitTs: s.clock, reads: t.reads, writes: writes, rwIn: len(inFrom) > 0, rwOut: len(outTo) > 0})
			delete(s.txns, req.Txn)
			s.collect()
		}
		s.mu.Unlock()
		if !ok {
			return n.Reply(msg, aborted)
		}
		return n.Reply(msg, map[string]any{"type": "commit_ok"})
	})

	n.Handle("abort", func(ctx context.Context, msg quorumlab.Message) error {
		req, err := decode(msg)
		if err != nil {
			return err
		}
		s.mu.Lock()
		delete(s.txns, req.Txn)
		s.collect()
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "abort_ok"})
	})

	n.Handle("versions", func(ctx context.Context, msg quorumlab.Message) error {
		s.mu.Lock()
		count := 0
		for _, list := range s.versions {
			count += len(list)
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "versions_ok", "count": count})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
