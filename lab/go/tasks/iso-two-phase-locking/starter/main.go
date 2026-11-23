// Two-phase locking. Read the brief: npm run lab -- brief iso-two-phase-locking
//
// This is the snapshot-isolation store with first-committer-wins from the
// previous task. It never loses an update, but two doctors who each read the
// whole shift and each take only themselves off call write different keys:
// nothing conflicts, both commit, and nobody is on call (write skew).
//
// TODO: replace snapshots with strict two-phase locking.
//   - a read takes a shared lock on its key, a write an exclusive lock;
//     locks are held until commit or abort
//   - a request whose lock is held by another transaction waits for it
//   - when waiting would form a cycle (a deadlock), abort one transaction
//   - the "versions" message is not used in this task
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
}

// store is the whole database; one mutex guards it.
type store struct {
	mu       sync.Mutex
	versions map[string][]version // key -> versions, oldest first
	clock    int                  // timestamp of the newest commit
	txns     map[string]*txn
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
}

// conflicts reports whether another transaction committed a key t wrote after t's snapshot.
func (s *store) conflicts(t *txn) bool {
	for k := range t.writes {
		list := s.versions[k]
		if len(list) > 0 && list[len(list)-1].ts > t.snap {
			return true
		}
	}
	return false
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
		s.txns[req.Txn] = &txn{snap: s.clock, writes: map[string]json.RawMessage{}}
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
		if ok && s.conflicts(t) {
			delete(s.txns, req.Txn)
			s.collect()
			ok = false
		}
		if ok {
			s.clock++
			for k, v := range t.writes {
				s.versions[k] = append(s.versions[k], version{ts: s.clock, value: v})
			}
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
