// Preventing lost updates. Read the brief: npm run lab -- brief iso-lost-updates
//
// This is the snapshot-isolation store from the previous task. Two
// transactions that read the same counter from their snapshots and both write
// it back plus one both commit, and one increment is lost.
//
// TODO: at commit, abort a transaction if any key it wrote has a version
// committed after its snapshot (first committer wins).
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
		// TODO: first committer wins. Check t.writes against newer versions here.
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
