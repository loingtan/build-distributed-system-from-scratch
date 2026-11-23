// Read committed: a transaction's writes stay private until it commits, then
// become visible all at once.
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

type txn struct {
	writes map[string]json.RawMessage
}

func main() {
	n := quorumlab.NewNode()

	// Handlers run concurrently. One mutex makes each request, and above all
	// each commit, a single indivisible step.
	var mu sync.Mutex
	committed := map[string]json.RawMessage{} // key -> the latest committed value
	txns := map[string]*txn{}

	aborted := map[string]any{"type": "aborted"}

	n.Handle("begin", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		txns[req.Txn] = &txn{writes: map[string]json.RawMessage{}}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "begin_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		t, ok := txns[req.Txn]
		var value json.RawMessage
		if ok {
			// Own writes first, then the latest committed value. Never another
			// transaction's uncommitted write: that would be a dirty read.
			if v, mine := t.writes[req.Key]; mine {
				value = v
			} else {
				value = committed[req.Key]
			}
		}
		mu.Unlock()
		if !ok {
			return n.Reply(msg, aborted)
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		t, ok := txns[req.Txn]
		if ok {
			t.writes[req.Key] = req.Value
		}
		mu.Unlock()
		if !ok {
			return n.Reply(msg, aborted)
		}
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	n.Handle("commit", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		t, ok := txns[req.Txn]
		if ok {
			// Under the mutex every write lands before anyone reads again, so
			// no reader sees half a commit and no two commits interleave.
			for k, v := range t.writes {
				committed[k] = v
			}
			delete(txns, req.Txn)
		}
		mu.Unlock()
		if !ok {
			return n.Reply(msg, aborted)
		}
		return n.Reply(msg, map[string]any{"type": "commit_ok"})
	})

	n.Handle("abort", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// Nothing was published, so rolling back is just forgetting the buffer.
		mu.Lock()
		delete(txns, req.Txn)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "abort_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
