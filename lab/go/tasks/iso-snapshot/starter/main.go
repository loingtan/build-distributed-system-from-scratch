// Snapshot isolation. Read the brief: npm run lab -- brief iso-snapshot
//
// This is a read-committed store: each read returns the latest committed
// value. A transaction that reads twice, or reads two related keys, can see a
// commit land in between (a non-repeatable read, read skew).
//
// TODO: keep every committed write as a version stamped with a commit
// timestamp, give each transaction a snapshot when it begins, and read the
// newest version at or below the snapshot.
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
		// TODO: remember which commits this transaction may see.
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
			// TODO: read from the transaction's snapshot, not the latest commit.
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
			// TODO: add new versions instead of overwriting, then drop versions
			// no open snapshot can see.
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
		mu.Lock()
		delete(txns, req.Txn)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "abort_ok"})
	})

	n.Handle("versions", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: count every version still stored, across all keys.
		mu.Lock()
		count := len(committed)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "versions_ok", "count": count})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
