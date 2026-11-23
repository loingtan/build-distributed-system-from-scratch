// Read committed. Read the brief: npm run lab -- brief iso-read-committed
//
// This store applies every write the moment it arrives. Other transactions can
// read it before it commits (a dirty read), and an abort cannot take it back.
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

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	store := map[string]json.RawMessage{} // key -> value

	n.Handle("begin", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: remember the transaction, with somewhere to keep its writes.
		return n.Reply(msg, map[string]any{"type": "begin_ok"})
	})

	n.Handle("read", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: a transaction sees its own writes, and otherwise only committed values.
		mu.Lock()
		value := store[req.Key]
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
	})

	n.Handle("write", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: keep the write private to its transaction until it commits.
		mu.Lock()
		store[req.Key] = req.Value
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "write_ok"})
	})

	n.Handle("commit", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: publish the transaction's writes, all at once.
		return n.Reply(msg, map[string]any{"type": "commit_ok"})
	})

	n.Handle("abort", func(ctx context.Context, msg quorumlab.Message) error {
		// TODO: throw the transaction's writes away.
		return n.Reply(msg, map[string]any{"type": "abort_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
