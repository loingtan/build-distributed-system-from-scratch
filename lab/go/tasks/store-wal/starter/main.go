// Write-ahead log. Read the brief: npm run lab -- brief store-wal
//
// A key-value store that lives in memory. Fast, and empty after every crash.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	data := map[string]string{}

	// n.Disk() is the only thing that survives a crash.
	// TODO: keep a log of changes there, under the key "log" (a JSON array):
	//       n.Disk().Set("log", entries). Nothing is durable until Set returns.
	// TODO: at startup (n.OnStart runs again after a restart), read the log
	//       back with n.Disk().Get("log", &entries) and replay it to rebuild data.

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: append a record of this change to the log on disk before replying.
		mu.Lock()
		data[req.Key] = req.Value
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("delete", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: a delete is a change too. Log it.
		mu.Lock()
		delete(data, req.Key)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "delete_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value, ok := data[req.Key]
		mu.Unlock()
		if !ok {
			return n.Reply(msg, map[string]any{"type": "get_ok", "value": nil})
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
