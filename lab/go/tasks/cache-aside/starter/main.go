// Cache-aside. Read the brief: npm run lab -- brief cache-aside
//
// Every get goes straight to the database, which answers in 30-60ms.
// Most clients ask for the same few keys, so most of those reads are repeats.
//
// TODO: keep the values you have read in memory (a map guarded by a mutex is
// enough) and answer repeated gets from there. Only a key you have never read
// needs the database.
package main

import (
	"context"
	"log"
	"time"

	"quorumlab"
)

const dbTimeout = 1000 * time.Millisecond

func main() {
	n := quorumlab.NewNode()

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		res, err := n.Call(ctx, "db", map[string]any{"type": "read", "key": req.Key}, dbTimeout)
		var body struct {
			Value any `json:"value"`
		}
		if err != nil || res.Type() != "read_ok" || res.Decode(&body) != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": body.Value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
