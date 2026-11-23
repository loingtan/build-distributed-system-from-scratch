// Multi-node log. Read the brief: npm run lab -- brief log-multi
//
// Your single-node log. Each node has its own maps, so three nodes means
// three different logs. n.KV() is a store all nodes share:
//
//	ok, err := n.KV().Read(ctx, key, &v)        // ok is false if the key is missing
//	err := n.KV().Write(ctx, key, value)
//	won, err := n.KV().CAS(ctx, key, from, to)  // true if it held from and now holds to; from nil = "missing"
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
	logs := map[string][]any{}
	committed := map[string]int{}

	n.Handle("send", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
			Msg any    `json:"msg"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		logs[req.Key] = append(logs[req.Key], req.Msg)
		offset := len(logs[req.Key]) - 1
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		msgs := map[string][][2]any{}
		for key, from := range req.Offsets {
			list := [][2]any{}
			l := logs[key]
			for off := max(from, 0); off < len(l); off++ {
				list = append(list, [2]any{off, l[off]})
			}
			msgs[key] = list
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		// Committed offsets never move backwards.
		for key, offset := range req.Offsets {
			committed[key] = max(committed[key], offset)
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Keys []string `json:"keys"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		offsets := map[string]int{}
		for _, key := range req.Keys {
			if off, ok := committed[key]; ok {
				offsets[key] = off
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
