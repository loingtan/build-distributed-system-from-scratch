// Multi-node log: every node serves the same logs, because all shared state
// lives in the linearizable store:
//
//	next/<key>          the next free offset for that key
//	log/<key>/<offset>  one message
//	commit/<key>        the committed offset
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"quorumlab"
)

func main() {
	n := quorumlab.NewNode()
	kv := n.KV()

	// Entries never change once written, so reads of them can be cached forever.
	var mu sync.Mutex
	cache := map[string]any{}

	entryKey := func(key string, offset int) string { return fmt.Sprintf("log/%s/%d", key, offset) }

	// readInt reads an integer key; ok is false if the key is missing.
	readInt := func(ctx context.Context, key string) (value int, ok bool, err error) {
		ok, err = kv.Read(ctx, key, &value)
		return value, ok, err
	}

	// orNil turns a missing key into nil, the "from" that means "only if the
	// key does not exist yet".
	orNil := func(value int, ok bool) any {
		if !ok {
			return nil
		}
		return value
	}

	// claimOffset claims an offset with compare-and-swap. If another node
	// claims it first the cas fails and we try again with the value it left
	// behind.
	claimOffset := func(ctx context.Context, key string) (int, error) {
		for {
			current, ok, err := readInt(ctx, "next/"+key)
			if err != nil {
				return 0, err
			}
			won, err := kv.CAS(ctx, "next/"+key, orNil(current, ok), current+1)
			if err != nil {
				return 0, err
			}
			if won {
				return current, nil
			}
		}
	}

	// entry returns the message at offset, or ok=false if none is written there.
	entry := func(ctx context.Context, key string, offset int) (any, bool, error) {
		id := entryKey(key, offset)
		mu.Lock()
		v, ok := cache[id]
		mu.Unlock()
		if ok {
			return v, true, nil
		}
		ok, err := kv.Read(ctx, id, &v)
		if err != nil || !ok {
			return nil, false, err
		}
		mu.Lock()
		cache[id] = v
		mu.Unlock()
		return v, true, nil
	}

	n.Handle("send", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
			Msg any    `json:"msg"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		offset, err := claimOffset(ctx, req.Key)
		if err != nil {
			return err
		}
		id := entryKey(req.Key, offset)
		if err := kv.Write(ctx, id, req.Msg); err != nil {
			return err
		}
		mu.Lock()
		cache[id] = req.Msg
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
		msgs := map[string][][2]any{}
		for key, from := range req.Offsets {
			list := [][2]any{}
			// Stop at the first gap: an offset may be claimed but not written yet.
			for offset := max(from, 0); ; offset++ {
				v, ok, err := entry(ctx, key, offset)
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				list = append(list, [2]any{offset, v})
			}
			msgs[key] = list
		}
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		for key, offset := range req.Offsets {
			// Move the committed offset forward only, with cas so that two
			// nodes committing at once cannot move it backwards.
			for {
				current, ok, err := readInt(ctx, "commit/"+key)
				if err != nil {
					return err
				}
				if ok && current >= offset {
					break
				}
				won, err := kv.CAS(ctx, "commit/"+key, orNil(current, ok), offset)
				if err != nil {
					return err
				}
				if won {
					break
				}
			}
		}
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Keys []string `json:"keys"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		offsets := map[string]int{}
		for _, key := range req.Keys {
			value, ok, err := readInt(ctx, "commit/"+key)
			if err != nil {
				return err
			}
			if ok {
				offsets[key] = value
			}
		}
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
