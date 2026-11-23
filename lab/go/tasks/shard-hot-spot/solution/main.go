// Hot spots: the owner notices a key that gets most of the traffic, and its
// increments are salted over every node while reads add up all the parts.
package main

import (
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"time"
	"unicode/utf16"

	"quorumlab"
)

const (
	hotLimit = 20 // requests for one key within one window make it hot
	window   = 200 * time.Millisecond
)

// hash is a deterministic string hash (FNV-1a with a final mix). The same
// string gives the same 32-bit number (0 .. 4294967295) on every node, every
// time. It works on UTF-16 code units, exactly like the browser version.
func hash(s string) uint32 {
	h := uint32(2166136261)
	for _, c := range utf16.Encode([]rune(s)) {
		h ^= uint32(c)
		h *= 16777619
	}
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

type keyReq struct {
	Key   string `json:"key"`
	Delta int    `json:"delta"`
	Route bool   `json:"route"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	// A counter may be split into N parts. Part i of a key lives on the node
	// (hash(key) + i) % N, so part 0 is on the owner and the parts of one key
	// land on N different nodes. This node keeps its own parts here.
	parts := map[string]int{}
	// Keys known to be hot. Only the owner decides, and a key never cools
	// down, so every part that is not on the owner was written after the
	// owner knew.
	hot := map[string]bool{}
	// The owner counts the requests it serves per key, per window.
	counts := map[string]int{}

	partNode := func(key string, i int) string {
		ids := n.NodeIDs()
		return ids[(int(hash(key)%uint32(len(ids)))+i)%len(ids)]
	}
	ownerOf := func(key string) string { return partNode(key, 0) }
	isHot := func(key string) bool {
		mu.Lock()
		defer mu.Unlock()
		return hot[key]
	}
	bump := func(key string, delta int) {
		mu.Lock()
		parts[key] += delta
		mu.Unlock()
	}
	part := func(key string) int {
		mu.Lock()
		defer mu.Unlock()
		return parts[key]
	}

	n.OnStart(func(ctx context.Context) {
		n.Every(window, func(ctx context.Context) {
			mu.Lock()
			counts = map[string]int{}
			mu.Unlock()
		})
	})

	// noticeRequest counts a request served by the owner; past the limit the
	// key is hot and every node is told.
	noticeRequest := func(ctx context.Context, key string) {
		mu.Lock()
		if hot[key] {
			mu.Unlock()
			return
		}
		counts[key]++
		becameHot := counts[key] >= hotLimit
		if becameHot {
			hot[key] = true
		}
		mu.Unlock()
		if becameHot {
			for _, peer := range n.Peers() {
				n.Send(ctx, peer, map[string]any{"type": "hot", "key": key})
			}
		}
	}

	n.Handle("hot", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		hot[req.Key] = true
		mu.Unlock()
		return nil
	})

	// sumParts asks each of the other N - 1 nodes for its part and adds them up.
	sumParts := func(ctx context.Context, key string) (int, bool) {
		ids := n.NodeIDs()
		got := make([]int, len(ids))
		ok := true
		var wg sync.WaitGroup
		var omu sync.Mutex
		for i, id := range ids {
			if id == n.ID() {
				got[i] = part(key)
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := n.Call(ctx, id, map[string]any{"type": "read_part", "key": key}, time.Second)
				var body struct {
					Value int `json:"value"`
				}
				if err == nil {
					err = res.Decode(&body)
				}
				omu.Lock()
				defer omu.Unlock()
				if err != nil {
					ok = false
					return
				}
				got[i] = body.Value
			}()
		}
		wg.Wait()
		sum := 0
		for _, v := range got {
			sum += v
		}
		return sum, ok
	}

	n.Handle("incr", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// A hot key: any part will do, so pick one at random (salting).
		keyHot := isHot(req.Key)
		dest := ownerOf(req.Key)
		if keyHot {
			dest = partNode(req.Key, rand.IntN(len(n.NodeIDs())))
		}
		if dest != n.ID() {
			body := map[string]any{"type": "incr_part", "key": req.Key, "delta": req.Delta, "route": !keyHot}
			if _, err := n.Call(ctx, dest, body, time.Second); err != nil {
				return nil
			}
			return n.Reply(msg, map[string]any{"type": "incr_ok"})
		}
		if dest == ownerOf(req.Key) {
			noticeRequest(ctx, req.Key)
		}
		bump(req.Key, req.Delta)
		return n.Reply(msg, map[string]any{"type": "incr_ok"})
	})

	// route: the sender treated the key as cold, so this is the owner, which
	// counts the request.
	n.Handle("incr_part", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if req.Route {
			noticeRequest(ctx, req.Key)
		}
		bump(req.Key, req.Delta)
		return n.Reply(msg, map[string]any{"type": "incr_part_ok"})
	})

	n.Handle("read_part", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "read_part_ok", "value": part(req.Key)})
	})

	// readAsOwner: a node that thinks the key is cold asks the owner, and the
	// owner knows best: if the key is hot, it collects all the parts itself.
	readAsOwner := func(ctx context.Context, key string) (int, bool) {
		noticeRequest(ctx, key)
		if !isHot(key) {
			return part(key), true
		}
		return sumParts(ctx, key)
	}

	n.Handle("owner_read", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		v, ok := readAsOwner(ctx, req.Key)
		if !ok {
			return nil
		}
		return n.Reply(msg, map[string]any{"type": "owner_read_ok", "value": v})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req keyReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		var v int
		ok := true
		switch {
		case isHot(req.Key):
			v, ok = sumParts(ctx, req.Key)
		case ownerOf(req.Key) == n.ID():
			v, ok = readAsOwner(ctx, req.Key)
		default:
			res, err := n.Call(ctx, ownerOf(req.Key), map[string]any{"type": "owner_read", "key": req.Key}, time.Second)
			var body struct {
				Value int `json:"value"`
			}
			if err == nil {
				err = res.Decode(&body)
			}
			v, ok = body.Value, err == nil
		}
		if !ok {
			return nil
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": v})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
