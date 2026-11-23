// Quorum writes: the coordinator sends every put to all N replicas, itself
// included, and answers put_ok once W of them have stored it.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// request is what a coordinator asks of a replica.
type request struct {
	Type  string `json:"type"` // "store"
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// answer is a replica's reply.
type answer struct {
	Type string `json:"type"` // "store_ok"
}

func main() {
	n := quorumlab.NewNode()

	// Every node is a replica of every key. W, the acknowledgements needed
	// before put_ok, is a majority. Node ids are known once the node has
	// started.
	var N, W int
	n.OnStart(func(ctx context.Context) {
		N = len(n.NodeIDs())
		W = N/2 + 1
	})

	var mu sync.Mutex
	store := map[string]any{} // key -> value: this replica's own copy

	// What this node does as a replica, whether the request came over the
	// network or from itself acting as coordinator.
	local := func(req request) answer {
		mu.Lock()
		defer mu.Unlock()
		store[req.Key] = req.Value
		return answer{Type: "store_ok"}
	}

	// ask sends req to every replica and returns the answers as soon as need
	// of them have come in, or nil once that has become impossible (every
	// replica has answered or timed out). Each Call runs in its own goroutine,
	// so one slow or dead replica holds nobody up.
	ask := func(ctx context.Context, req request, need int, timeout time.Duration) []answer {
		peers := n.Peers()
		// Room for every answer, so a late one never blocks its goroutine.
		answers := make(chan *answer, len(peers)+1)
		for _, peer := range peers {
			go func() {
				res, err := n.Call(ctx, peer, req, timeout)
				var a answer
				if err != nil || res.Decode(&a) != nil {
					answers <- nil // no answer in time
					return
				}
				answers <- &a
			}()
		}
		mine := local(req) // the coordinator is a replica too
		answers <- &mine

		var got []answer
		for range len(peers) + 1 {
			if a := <-answers; a != nil {
				got = append(got, *a)
				if len(got) >= need {
					return got
				}
			}
		}
		return nil
	}

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		acks := ask(ctx, request{Type: "store", Key: req.Key, Value: req.Value}, W, 300*time.Millisecond)
		if acks == nil {
			return n.Reply(msg, map[string]any{"type": "error", "text": fmt.Sprintf("fewer than %d replicas acknowledged the write", W)})
		}
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("store", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, local(req))
	})

	// Debug request used by the tests: this replica's own copy, no quorum.
	n.Handle("local_get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		value := store[req.Key] // nil (null) for a key this replica does not hold
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "local_get_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
