// Versions and quorum reads. Read the brief: npm run lab -- brief quorum-reads
//
// Leaderless replication, step 2: versions and quorum reads.
// This is the quorum write from the previous task, plus a get that only
// looks at this node's own copy. A node that missed a write answers with
// an old value, and a late "store" can overwrite a newer one.
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

	// Node ids are known once the node has started.
	var N, W, R int
	n.OnStart(func(ctx context.Context) {
		N = len(n.NodeIDs())
		W = N/2 + 1
		R = N - W + 1 // the smallest R with W + R > N
	})
	_ = R // Go rejects unused variables; delete this line once you use R

	// TODO: keep a version next to each value: key -> {value, counter, origin}.
	// (counter, origin) is the version: the higher counter is newer, and
	// origin (the coordinator's node id) breaks ties.
	var mu sync.Mutex
	store := map[string]any{} // key -> value

	// What this node does as a replica.
	local := func(req request) answer {
		mu.Lock()
		defer mu.Unlock()
		// TODO: only let a "store" replace the copy if its version is newer.
		// TODO: answer a second kind of request, "read", with this replica's
		// copy and its version.
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
		// TODO: give the write a version higher than every earlier write to this
		// key. This node may have missed some, so ask R replicas for their
		// current version first and use the highest counter plus one.
		acks := ask(ctx, request{Type: "store", Key: req.Key, Value: req.Value}, W, 300*time.Millisecond)
		if acks == nil {
			return n.Reply(msg, map[string]any{"type": "error", "text": fmt.Sprintf("fewer than %d replicas acknowledged the write", W)})
		}
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// TODO: ask every replica, wait for R answers, and return the value with
		// the highest version. Reply {"type": "error", "text": ...} if R do not answer.
		mu.Lock()
		value := store[req.Key]
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
	})

	n.Handle("store", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, local(req))
	})

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
