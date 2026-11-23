// Read repair. Read the brief: npm run lab -- brief quorum-read-repair
//
// Leaderless replication, step 3: read repair.
// This is the quorum store from the previous task. A get already hears from
// the replicas and can tell which of them are behind, but it does nothing
// about it, so a replica that missed a write stays stale forever.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// entry is a value with its version. (Counter, Origin) is the version: the
// higher counter is newer, and Origin, the coordinator's node id, breaks ties.
type entry struct {
	Value   any    `json:"value"`
	Counter int    `json:"counter"`
	Origin  string `json:"origin"`
}

// newer reports whether a is a newer version than b. Having no entry (nil)
// is older than any entry.
func newer(a, b *entry) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	return a.Counter > b.Counter || (a.Counter == b.Counter && a.Origin > b.Origin)
}

// newest is the entry with the highest version among the answers, or nil.
func newest(answers []answer) *entry {
	var best *entry
	for _, a := range answers {
		if newer(a.Entry, best) {
			best = a.Entry
		}
	}
	return best
}

// request is what a coordinator asks of a replica: "store" an entry, or
// "read" its copy of a key.
type request struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Entry *entry `json:"entry,omitempty"`
}

// answer is a replica's reply: "store_ok", or "read_ok" with its entry
// (nil if it holds none).
type answer struct {
	Type  string `json:"type"`
	Entry *entry `json:"entry,omitempty"`
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

	var mu sync.Mutex
	store := map[string]*entry{} // key -> {value, counter, origin}

	// What this node does as a replica, whether the request came over the
	// network or from itself acting as coordinator. Entries are never changed
	// once made, only replaced, so they can be shared without the lock.
	local := func(req request) answer {
		mu.Lock()
		defer mu.Unlock()
		if req.Type == "store" {
			// Writes can arrive late or twice; only a newer version replaces ours.
			if newer(req.Entry, store[req.Key]) {
				store[req.Key] = req.Entry
			}
			return answer{Type: "store_ok"}
		}
		return answer{Type: "read_ok", Entry: store[req.Key]}
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
		// This node may have missed writes, so its own copy cannot tell it which
		// version comes next. R replicas together can: one of them saw the last write.
		seen := ask(ctx, request{Type: "read", Key: req.Key}, R, 300*time.Millisecond)
		if seen == nil {
			return n.Reply(msg, map[string]any{"type": "error", "text": fmt.Sprintf("fewer than %d replicas answered", R)})
		}
		e := &entry{Value: req.Value, Counter: 1, Origin: n.ID()}
		if latest := newest(seen); latest != nil {
			e.Counter = latest.Counter + 1
		}

		acks := ask(ctx, request{Type: "store", Key: req.Key, Entry: e}, W, 300*time.Millisecond)
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
		answers := ask(ctx, request{Type: "read", Key: req.Key}, R, 300*time.Millisecond)
		if answers == nil {
			return n.Reply(msg, map[string]any{"type": "error", "text": fmt.Sprintf("fewer than %d replicas answered", R)})
		}
		var value any // null for a key that was never written
		latest := newest(answers)
		if latest != nil {
			value = latest.Value
		}
		// TODO: read repair. Send latest (same value, same version) in a "store"
		// to every replica that answered with something older.
		// Careful: ask stops listening after the first R answers, and the stale
		// replicas are often the slow ones. Reply to the client after R answers as
		// now, but keep collecting the rest, and remember who sent each answer.
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": value})
	})

	replica := func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		return n.Reply(msg, local(req))
	}
	n.Handle("store", replica)
	n.Handle("read", replica)

	n.Handle("local_get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		var value any // null for a key this replica does not hold
		mu.Lock()
		if e := store[req.Key]; e != nil {
			value = e.Value
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "local_get_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
