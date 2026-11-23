// Anti-entropy: besides read repair, every replica regularly compares
// versions with a random peer, so missed writes arrive even for keys nobody
// reads.
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
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

// version is an entry without its value: all a sync round sends at first.
type version struct {
	Counter int    `json:"counter"`
	Origin  string `json:"origin"`
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
// (nil if it holds none). From is the replica that sent it.
type answer struct {
	From  string `json:"-"`
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
	// of them have come in, or nil once that has become impossible. Each
	// answer is tagged with its sender (From). The slower replicas are still
	// listened to after that: once every replica has answered or timed out,
	// whenAll (if not nil) is called with the complete list of answers.
	ask := func(ctx context.Context, req request, need int, timeout time.Duration, whenAll func([]answer)) []answer {
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
				a.From = peer
				answers <- &a
			}()
		}
		mine := local(req) // the coordinator is a replica too
		mine.From = n.ID()
		answers <- &mine

		// The collector outlives the caller's wait: it hands over the first
		// need answers, then keeps going until every replica is accounted for.
		enough := make(chan []answer, 1)
		go func() {
			var got []answer
			handed := false
			for range len(peers) + 1 {
				if a := <-answers; a != nil {
					got = append(got, *a)
				}
				if !handed && len(got) >= need {
					enough <- slices.Clone(got)
					handed = true
				}
			}
			if !handed {
				enough <- nil
			}
			if whenAll != nil {
				whenAll(got)
			}
		}()
		return <-enough
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
		seen := ask(ctx, request{Type: "read", Key: req.Key}, R, 300*time.Millisecond, nil)
		if seen == nil {
			return n.Reply(msg, map[string]any{"type": "error", "text": fmt.Sprintf("fewer than %d replicas answered", R)})
		}
		e := &entry{Value: req.Value, Counter: 1, Origin: n.ID()}
		if latest := newest(seen); latest != nil {
			e.Counter = latest.Counter + 1
		}

		acks := ask(ctx, request{Type: "store", Key: req.Key, Entry: e}, W, 300*time.Millisecond, nil)
		if acks == nil {
			return n.Reply(msg, map[string]any{"type": "error", "text": fmt.Sprintf("fewer than %d replicas acknowledged the write", W)})
		}
		return n.Reply(msg, map[string]any{"type": "put_ok"})
	})

	// Read repair: every replica that answered with something older than the
	// newest version gets that version sent back, unchanged. The replica's own
	// version check makes this safe to repeat or to lose.
	repair := func(ctx context.Context, key string, answers []answer) {
		latest := newest(answers)
		if latest == nil {
			return
		}
		fix := request{Type: "store", Key: key, Entry: latest}
		for _, a := range answers {
			if !newer(latest, a.Entry) {
				continue
			}
			if a.From == n.ID() {
				local(fix) // the coordinator may be the stale one
				continue
			}
			go n.Call(ctx, a.From, fix, 300*time.Millisecond)
		}
	}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// The client is answered after R replies; the repair waits for the rest.
		answers := ask(ctx, request{Type: "read", Key: req.Key}, R, 300*time.Millisecond, func(all []answer) {
			repair(ctx, req.Key, all)
		})
		if answers == nil {
			return n.Reply(msg, map[string]any{"type": "error", "text": fmt.Sprintf("fewer than %d replicas answered", R)})
		}
		var value any // null for a key that was never written
		if latest := newest(answers); latest != nil {
			value = latest.Value
		}
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

	// Anti-entropy: a few times a second, compare versions with one random
	// peer. Only the versions travel; values are sent just for the keys that
	// differ. A lost round needs no retry: the next one covers it.
	syncWith := func(ctx context.Context, peer string) {
		mu.Lock()
		versions := make(map[string]version, len(store))
		for key, e := range store {
			versions[key] = version{Counter: e.Counter, Origin: e.Origin}
		}
		mu.Unlock()
		res, err := n.Call(ctx, peer, map[string]any{"type": "sync", "versions": versions}, 200*time.Millisecond)
		if err != nil {
			return // unreachable for now; some later round will get through
		}
		var reply struct {
			Entries map[string]*entry `json:"entries"`
			Wanted  []string          `json:"wanted"`
		}
		if err := res.Decode(&reply); err != nil {
			return
		}
		for key, e := range reply.Entries {
			local(request{Type: "store", Key: key, Entry: e})
		}
		if len(reply.Wanted) == 0 {
			return
		}
		entries := map[string]*entry{}
		mu.Lock()
		for _, key := range reply.Wanted {
			entries[key] = store[key]
		}
		mu.Unlock()
		n.Send(ctx, peer, map[string]any{"type": "sync_push", "entries": entries})
	}

	n.OnStart(func(ctx context.Context) {
		n.Every(200*time.Millisecond, func(ctx context.Context) {
			peers := n.Peers()
			if len(peers) == 0 {
				return
			}
			syncWith(ctx, peers[int(n.Rand()*float64(len(peers)))])
		})
	})

	n.Handle("sync", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Versions map[string]*entry `json:"versions"` // versions only: Value stays nil
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		entries := map[string]*entry{} // what we hold that is newer than the sender's copy
		wanted := []string{}           // keys where the sender is ahead of us
		mu.Lock()
		for key, e := range store {
			if newer(e, req.Versions[key]) {
				entries[key] = e
			}
		}
		for key, v := range req.Versions {
			if newer(v, store[key]) {
				wanted = append(wanted, key)
			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "sync_ok", "entries": entries, "wanted": wanted})
	})

	n.Handle("sync_push", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Entries map[string]*entry `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		for key, e := range req.Entries {
			local(request{Type: "store", Key: key, Entry: e})
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
