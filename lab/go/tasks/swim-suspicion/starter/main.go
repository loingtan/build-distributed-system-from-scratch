// Suspicion and incarnation numbers. Read the brief: npm run lab -- brief swim-suspicion
//
// Gossip dissemination, from the previous task. A failed probe declares a
// member dead at once, and dead is final. A node that was only cut off for a
// moment is thrown out for good, and a crashed node that restarts can never
// come back.
//
// TODO: incarnation numbers and suspicion.
//   - Keep your own incarnation on disk (n.Disk(), key "incarnation"); add one
//     to it every time you start.
//   - Updates become {id, status, inc}. Higher inc wins; at equal inc
//     dead > suspect > alive. Ignore anything that does not win.
//   - A failed probe makes the target "suspect" (at its current inc). If it
//     is still suspect at that inc after suspectTimeout, declare it "dead".
//   - Hearing that YOU are suspect or dead at inc >= yours: set your
//     incarnation to that inc + 1 and spread "alive" at the new number.
//   - At start, spread "alive" about yourself so a restarted node rejoins.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"quorumlab"
)

const (
	period          = 200 * time.Millisecond
	ackTimeout      = 60 * time.Millisecond
	indirectK       = 3
	indirectTimeout = 120 * time.Millisecond
	suspectTimeout  = 2500 * time.Millisecond
	maxPiggyback    = 8
)

type update struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type entry struct {
	update
	sends int
}

// gossipBody is what every protocol message carries besides its own fields.
type gossipBody struct {
	Target  string   `json:"target"`
	OK      bool     `json:"ok"`
	Updates []update `json:"updates"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	status := map[string]string{} // node id -> "alive" | "dead"
	var buffer []*entry           // updates still being spread
	var order []string
	maxSends := 0

	shuffled := func(list []string) []string {
		out := append([]string(nil), list...)
		for i := len(out) - 1; i > 0; i-- {
			k := int(n.Rand() * float64(i+1))
			out[i], out[k] = out[k], out[i]
		}
		return out
	}

	// The callers below hold mu.
	remember := func(id, st string) {
		kept := buffer[:0]
		for _, e := range buffer {
			if e.ID != id {
				kept = append(kept, e)
			}
		}
		buffer = append(kept, &entry{update: update{id, st}})
	}

	// The entries to attach to one outgoing message: the least-sent first.
	piggyback := func() []update {
		mu.Lock()
		defer mu.Unlock()
		sort.SliceStable(buffer, func(i, j int) bool { return buffer[i].sends < buffer[j].sends })
		out := []update{}
		for i := 0; i < len(buffer) && i < maxPiggyback; i++ {
			buffer[i].sends++
			out = append(out, buffer[i].update)
		}
		kept := buffer[:0]
		for _, e := range buffer {
			if e.sends < maxSends {
				kept = append(kept, e)
			}
		}
		buffer = kept
		return out
	}

	// Without incarnation numbers, "dead" is final: it beats "alive".
	learn := func(raw json.RawMessage) gossipBody {
		var b gossipBody
		json.Unmarshal(raw, &b)
		mu.Lock()
		defer mu.Unlock()
		for _, u := range b.Updates {
			if u.ID == n.ID() || status[u.ID] == "" {
				continue
			}
			if u.Status == "dead" && status[u.ID] != "dead" {
				status[u.ID] = "dead"
				remember(u.ID, "dead")
			}
		}
		return b
	}

	notDead := func(except string) []string {
		var out []string
		for _, p := range n.Peers() {
			if p != except && status[p] != "dead" {
				out = append(out, p)
			}
		}
		return out
	}

	// Dead members are no longer probed.
	nextTarget := func() string {
		mu.Lock()
		defer mu.Unlock()
		kept := order[:0]
		for _, p := range order {
			if status[p] != "dead" {
				kept = append(kept, p)
			}
		}
		order = kept
		if len(order) == 0 {
			order = shuffled(notDead(""))
		}
		if len(order) == 0 {
			return ""
		}
		target := order[len(order)-1]
		order = order[:len(order)-1]
		return target
	}

	ping := func(ctx context.Context, target string) bool {
		reply, err := n.Call(ctx, target, map[string]any{"type": "ping", "updates": piggyback()}, ackTimeout)
		if err != nil {
			return false
		}
		learn(reply.Body)
		return true
	}

	probe := func(ctx context.Context, target string) bool {
		if ping(ctx, target) {
			return true
		}
		mu.Lock()
		helpers := shuffled(notDead(target))
		mu.Unlock()
		if len(helpers) > indirectK {
			helpers = helpers[:indirectK]
		}
		results := make(chan bool, len(helpers))
		for _, h := range helpers {
			go func(h string) {
				reply, err := n.Call(ctx, h, map[string]any{"type": "ping_req", "target": target, "updates": piggyback()}, indirectTimeout)
				results <- err == nil && learn(reply.Body).OK
			}(h)
		}
		ok := false
		for range helpers {
			if <-results {
				ok = true
			}
		}
		return ok
	}

	n.OnStart(func(ctx context.Context) {
		mu.Lock()
		for _, id := range n.NodeIDs() {
			status[id] = "alive"
		}
		maxSends = 3 * int(math.Ceil(math.Log2(float64(len(n.NodeIDs())+1))))
		mu.Unlock()

		n.Every(period, func(ctx context.Context) {
			target := nextTarget()
			if target == "" || probe(ctx, target) {
				return
			}
			mu.Lock()
			if status[target] != "dead" {
				status[target] = "dead"
				remember(target, "dead")
			}
			mu.Unlock()
		})
	})

	n.Handle("ping", func(ctx context.Context, msg quorumlab.Message) error {
		learn(msg.Body)
		return n.Reply(msg, map[string]any{"type": "ack", "updates": piggyback()})
	})

	n.Handle("ping_req", func(ctx context.Context, msg quorumlab.Message) error {
		req := learn(msg.Body)
		ok := ping(ctx, req.Target)
		return n.Reply(msg, map[string]any{"type": "ping_req_ok", "ok": ok, "updates": piggyback()})
	})

	n.Handle("members", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		members := map[string]string{}
		for id, st := range status {
			members[id] = st
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "members_ok", "members": members})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
