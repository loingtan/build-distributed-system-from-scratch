// SWIM, part 4: suspicion and incarnation numbers.
//
// A failed probe no longer kills a member; it makes it a suspect. A suspect
// that is alive hears the rumour about itself and refutes it by bumping its
// incarnation number and spreading "alive" at the new number. A suspect
// nobody vouches for within suspectTimeout is declared dead.
//
// Every update carries (status, incarnation). Of two updates about the same
// member, the one with the higher incarnation wins; at equal incarnations
// dead beats suspect beats alive. That order is all the merge logic needed.
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

var rank = map[string]int{"alive": 0, "suspect": 1, "dead": 2}

type update struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Inc    int    `json:"inc"`
}

type entry struct {
	update
	sends int
}

type member struct {
	status string
	inc    int
}

type gossipBody struct {
	Target  string   `json:"target"`
	OK      bool     `json:"ok"`
	Updates []update `json:"updates"`
}

func main() {
	n := quorumlab.NewNode()

	var mu sync.Mutex
	members := map[string]member{}
	incarnation := 0
	var buffer []*entry
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

	// The functions below that do not lock themselves expect mu to be held.
	remember := func(u update) {
		kept := buffer[:0]
		for _, e := range buffer {
			if e.ID != u.ID {
				kept = append(kept, e)
			}
		}
		buffer = append(kept, &entry{update: u})
	}

	// Entries about the receiver go first, so a suspect hears about itself.
	piggyback := func(dest string) []update {
		mu.Lock()
		defer mu.Unlock()
		sort.SliceStable(buffer, func(i, j int) bool {
			di, dj := buffer[i].ID == dest, buffer[j].ID == dest
			if di != dj {
				return di
			}
			return buffer[i].sends < buffer[j].sends
		})
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

	var apply func(u update)
	apply = func(u update) {
		if u.ID == n.ID() {
			// Someone thinks we are suspect or dead: refute with a higher incarnation.
			if u.Status != "alive" && u.Inc >= incarnation {
				incarnation = u.Inc + 1
				// Persisted so that a restart starts above it. Rare: only on a refutation.
				if err := n.Disk().Set("incarnation", incarnation); err != nil {
					n.Log("saving incarnation: %v", err)
				}
				members[n.ID()] = member{"alive", incarnation}
				remember(update{n.ID(), "alive", incarnation})
			}
			return
		}
		cur, known := members[u.ID]
		r, valid := rank[u.Status]
		if !known || !valid {
			return
		}
		if u.Inc < cur.inc || (u.Inc == cur.inc && r <= rank[cur.status]) {
			return
		}
		members[u.ID] = member{u.Status, u.Inc}
		remember(u)
		if u.Status == "suspect" {
			n.After(suspectTimeout, func(ctx context.Context) {
				mu.Lock()
				defer mu.Unlock()
				if m := members[u.ID]; m.status == "suspect" && m.inc == u.Inc {
					apply(update{u.ID, "dead", u.Inc})
				}
			})
		}
	}

	learn := func(raw json.RawMessage) gossipBody {
		var b gossipBody
		json.Unmarshal(raw, &b)
		mu.Lock()
		defer mu.Unlock()
		for _, u := range b.Updates {
			apply(u)
		}
		return b
	}

	live := func(except string) []string {
		var out []string
		for _, p := range n.Peers() {
			if p != except && members[p].status != "dead" {
				out = append(out, p)
			}
		}
		return out
	}

	nextTarget := func() (string, int) {
		mu.Lock()
		defer mu.Unlock()
		kept := order[:0]
		for _, p := range order {
			if members[p].status != "dead" {
				kept = append(kept, p)
			}
		}
		order = kept
		if len(order) == 0 {
			order = shuffled(live(""))
		}
		if len(order) == 0 {
			return "", 0
		}
		target := order[len(order)-1]
		order = order[:len(order)-1]
		return target, members[target].inc
	}

	ping := func(ctx context.Context, target string) bool {
		reply, err := n.Call(ctx, target, map[string]any{"type": "ping", "updates": piggyback(target)}, ackTimeout)
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
		helpers := shuffled(live(target))
		mu.Unlock()
		if len(helpers) > indirectK {
			helpers = helpers[:indirectK]
		}
		results := make(chan bool, len(helpers))
		for _, h := range helpers {
			go func(h string) {
				reply, err := n.Call(ctx, h, map[string]any{"type": "ping_req", "target": target, "updates": piggyback(h)}, indirectTimeout)
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
		defer mu.Unlock()
		// Survives a crash: a restarted node comes back with a higher
		// incarnation than any rumour about its previous life.
		prev := -1
		if found, _ := n.Disk().Get("incarnation", &prev); !found {
			prev = -1
		}
		incarnation = prev + 1
		if err := n.Disk().Set("incarnation", incarnation); err != nil {
			n.Log("saving incarnation: %v", err)
		}
		for _, id := range n.NodeIDs() {
			members[id] = member{"alive", 0}
		}
		members[n.ID()] = member{"alive", incarnation}
		remember(update{n.ID(), "alive", incarnation})
		maxSends = 3 * int(math.Ceil(math.Log2(float64(len(n.NodeIDs())+1))))

		n.Every(period, func(ctx context.Context) {
			target, inc := nextTarget()
			if target == "" || probe(ctx, target) {
				return
			}
			mu.Lock()
			apply(update{target, "suspect", inc})
			mu.Unlock()
		})
	})

	n.Handle("ping", func(ctx context.Context, msg quorumlab.Message) error {
		learn(msg.Body)
		return n.Reply(msg, map[string]any{"type": "ack", "updates": piggyback(msg.Src)})
	})

	n.Handle("ping_req", func(ctx context.Context, msg quorumlab.Message) error {
		req := learn(msg.Body)
		ok := ping(ctx, req.Target)
		return n.Reply(msg, map[string]any{"type": "ping_req_ok", "ok": ok, "updates": piggyback(msg.Src)})
	})

	n.Handle("members", func(ctx context.Context, msg quorumlab.Message) error {
		mu.Lock()
		view := map[string]string{}
		for id, m := range members {
			view[id] = m.status
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "members_ok", "members": view})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
