// Changes under load: membership changes carry epochs, values carry versions,
// and a node that becomes the owner of a key collects the newest copy from
// every other node before it serves the key.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf16"

	"quorumlab"
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

type point struct {
	pos uint32
	id  string
}

// points is the number of virtual nodes per member.
const points = 200

// buildRing puts many points per member. Each member's share of the ring is
// the sum of many small arcs, so the shares even out.
func buildRing(members []string) []point {
	ring := make([]point, 0, len(members)*points)
	for _, id := range members {
		for i := 0; i < points; i++ {
			ring = append(ring, point{hash(id + "#" + strconv.Itoa(i)), id})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		if ring[i].pos != ring[j].pos {
			return ring[i].pos < ring[j].pos
		}
		return ring[i].id < ring[j].id
	})
	return ring
}

// ownerOf: the owner of a key is the node at the first point at or after the
// key's hash, going clockwise; past the last point, wrap round to the first.
func ownerOf(ring []point, key string) string {
	h := hash(key)
	i := sort.Search(len(ring), func(i int) bool { return ring[i].pos >= h })
	if i == len(ring) {
		i = 0
	}
	return ring[i].id
}

// versioned is one stored copy of a key.
type versioned struct {
	Value   json.RawMessage
	Version int64
}

// entry is a copy on the wire: [key, value, version].
func entry(key string, v versioned) []any { return []any{key, v.Value, v.Version} }

func decodeEntry(raw []json.RawMessage) (string, versioned, bool) {
	var key string
	var version int64
	if len(raw) != 3 || json.Unmarshal(raw[0], &key) != nil || json.Unmarshal(raw[2], &version) != nil {
		return "", versioned{}, false
	}
	return key, versioned{raw[1], version}, true
}

// membership is the epoch and members every message between nodes carries.
type membership struct {
	Epoch   *int64   `json:"epoch"`
	Members []string `json:"members"`
}

type node struct {
	n  *quorumlab.Node
	mu sync.Mutex

	store   map[string]versioned // key -> { value, version }
	epoch   int64                // the newest membership this node knows of
	members []string             // the members of that epoch
	ring    []point              // sorted by pos

	ready   bool          // has this node collected every key it owns in this epoch?
	waiting chan struct{} // closed (and replaced) to wake requests held back until it has
	seq     int64
	busy    map[string]bool // owners with a handoff in flight
}

// wake releases every held request. The caller holds mu.
func (s *node) wake() {
	close(s.waiting)
	s.waiting = make(chan struct{})
}

// learn adopts a membership only if it is newer than the one we have. Every
// message between nodes carries the sender's epoch and members, so a node
// that missed a set_members catches up from whoever it talks to. The caller
// holds mu.
func (s *node) learn(m membership) {
	if m.Epoch == nil || *m.Epoch <= s.epoch {
		return // stale or already known
	}
	e := *m.Epoch
	s.epoch = e
	s.members = append([]string(nil), m.Members...)
	s.ring = buildRing(s.members)
	s.ready = false
	s.wake() // held requests re-route under the new members
	go s.collect(e, s.members)
}

func (s *node) self() membership {
	e := s.epoch
	return membership{&e, s.members}
}

// merge: the newest version wins, whatever order copies arrive in. The caller
// holds mu.
func (s *node) merge(key string, v versioned) {
	if cur, ok := s.store[key]; !ok || v.Version > cur.Version {
		s.store[key] = v
	}
}

// collect: before serving in epoch e, ask every other node for its copies of
// the keys this node now owns. A node that answers has moved to epoch e (or
// later), so it will never again accept a write for these keys as their
// owner. After the last answer, this node holds the newest value of every key
// it owns.
func (s *node) collect(e int64, members []string) {
	ctx := context.Background()
	var wg sync.WaitGroup
	for _, peer := range s.n.Peers() {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			for {
				s.mu.Lock()
				current := s.epoch == e
				s.mu.Unlock()
				if !current {
					return
				}
				res, err := s.n.Call(ctx, peer, map[string]any{"type": "collect", "epoch": e, "members": members}, 400*time.Millisecond)
				if err != nil {
					continue
				}
				var body struct {
					membership
					Entries [][]json.RawMessage `json:"entries"`
				}
				if res.Decode(&body) != nil {
					continue
				}
				s.mu.Lock()
				if body.Epoch != nil && *body.Epoch > e {
					s.learn(body.membership) // someone knows a newer one
				} else {
					for _, raw := range body.Entries {
						if key, v, ok := decodeEntry(raw); ok {
							s.merge(key, v)
						}
					}
				}
				s.mu.Unlock()
				return
			}
		}(peer)
	}
	wg.Wait()
	s.mu.Lock()
	if s.epoch == e {
		s.ready = true
		s.wake()
	}
	s.mu.Unlock()
}

// handoff moves keys this node holds but no longer owns.
func (s *node) handoff(ctx context.Context) {
	s.mu.Lock()
	batches := map[string][]versionedKey{} // owner -> copies
	for key, v := range s.store {
		o := ownerOf(s.ring, key)
		if o == s.n.ID() {
			continue
		}
		batches[o] = append(batches[o], versionedKey{key, v})
	}
	for o := range batches {
		if s.busy[o] {
			delete(batches, o)
			continue
		}
		s.busy[o] = true
	}
	m := s.self()
	s.mu.Unlock()

	for o, copies := range batches {
		go func(o string, copies []versionedKey) {
			entries := make([][]any, len(copies))
			for i, c := range copies {
				entries[i] = entry(c.key, c.v)
			}
			res, err := s.n.Call(ctx, o, map[string]any{"type": "handoff", "epoch": m.Epoch, "members": m.Members, "entries": entries}, 400*time.Millisecond)
			s.mu.Lock()
			defer s.mu.Unlock()
			s.busy[o] = false
			if err != nil {
				return // the timer tries again
			}
			var body struct {
				membership
				Accepted bool `json:"accepted"`
			}
			if res.Decode(&body) != nil {
				return
			}
			if !body.Accepted {
				s.learn(body.membership)
				return
			}
			// Only now is it safe to forget the keys: the new owner has them.
			for _, c := range copies {
				if cur, ok := s.store[c.key]; ok && cur.Version == c.v.Version && ownerOf(s.ring, c.key) != s.n.ID() {
					delete(s.store, c.key)
				}
			}
		}(o, copies)
	}
}

type versionedKey struct {
	key string
	v   versioned
}

// serve handles a put or get here if this node owns the key and has collected
// it; waits if it owns the key but has not; otherwise passes it to the owner.
// It returns the reply body, or false if the owner never answered.
func (s *node) serve(ctx context.Context, typ, key string, value json.RawMessage) (quorumlab.Message, map[string]any, bool) {
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	s.mu.Lock()
	for {
		o := ownerOf(s.ring, key)
		if o != s.n.ID() {
			body := map[string]any{"type": typ, "key": key, "epoch": s.epoch, "members": s.members}
			if typ == "put" {
				body["value"] = value
			}
			s.mu.Unlock()
			res, err := s.n.Call(ctx, o, body, 900*time.Millisecond)
			return res, nil, err == nil
		}
		if !s.ready {
			ch := s.waiting
			s.mu.Unlock()
			select {
			case <-ch:
			case <-deadline.C:
				return quorumlab.Message{}, nil, false // the client has given up
			}
			s.mu.Lock()
			continue // the members may have changed while we waited
		}
		defer s.mu.Unlock()
		if typ == "put" {
			// The epoch goes first: a write in a later epoch beats any earlier one.
			s.seq++
			s.store[key] = versioned{value, s.epoch*1000000 + s.seq}
			return quorumlab.Message{}, map[string]any{"type": "put_ok"}, true
		}
		v, ok := s.store[key]
		if !ok {
			v.Value = json.RawMessage("null")
		}
		return quorumlab.Message{}, map[string]any{"type": "get_ok", "value": v.Value}, true
	}
}

type request struct {
	membership
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

func main() {
	n := quorumlab.NewNode()
	s := &node{
		n:       n,
		store:   map[string]versioned{},
		ready:   true,
		waiting: make(chan struct{}),
		busy:    map[string]bool{},
	}

	n.OnStart(func(ctx context.Context) {
		s.mu.Lock()
		s.members = n.NodeIDs()
		s.ring = buildRing(s.members)
		s.mu.Unlock()
		n.Every(100*time.Millisecond, s.handoff)
	})

	n.Handle("collect", func(ctx context.Context, msg quorumlab.Message) error {
		var req membership
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		s.learn(req)
		entries := [][]any{}
		if req.Epoch != nil && *req.Epoch == s.epoch {
			for key, v := range s.store {
				if ownerOf(s.ring, key) == msg.Src {
					entries = append(entries, entry(key, v))
				}
			}
		}
		reply := map[string]any{"type": "collect_ok", "epoch": s.epoch, "members": s.members, "entries": entries}
		s.mu.Unlock()
		return n.Reply(msg, reply)
	})

	n.Handle("handoff", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			membership
			Entries [][]json.RawMessage `json:"entries"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		s.learn(req.membership)
		// From an older epoch the sender may be wrong about who owns these keys,
		// and this node may already have answered the real owner's collect.
		if req.Epoch == nil || *req.Epoch < s.epoch {
			reply := map[string]any{"type": "handoff_ok", "accepted": false, "epoch": s.epoch, "members": s.members}
			s.mu.Unlock()
			return n.Reply(msg, reply)
		}
		for _, raw := range req.Entries {
			if key, v, ok := decodeEntry(raw); ok {
				s.merge(key, v)
			}
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "handoff_ok", "accepted": true})
	})

	n.Handle("set_members", func(ctx context.Context, msg quorumlab.Message) error {
		var req membership
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		s.learn(req)
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "set_members_ok"})
	})

	n.Handle("lookup", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Keys []string `json:"keys"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		owners := make([]string, len(req.Keys))
		for i, k := range req.Keys {
			owners[i] = ownerOf(s.ring, k)
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "lookup_ok", "owners": owners})
	})

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		s.learn(req.membership)
		s.mu.Unlock()
		if _, _, ok := s.serve(ctx, "put", req.Key, req.Value); ok {
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		}
		return nil
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req request
		if err := msg.Decode(&req); err != nil {
			return err
		}
		s.mu.Lock()
		s.learn(req.membership)
		s.mu.Unlock()
		res, local, ok := s.serve(ctx, "get", req.Key, nil)
		if !ok {
			return nil
		}
		if local != nil {
			return n.Reply(msg, local)
		}
		var body struct {
			Value json.RawMessage `json:"value"`
		}
		if err := res.Decode(&body); err != nil {
			return err
		}
		if body.Value == nil {
			body.Value = json.RawMessage("null")
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "value": body.Value})
	})

	n.Handle("local_keys", func(ctx context.Context, msg quorumlab.Message) error {
		s.mu.Lock()
		keys := make([]string, 0, len(s.store))
		for k := range s.store {
			keys = append(keys, k)
		}
		s.mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "local_keys_ok", "keys": keys})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
