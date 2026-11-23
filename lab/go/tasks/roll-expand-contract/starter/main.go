// Expand and contract. Read the brief: npm run lab -- brief roll-expand-contract
//
// A user directory spread over the cluster. Each key lives on one node, its
// home; any node accepts a request and forwards it there.
//
// Every node runs this same program. The harness upgrades a node by stopping
// it, setting the disk key "version" to 2 and starting it again, one node at
// a time, while clients keep sending requests. In this task it may also roll
// a node back by restarting it with "version" set to 1.
package main

import (
	"context"
	"log"

	"sync"
	"time"

	"quorumlab"
)

// record is one user as stored under the disk key "users"; see below for its
// shape. A map, not a struct, so that fields one version does not know
// survive an update by the other.
type record map[string]any

var (
	n = quorumlab.NewNode()

	// users is node.disk.users: key -> record. Handlers run concurrently, so
	// mu guards it, and every change is written to disk before it is
	// acknowledged.
	mu    sync.Mutex
	users = map[string]record{}
)

func home(key string) string {
	sum := 0
	for _, c := range key {
		sum += int(c)
	}
	ids := n.NodeIDs()
	return ids[sum%len(ids)]
}

// saveUsers writes users to disk. The caller holds mu.
func saveUsers() error { return n.Disk().Set("users", users) }

// store is how a version keeps names. write saves a name (the caller holds
// mu, and write saves to disk before returning); read returns it, or nil.
type store interface {
	write(key, name string) error
	read(key string) any
}

const fwdTimeout = 300 * time.Millisecond

// The request handling is the same in both versions; only the storage
// differs.
func serve(s store) {
	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key  string `json:"key"`
			Name string `json:"name"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		owner := home(req.Key)
		if owner == n.ID() {
			mu.Lock()
			defer mu.Unlock()
			if err := s.write(req.Key, req.Name); err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		}
		_, err := n.Call(ctx, owner, map[string]any{"type": "fwd_put", "key": req.Key, "name": req.Name}, fwdTimeout)
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
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
		owner := home(req.Key)
		if owner == n.ID() {
			mu.Lock()
			name := s.read(req.Key)
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "get_ok", "name": name})
		}
		res, err := n.Call(ctx, owner, map[string]any{"type": "fwd_get", "key": req.Key}, fwdTimeout)
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		var body struct {
			Name any `json:"name"`
		}
		res.Decode(&body)
		return n.Reply(msg, map[string]any{"type": "get_ok", "name": body.Name})
	})

	n.Handle("fwd_put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key  string `json:"key"`
			Name string `json:"name"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if err := s.write(req.Key, req.Name); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "fwd_put_ok"})
	})

	n.Handle("fwd_get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Key string `json:"key"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		name := s.read(req.Key)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "fwd_get_ok", "name": name})
	})
}

// ---------------------------------------------------------------- version 1
// Already running in production. Do not change it: a node that has not been
// upgraded, or has been rolled back, runs exactly this code.
//
// A record is {"name": "Ada Lovelace"}. Version 1 updates a record in place:
// it sets name and leaves every other field as it was.
type v1Store struct{}

func (v1Store) write(key, name string) error {
	if users[key] == nil {
		users[key] = record{}
	}
	users[key]["name"] = name
	return saveUsers()
}

func (v1Store) read(key string) any {
	if rec, ok := users[key]; ok {
		return rec["name"]
	}
	return nil
}

func runVersion1() {
	serve(v1Store{})
}

// ---------------------------------------------------------------- version 2
// Version 2 keeps first and last names apart. Names are always two words.
//
//	before finalize: {"name": "Ada Lovelace", "first": "Ada", "last": "Lovelace"}
//	after finalize:  {"first": "Ada", "last": "Lovelace"}
//
// The harness sends {"type": "finalize"} to every node once every node runs
// version 2 for good; version 1 never runs again after that. Reply
// {"type": "finalize_ok"}.
type v2Store struct{}

// TODO: while version 1 may still run (until finalize), a record must stay
// readable by version 1, and version 1 may change name behind your back
// after a rollback.
func (v2Store) write(key, name string) error {
	return v1Store{}.write(key, name)
}

func (v2Store) read(key string) any {
	return v1Store{}.read(key)
}

func runVersion2() {
	serve(v2Store{})
	// TODO: a "finalize" handler. Rewrite every record to the final shape, and
	// make sure the node still knows it has finalized after a restart.
}

func main() {
	// The disk is open once the node has started: read the version there, and
	// register this version's handlers before the first message arrives.
	n.OnStart(func(ctx context.Context) {
		version := 1
		n.Disk().Get("version", &version)
		mu.Lock()
		if ok, err := n.Disk().Get("users", &users); err != nil || !ok {
			users = map[string]record{}
			saveUsers()
		}
		mu.Unlock()
		if version == 1 {
			runVersion1()
		} else {
			runVersion2()
		}
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
