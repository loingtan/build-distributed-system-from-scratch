// Flags and canaries. Read the brief: npm run lab -- brief roll-feature-flags
//
// A user directory spread over the cluster. Each key lives on one node, its
// home; any node accepts a request and forwards it there.
//
// Every node runs this same program. The harness upgrades a node by stopping
// it, setting the disk key "version" to 2 and starting it again, one node at
// a time, while clients keep sending requests.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// record is one user as stored under the disk key "users". A map, not a
// struct, so that fields this version does not know survive an update.
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

// nameOf is users[key].name, or nil (JSON null) if there is no such record.
// The caller holds mu.
func nameOf(key string) any {
	if rec, ok := users[key]; ok {
		return rec["name"]
	}
	return nil
}

type putReq struct {
	Key  string `json:"key"`
	Name string `json:"name"`
}

type getReq struct {
	Key string `json:"key"`
}

const fwdTimeout = 300 * time.Millisecond

// ---------------------------------------------------------------- version 1
// Already running in production. Do not change it: a node that has not been
// upgraded yet runs exactly this code, and you cannot edit it there.
func runVersion1() {
	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req putReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		owner := home(req.Key)
		if owner == n.ID() {
			mu.Lock()
			defer mu.Unlock()
			if users[req.Key] == nil {
				users[req.Key] = record{}
			}
			users[req.Key]["name"] = req.Name
			if err := saveUsers(); err != nil {
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
		var req getReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		owner := home(req.Key)
		if owner == n.ID() {
			mu.Lock()
			name := nameOf(req.Key)
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

	// Version 1 reads only the fields it knows (key, name) and ignores the rest.
	n.Handle("fwd_put", func(ctx context.Context, msg quorumlab.Message) error {
		var req putReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if users[req.Key] == nil {
			users[req.Key] = record{}
		}
		users[req.Key]["name"] = req.Name
		if err := saveUsers(); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "fwd_put_ok"})
	})

	n.Handle("fwd_get", func(ctx context.Context, msg quorumlab.Message) error {
		var req getReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		name := nameOf(req.Key)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "fwd_get_ok", "name": name})
	})
}

// ---------------------------------------------------------------- version 2
// Version 2 can answer gets from the platform team's new read cache, behind
// the flag cache_reads, which is off until set_flag turns it on:
//
//	-> {"type":"set_flag","flag":"cache_reads","on":true}   <- {"type":"set_flag_ok"}
//
// With the flag on, every get asks the cache first:
//
//	n.Call(ctx, "cache", map[string]any{"type": "read", "key": key}, 100*time.Millisecond)
//	<- {"type":"read_ok","name":…}  or  {"type":"error","code":"unavailable"}
//
// When the cache fails, answer from the normal path instead. When 5 of the
// node's last 20 cache reads failed, the node turns the flag off by itself.
// Turning the flag on starts a fresh count. Flags live on the disk.
func runVersion2() {
	runVersion1()
	// TODO: set_flag, and a get handler that uses the cache while the flag is
	// on. Calling n.Handle again for a type replaces the earlier handler.
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
