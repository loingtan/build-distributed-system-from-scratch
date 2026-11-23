// Flags and canaries: the cache path ships dark behind cache_reads, flags
// live on disk, and the node switches the flag off itself after 5 errors in 20.
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
const (
	cacheTimeout = 100 * time.Millisecond
	window       = 20 // cache reads remembered by the guard
	tripAfter    = 5  // errors among them that switch the flag off
)

func runVersion2() {
	runVersion1() // puts and forwarding are unchanged

	// flags survive a restart, like the data: stored under the disk key
	// "flags". recent holds the outcomes of the last cache reads (true = ok)
	// and lives in memory only. flagMu guards both.
	var flagMu sync.Mutex
	flags := map[string]bool{}
	n.Disk().Get("flags", &flags)
	n.Disk().Set("flags", flags)
	var recent []bool

	n.Handle("set_flag", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Flag string `json:"flag"`
			On   bool   `json:"on"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		flagMu.Lock()
		defer flagMu.Unlock()
		flags[req.Flag] = req.On
		if req.On {
			recent = nil // a fresh start for the guard
		}
		if err := n.Disk().Set("flags", flags); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "set_flag_ok"})
	})

	cacheOn := func() bool {
		flagMu.Lock()
		defer flagMu.Unlock()
		return flags["cache_reads"]
	}

	// fromCache asks the cache and feeds the outcome to the guard. It returns
	// the name and whether the cache answered.
	fromCache := func(ctx context.Context, key string) (any, bool) {
		res, err := n.Call(ctx, "cache", map[string]any{"type": "read", "key": key}, cacheTimeout)
		var body struct {
			Type string `json:"type"`
			Name any    `json:"name"`
		}
		ok := err == nil && res.Decode(&body) == nil && body.Type == "read_ok"

		flagMu.Lock()
		defer flagMu.Unlock()
		recent = append(recent, ok)
		if len(recent) > window {
			recent = recent[len(recent)-window:]
		}
		failed := 0
		for _, r := range recent {
			if !r {
				failed++
			}
		}
		if flags["cache_reads"] && failed >= tripAfter {
			// Automatic rollback: a flag change like any other, so on disk.
			flags["cache_reads"] = false
			n.Disk().Set("flags", flags)
			n.Log("cache_reads: %d of the last %d cache reads failed, turning it off", failed, len(recent))
		}
		return body.Name, ok
	}

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req getReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		// Looked up on every get, so set_flag takes effect at once.
		if cacheOn() {
			if name, ok := fromCache(ctx, req.Key); ok {
				return n.Reply(msg, map[string]any{"type": "get_ok", "name": name})
			}
			// The cache failed: fall through. The client never notices.
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
