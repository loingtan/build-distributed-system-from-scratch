// Adding a field: version 2 adds "by" to messages version 1 already reads,
// and treats a missing "by" as unknown.
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
// Version 2 records who made each change: get_ok carries "by", the client id
// whose put wrote the current name, or null when that is unknown.
func runVersion2() {
	runVersion1()

	// The record keeps "name" where version 1 put it and gains "by" beside it.
	// A missing writer (nil) is stored as null: unknown, not "same as before".
	// The caller holds mu.
	store := func(key, name string, by any) error {
		if users[key] == nil {
			users[key] = record{}
		}
		users[key]["name"] = name
		users[key]["by"] = by
		return saveUsers()
	}
	// byOf is users[key].by, or nil. The caller holds mu.
	byOf := func(key string) any {
		if rec, ok := users[key]; ok {
			return rec["by"]
		}
		return nil
	}

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req putReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		owner := home(req.Key)
		if owner == n.ID() {
			mu.Lock()
			defer mu.Unlock()
			if err := store(req.Key, req.Name, msg.Src); err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		}
		// Same message as version 1, every field it reads unchanged, plus "by".
		// A version-1 home ignores the extra field.
		_, err := n.Call(ctx, owner, map[string]any{"type": "fwd_put", "key": req.Key, "name": req.Name, "by": msg.Src}, fwdTimeout)
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
			name, by := nameOf(req.Key), byOf(req.Key)
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "get_ok", "name": name, "by": by})
		}
		res, err := n.Call(ctx, owner, map[string]any{"type": "fwd_get", "key": req.Key}, fwdTimeout)
		if err != nil {
			return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
		}
		// A version-1 home does not send "by": the writer is unknown (nil).
		var body struct {
			Name any `json:"name"`
			By   any `json:"by"`
		}
		if err := res.Decode(&body); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "get_ok", "name": body.Name, "by": body.By})
	})

	// A version-1 forwarder sends no "by", which decodes as nil.
	n.Handle("fwd_put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			putReq
			By any `json:"by"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if err := store(req.Key, req.Name, req.By); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "fwd_put_ok"})
	})

	// "name" stays where a version-1 forwarder looks for it.
	n.Handle("fwd_get", func(ctx context.Context, msg quorumlab.Message) error {
		var req getReq
		if err := msg.Decode(&req); err != nil {
			return err
		}
		mu.Lock()
		name, by := nameOf(req.Key), byOf(req.Key)
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "fwd_get_ok", "name": name, "by": by})
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
