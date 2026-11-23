// Adding a field. Read the brief: npm run lab -- brief roll-tolerant-reader
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
	// TODO: replace the handlers that change. Calling n.Handle again for a
	// type replaces the earlier handler.
	//  - The node a client's put arrives at knows the client (msg.Src); the
	//    home only learns it if the forwarded message says so.
	//  - Version-1 nodes are still running. They read key and name, ignore
	//    fields they do not know, and have no handler for message types they
	//    do not know.
	//  - A message from a version-1 node has no "by" at all.
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
