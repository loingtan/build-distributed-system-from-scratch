// Negotiating a new format: use fwd2 only with peers known to run version 2,
// found by a hello handshake at startup.
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
// Version 2 forwards in a new format, fwd2, and the home answers the client
// itself, which saves a hop:
//
//	{"type":"fwd2","op":"put","key":…,"name":…,"client":…,"reply_to":…}
//	{"type":"fwd2","op":"get","key":…,"client":…,"reply_to":…}
//
// client and reply_to are the client's msg.Src and msg_id. The home replies
// with n.Send(ctx, client, map[string]any{"type": "get_ok", "name": name,
// "in_reply_to": replyTo}) (or put_ok); the forwarding node does not reply at all.
func runVersion2() {
	runVersion1() // fwd_put and fwd_get stay: version-1 nodes still send them

	// Peers known to run version 2. Memory only: after a restart we know
	// nothing, and ask again.
	var modernMu sync.Mutex
	modern := map[string]bool{}
	learn := func(peer string) {
		modernMu.Lock()
		modern[peer] = true
		modernMu.Unlock()
	}
	isModern := func(peer string) bool {
		modernMu.Lock()
		defer modernMu.Unlock()
		return modern[peer]
	}

	// Handshake. A version-2 peer answers; a version-1 peer has no handler for
	// hello, so the call times out and the peer stays "old".
	greet := func(ctx context.Context, peer string) {
		res, err := n.Call(ctx, peer, map[string]any{"type": "hello", "version": 2}, fwdTimeout)
		if err != nil {
			return
		}
		var body struct {
			Version int `json:"version"`
		}
		if res.Decode(&body) == nil && body.Version >= 2 {
			learn(peer)
		}
	}
	// A hello we receive is evidence as well: this is how the nodes that
	// upgraded earlier learn about us.
	n.Handle("hello", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Version int `json:"version"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		if req.Version >= 2 {
			learn(msg.Src)
		}
		return n.Reply(msg, map[string]any{"type": "hello_ok", "version": 2})
	})
	// runVersion2 runs at startup, before the node answers init: greet in the
	// background rather than hold up the start.
	for _, peer := range n.Peers() {
		go greet(context.Background(), peer)
	}

	// write and read are the version-1 record, unchanged. write's caller holds mu.
	write := func(key, name string) error {
		if users[key] == nil {
			users[key] = record{}
		}
		users[key]["name"] = name
		return saveUsers()
	}
	read := func(key string) any {
		mu.Lock()
		defer mu.Unlock()
		return nameOf(key)
	}

	n.Handle("put", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			putReq
			MsgID int64 `json:"msg_id"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		owner := home(req.Key)
		switch {
		case owner == n.ID():
			mu.Lock()
			defer mu.Unlock()
			if err := write(req.Key, req.Name); err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		case isModern(owner):
			return n.Send(ctx, owner, map[string]any{
				"type": "fwd2", "op": "put", "key": req.Key, "name": req.Name,
				"client": msg.Src, "reply_to": req.MsgID,
			})
		default:
			_, err := n.Call(ctx, owner, map[string]any{"type": "fwd_put", "key": req.Key, "name": req.Name}, fwdTimeout)
			if err != nil {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
			return n.Reply(msg, map[string]any{"type": "put_ok"})
		}
	})

	n.Handle("get", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			getReq
			MsgID int64 `json:"msg_id"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		owner := home(req.Key)
		switch {
		case owner == n.ID():
			return n.Reply(msg, map[string]any{"type": "get_ok", "name": read(req.Key)})
		case isModern(owner):
			return n.Send(ctx, owner, map[string]any{
				"type": "fwd2", "op": "get", "key": req.Key,
				"client": msg.Src, "reply_to": req.MsgID,
			})
		default:
			res, err := n.Call(ctx, owner, map[string]any{"type": "fwd_get", "key": req.Key}, fwdTimeout)
			if err != nil {
				return n.Reply(msg, map[string]any{"type": "error", "code": "unavailable"})
			}
			var body struct {
				Name any `json:"name"`
			}
			res.Decode(&body)
			return n.Reply(msg, map[string]any{"type": "get_ok", "name": body.Name})
		}
	})

	n.Handle("fwd2", func(ctx context.Context, msg quorumlab.Message) error {
		var req struct {
			Op      string `json:"op"`
			Key     string `json:"key"`
			Name    string `json:"name"`
			Client  string `json:"client"`
			ReplyTo int64  `json:"reply_to"`
		}
		if err := msg.Decode(&req); err != nil {
			return err
		}
		learn(msg.Src)
		if req.Op == "put" {
			mu.Lock()
			err := write(req.Key, req.Name)
			mu.Unlock()
			if err != nil {
				return err
			}
			return n.Send(ctx, req.Client, map[string]any{"type": "put_ok", "in_reply_to": req.ReplyTo})
		}
		return n.Send(ctx, req.Client, map[string]any{"type": "get_ok", "name": read(req.Key), "in_reply_to": req.ReplyTo})
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
