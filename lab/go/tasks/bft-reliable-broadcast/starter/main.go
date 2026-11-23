// Echo, ready, deliver. Read the brief: npm run lab -- brief bft-reliable-broadcast
//
// Bracha's reliable broadcast, tolerating f = 1 Byzantine node out of n = 4.
// One broadcast per sender; the sender of a propose is msg.Src.
package main

import (
	"context"
	"log"
	"slices"
	"sync"

	"quorumlab"
)

const f = 1

type node struct {
	n  *quorumlab.Node
	mu sync.Mutex // guards every field below

	proposed  bool
	echoed    map[string]bool                       // sender -> we sent our echo for it
	readied   map[string]bool                       // sender -> we sent our ready for it
	delivered map[string]string                     // sender -> the value we delivered
	echoes    map[string]map[string]map[string]bool // sender -> value -> set of nodes
	readies   map[string]map[string]map[string]bool // sender -> value -> set of nodes
}

type vouch struct {
	Sender string `json:"sender"`
	Value  any    `json:"value"`
}

func main() {
	n := quorumlab.NewNode()
	s := &node{
		n:         n,
		echoed:    map[string]bool{},
		readied:   map[string]bool{},
		delivered: map[string]string{},
		echoes:    map[string]map[string]map[string]bool{},
		readies:   map[string]map[string]map[string]bool{},
	}
	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("propose", s.handlePropose)
	n.Handle("echo", s.handleEcho)
	n.Handle("ready", s.handleReady)
	n.Handle("read", s.handleRead)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *node) toAll(ctx context.Context, body map[string]any) {
	for _, id := range s.n.NodeIDs() {
		s.n.Send(ctx, id, body)
	}
}

// add records that from vouched for (sender, value) and returns how many
// distinct nodes have vouched for exactly that value.
func add(table map[string]map[string]map[string]bool, sender, value, from string) int {
	if table[sender] == nil {
		table[sender] = map[string]map[string]bool{}
	}
	if table[sender][value] == nil {
		table[sender][value] = map[string]bool{}
	}
	table[sender][value][from] = true
	return len(table[sender][value])
}

// decode reads a { sender, value } body and checks it names a known sender
// and a string value.
func (s *node) decode(msg quorumlab.Message) (string, string, bool) {
	var b vouch
	if err := msg.Decode(&b); err != nil {
		return "", "", false
	}
	v, ok := b.Value.(string)
	return b.Sender, v, ok && slices.Contains(s.n.NodeIDs(), b.Sender)
}

func (s *node) handleBroadcast(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Value string `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	s.mu.Lock()
	if !s.proposed {
		s.proposed = true
		s.toAll(ctx, map[string]any{"type": "propose", "value": req.Value})
	}
	s.mu.Unlock()
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *node) handlePropose(ctx context.Context, msg quorumlab.Message) error {
	// TODO: the sender is msg.Src. Echo the first value each sender proposes,
	// once, to every node: {"type": "echo", "sender": sender, "value": value}.
	return nil
}

func (s *node) handleEcho(ctx context.Context, msg quorumlab.Message) error {
	// TODO: count distinct nodes per (sender, value) with add(s.echoes, ...).
	// On 2f+1 matching echoes, send {"type": "ready", "sender", "value"} to
	// every node (at most one ready per sender).
	return nil
}

func (s *node) handleReady(ctx context.Context, msg quorumlab.Message) error {
	// TODO: count distinct nodes per (sender, value) with add(s.readies, ...).
	// On f+1 matching readies, send your own ready if you have not yet.
	// On 2f+1 matching readies, deliver: s.delivered[sender] = value.
	return nil
}

func (s *node) handleRead(ctx context.Context, msg quorumlab.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n.Reply(msg, map[string]any{"type": "read_ok", "delivered": s.delivered})
}
