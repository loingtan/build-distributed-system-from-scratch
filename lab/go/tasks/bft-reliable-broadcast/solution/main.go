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

// sendReady is called with s.mu held.
func (s *node) sendReady(ctx context.Context, sender, value string) {
	if s.readied[sender] {
		return
	}
	s.readied[sender] = true
	s.toAll(ctx, map[string]any{"type": "ready", "sender": sender, "value": value})
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

// Echo the first value a sender proposes to us, and only that one.
func (s *node) handlePropose(ctx context.Context, msg quorumlab.Message) error {
	var b struct {
		Value any `json:"value"`
	}
	if err := msg.Decode(&b); err != nil {
		return nil
	}
	value, ok := b.Value.(string)
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.echoed[msg.Src] {
		return nil
	}
	s.echoed[msg.Src] = true
	s.toAll(ctx, map[string]any{"type": "echo", "sender": msg.Src, "value": value})
	return nil
}

// 2f+1 matching echoes: any two such groups share an honest node, and an
// honest node echoes one value only, so no other value can get there.
func (s *node) handleEcho(ctx context.Context, msg quorumlab.Message) error {
	sender, value, ok := s.decode(msg)
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if add(s.echoes, sender, value, msg.Src) >= 2*f+1 {
		s.sendReady(ctx, sender, value)
	}
	return nil
}

// f+1 matching readies: at least one honest node is ready, so join it
// (this is what makes delivery all-or-nothing). 2f+1: deliver.
func (s *node) handleReady(ctx context.Context, msg quorumlab.Message) error {
	sender, value, ok := s.decode(msg)
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	count := add(s.readies, sender, value, msg.Src)
	if count >= f+1 {
		s.sendReady(ctx, sender, value)
	}
	if _, done := s.delivered[sender]; count >= 2*f+1 && !done {
		s.delivered[sender] = value
	}
	return nil
}

func (s *node) handleRead(ctx context.Context, msg quorumlab.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n.Reply(msg, map[string]any{"type": "read_ok", "delivered": s.delivered})
}
