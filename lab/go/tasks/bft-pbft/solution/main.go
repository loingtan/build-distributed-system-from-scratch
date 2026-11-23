// One PBFT agreement round (view 0, sequence number 1) among n = 4
// replicas, tolerating f = 1 Byzantine replica. The primary is n1.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"quorumlab"
)

const (
	f      = 1
	quorum = 2*f + 1
	view   = 0
	seq    = 1
)

// ---- Given: toy signatures ---------------------------------------------
// A real system would use public-key signatures (for example Ed25519). Here
// every key sits in one table to keep the code short. The rule of the game:
// a node signs only with its own key. The Byzantine node (played by the
// tests) also knows only its own key.
var keys = map[string]string{"n1": "key-n1-7f3a", "n2": "key-n2-19be", "n3": "key-n3-c05d", "n4": "key-n4-62f8", "client": "key-client-d4e1"}

// digest is 32-bit FNV-1a, printed as 8 hex digits.
func digest(text string) string {
	h := uint32(0x811c9dc5)
	for i := 0; i < len(text); i++ {
		h ^= uint32(text[i])
		h *= 0x01000193
	}
	return fmt.Sprintf("%08x", h)
}

// verify reports whether signer signed text.
func verify(signer, text, sig string) bool {
	key, ok := keys[signer]
	return ok && digest(key+"|"+text) == sig
}

// ------------------------------------------------------------------------

// phase is the body of pre_prepare, prepare and commit messages.
type phase struct {
	View      *int   `json:"view"`
	Seq       *int   `json:"seq"`
	Value     any    `json:"value"`
	ClientSig string `json:"client_sig"`
}

// value returns the message's value if it is for our view and sequence number.
func (p phase) value() (string, bool) {
	v, ok := p.Value.(string)
	return v, ok && p.View != nil && *p.View == view && p.Seq != nil && *p.Seq == seq
}

type node struct {
	n  *quorumlab.Node
	mu sync.Mutex // guards every field below

	proposed  bool    // primary: sent its pre_prepare
	accepted  *string // the value of the pre_prepare we accepted
	committed bool    // sent our commit
	decided   *string
	prepares  map[string]map[string]bool // value -> set of senders
	commits   map[string]map[string]bool // value -> set of senders
}

func main() {
	n := quorumlab.NewNode()
	s := &node{n: n, prepares: map[string]map[string]bool{}, commits: map[string]map[string]bool{}}
	n.Handle("request", s.handleRequest)
	n.Handle("pre_prepare", s.handlePrePrepare)
	n.Handle("prepare", s.handlePrepare)
	n.Handle("commit", s.handleCommit)
	n.Handle("read", s.handleRead)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// primary is the first node id: n1.
func (s *node) primary() string { return s.n.NodeIDs()[0] }

func signedByClient(value, sig string) bool { return verify("client", "request:"+value, sig) }

func (s *node) toAll(ctx context.Context, body map[string]any) {
	for _, id := range s.n.NodeIDs() {
		s.n.Send(ctx, id, body)
	}
}

func add(table map[string]map[string]bool, value, from string) int {
	if table[value] == nil {
		table[value] = map[string]bool{}
	}
	table[value][from] = true
	return len(table[value])
}

// maybeCommit is called with s.mu held. Prepared: we accepted the primary's
// value and 2f+1 replicas prepared that same value. No other value can ever
// be prepared by an honest replica now.
func (s *node) maybeCommit(ctx context.Context) {
	if s.committed || s.accepted == nil || len(s.prepares[*s.accepted]) < quorum {
		return
	}
	s.committed = true
	s.toAll(ctx, map[string]any{"type": "commit", "view": view, "seq": seq, "value": *s.accepted})
}

func (s *node) handleRequest(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Value string `json:"value"`
		Sig   string `json:"sig"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	if err := s.n.Reply(msg, map[string]any{"type": "request_ok"}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n.ID() != s.primary() || s.proposed || !signedByClient(req.Value, req.Sig) {
		return nil
	}
	s.proposed = true
	s.toAll(ctx, map[string]any{"type": "pre_prepare", "view": view, "seq": seq, "value": req.Value, "client_sig": req.Sig})
	return nil
}

// Only the primary may propose, only once, and only what the client signed.
func (s *node) handlePrePrepare(ctx context.Context, msg quorumlab.Message) error {
	var p phase
	if err := msg.Decode(&p); err != nil {
		return nil
	}
	value, ok := p.value()
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.Src != s.primary() || s.accepted != nil || !ok || !signedByClient(value, p.ClientSig) {
		return nil
	}
	s.accepted = &value
	s.toAll(ctx, map[string]any{"type": "prepare", "view": view, "seq": seq, "value": value})
	s.maybeCommit(ctx)
	return nil
}

func (s *node) handlePrepare(ctx context.Context, msg quorumlab.Message) error {
	var p phase
	if err := msg.Decode(&p); err != nil {
		return nil
	}
	value, ok := p.value()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	add(s.prepares, value, msg.Src)
	s.maybeCommit(ctx)
	return nil
}

// 2f+1 matching commits include f+1 honest replicas that saw the value
// prepared, so the decision is final even if we never prepared it ourselves.
func (s *node) handleCommit(ctx context.Context, msg quorumlab.Message) error {
	var p phase
	if err := msg.Decode(&p); err != nil {
		return nil
	}
	value, ok := p.value()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if add(s.commits, value, msg.Src) >= quorum && s.decided == nil {
		s.decided = &value
	}
	return nil
}

func (s *node) handleRead(ctx context.Context, msg quorumlab.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var value any // JSON null until decided
	if s.decided != nil {
		value = *s.decided
	}
	return s.n.Reply(msg, map[string]any{"type": "read_ok", "value": value})
}
