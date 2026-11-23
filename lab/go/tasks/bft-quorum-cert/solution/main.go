// Signed votes and quorum certificates, tolerating f = 1 Byzantine node.
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sync"

	"quorumlab"
)

const (
	f      = 1
	quorum = 2*f + 1
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

// sign signs text with this node's own key.
func sign(self, text string) string { return digest(keys[self] + "|" + text) }

// verify reports whether signer signed text.
func verify(signer, text, sig string) bool {
	key, ok := keys[signer]
	return ok && digest(key+"|"+text) == sig
}

// ------------------------------------------------------------------------

// vote is one signed vote, as it appears in a certificate.
type vote struct {
	Voter string `json:"voter"`
	Value string `json:"value"`
	Sig   string `json:"sig"`
}

type node struct {
	n  *quorumlab.Node
	mu sync.Mutex // guards every field below

	myVote  string          // "" until we have an input
	votes   map[string]vote // voter -> the first valid vote from that node
	decided *string
	cert    []vote
}

func main() {
	n := quorumlab.NewNode()
	s := &node{n: n, votes: map[string]vote{}}
	n.Handle("input", s.handleInput)
	n.Handle("vote", s.handleVote)
	n.Handle("decided", s.handleDecided)
	n.Handle("read", s.handleRead)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// validVotes returns the entries that are valid votes for value, one per voter.
func (s *node) validVotes(value string, entries []vote) []vote {
	seen := map[string]bool{}
	var out []vote
	for _, e := range entries {
		if seen[e.Voter] || !slices.Contains(s.n.NodeIDs(), e.Voter) {
			continue
		}
		if e.Value == value && verify(e.Voter, "vote:"+value, e.Sig) {
			seen[e.Voter] = true
			out = append(out, e)
		}
	}
	return out
}

// decide is called with s.mu held. A certificate of 2f+1 signed votes is a
// proof anyone can check, so pass it on: a node that missed some votes can
// decide from it.
func (s *node) decide(ctx context.Context, value string, cert []vote) {
	if s.decided != nil {
		return
	}
	s.decided = &value
	s.cert = cert
	for _, peer := range s.n.Peers() {
		s.n.Send(ctx, peer, map[string]any{"type": "decided", "value": value, "cert": cert})
	}
}

func (s *node) handleInput(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Value string `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	s.mu.Lock()
	if s.myVote == "" {
		s.myVote = req.Value
		body := map[string]any{"type": "vote", "value": req.Value, "sig": sign(s.n.ID(), "vote:"+req.Value)}
		for _, id := range s.n.NodeIDs() {
			s.n.Send(ctx, id, body)
		}
	}
	s.mu.Unlock()
	return s.n.Reply(msg, map[string]any{"type": "input_ok"})
}

func (s *node) handleVote(ctx context.Context, msg quorumlab.Message) error {
	var b struct {
		Value string `json:"value"`
		Sig   string `json:"sig"`
	}
	if err := msg.Decode(&b); err != nil {
		return nil // not a well-formed vote: ignore it
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.votes[msg.Src]; dup || !verify(msg.Src, "vote:"+b.Value, b.Sig) {
		return nil
	}
	s.votes[msg.Src] = vote{Voter: msg.Src, Value: b.Value, Sig: b.Sig}
	var matching []vote
	for _, id := range s.n.NodeIDs() { // node order, so the certificate is stable
		if v, ok := s.votes[id]; ok && v.Value == b.Value {
			matching = append(matching, v)
		}
	}
	if len(matching) >= quorum {
		s.decide(ctx, b.Value, matching)
	}
	return nil
}

func (s *node) handleDecided(ctx context.Context, msg quorumlab.Message) error {
	var b struct {
		Value string `json:"value"`
		Cert  []vote `json:"cert"`
	}
	if err := msg.Decode(&b); err != nil {
		return nil // not a well-formed certificate: ignore it
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if good := s.validVotes(b.Value, b.Cert); len(good) >= quorum {
		s.decide(ctx, b.Value, good)
	}
	return nil
}

func (s *node) handleRead(ctx context.Context, msg quorumlab.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var value, cert any // JSON null until decided
	if s.decided != nil {
		value, cert = *s.decided, s.cert
	}
	return s.n.Reply(msg, map[string]any{"type": "read_ok", "value": value, "cert": cert})
}
