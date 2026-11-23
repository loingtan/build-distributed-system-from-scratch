// Single-decree Paxos. Read the brief: npm run lab -- brief paxos-single
//
// Every node plays all three roles: proposer, acceptor and learner.
//
// This starter "decides" the moment a client asks: it takes the client's
// value as chosen and tells everyone. With one proposer that looks fine.
// With two proposers at once, two nodes choose different values.
// Replace it with the two phases of Paxos.
//
// One sync.Mutex guards the state. Never hold it across n.Call: send from a
// goroutine and take the lock again to look at the answer.
package main

import (
	"context"
	"log"
	"sync"

	"quorumlab"
)

// Accepted is the last proposal an acceptor accepted.
type Accepted struct {
	Ballot int `json:"ballot"`
	Value  any `json:"value"`
}

type paxos struct {
	n        *quorumlab.Node
	size     int
	majority int
	me       int

	mu sync.Mutex // guards every field below

	// Acceptor: the highest ballot promised, and the last proposal accepted.
	promised int
	accepted *Accepted

	// Learner: the chosen value once known (nil until then).
	chosen any

	// Proposer.
	myValue any // what a client asked this node to propose
	round   int
	waiting []quorumlab.Message // propose requests to answer once a value is chosen
}

func main() {
	n := quorumlab.NewNode()
	p := &paxos{n: n}

	n.Handle("decided", p.handleDecided)
	n.Handle("propose", p.handlePropose)
	n.Handle("get", p.handleGet)
	// TODO: acceptor handlers.
	//   prepare { ballot }        -> promise not to accept lower ballots; report accepted
	//   accept  { ballot, value } -> accept unless a higher ballot was promised

	n.OnStart(func(ctx context.Context) {
		p.size = len(n.NodeIDs())
		p.majority = p.size/2 + 1
		for i, id := range n.NodeIDs() {
			if id == n.ID() {
				p.me = i
			}
		}
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// nextBallot (p.mu held): ballots must be unique per node and grow;
// round * N + index works.
func (p *paxos) nextBallot() int {
	p.round++
	return p.round*p.size + p.me
}

// learn (p.mu held) records the chosen value and answers waiting clients.
func (p *paxos) learn(ctx context.Context, value any) {
	if p.chosen != nil || value == nil {
		return
	}
	p.chosen = value
	for _, msg := range p.waiting {
		p.n.Reply(msg, map[string]any{"type": "propose_ok", "value": value})
	}
	p.waiting = nil
	// TODO: messages get lost. Keep telling each peer until it confirms.
	for _, peer := range p.n.Peers() {
		p.n.Send(ctx, peer, map[string]any{"type": "decided", "value": value})
	}
}

func (p *paxos) handleDecided(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Value any `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.learn(ctx, req.Value)
	return nil
}

// propose (p.mu held).
func (p *paxos) propose(ctx context.Context) {
	// TODO: phase 1 (prepare) to a majority, pick the accepted value with the
	// highest ballot (or myValue if none), phase 2 (accept) to a majority.
	// Retry with a higher ballot, after a random pause, until a value is
	// chosen. Run it in a goroutine and do not hold p.mu across n.Call.
	p.learn(ctx, p.myValue) // WRONG: nobody else was asked
}

func (p *paxos) handlePropose(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Value any `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.chosen != nil {
		return p.n.Reply(msg, map[string]any{"type": "propose_ok", "value": p.chosen})
	}
	p.waiting = append(p.waiting, msg)
	if p.myValue == nil {
		p.myValue = req.Value
	}
	p.propose(ctx)
	return nil
}

func (p *paxos) handleGet(ctx context.Context, msg quorumlab.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n.Reply(msg, map[string]any{"type": "get_ok", "value": p.chosen})
}
