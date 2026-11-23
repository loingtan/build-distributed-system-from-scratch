// Single-decree Paxos that survives crashes. An acceptor's state is a set
// of promises, so it lives on disk and is written (fsynced) before any reply
// that depends on it. Proposer and learner state is rebuilt after a restart.
//
// onPrepare, onAccept, learn, see and nextBallot are called with p.mu held.
// Never hold p.mu across n.Call: the proposer gathers replies in ask, which
// takes the lock only to look at each one.
package main

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"quorumlab"
)

// Accepted is the last proposal an acceptor accepted.
type Accepted struct {
	Ballot int `json:"ballot"`
	Value  any `json:"value"`
}

// reply is the answer to prepare and accept.
type reply struct {
	Type     string    `json:"type"`
	OK       bool      `json:"ok"`
	Promised int       `json:"promised"`
	Accepted *Accepted `json:"accepted"`
	Chosen   any       `json:"chosen"`
}

type paxos struct {
	n        *quorumlab.Node
	size     int
	majority int
	me       int

	mu sync.Mutex // guards every field below

	// Acceptor: the highest ballot promised, and the last proposal accepted.
	// Both are on disk: a forgotten promise lets a lower ballot through, a
	// forgotten accepted value can un-choose a chosen one.
	promised int
	accepted *Accepted

	// Learner: the chosen value once known (nil until then), and the peers
	// that confirmed they know it.
	chosen   any
	informed map[string]bool

	// Proposer.
	myValue any // what a client asked this node to propose
	round   int
	highest int // the highest ballot seen in any reply
	running bool
	waiting []quorumlab.Message // propose requests to answer once a value is chosen
}

func main() {
	n := quorumlab.NewNode()
	p := &paxos{n: n, informed: map[string]bool{}}

	n.Handle("prepare", p.handlePrepare)
	n.Handle("accept", p.handleAccept)
	n.Handle("decided", p.handleDecided)
	n.Handle("propose", p.handlePropose)
	n.Handle("get", p.handleGet)

	n.OnStart(func(ctx context.Context) {
		p.size = len(n.NodeIDs())
		p.majority = p.size/2 + 1
		for i, id := range n.NodeIDs() {
			if id == n.ID() {
				p.me = i
			}
		}
		p.mu.Lock()
		p.load() // OnStart runs before any message, after every restart too
		p.mu.Unlock()
		n.Every(250*time.Millisecond, p.announce)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// nextBallot (p.mu held): ballots are unique per node, round * N + index,
// and above every ballot this node has seen.
func (p *paxos) nextBallot() int {
	p.round = max(p.round, p.highest/p.size) + 1
	return p.round*p.size + p.me
}

// ---------------------------------------------------------------- acceptor

// load (p.mu held) reads the acceptor state back. On first start the disk is empty.
func (p *paxos) load() {
	if _, err := p.n.Disk().Get("promised", &p.promised); err != nil {
		log.Fatalf("reading promised: %v", err)
	}
	if _, err := p.n.Disk().Get("accepted", &p.accepted); err != nil {
		log.Fatalf("reading accepted: %v", err)
	}
}

// persist (p.mu held) writes promised and accepted in one atomic, fsynced
// write. A node that cannot keep its promises must not go on.
func (p *paxos) persist() {
	err := p.n.Disk().Update(func(tx *quorumlab.Tx) error {
		if err := tx.Set("promised", p.promised); err != nil {
			return err
		}
		return tx.Set("accepted", p.accepted)
	})
	if err != nil {
		log.Fatalf("writing to disk: %v", err)
	}
}

func (p *paxos) onPrepare(ballot int) reply {
	if ballot > p.promised {
		p.promised = ballot
		p.persist() // on disk before the reply leaves
	}
	return reply{OK: ballot >= p.promised, Promised: p.promised, Accepted: p.accepted, Chosen: p.chosen}
}

func (p *paxos) onAccept(ballot int, value any) reply {
	if ballot >= p.promised {
		p.promised = ballot
		p.accepted = &Accepted{Ballot: ballot, Value: value}
		p.persist()
		return reply{OK: true, Promised: p.promised, Chosen: p.chosen}
	}
	return reply{OK: false, Promised: p.promised, Chosen: p.chosen}
}

func (p *paxos) handlePrepare(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Ballot int `json:"ballot"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	r := p.onPrepare(req.Ballot)
	p.mu.Unlock()
	r.Type = "prepare_ok"
	return p.n.Reply(msg, r)
}

func (p *paxos) handleAccept(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Ballot int `json:"ballot"`
		Value  any `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	r := p.onAccept(req.Ballot, req.Value)
	p.mu.Unlock()
	r.Type = "accept_ok"
	return p.n.Reply(msg, r)
}

// ----------------------------------------------------------------- learner

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
	go p.announce(ctx)
}

// announce keeps telling every peer until it confirms: messages get lost.
func (p *paxos) announce(ctx context.Context) {
	p.mu.Lock()
	value := p.chosen
	var todo []string
	for _, peer := range p.n.Peers() {
		if !p.informed[peer] {
			todo = append(todo, peer)
		}
	}
	p.mu.Unlock()
	if value == nil {
		return
	}
	for _, peer := range todo {
		go func(peer string) {
			if _, err := p.n.Call(ctx, peer, map[string]any{"type": "decided", "value": value}, 200*time.Millisecond); err == nil {
				p.mu.Lock()
				p.informed[peer] = true
				p.mu.Unlock()
			}
		}(peer)
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
	p.learn(ctx, req.Value)
	p.mu.Unlock()
	return p.n.Reply(msg, map[string]any{"type": "decided_ok"})
}

// ---------------------------------------------------------------- proposer

// see (p.mu held) takes in one reply: it may carry the chosen value or a
// higher ballot.
func (p *paxos) see(ctx context.Context, r *reply, oks *[]reply) {
	if r == nil {
		return
	}
	p.learn(ctx, r.Chosen)
	p.highest = max(p.highest, r.Promised)
	if r.OK {
		*oks = append(*oks, *r)
	}
}

// ask sends body to every acceptor (this node's own answer is local) and
// returns the positive replies as soon as a majority has said yes, or
// everyone has answered or timed out.
func (p *paxos) ask(ctx context.Context, body map[string]any, local reply) []reply {
	peers := p.n.Peers()
	results := make(chan *reply, len(peers))
	for _, peer := range peers {
		go func(peer string) {
			res, err := p.n.Call(ctx, peer, body, 200*time.Millisecond)
			if err != nil {
				results <- nil
				return
			}
			var r reply
			if err := res.Decode(&r); err != nil {
				results <- nil
				return
			}
			results <- &r
		}(peer)
	}
	var oks []reply
	p.mu.Lock()
	p.see(ctx, &local, &oks)
	p.mu.Unlock()
	for pending := len(peers); pending > 0 && len(oks) < p.majority; pending-- {
		r := <-results
		p.mu.Lock()
		p.see(ctx, r, &oks)
		p.mu.Unlock()
	}
	return oks
}

func (p *paxos) isChosen() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.chosen != nil
}

func (p *paxos) propose(ctx context.Context) {
	for attempt := 1; !p.isChosen(); attempt++ {
		p.mu.Lock()
		ballot := p.nextBallot()
		local := p.onPrepare(ballot)
		p.mu.Unlock()

		// Phase 1: ask a majority to promise to ignore lower ballots, and to
		// report what they have already accepted.
		promises := p.ask(ctx, map[string]any{"type": "prepare", "ballot": ballot}, local)
		if p.isChosen() {
			break
		}
		if len(promises) >= p.majority {
			// If any acceptor in the majority accepted something, a value
			// may already be chosen: propose the one with the highest ballot.
			p.mu.Lock()
			value := p.myValue
			p.mu.Unlock()
			best := 0
			for _, r := range promises {
				if r.Accepted != nil && r.Accepted.Ballot > best {
					best = r.Accepted.Ballot
					value = r.Accepted.Value
				}
			}

			// Phase 2: ask them to accept (ballot, value).
			p.mu.Lock()
			local := p.onAccept(ballot, value)
			p.mu.Unlock()
			accepts := p.ask(ctx, map[string]any{"type": "accept", "ballot": ballot, "value": value}, local)
			if len(accepts) >= p.majority {
				p.mu.Lock()
				p.learn(ctx, value)
				p.mu.Unlock()
				break
			}
		}

		// Someone else holds a higher ballot, or messages were lost. Wait a
		// random, growing time so that duelling proposers stop pre-empting
		// each other.
		wait := 20 + p.n.Rand()*40*math.Min(float64(attempt), 5)
		time.Sleep(time.Duration(wait * float64(time.Millisecond)))
	}
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()
}

// ------------------------------------------------------------------ clients

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
	if !p.running {
		p.running = true
		go p.propose(ctx)
	}
	return nil
}

func (p *paxos) handleGet(ctx context.Context, msg quorumlab.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n.Reply(msg, map[string]any{"type": "get_ok", "value": p.chosen})
}
