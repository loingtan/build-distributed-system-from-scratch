// Multi-Paxos: one Paxos instance per log slot. A leader runs phase 1 once,
// for every slot at the same time, then runs only phase 2 for each command.
//
// Methods documented "(p.mu held)" are called with the lock held. Never hold
// p.mu across n.Call: ask gathers replies and takes the lock only to look at
// each one.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"quorumlab"
)

// Accepted is what an acceptor accepted for one slot.
type Accepted struct {
	Ballot int `json:"ballot"`
	Value  any `json:"value"`
}

// Report is one accepted slot, as sent in a promise.
type Report struct {
	Slot   int `json:"slot"`
	Ballot int `json:"ballot"`
	Value  any `json:"value"`
}

// reply answers prepare, accept and heartbeat.
type reply struct {
	Type     string   `json:"type"`
	OK       bool     `json:"ok"`
	Promised int      `json:"promised"`
	Accepted []Report `json:"accepted,omitempty"`
}

type paxos struct {
	n        *quorumlab.Node
	size     int
	majority int
	me       int

	mu sync.Mutex // guards every field below

	// Acceptor. One promise covers every slot.
	promised int
	accepted map[int]Accepted

	// Learner. log holds the values of slots 1..len(log), all chosen; nil is a no-op.
	chosen map[int]any
	log    []any

	// Proposer / leader.
	role     string // "follower", "candidate" or "leader"
	ballot   int    // the ballot this node leads with
	leader   string // who this node believes leads ("" if unknown)
	round    int
	highest  int                       // the highest ballot seen anywhere
	nextSlot int                       // leader only: the next free slot
	waiting  map[int]quorumlab.Message // leader only: slot -> the append to answer

	electionTimer *quorumlab.Timer
	timerGen      int
}

func main() {
	n := quorumlab.NewNode()
	p := &paxos{
		n:        n,
		accepted: map[int]Accepted{},
		chosen:   map[int]any{},
		log:      []any{},
		role:     "follower",
		nextSlot: 1,
		waiting:  map[int]quorumlab.Message{},
	}

	n.Handle("prepare", p.handlePrepare)
	n.Handle("accept", p.handleAccept)
	n.Handle("heartbeat", p.handleHeartbeat)
	n.Handle("decide", p.handleDecide)
	n.Handle("append", p.handleAppend)
	n.Handle("log", p.handleLog)

	n.OnStart(func(ctx context.Context) {
		p.size = len(n.NodeIDs())
		p.majority = p.size/2 + 1
		for i, id := range n.NodeIDs() {
			if id == n.ID() {
				p.me = i
			}
		}
		p.mu.Lock()
		p.resetElectionTimer()
		p.mu.Unlock()
		n.Every(100*time.Millisecond, p.heartbeat)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// nextBallot (p.mu held): unique per node, above every ballot seen.
func (p *paxos) nextBallot() int {
	p.round = max(p.round, max(p.highest, p.promised)/p.size) + 1
	return p.round*p.size + p.me
}

// resetElectionTimer (p.mu held).
func (p *paxos) resetElectionTimer() {
	p.electionTimer.Stop()
	p.timerGen++
	gen := p.timerGen
	timeout := time.Duration((300 + 300*p.n.Rand()) * float64(time.Millisecond))
	p.electionTimer = p.n.After(timeout, func(ctx context.Context) {
		p.mu.Lock()
		current := gen == p.timerGen
		p.mu.Unlock()
		if current {
			p.campaign(ctx)
		}
	})
}

// stepDown (p.mu held).
func (p *paxos) stepDown() {
	if p.role == "leader" {
		for _, msg := range p.waiting {
			p.n.Reply(msg, map[string]any{"type": "not_leader", "leader": nil})
		}
		clear(p.waiting)
	}
	p.role = "follower"
	p.resetElectionTimer()
}

// heardBallot (p.mu held): a message carried ballot b. A higher ballot than
// ours means someone else is (trying to be) leader.
func (p *paxos) heardBallot(b int, from string) {
	p.highest = max(p.highest, b)
	if p.role != "follower" && b > p.ballot && from != p.n.ID() {
		p.stepDown()
	}
}

// ---------------------------------------------------------------- acceptor

// setPromised (p.mu held).
func (p *paxos) setPromised(b int) { p.promised = b }

// setAccepted (p.mu held).
func (p *paxos) setAccepted(slot int, a Accepted) { p.accepted[slot] = a }

// onPrepare (p.mu held).
func (p *paxos) onPrepare(b, from int) reply {
	if b > p.promised {
		p.setPromised(b)
	}
	list := []Report{}
	for s, a := range p.accepted {
		if s >= from {
			list = append(list, Report{Slot: s, Ballot: a.Ballot, Value: a.Value})
		}
	}
	return reply{Type: "prepare_ok", OK: b >= p.promised, Promised: p.promised, Accepted: list}
}

// onAccept (p.mu held).
func (p *paxos) onAccept(b, slot int, value any) reply {
	if b < p.promised {
		return reply{Type: "accept_ok", OK: false, Promised: p.promised}
	}
	if b > p.promised {
		p.setPromised(b)
	}
	p.setAccepted(slot, Accepted{Ballot: b, Value: value})
	return reply{Type: "accept_ok", OK: true, Promised: p.promised}
}

func (p *paxos) handlePrepare(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Ballot int `json:"ballot"`
		From   int `json:"from"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.heardBallot(req.Ballot, msg.Src)
	r := p.onPrepare(req.Ballot, req.From)
	if r.OK {
		p.resetElectionTimer() // give the candidate time to win
	}
	return p.n.Reply(msg, r)
}

func (p *paxos) handleAccept(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Ballot int `json:"ballot"`
		Slot   int `json:"slot"`
		Value  any `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.heardBallot(req.Ballot, msg.Src)
	r := p.onAccept(req.Ballot, req.Slot, req.Value)
	if r.OK {
		p.leader = msg.Src
		p.resetElectionTimer()
	}
	return p.n.Reply(msg, r)
}

func (p *paxos) handleHeartbeat(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Ballot int `json:"ballot"`
		Known  int `json:"known"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.heardBallot(req.Ballot, msg.Src)
	ok := req.Ballot >= p.promised
	if ok {
		if req.Ballot > p.promised {
			p.setPromised(req.Ballot)
		}
		p.leader = msg.Src
		p.resetElectionTimer()
	}
	return p.n.Reply(msg, reply{Type: "heartbeat_ok", OK: ok, Promised: p.promised})
}

// ----------------------------------------------------------------- learner

// learn (p.mu held) records a chosen slot, extends the log and answers every
// waiting client whose slot is now in it.
func (p *paxos) learn(slot int, value any) {
	if _, ok := p.chosen[slot]; ok {
		return
	}
	p.chosen[slot] = value
	for {
		v, ok := p.chosen[len(p.log)+1]
		if !ok {
			break
		}
		p.log = append(p.log, v)
	}
	for s, msg := range p.waiting {
		if s > len(p.log) {
			continue
		}
		delete(p.waiting, s)
		var req struct {
			Value any `json:"value"`
		}
		msg.Decode(&req)
		if p.log[s-1] == req.Value {
			p.n.Reply(msg, map[string]any{"type": "append_ok", "slot": s})
		} else {
			p.n.Reply(msg, map[string]any{"type": "not_leader", "leader": nil})
		}
	}
}

func (p *paxos) handleDecide(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Slot  int `json:"slot"`
		Value any `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.learn(req.Slot, req.Value)
	return nil
}

// ------------------------------------------------------------------ leader

// ask sends body to every acceptor (this node's answer is local) and returns
// the positive replies once a majority said yes or all have answered.
func (p *paxos) ask(ctx context.Context, body map[string]any, local reply) []reply {
	peers := p.n.Peers()
	results := make(chan *reply, len(peers))
	for _, peer := range peers {
		go func(peer string) {
			res, err := p.n.Call(ctx, peer, body, 250*time.Millisecond)
			var r reply
			if err != nil || res.Decode(&r) != nil {
				results <- nil
				return
			}
			results <- &r
		}(peer)
	}
	var oks []reply
	see := func(r *reply) {
		if r == nil {
			return
		}
		p.mu.Lock()
		p.highest = max(p.highest, r.Promised)
		p.mu.Unlock()
		if r.OK {
			oks = append(oks, *r)
		}
	}
	see(&local)
	for pending := len(peers); pending > 0 && len(oks) < p.majority; pending-- {
		see(<-results)
	}
	return oks
}

// campaign runs phase 1, once, for every slot from our first unknown one on.
func (p *paxos) campaign(ctx context.Context) {
	p.mu.Lock()
	if p.role == "leader" {
		p.mu.Unlock()
		return
	}
	p.role = "candidate"
	p.leader = ""
	b := p.nextBallot()
	p.ballot = b
	p.resetElectionTimer()
	from := len(p.log) + 1
	local := p.onPrepare(b, from)
	p.mu.Unlock()

	promises := p.ask(ctx, map[string]any{"type": "prepare", "ballot": b, "from": from}, local)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != "candidate" || p.ballot != b {
		return
	}
	if len(promises) < p.majority || p.highest > b {
		p.role = "follower"
		return // the election timer tries again later
	}
	p.role = "leader"
	p.leader = p.n.ID()
	p.electionTimer.Stop()
	p.timerGen++

	// For every slot a promise mentions, the accepted value with the highest
	// ballot may already be chosen: re-propose it. Holes get a no-op (nil).
	best := map[int]Accepted{}
	top := len(p.log)
	for _, r := range promises {
		for _, a := range r.Accepted {
			if cur, ok := best[a.Slot]; !ok || a.Ballot > cur.Ballot {
				best[a.Slot] = Accepted{Ballot: a.Ballot, Value: a.Value}
			}
			top = max(top, a.Slot)
		}
	}
	for s := range p.chosen {
		top = max(top, s)
	}
	p.nextSlot = top + 1
	go p.heartbeat(ctx)
	for s := from; s <= top; s++ {
		value, ok := p.chosen[s]
		if !ok {
			value = best[s].Value // nil (a no-op) if nobody reported the slot
		}
		go p.propose(ctx, b, s, value)
	}
}

// propose runs phase 2 for one slot, retried while we lead with ballot b.
func (p *paxos) propose(ctx context.Context, b, slot int, value any) {
	for {
		p.mu.Lock()
		if p.role != "leader" || p.ballot != b {
			p.mu.Unlock()
			return
		}
		local := p.onAccept(b, slot, value)
		p.mu.Unlock()

		oks := p.ask(ctx, map[string]any{"type": "accept", "ballot": b, "slot": slot, "value": value}, local)

		p.mu.Lock()
		if p.role != "leader" || p.ballot != b {
			p.mu.Unlock()
			return
		}
		if len(oks) >= p.majority {
			p.learn(slot, value)
			p.mu.Unlock()
			for _, peer := range p.n.Peers() {
				p.n.Send(ctx, peer, map[string]any{"type": "decide", "slot": slot, "value": value})
			}
			return
		}
		if p.highest > b {
			p.stepDown()
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *paxos) heartbeat(ctx context.Context) {
	p.mu.Lock()
	if p.role != "leader" {
		p.mu.Unlock()
		return
	}
	body := map[string]any{"type": "heartbeat", "ballot": p.ballot, "known": len(p.log)}
	p.mu.Unlock()
	for _, peer := range p.n.Peers() {
		go func(peer string) {
			res, err := p.n.Call(ctx, peer, body, 250*time.Millisecond)
			var r reply
			if err != nil || res.Decode(&r) != nil || r.OK {
				return
			}
			p.mu.Lock()
			p.heardBallot(r.Promised, peer)
			p.mu.Unlock()
		}(peer)
	}
}

// ------------------------------------------------------------------ clients

func (p *paxos) handleAppend(ctx context.Context, msg quorumlab.Message) error {
	var req struct {
		Value any `json:"value"`
	}
	if err := msg.Decode(&req); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != "leader" {
		return p.n.Reply(msg, map[string]any{"type": "not_leader", "leader": orNull(p.leader)})
	}
	slot := p.nextSlot
	p.nextSlot++
	p.waiting[slot] = msg
	go p.propose(ctx, p.ballot, slot, req.Value)
	return nil
}

func (p *paxos) handleLog(ctx context.Context, msg quorumlab.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n.Reply(msg, map[string]any{"type": "log_ok", "entries": p.log})
}

func orNull(s string) any {
	if s == "" {
		return nil
	}
	return s
}
