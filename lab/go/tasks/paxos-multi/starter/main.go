// A log of slots. Read the brief: npm run lab -- brief paxos-multi
//
// Multi-Paxos: one Paxos instance per log slot. A leader runs phase 1 once,
// for every slot at the same time, then runs only phase 2 for each command.
//
// This starter has a fixed leader, n1, that numbers the commands and tells
// everyone. Nobody accepts or promises anything, so when n1 dies the log
// stops, and nothing records which slots were really chosen.
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

// Accepted is what an acceptor accepted for one slot.
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

	// Acceptor. One promise covers every slot.
	promised int
	accepted map[int]Accepted

	// Learner. log holds the values of slots 1..len(log), all chosen; nil is a no-op.
	chosen map[int]any
	log    []any

	// Proposer / leader.
	role     string                    // TODO: elect with phase 1
	ballot   int                       // the ballot this node leads with
	leader   string                    // who this node believes leads
	nextSlot int                       // leader only: the next free slot
	waiting  map[int]quorumlab.Message // leader only: slot -> the append to answer
}

func main() {
	n := quorumlab.NewNode()
	p := &paxos{
		n:        n,
		accepted: map[int]Accepted{},
		chosen:   map[int]any{},
		log:      []any{},
		leader:   "n1",
		nextSlot: 1,
		waiting:  map[int]quorumlab.Message{},
	}

	n.Handle("decide", p.handleDecide)
	n.Handle("append", p.handleAppend)
	n.Handle("log", p.handleLog)
	// TODO: acceptor handlers.
	//   prepare { ballot, from }        -> promise ballot for every slot; reply with
	//                                      everything accepted in slots >= from
	//   accept  { ballot, slot, value } -> accept unless a higher ballot was promised

	// TODO: election. A node that hears nothing from a leader for a random
	// 300-600ms runs phase 1 with a higher ballot. With a majority of promises
	// it leads: it re-proposes, for every slot mentioned, the accepted value
	// with the highest ballot, fills holes with nil, and sends heartbeats
	// every 100ms.

	n.OnStart(func(ctx context.Context) {
		p.size = len(n.NodeIDs())
		p.majority = p.size/2 + 1
		for i, id := range n.NodeIDs() {
			if id == n.ID() {
				p.me = i
			}
		}
		p.role = "follower"
		if n.ID() == "n1" {
			p.role = "leader"
		}
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

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

// propose (p.mu held).
func (p *paxos) propose(ctx context.Context, slot int, value any) {
	// TODO: phase 2. Send accept { ballot, slot, value } to every node and
	// count the yeses (yours too); the slot is chosen with a majority. Do it
	// from a goroutine: never hold p.mu across n.Call.
	p.learn(slot, value) // WRONG: nobody else stored it
	for _, peer := range p.n.Peers() {
		p.n.Send(ctx, peer, map[string]any{"type": "decide", "slot": slot, "value": value})
	}
}

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
		return p.n.Reply(msg, map[string]any{"type": "not_leader", "leader": p.leader})
	}
	slot := p.nextSlot
	p.nextSlot++
	p.waiting[slot] = msg
	p.propose(ctx, slot, req.Value)
	return nil
}

func (p *paxos) handleLog(ctx context.Context, msg quorumlab.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n.Reply(msg, map[string]any{"type": "log_ok", "entries": p.log})
}
