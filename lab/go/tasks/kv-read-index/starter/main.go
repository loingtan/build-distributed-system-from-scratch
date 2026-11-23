// Reads that are never stale. Read the brief: npm run lab -- brief kv-read-index
//
// This is the sessions solution with one change. A get through the log
// costs a log entry, a disk write on a majority and a round of replication,
// for an operation that changes nothing. So here the leader answers get
// straight from its own store. It is fast, and it is wrong: a leader that
// has been cut off, or that was only just elected, does not know that its
// store is current.
//
// TODO: before answering a get, (1) make sure this term has committed an
// entry (append a no-op when elected), (2) note the commit index, (3) hear
// from a majority that we are still the leader, (4) wait until the noted
// index is applied. Gets must not be written to the log.
//
// Term, vote and log are on disk (one atomic n.Disk().Update before anything
// that depends on them is sent); the store is rebuilt from the log.
//
// The disk is written with one atomic n.Disk().Update BEFORE anything that
// depends on it is sent: a crash can land the instant a message leaves.
//
// Every method on raft is called with r.mu held. RPCs are sent from
// goroutines, with a copy of the entries, because r.mu must never be held
// across n.Call.
package main

import (
	"context"
	"log"
	"reflect"
	"sync"
	"time"

	"quorumlab"
)

// Entry is one log entry. Index 1 is log[0].
type Entry struct {
	Term  int    `json:"term"`
	Op    string `json:"op"` // "put", "get" or "cas"
	Key   string `json:"key,omitempty"`
	Value any    `json:"value,omitempty"` // put
	From  any    `json:"from,omitempty"`  // cas; absent means null
	To    any    `json:"to,omitempty"`    // cas

	// The session fields travel with the entry, so the decision can be made at apply time.
	ClientID string `json:"client_id,omitempty"`
	Seq      int    `json:"seq,omitempty"`
}

// session is a client's last applied write and the reply it produced.
type session struct {
	Seq   int            `json:"seq"`
	Reply map[string]any `json:"reply"`
}

type requestVote struct {
	Type         string `json:"type"`
	Term         int    `json:"term"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type appendEntries struct {
	Type      string  `json:"type"`
	Term      int     `json:"term"`
	PrevIndex int     `json:"prevIndex"`
	PrevTerm  int     `json:"prevTerm"`
	Entries   []Entry `json:"entries"`
	Commit    int     `json:"commit"`
}

type appendEntriesReply struct {
	Term       int  `json:"term"`
	Success    bool `json:"success"`
	MatchIndex int  `json:"matchIndex"`
	LogLength  int  `json:"logLength"`
}

type raft struct {
	n  *quorumlab.Node
	mu sync.Mutex // guards every field below

	term     int
	role     string // "follower", "candidate" or "leader"
	votedFor string // "" for nobody
	leader   string // "" if unknown
	votes    int

	electionTimer *quorumlab.Timer
	timerGen      int

	log         []Entry                   // index 1 is log[0]; durable, like term and votedFor
	commitIndex int                       // relearned from the leader after a restart
	store       map[string]any            // rebuilt by applying the log again as commitIndex advances
	sessions    map[string]session        // part of the state machine too: client_id -> its last write
	nextIndex   map[string]int            // leader only: the next index to send to each peer
	matchIndex  map[string]int            // leader only: how much of each peer's log is known to match ours
	waiting     map[int]quorumlab.Message // leader only: index -> the client request to answer when it commits
}

func main() {
	n := quorumlab.NewNode()
	r := &raft{
		n:          n,
		role:       "follower",
		log:        []Entry{},
		store:      map[string]any{},
		sessions:   map[string]session{},
		nextIndex:  map[string]int{},
		matchIndex: map[string]int{},
		waiting:    map[int]quorumlab.Message{},
	}

	n.Handle("request_vote", r.handleRequestVote)
	n.Handle("append_entries", r.handleAppendEntries)
	n.Handle("put", r.handlePut)
	n.Handle("get", r.handleGet)
	n.Handle("cas", r.handleCas)
	n.Handle("log", r.handleLog)
	n.Handle("status", r.handleStatus)

	n.OnStart(func(ctx context.Context) {
		r.mu.Lock()
		r.load() // OnStart runs before any message is handled, after every restart too
		r.resetElectionTimer()
		r.mu.Unlock()

		n.Every(100*time.Millisecond, func(ctx context.Context) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.role == "leader" {
				r.replicate(ctx)
			}
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// load reads the durable state back. On first start the disk is empty.
func (r *raft) load() {
	disk := r.n.Disk()
	if _, err := disk.Get("term", &r.term); err != nil {
		log.Fatalf("reading term: %v", err)
	}
	if _, err := disk.Get("votedFor", &r.votedFor); err != nil {
		log.Fatalf("reading votedFor: %v", err)
	}
	if _, err := disk.Get("log", &r.log); err != nil {
		log.Fatalf("reading log: %v", err)
	}
	if r.log == nil {
		r.log = []Entry{}
	}
}

// persist writes term, votedFor and the log in one atomic, fsynced write.
// Call it after changing any of them and before sending anything. A node
// that cannot keep its promises must not go on: a failed write is fatal.
func (r *raft) persist() {
	err := r.n.Disk().Update(func(tx *quorumlab.Tx) error {
		if err := tx.Set("term", r.term); err != nil {
			return err
		}
		if err := tx.Set("votedFor", orNull(r.votedFor)); err != nil {
			return err
		}
		return tx.Set("log", r.log)
	})
	if err != nil {
		log.Fatalf("writing to disk: %v", err)
	}
}

func (r *raft) majority() int { return len(r.n.NodeIDs())/2 + 1 }

// termAt is the term of entry i, or 0 for i == 0.
func (r *raft) termAt(i int) int {
	if i == 0 {
		return 0
	}
	return r.log[i-1].Term
}

func (r *raft) lastLogTerm() int { return r.termAt(len(r.log)) }

func (r *raft) resetElectionTimer() {
	r.electionTimer.Stop()
	r.timerGen++
	gen := r.timerGen
	timeout := time.Duration((300 + 300*r.n.Rand()) * float64(time.Millisecond))
	r.electionTimer = r.n.After(timeout, func(ctx context.Context) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if gen == r.timerGen {
			r.startElection(ctx)
		}
	})
}

func (r *raft) stopElectionTimer() {
	r.electionTimer.Stop()
	r.timerGen++
}

func (r *raft) adoptTerm(term int) {
	wasLeader := r.role == "leader"
	r.term = term
	r.role = "follower"
	r.votedFor = ""
	r.persist() // the new term is on disk before anyone hears of it
	r.leader = ""
	// These requests are in our log and may still commit under the next
	// leader. "not_leader" would tell the client they did not happen, so say
	// nothing: the client times out and knows the outcome is unknown.
	clear(r.waiting)
	// Hearing of a newer term is not hearing from a leader: keep the election
	// timer running (a deposed leader starts one). Otherwise candidates whose
	// logs are too stale to win keep resetting it, and the node that could win
	// never stands.
	if wasLeader {
		r.resetElectionTimer()
	}
}

func (r *raft) startElection(ctx context.Context) {
	r.term++
	r.role = "candidate"
	r.votedFor = r.n.ID()
	r.persist()
	r.leader = ""
	r.votes = 1
	r.resetElectionTimer()

	ask := requestVote{Type: "request_vote", Term: r.term, LastLogIndex: len(r.log), LastLogTerm: r.lastLogTerm()}
	for _, peer := range r.n.Peers() {
		go r.requestVote(ctx, peer, ask)
	}
}

func (r *raft) requestVote(ctx context.Context, peer string, ask requestVote) {
	res, err := r.n.Call(ctx, peer, ask, 200*time.Millisecond)
	if err != nil {
		return
	}
	var vote struct {
		Term    int  `json:"term"`
		Granted bool `json:"granted"`
	}
	if err := res.Decode(&vote); err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if vote.Term > r.term {
		r.adoptTerm(vote.Term)
		return
	}
	if r.role != "candidate" || r.term != ask.Term || !vote.Granted {
		return
	}
	r.votes++
	if r.votes >= r.majority() {
		r.becomeLeader(ctx)
	}
}

func (r *raft) becomeLeader(ctx context.Context) {
	r.role = "leader"
	r.leader = r.n.ID()
	r.stopElectionTimer()
	for _, peer := range r.n.Peers() {
		r.nextIndex[peer] = len(r.log) + 1
		r.matchIndex[peer] = 0 // nothing is known until the peer says so
	}
	r.replicate(ctx)
}

func (r *raft) replicate(ctx context.Context) {
	for _, peer := range r.n.Peers() {
		r.sendEntries(ctx, peer)
	}
}

func (r *raft) sendEntries(ctx context.Context, peer string) {
	sentTerm := r.term
	prevIndex := r.nextIndex[peer] - 1
	req := appendEntries{
		Type:      "append_entries",
		Term:      r.term,
		PrevIndex: prevIndex,
		PrevTerm:  r.termAt(prevIndex),
		Entries:   append([]Entry{}, r.log[prevIndex:]...), // a copy: r.log changes once we unlock
		Commit:    r.commitIndex,
	}
	go func() {
		res, err := r.n.Call(ctx, peer, req, 100*time.Millisecond)
		if err != nil {
			return
		}
		var reply appendEntriesReply
		if err := res.Decode(&reply); err != nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if reply.Term > r.term {
			r.adoptTerm(reply.Term)
			return
		}
		if r.role != "leader" || r.term != sentTerm {
			return
		}
		if reply.Success {
			r.nextIndex[peer] = max(r.nextIndex[peer], reply.MatchIndex+1)
			r.matchIndex[peer] = max(r.matchIndex[peer], reply.MatchIndex)
			r.advanceCommit()
		} else {
			// The follower's log differs from ours at or before prevIndex.
			// Step back one entry (or straight to the end of its log) and try again.
			r.nextIndex[peer] = max(1, min(r.nextIndex[peer]-1, reply.LogLength+1))
			r.sendEntries(ctx, peer)
		}
	}()
}

// advanceCommit (leader): the highest index stored on a majority of the
// whole cluster is committed, but only if that entry is from our own term.
// An older entry on a majority can still be overwritten; it commits along
// with a newer one.
func (r *raft) advanceCommit() {
	for n := len(r.log); n > r.commitIndex; n-- {
		if r.termAt(n) != r.term {
			break
		}
		copies := 1 // our own
		for _, peer := range r.n.Peers() {
			if r.matchIndex[peer] >= n {
				copies++
			}
		}
		if copies >= r.majority() {
			r.commitTo(n)
			break
		}
	}
}

// apply is the state machine. Every node applies the same entries in the
// same order, so every node computes the same result for each one. The
// result is the reply.
func (r *raft) apply(entry Entry) map[string]any {
	current := r.store[entry.Key] // nil (null) when unset
	if entry.Op == "get" {
		return map[string]any{"type": "get_ok", "value": current}
	}
	if entry.Op != "put" && entry.Op != "cas" {
		return nil
	}

	// Exactly once. The same request can be in the log several times: client
	// retries, duplicated messages, a retry sent to a new leader. Deciding here,
	// when the entry is applied, means every node (and every future leader)
	// makes the same decision, from the same log.
	if last, ok := r.sessions[entry.ClientID]; ok && entry.Seq <= last.Seq {
		if entry.Seq == last.Seq {
			return last.Reply
		}
		return nil // a leftover: the client has moved on
	}
	var reply map[string]any
	switch {
	case entry.Op == "put":
		r.store[entry.Key] = entry.Value
		reply = map[string]any{"type": "put_ok"}
	case reflect.DeepEqual(current, entry.From):
		r.store[entry.Key] = entry.To
		reply = map[string]any{"type": "cas_ok"}
	default:
		reply = map[string]any{"type": "cas_failed"}
	}
	r.sessions[entry.ClientID] = session{Seq: entry.Seq, Reply: reply}
	return reply
}

// commitTo applies entries in order, up to index, and answers the clients
// waiting on them. This is the only place the state machine changes.
func (r *raft) commitTo(index int) {
	for r.commitIndex < index {
		entry := r.log[r.commitIndex]
		r.commitIndex++
		result := r.apply(entry)
		if client, ok := r.waiting[r.commitIndex]; ok {
			delete(r.waiting, r.commitIndex)
			if result != nil {
				r.n.Reply(client, result)
			}
		}
	}
}

func (r *raft) handleRequestVote(ctx context.Context, msg quorumlab.Message) error {
	var req requestVote
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	// The election restriction: never vote for a log that is behind ours.
	// A later last term wins; with equal last terms the longer log wins.
	upToDate := req.LastLogTerm > r.lastLogTerm() || (req.LastLogTerm == r.lastLogTerm() && req.LastLogIndex >= len(r.log))
	granted := req.Term == r.term && upToDate && (r.votedFor == "" || r.votedFor == msg.Src)
	if granted {
		r.votedFor = msg.Src
		r.persist() // the vote is on disk before the candidate hears about it
		r.resetElectionTimer()
	}
	return r.n.Reply(msg, map[string]any{"type": "request_vote_ok", "term": r.term, "granted": granted})
}

func (r *raft) handleAppendEntries(ctx context.Context, msg quorumlab.Message) error {
	var req appendEntries
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term < r.term {
		return r.refuseEntries(msg)
	}
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	r.role = "follower"
	r.leader = msg.Src
	r.resetElectionTimer()

	// Log matching: accept the entries only if our log is identical to the
	// leader's up to prevIndex. Same index and same term means same history.
	if req.PrevIndex > len(r.log) || r.termAt(req.PrevIndex) != req.PrevTerm {
		return r.refuseEntries(msg)
	}
	changed := false
	for i, entry := range req.Entries {
		index := req.PrevIndex + 1 + i
		// An entry of ours that conflicts with the leader's was never
		// committed. Drop it and all after it. Matching entries stay, even
		// when this message is a late duplicate.
		if index <= len(r.log) && r.log[index-1].Term != entry.Term {
			r.log = r.log[:index-1]
			changed = true
		}
		if index > len(r.log) {
			r.log = append(r.log, entry)
			changed = true
		}
	}
	if changed {
		r.persist() // "I have these entries" is a promise: keep it on disk first
	}
	// The leader's commit index may be ahead of what this message covered.
	matched := req.PrevIndex + len(req.Entries)
	r.commitTo(min(req.Commit, matched))
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": true, "matchIndex": matched})
}

// refuseEntries answers an append_entries that is stale or does not attach
// to our log, with our term and how long our log is.
func (r *raft) refuseEntries(msg quorumlab.Message) error {
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": false, "logLength": len(r.log)})
}

// propose: every client operation, reads included, becomes a log entry.
// The leader answers once that entry is committed and applied. A retry
// becomes a new entry too; apply recognises it.
func (r *raft) propose(ctx context.Context, msg quorumlab.Message, entry Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != "leader" {
		return r.n.Reply(msg, map[string]any{"type": "not_leader", "leader": orNull(r.leader)})
	}
	entry.Term = r.term
	r.log = append(r.log, entry)
	r.persist()
	r.waiting[len(r.log)] = msg // answered in commitTo, not here
	r.replicate(ctx)
	return nil
}

// clientRequest is the body of put, get and cas.
type clientRequest struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
	From  any    `json:"from"`
	To    any    `json:"to"`

	ClientID string `json:"client_id"`
	Seq      int    `json:"seq"`
}

func (r *raft) handlePut(ctx context.Context, msg quorumlab.Message) error {
	var req clientRequest
	if err := msg.Decode(&req); err != nil {
		return err
	}
	return r.propose(ctx, msg, Entry{Op: "put", Key: req.Key, Value: req.Value, ClientID: req.ClientID, Seq: req.Seq})
}

func (r *raft) handleGet(ctx context.Context, msg quorumlab.Message) error {
	var req clientRequest
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != "leader" {
		return r.n.Reply(msg, map[string]any{"type": "not_leader", "leader": orNull(r.leader)})
	}
	return r.n.Reply(msg, map[string]any{"type": "get_ok", "value": r.store[req.Key]}) // TODO: may be stale
}

func (r *raft) handleCas(ctx context.Context, msg quorumlab.Message) error {
	var req clientRequest
	if err := msg.Decode(&req); err != nil {
		return err
	}
	return r.propose(ctx, msg, Entry{Op: "cas", Key: req.Key, From: req.From, To: req.To, ClientID: req.ClientID, Seq: req.Seq})
}

func (r *raft) handleLog(ctx context.Context, msg quorumlab.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n.Reply(msg, map[string]any{"type": "log_ok", "entries": r.log, "commit": r.commitIndex})
}

func (r *raft) handleStatus(ctx context.Context, msg quorumlab.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n.Reply(msg, map[string]any{"type": "status_ok", "role": r.role, "term": r.term, "leader": orNull(r.leader)})
}

// orNull turns "" (nobody) into JSON null.
func orNull(id string) any {
	if id == "" {
		return nil
	}
	return id
}
