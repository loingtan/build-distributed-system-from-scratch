// Log compaction on a linearizable Raft key-value store. Once the log holds
// enough applied entries, every node folds them into a snapshot of the state
// machine (store and session table) and drops them. A follower that needs
// entries the leader has dropped gets the snapshot instead.
//
// Durable state, on disk under the same keys as the JS code: "term",
// "votedFor", "snap" and "log" (the entries after the snapshot). They are
// written with one atomic n.Disk().Update before anything that depends on
// them is sent: a crash can land the instant a message leaves.
//
// Every method on raft is called with r.mu held. RPCs are sent from
// goroutines, with a copy of the entries, because r.mu must never be held
// across n.Call.
package main

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"sync"
	"time"

	"quorumlab"
)

// snapshotEvery is how many applied entries the log keeps before they are
// folded into the snapshot.
const snapshotEvery = 25

// Entry is one log entry. Entry i is log[i - snap.Index - 1].
type Entry struct {
	Term  int    `json:"term"`
	Op    string `json:"op"` // "put", "cas" or "noop"
	Key   string `json:"key,omitempty"`
	Value any    `json:"value,omitempty"` // put
	From  any    `json:"from,omitempty"`  // cas; absent means null
	To    any    `json:"to,omitempty"`    // cas

	ClientID string `json:"client_id,omitempty"`
	Seq      int    `json:"seq,omitempty"`
}

// Snapshot is the state machine as it stood after entry Index, which had
// term Term. It stands for entries 1..Index.
type Snapshot struct {
	Index    int                `json:"index"`
	Term     int                `json:"term"`
	Store    map[string]any     `json:"store"`
	Sessions map[string]session `json:"sessions"`
}

// session is a client's last applied write and the reply it produced.
type session struct {
	Seq   int            `json:"seq"`
	Reply map[string]any `json:"reply"`
}

// readReq is a get waiting for its leadership check and its read index.
type readReq struct {
	msg       quorumlab.Message
	key       string
	index     int  // the commit index when it arrived (at least this term's no-op)
	confirmed bool // a majority answered a round sent after it arrived
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

type installSnapshot struct {
	Type     string   `json:"type"`
	Term     int      `json:"term"`
	Snapshot Snapshot `json:"snapshot"`
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

	snap        Snapshot                  // durable: entries 1..snap.Index, folded into a state machine
	log         []Entry                   // durable: entries snap.Index+1 .. lastIndex()
	commitIndex int                       // starts at snap.Index after a restart
	store       map[string]any            // the state machine, starting from the snapshot
	sessions    map[string]session        // part of the state machine: client_id -> its last write
	nextIndex   map[string]int            // leader only: the next index to send to each peer
	matchIndex  map[string]int            // leader only: how much of each peer's log is known to match ours
	waiting     map[int]quorumlab.Message // leader only: index -> the client request to answer when it commits
	reads       []*readReq                // leader only: gets waiting to be answered
	termStart   int                       // leader only: index of this term's no-op entry
}

func main() {
	n := quorumlab.NewNode()
	r := &raft{
		n:          n,
		role:       "follower",
		nextIndex:  map[string]int{},
		matchIndex: map[string]int{},
		waiting:    map[int]quorumlab.Message{},
	}

	n.Handle("request_vote", r.handleRequestVote)
	n.Handle("append_entries", r.handleAppendEntries)
	n.Handle("install_snapshot", r.handleInstallSnapshot)
	n.Handle("put", r.handlePut)
	n.Handle("get", r.handleGet)
	n.Handle("cas", r.handleCas)
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

// clone deep-copies a JSON-shaped value, so the snapshot never shares maps
// with the live state machine.
func clone[T any](v T) T {
	b, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("clone: %v", err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		log.Fatalf("clone: %v", err)
	}
	return out
}

// load reads the durable state back and starts the state machine from the
// snapshot. On first start the disk is empty.
func (r *raft) load() {
	disk := r.n.Disk()
	for key, v := range map[string]any{"term": &r.term, "votedFor": &r.votedFor, "snap": &r.snap, "log": &r.log} {
		if _, err := disk.Get(key, v); err != nil {
			log.Fatalf("reading %s: %v", key, err)
		}
	}
	if r.log == nil {
		r.log = []Entry{}
	}
	if r.snap.Store == nil {
		r.snap.Store = map[string]any{}
	}
	if r.snap.Sessions == nil {
		r.snap.Sessions = map[string]session{}
	}
	r.store = clone(r.snap.Store)
	r.sessions = clone(r.snap.Sessions)
	r.commitIndex = r.snap.Index // the snapshot is committed and applied
}

// persist writes term, votedFor, the snapshot and the log in one atomic,
// fsynced write. A node that cannot keep its promises must not go on: a
// failed write is fatal.
func (r *raft) persist() {
	err := r.n.Disk().Update(func(tx *quorumlab.Tx) error {
		for key, v := range map[string]any{"term": r.term, "votedFor": orNull(r.votedFor), "snap": r.snap, "log": r.log} {
			if err := tx.Set(key, v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("writing to disk: %v", err)
	}
}

func (r *raft) majority() int { return len(r.n.NodeIDs())/2 + 1 }

// The log no longer starts at index 1. The snapshot stands for entries
// 1..base(); log[0] is entry base()+1.
func (r *raft) base() int      { return r.snap.Index }
func (r *raft) lastIndex() int { return r.base() + len(r.log) }

// termAt is the term of entry i, for base() <= i <= lastIndex().
func (r *raft) termAt(i int) int {
	if i == r.base() {
		return r.snap.Term
	}
	return r.log[i-r.base()-1].Term
}

func (r *raft) entryAt(i int) Entry { return r.log[i-r.base()-1] }
func (r *raft) lastLogTerm() int    { return r.termAt(r.lastIndex()) }

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
	r.persist()
	r.leader = ""
	clear(r.waiting) // these may still commit: say nothing
	for _, rd := range r.reads {
		r.n.Reply(rd.msg, map[string]any{"type": "not_leader", "leader": nil})
	}
	r.reads = nil
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

	ask := requestVote{Type: "request_vote", Term: r.term, LastLogIndex: r.lastIndex(), LastLogTerm: r.lastLogTerm()}
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
		r.nextIndex[peer] = r.lastIndex() + 1
		r.matchIndex[peer] = 0
	}
	r.log = append(r.log, Entry{Term: r.term, Op: "noop"})
	r.persist()
	r.termStart = r.lastIndex()
	r.replicate(ctx)
}

// replicate sends a round; every round is also a leadership check for the
// reads that arrived before it.
func (r *raft) replicate(ctx context.Context) {
	var pending []*readReq
	for _, rd := range r.reads {
		if !rd.confirmed {
			pending = append(pending, rd)
		}
	}
	var onAck func()
	if len(pending) > 0 {
		acks := 1 // ourselves; onAck runs with r.mu held
		onAck = func() {
			acks++
			if acks != r.majority() {
				return
			}
			for _, rd := range pending {
				rd.confirmed = true
			}
			r.serveReads()
		}
	}
	for _, peer := range r.n.Peers() {
		r.sendEntries(ctx, peer, onAck)
	}
}

func (r *raft) sendEntries(ctx context.Context, peer string, onAck func()) {
	// The entries this follower needs next are gone from our log: send the snapshot.
	if r.nextIndex[peer] <= r.base() {
		r.sendSnapshot(ctx, peer, onAck)
		return
	}
	sentTerm := r.term
	prevIndex := r.nextIndex[peer] - 1
	req := appendEntries{
		Type:      "append_entries",
		Term:      r.term,
		PrevIndex: prevIndex,
		PrevTerm:  r.termAt(prevIndex),
		Entries:   append([]Entry{}, r.log[prevIndex-r.base():]...), // a copy: r.log changes once we unlock
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
		if onAck != nil {
			onAck()
		}
		if reply.Success {
			r.nextIndex[peer] = max(r.nextIndex[peer], reply.MatchIndex+1)
			r.matchIndex[peer] = max(r.matchIndex[peer], reply.MatchIndex)
			r.advanceCommit()
		} else {
			r.nextIndex[peer] = max(1, min(r.nextIndex[peer]-1, reply.LogLength+1))
			r.sendEntries(ctx, peer, nil)
		}
	}()
}

func (r *raft) sendSnapshot(ctx context.Context, peer string, onAck func()) {
	sentTerm := r.term
	// r.snap is replaced as a whole on compaction, never changed in place,
	// so the goroutine may encode it without the lock.
	req := installSnapshot{Type: "install_snapshot", Term: r.term, Snapshot: r.snap}
	go func() {
		res, err := r.n.Call(ctx, peer, req, 100*time.Millisecond)
		if err != nil {
			return
		}
		var reply struct {
			Term int `json:"term"`
		}
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
		if onAck != nil {
			onAck()
		}
		r.nextIndex[peer] = max(r.nextIndex[peer], req.Snapshot.Index+1)
		r.matchIndex[peer] = max(r.matchIndex[peer], req.Snapshot.Index)
		r.advanceCommit()
	}()
}

func (r *raft) advanceCommit() {
	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.term {
			break
		}
		copies := 1
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

func (r *raft) apply(entry Entry) map[string]any {
	current := r.store[entry.Key]
	if entry.Op != "put" && entry.Op != "cas" {
		return nil // a no-op
	}
	if last, ok := r.sessions[entry.ClientID]; ok && entry.Seq <= last.Seq {
		if entry.Seq == last.Seq {
			return last.Reply
		}
		return nil
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

func (r *raft) commitTo(index int) {
	for r.commitIndex < index {
		entry := r.entryAt(r.commitIndex + 1)
		r.commitIndex++
		result := r.apply(entry)
		if client, ok := r.waiting[r.commitIndex]; ok {
			delete(r.waiting, r.commitIndex)
			if result != nil {
				r.n.Reply(client, result)
			}
		}
	}
	r.serveReads()
	r.compact()
}

// compact folds applied entries into the snapshot. The snapshot is the state
// machine as it stood after entry commitIndex: the store and the session
// table (or a restarted node would apply an old duplicate again). Only
// committed entries are ever dropped, and every node compacts on its own.
func (r *raft) compact() {
	drop := r.commitIndex - r.base()
	if drop < snapshotEvery {
		return
	}
	r.snap = Snapshot{Index: r.commitIndex, Term: r.termAt(r.commitIndex), Store: clone(r.store), Sessions: clone(r.sessions)}
	r.log = append([]Entry{}, r.log[drop:]...)
	r.persist() // snapshot and shortened log in one atomic write
}

func (r *raft) serveReads() {
	kept := r.reads[:0]
	for _, rd := range r.reads {
		if rd.confirmed && r.commitIndex >= rd.index {
			r.n.Reply(rd.msg, map[string]any{"type": "get_ok", "value": r.store[rd.key]})
		} else {
			kept = append(kept, rd)
		}
	}
	r.reads = kept
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
	upToDate := req.LastLogTerm > r.lastLogTerm() || (req.LastLogTerm == r.lastLogTerm() && req.LastLogIndex >= r.lastIndex())
	granted := req.Term == r.term && upToDate && (r.votedFor == "" || r.votedFor == msg.Src)
	if granted {
		r.votedFor = msg.Src
		r.persist()
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

	matched := req.PrevIndex + len(req.Entries)
	// Entries up to our snapshot are committed, so they match the leader's.
	if matched <= r.base() {
		return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": true, "matchIndex": matched})
	}
	prevIndex, prevTerm, entries := req.PrevIndex, req.PrevTerm, req.Entries
	if prevIndex < r.base() {
		entries = entries[r.base()-prevIndex:]
		prevIndex, prevTerm = r.base(), r.snap.Term
	}
	if prevIndex > r.lastIndex() || r.termAt(prevIndex) != prevTerm {
		return r.refuseEntries(msg)
	}
	changed := false
	for i, entry := range entries {
		index := prevIndex + 1 + i
		if index <= r.lastIndex() && r.termAt(index) != entry.Term {
			r.log = r.log[:index-r.base()-1]
			changed = true
		}
		if index > r.lastIndex() {
			r.log = append(r.log, entry)
			changed = true
		}
	}
	if changed {
		r.persist()
	}
	r.commitTo(min(req.Commit, matched))
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": true, "matchIndex": matched})
}

func (r *raft) refuseEntries(msg quorumlab.Message) error {
	return r.n.Reply(msg, map[string]any{"type": "append_entries_ok", "term": r.term, "success": false, "logLength": r.lastIndex()})
}

// handleInstallSnapshot: a follower too far behind gets the leader's
// snapshot instead of entries.
func (r *raft) handleInstallSnapshot(ctx context.Context, msg quorumlab.Message) error {
	var req installSnapshot
	if err := msg.Decode(&req); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term < r.term {
		return r.n.Reply(msg, map[string]any{"type": "install_snapshot_ok", "term": r.term})
	}
	if req.Term > r.term {
		r.adoptTerm(req.Term)
	}
	r.role = "follower"
	r.leader = msg.Src
	r.resetElectionTimer()

	s := req.Snapshot
	if s.Index > r.commitIndex {
		// Keep what follows the snapshot if our log agrees with it there.
		if s.Index <= r.lastIndex() && r.termAt(s.Index) == s.Term {
			r.log = append([]Entry{}, r.log[s.Index-r.base():]...)
		} else {
			r.log = []Entry{}
		}
		if s.Store == nil {
			s.Store = map[string]any{}
		}
		if s.Sessions == nil {
			s.Sessions = map[string]session{}
		}
		r.snap = s
		r.store = clone(s.Store)
		r.sessions = clone(s.Sessions)
		r.commitIndex = s.Index
		r.persist()
	}
	return r.n.Reply(msg, map[string]any{"type": "install_snapshot_ok", "term": r.term})
}

func (r *raft) propose(ctx context.Context, msg quorumlab.Message, entry Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != "leader" {
		return r.n.Reply(msg, map[string]any{"type": "not_leader", "leader": orNull(r.leader)})
	}
	entry.Term = r.term
	r.log = append(r.log, entry)
	r.persist()
	r.waiting[r.lastIndex()] = msg
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

func (r *raft) handleCas(ctx context.Context, msg quorumlab.Message) error {
	var req clientRequest
	if err := msg.Decode(&req); err != nil {
		return err
	}
	return r.propose(ctx, msg, Entry{Op: "cas", Key: req.Key, From: req.From, To: req.To, ClientID: req.ClientID, Seq: req.Seq})
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
	r.reads = append(r.reads, &readReq{msg: msg, key: req.Key, index: max(r.commitIndex, r.termStart)})
	r.replicate(ctx)
	return nil
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
