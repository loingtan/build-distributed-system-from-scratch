# Quorum Lab

Learn distributed systems by building them. You write the code for one node; a
cluster of them runs against simulated clients, with latency, lost messages,
network partitions and crashes, and a checker reports which guarantee broke.

Two ways to practise, with the same 112 tasks, tests and checkers:

- **In Go, as real processes** (`lab/`): each node is its own OS process, or its
  own Docker container. Real concurrency, real `SIGKILL` crashes, a real fsynced
  disk. See [lab/README.md](lab/README.md).
- **In the browser** (JavaScript): a deterministic simulator with a message
  timeline. Nothing to install.

## Practise in Go

**In the web page:** run `npm run dev`, open a task, and switch the editor from **JS**
to **Go**. Your code is saved to `practice/<task>/main.go`; **Run tests** builds it and
runs every node as a real process (tick **Docker** for one container per node), with
results in Checks, Timeline and Logs. Needs Go installed on the machine running
`npm run dev`.

**In the terminal:**

```sh
npm install
npm run lab -- list                      # every task, with your progress
npm run lab -- new broadcast-faults      # creates practice/broadcast-faults/main.go
npm run lab -- brief broadcast-faults    # what to build (Go API names)
npm run lab -- test broadcast-faults     # build and run the tests
npm run lab -- test broadcast-faults --docker   # one container per node
npm run lab -- hints broadcast-faults
```

Needs Go 1.22+ and Node 20+; `--docker` needs Docker.

## The browser site

```sh
npm run dev       # http://localhost:5173
npm run build     # static site in dist/
```

## Checking the curriculum

```sh
npm run verify                              # browser engine: every JS solution passes, every starter fails
npm run lab -- verify --kind both           # real processes: every Go solution passes, every Go starter fails
npm run lab -- verify --lang js             # real processes, JS reference code
```

## Structure

Categories → tracks → subtracks → tasks, plus capstone projects that string
tasks from several tracks into one system. 28 tracks, 112 tasks, 5 projects. Every track has a long-form guide (`#/tracks/<id>/guide`), and a glossary of about 280 concepts lives at `#/concepts`.

| # | Track | Tasks |
|---|---|---|
| 01 | Messaging | Echo, Idempotent requests |
| 02 | Identifiers | Unique IDs, Sortable IDs |
| 03 | Broadcast | Single-node broadcast, Multi-node broadcast, Fault-tolerant broadcast, Efficient broadcast |
| 04 | Logical time | Lamport clocks, Vector clocks |
| 05 | Counters and CRDTs | Grow-only counter, Counter with decrements |
| 06 | Transactions | Single-node transactions, Totally-available transactions |
| 07 | Primary and backups | Asynchronous replication, Synchronous replication, Catching up, Chain replication, Monotonic reads, Consistent prefix, Multi-leader writes |
| 08 | Quorum replication | Quorum writes, Versions and quorum reads, Read repair, Anti-entropy |
| 09 | Distributed transactions | Prepare, then commit, Concurrent transfers, Timeouts and partitions, Crash recovery |
| 10 | Sets, registers and maps | Grow-only set, Observed-remove set, Last-writer-wins register, Multi-value register, Shopping cart (OR-Map) |
| 11 | Isolation levels | Read committed, Snapshot isolation, Preventing lost updates, Write skew and two-phase locking, Serializable snapshot isolation |
| 12 | Replicated logs | Single-node log, Multi-node log |
| 13 | Storage engine | Write-ahead log, Checkpoints, Memtable and segments, Compaction, Bloom filters, B-tree pages |
| 14 | Sharding | Hash partitioning, Consistent hashing, Virtual nodes, Rebalancing, Changes under load, Key-range partitioning, Hot spots, Global secondary index |
| 15 | Failure and leadership | Failure detector, Leader election |
| 16 | Raft log replication | Replicate the log, Commit and apply, Repair diverged logs, Crash recovery |
| 17 | Linearizable key-value store | Get, put and cas through the log, Exactly-once with client sessions, Reads that are never stale, Compact the log with snapshots |
| 18 | Paxos | Single-decree Paxos, Crashes and partitions, A log of slots, Catching up |
| 19 | Gossip membership | Probe one member at a time, Indirect probes, Spread the news by piggybacking, Suspicion and incarnation numbers |
| 20 | Byzantine faults | Echo, ready, deliver, Signed votes and 3f+1, A PBFT round |
| 21 | Leases and fencing | A lock with a lease, Fencing tokens, Tokens that survive a crash, Stop before the lease ends |
| 22 | Stream processing | Change data capture, Event-time windows, Stream-table join, Exactly-once output |
| 23 | Batch processing | Map, shuffle, reduce, Workers that die, Stragglers and backup tasks, A two-stage workflow |
| 24 | Message queues | A FIFO queue, Acknowledge and redeliver, Effectively once, Dead letters, A durable broker |
| 25 | Load balancing | Round robin, Least outstanding requests, Sticky routing with consistent hashing, Health checks and retries, Slow start |
| 26 | Caching | Cache-aside reads, Bounded memory with LRU, Expiry with a TTL, Stampede protection, Invalidation on writes |
| 27 | Resilience patterns | Timeouts and deadlines, Retries with backoff and jitter, Circuit breaker, Rate limiting with a token bucket, Bulkhead: bounded concurrency |
| 28 | Rolling changes | Adding a field, Negotiating a new format, Expand and contract, Flags and canaries |

Projects: Mini-Dynamo, Mini-Kafka, Mini-etcd, Mini-Bank, Mini-Gateway (`src/curriculum/projects.js`).

Pages: home (`#/`), tracks (`#/tracks`, `#/tracks/<id>`), projects
(`#/projects`, `#/projects/<id>`) and the task workspace (`#/c/<slug>`).

The simulator supports partitions, message loss, node crashes and restarts
(only `node.disk` survives), a shared linearizable `kv` store, and fake
upstream services for the resilience track.

## Layout

```
src/sim/engine.js        the simulator: virtual clock, network, node API, kv service
src/sim/workloads.js     workloads and checkers for the first 8 tracks, shared helpers
src/sim/workloads/       one workload file per newer track
src/sim/worker.js        runs tests off the main thread
src/curriculum/catalog.js  categories, the first 8 tracks, numbering
src/curriculum/tracks/     one file per newer track (tasks, tests, hints, track-page material)
src/curriculum/projects.js capstone projects
src/curriculum/tasks.js    task definitions: tests and hints
src/curriculum/briefs/   one markdown brief per challenge
src/curriculum/code/     <slug>.starter.js and <slug>.solution.js
src/components/          Home, Tracks (index + track page), Workspace, CodeEditor, Results, Timeline
scripts/verify.mjs       curriculum self-check (runs under Node)
```

## Adding a track

Follow `docs/authoring.md`: a track file in `src/curriculum/tracks/`, its
workloads in `src/sim/workloads/`, and a brief, starter and solution per task.
Register it in `src/curriculum/tracks/index.js`, then run
`npm run verify -- --track <id>`.
