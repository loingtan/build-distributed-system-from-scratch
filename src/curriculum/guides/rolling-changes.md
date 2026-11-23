## The problem

A user directory runs on three nodes, n1, n2 and n3. Each user key has a **home** node; any node accepts a request and forwards it to the home with a small internal message:

```json
{ "type": "fwd_put", "key": "u7", "name": "Ada Lovelace" }
```

The team wants a new feature: record which client made each change. The developer changes the internal message to carry an object instead of a string, `{ "type": "fwd_put", "key": "u7", "user": { "name": "Ada Lovelace", "by": "c2" } }`, tests it on a laptop where all three nodes run the new code, and deploys.

The deploy upgrades one node at a time, so the site never goes down. At 10:00 n1 is restarted with the new code. At 10:00:01 a client asks n1 to rename u7, whose home is n3. n1 sends the new-style message. n3 still runs the old code; it reads `name`, finds nothing, and stores `undefined`. It replies OK. The client was told the write succeeded, and Ada Lovelace's record now has no name. Every write that passes from an upgraded node to an old one does the same until n3 is upgraded too.

Now roll that back: the team notices, rolls n1 back to the old code, and n1 finds records written by the new code with `user.name` instead of `name`. They are unreadable to the old version as well.

Nothing was wrong with the new code on its own, or with the old code on its own. The bug lived in the period when both ran at once, and in the data that outlived both. Changing a running cluster without downtime means designing for that period.

## The core idea

### The rolling upgrade and the mixed-version window

A **rolling upgrade** replaces a cluster's software one node at a time: drain a node (stop sending it new traffic), stop it, install the new version, start it, let traffic back in, and move on to the next. The cluster as a whole stays up, because at any moment all but one node are serving.

The price is a **mixed-version cluster**. Between the first node's upgrade and the last, old and new code run side by side and talk to each other. With five nodes upgraded in random order, every pairing occurs: old sends to new, new sends to old, new sends to new. And a rollout can stop or reverse: a node can be rolled back to the old version after it has already written data in the new one.

So two compatibility questions must be answered for every change.

- **Backward compatibility**: can new code read what old code writes (messages and stored data)?
- **Forward compatibility**: can old code read what new code writes?

Backward compatibility is the familiar one. Forward compatibility is the one the opening story broke, and it is harder, because the old code is already deployed: you cannot change it to cope with what the new code will do. You can only make sure the new code stays within what the old code already tolerates.

<figure>
<svg viewBox="0 0 620 210" role="img" aria-label="Three nodes upgraded one after another; between the first and last upgrade old and new versions run together">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="54">n1</text><text x="10" y="104">n2</text><text x="10" y="154">n3</text>
  </g>
  <g>
    <rect x="45" y="40" width="95" height="22" style="fill: var(--m1)"/><rect x="150" y="40" width="460" height="22" style="fill: var(--m3)"/>
    <rect x="45" y="90" width="245" height="22" style="fill: var(--m1)"/><rect x="300" y="90" width="310" height="22" style="fill: var(--m3)"/>
    <rect x="45" y="140" width="395" height="22" style="fill: var(--m1)"/><rect x="450" y="140" width="160" height="22" style="fill: var(--m3)"/>
  </g>
  <g style="fill: var(--bad)"><rect x="140" y="40" width="10" height="22"/><rect x="290" y="90" width="10" height="22"/><rect x="440" y="140" width="10" height="22"/></g>
  <rect x="140" y="28" width="310" height="146" style="fill: none; stroke: var(--warn); stroke-dasharray: 6 4; stroke-width: 1.5"/>
  <text x="210" y="190" style="fill: var(--warn); font: 12px var(--sans)">mixed-version window: every message must suit both</text>
  <g style="font: 12px var(--sans); fill: var(--ink)">
    <text x="60" y="20">v1</text><text x="500" y="20">v2</text>
  </g>
  <rect x="80" y="10" width="12" height="12" style="fill: var(--m1)"/><rect x="520" y="10" width="12" height="12" style="fill: var(--m3)"/>
  <text x="470" y="206" style="fill: var(--muted); font: 11px var(--sans)">red: drained and restarting</text>
</svg>
<figcaption>The rollout keeps the service up, at the cost of a period where version 1 and version 2 must understand each other.</figcaption>
</figure>

### Tolerant readers: adding a field

The cheapest kind of change is adding a field. It works if every reader is a **tolerant reader**: it reads the fields it knows and ignores the rest. Version 1's handler for `fwd_put` reads `key` and `name`. If version 2 sends `{ "type": "fwd_put", "key": "u7", "name": "Ada Lovelace", "by": "c2" }`, version 1 stores the name and never looks at `by`. Old code tolerates the new message because the new message is a superset of the old one.

What breaks a tolerant reader:

- **Removing or renaming a field** the old code reads, as in the opening story.
- **Changing a field's meaning or type**: a string becomes an object, milliseconds become seconds.
- **A new message type.** Version 1 has no handler for `fwd_put_v2`, so the message is dropped and the sender waits for a reply that never comes.

The reverse direction needs a rule too. A message from version 1 lacks the new field. Version 2 must treat **absent as unknown**, not as "same as before". If version 1 forwards a rename without `by`, the stored writer must become `null`; keeping the previous writer would credit a change to someone who did not make it.

### Negotiation: changes that cannot be a field

Some changes cannot be made by adding a field. In the second task, version 2 forwards a request in a new way: the home answers the client directly, saving a hop. Version 1 cannot take part. A node may use the new format only with a peer it *knows* runs version 2. That knowledge comes from **version negotiation**.

The simplest form is a **handshake**. When version 2 starts, it sends each peer `{ "type": "hello", "version": 2 }`. A version-2 peer replies; a version-1 peer has no handler for `hello`, stays silent, and the call times out, so the sender keeps treating it as old. A received `hello` is evidence too: its sender runs version 2. That matters because peers upgrade *after* you as well as before. When n3 upgrades last, n1 and n2 learn about it from n3's hello, not from anything they sent.

<figure>
<svg viewBox="0 0 600 220" role="img" aria-label="n2 upgrades and greets n1 (v2) and n3 (v1); n3 stays silent; later n2 uses fwd2 only with n1">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="44">n1 v2</text><text x="10" y="114">n2 v2</text><text x="10" y="184">n3 v1</text>
  </g>
  <g style="stroke: var(--line)"><line x1="70" y1="40" x2="590" y2="40"/><line x1="70" y1="110" x2="590" y2="110"/><line x1="70" y1="180" x2="590" y2="180"/></g>
  <line x1="100" y1="110" x2="140" y2="40" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="150" y1="40" x2="190" y2="110" style="stroke: var(--good); stroke-width: 2"/>
  <line x1="100" y1="110" x2="140" y2="180" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="140" y1="180" x2="175" y2="125" style="stroke: var(--bad); stroke-width: 2; stroke-dasharray: 5 4"/>
  <text x="172" y="134" style="fill: var(--bad); font: 15px var(--sans)">×</text>
  <g style="font: 11px var(--mono); fill: var(--muted)">
    <text x="80" y="70">hello v2</text><text x="160" y="66">hello_ok</text><text x="80" y="160">hello v2</text><text x="190" y="155">no handler: silence</text>
  </g>
  <text x="200" y="128" style="fill: var(--ink); font: 11px var(--mono)">known v2 = {n1}</text>
  <line x1="380" y1="110" x2="420" y2="40" style="stroke: var(--m3); stroke-width: 2.5"/>
  <text x="420" y="66" style="fill: var(--m3); font: 11px var(--mono)">fwd2</text>
  <line x1="470" y1="110" x2="510" y2="180" style="stroke: var(--m1); stroke-width: 2.5"/>
  <text x="515" y="160" style="fill: var(--m1); font: 11px var(--mono)">fwd_put</text>
</svg>
<figcaption>n2 learns from the handshake that n1 understands the new format and n3 does not, and chooses the message per peer.</figcaption>
</figure>

Two rules keep negotiation safe. Knowledge lives in memory and starts empty on every restart: a restarted node has forgotten what it learned, and must ask again. And version 2 must keep handling the old messages, because version-1 nodes keep sending them until they are upgraded.

### Expand and contract: changing stored data

Messages live for milliseconds; stored data outlives the code that wrote it. In the third task, version 2 splits a stored `name` into `first` and `last`. If version 2 simply replaced `name`, a node rolled back to version 1 would find records it cannot read.

The safe sequence is **expand–contract** (also called parallel change), a three-step **schema migration**:

1. **Expand.** Add the new shape next to the old one. Version 2 writes both, `{ name, first, last }`. This is a **dual write**: every write updates both representations. Version 1 still finds `name`.
2. **Migrate.** Once every node runs version 2, convert every record, deriving the new fields from the old where needed.
3. **Contract.** Once **rollback** to version 1 is impossible, remove the old shape: records become `{ first, last }`, and code stops writing `name`.

The subtle part is which field to trust before the contract. Version 1 updates records in place: it sets `name` and leaves other fields alone. After a rollback, a record may hold a new `name` written by version 1 and stale `first`/`last` written by version 2 earlier. So until the contract, `name` is the truth, and version 2 reads from it. At the contract step, `first` and `last` are derived from `name` once more, for every record, because some were last written by version 1.

The contract step is a one-way door, so it is triggered explicitly (in the task, by a `finalize` message once every node runs version 2 for good), and the fact that it happened is stored durably: a node restarted later must not go back to writing `name`.

### Feature flags, canaries and kill switches

A deploy changes the code on a machine; a release changes what users get. **Feature flags** separate the two. The new code ships switched off, so deploying it changes nothing. The flag is then turned on for one node, a **canary release**: if the new path misbehaves, only a slice of traffic is affected. Turning the flag off again, in milliseconds and without a deploy, is the **kill switch**.

A flag that needs a human to notice a problem is slow. An **automatic rollback** lets each node watch its own outcomes and turn the flag off itself. In the fourth task the node keeps the outcomes of its last 20 reads from a new cache; as soon as 5 of them are failures, it switches the flag off. The threshold has to sit above normal noise (a healthy cache fails one read in seven, so about 3 in 20), or the guard trips on a healthy system. Flags live in durable storage, including one the node switched off itself, so a restart does not silently undo a decision.

And the new path must fail safe: when the cache fails a read, the node answers from the old path. The client never notices; only the guard does.

## A worked example

Three nodes; key `u7` has home n3. Version 2 adds `by` (the tolerant-reader task). The rollout handles one node every 800 ms, starting with n1 at t=800: traffic avoids the node from 250 ms before it stops, it restarts 100 ms later, and traffic returns 100 ms after that.

| t (ms) | n1 | n2 | n3 (home of u7) | event | stored u7 |
|---|---|---|---|---|---|
| 0 | v1 | v1 | v1 | c1 puts u7="Ada Lovelace" via n2; n2 sends `fwd_put {key, name}` | name=Ada Lovelace |
| 550–1000 | draining, restarting | v1 | v1 | traffic avoids n1 from 550; restart at 800–900 | |
| 1100 | v2 | v1 | v1 | c2 puts u7="Ada King" via n1; n1 sends `fwd_put {key, name, by:"c2"}` | n3 (v1) ignores `by`: name=Ada King |
| 1200 | v2 | v1 | v1 | c3 gets u7 via n1; n3 replies `fwd_get_ok {name}` | n1 replies name=Ada King, by=null |
| 2150–2600 | v2 | v2 | draining, restarting | n2 was upgraded the same way at 1600; now traffic avoids n3 | |
| 2700 | v2 | v2 | v2 | c1 gets u7 via n2; n3 now v2, record has no `by` | name=Ada King, by=null |
| 2800 | v2 | v2 | v2 | c2 puts u7="Ada Byron" via n1 with by:"c2"; the first `fwd_put` is lost, n1 times out after 300 ms and replies `error` | unchanged |
| 3150 | v2 | v2 | v2 | c2 retries via n1; delivered | name=Ada Byron, by=c2 |
| 3200 | v2 | v2 | v2 | c3 gets u7 via n2 | name=Ada Byron, by=c2 |

Each fault was harmless. The old home ignored a field it did not know. The new node reading from the old home reported `by` as unknown rather than guessing. The record written by version 1 carried no `by`, and version 2 reported `null`, not a stale writer. The lost forward was surfaced to the client as an error, and its retry, carrying the same fields, was applied once. Once every node ran version 2, every new write carried a correct `by`.

## Why it is correct

The guarantee to aim for is: **at every moment of the rollout, every pair of running versions can exchange every message the protocol allows, and every version that may still run can read every stored record.** Each technique is a way to keep that invariant true.

**Additive messages preserve forward compatibility.** If version 1 reads only fields `F` and version 2's messages contain `F` with unchanged meaning plus extra fields, version 1's behaviour on a version-2 message equals its behaviour on the version-1 message with the same `F`. Treating missing fields as unknown makes version 2's behaviour on version-1 messages well defined.

**Negotiation never sends an unknown message.** A node sends the new format to a peer only after evidence that the peer runs version 2: a reply to a handshake that only version 2 answers, or a message that only version 2 sends. Versions only move forward during the rollout in this task, so the evidence stays true until the next restart, and a restart clears it. Learning from received hellos guarantees that once every node runs version 2, every pair has exchanged at least one hello, so the cluster converges to the new format.

**Expand–contract keeps every record readable by every version that may run.** Before finalize, both versions can run, and every record has `name` (version 1 always writes it, version 2 always writes it too, and no one removes it), and version 2 reads only `name`. So rollbacks lose nothing. Finalize happens only after version 1 can no longer run, and recomputes the new fields from `name`, the field both versions kept current. After it, only version 2 runs, and it reads and writes only the new fields. The finalized marker on disk makes that decision survive restarts.

**A durable, self-tripping flag bounds the damage of a bad release.** With the guard reading the last 20 outcomes and tripping at 5 failures, a cache failing every other read is switched off within about 10 reads. Because the flag is stored, the decision survives restarts. Because a failed cache read falls back to the old path, no client sees the failure either way.

## Trade-offs and failure modes

**Mixed-version windows multiply testing.** Every change must be tested with old→new, new→old and new→new, plus restart and rollback cases. A two-version policy (each release must interoperate with the previous one) keeps this manageable; skipping a version during an upgrade usually breaks it.

**Expand–contract is slow and leaves debris.** A simple rename becomes three deploys, possibly days apart, and the data carries both shapes in between, which costs storage and invites bugs where one copy is updated and the other is not. Teams forget the contract step, and old fields live for years.

**Ignoring unknown fields can hide mistakes.** A tolerant reader will not complain about a misspelled field either. Schema systems like Protocol Buffers make this explicit: unknown fields are preserved and skipped, but required-field changes and renumbering are forbidden.

**Negotiation adds state and startup cost.** A node cannot use the new format until handshakes complete, and the knowledge goes stale if a peer can be downgraded; then a version field on every message, or periodic re-negotiation, is safer than a one-time handshake.

**Flags accumulate.** Every flag doubles the number of possible code paths. Old flags that nobody removes become dead code with a switch that can still be flipped. Flags also need a source of truth: a per-node flag set by hand, as in the task, is simple; real systems use a central flag service, which then becomes a dependency.

**Automatic rollback needs a good signal.** A threshold too low trips on noise; too high and a bad release runs for a long time. And a guard that watches only errors misses a release that returns wrong answers successfully.

## In real systems

**Kubernetes Deployments** perform rolling updates by default, replacing pods a few at a time under `maxUnavailable` and `maxSurge` limits, waiting for each new pod's readiness probe before continuing, and supporting `kubectl rollout undo` to go back. They do nothing about compatibility; that remains the application's job.

**Protocol Buffers** are designed for tolerant readers: fields are identified by number, unknown fields are skipped by older parsers (and kept when re-serialised), and the guidance is to add fields freely but never reuse or change the number or type of an existing one. Apache Avro achieves the same with reader and writer schemas resolved at read time, and default values for added fields.

**Apache Kafka** negotiates per connection: clients send an `ApiVersions` request first, the broker replies with the range of versions it supports for each API, and the client then uses the highest version both understand. For brokers, the `inter.broker.protocol.version` setting historically held the cluster on the old inter-broker protocol during a rolling upgrade, to be raised in a second rolling restart once all brokers ran the new code, a two-phase pattern much like expand–contract.

**Database migration tools and practice** (for example online schema change tools for MySQL, or the expand–contract pattern as described for PostgreSQL deployments) add columns first, backfill, switch reads, and drop the old column in a later release. Feature-flag services such as LaunchDarkly, and in-house systems at many large companies, provide percentage rollouts, per-user targeting and kill switches without redeploying.

## Common misconceptions

- **"A rolling upgrade means no compatibility work."** It is exactly what creates the compatibility problem: old and new code run together for the whole rollout.
- **"Adding a field is always safe."** Only if every old reader ignores unknown fields and the new reader treats a missing field as unknown. A strict parser that rejects unknown fields breaks on the first new one.
- **"A new message type is a clean way to add behaviour."** Old nodes have no handler for it. Use it only with peers known to understand it.
- **"Once version 2 is everywhere, you can drop the old data shape."** Only once rollback is impossible. Until then a rolled-back node will write and expect the old shape.
- **"Feature flags make deploys safe on their own."** They make *releases* reversible. The code is still running, the flag's off path must still work, and the data written while the flag was on must still be readable when it is off.

## How the tasks build it

[RC-1 Adding a field](#/c/roll-tolerant-reader) asks version 2 to record the writer of each name in a field version 1 ignores. The tests upgrade three nodes in order and five in random order, so every old–new pairing meets, and check that no request fails and that a non-null `by` is always correct.

[RC-2 Negotiating a new format](#/c/roll-negotiation) adds a message version 1 cannot read. The tests check that no version-1 node ever receives it, that every forward uses it once the whole cluster is upgraded, and they slow the links so handshakes take a while to come back.

[RC-3 Expand and contract](#/c/roll-expand-contract) splits a stored field while nodes are rolled back halfway and upgraded again. The tests inspect stored records before and after finalize, and restart a node after finalize to check it remembers.

[RC-4 Flags and canaries](#/c/roll-feature-flags) ships a new read path behind a per-node flag. The tests flip the flag on and off, restart nodes, and degrade the cache so each node must turn itself off and stay off; a healthy cache must never trip the guard, and clients must never see a cache failure.

## Further reading

- [ParallelChange (Martin Fowler's bliki)](https://martinfowler.com/bliki/ParallelChange.html) — the expand, migrate, contract pattern for interfaces and data, with examples.
- [TolerantReader (Martin Fowler's bliki)](https://martinfowler.com/bliki/TolerantReader.html) — why readers should take only what they need and ignore the rest.
- [Protocol Buffers: updating a message type](https://protobuf.dev/programming-guides/proto3/#updating) — concrete rules for changing a schema without breaking old or new readers.
- [Kafka protocol guide: versioning and compatibility](https://kafka.apache.org/protocol#protocol_compatibility) — how ApiVersions negotiation lets clients and brokers of different versions talk.
- [Kubernetes: performing a rolling update](https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/) — the mechanics of rolling out and rolling back a Deployment.
- [Feature Toggles (Pete Hodgson, martinfowler.com)](https://martinfowler.com/articles/feature-toggles.html) — kinds of flags, how long each should live, and how to keep them under control.
