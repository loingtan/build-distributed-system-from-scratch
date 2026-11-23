// Track: Isolation levels. See docs/authoring.md for the shape of this file.
import { isoReadCommitted, isoSnapshot, isoLostUpdate, isoWriteSkew, isoSerializable } from '../../sim/workloads/isolation.js';

export default {
  id: 'isolation',
  code: 'IS',
  category: 'convergence',
  title: 'Isolation levels',
  level: 'Advanced',
  blurb: 'What concurrent transactions are allowed to see: snapshot isolation with MVCC, lost updates, write skew, two-phase locking and serializable snapshot isolation.',
  subtracks: [
    {
      title: 'Weak isolation',
      tasks: [
        {
          slug: 'iso-read-committed',
          title: 'Read committed',
          level: 'Easy',
          minutes: 20,
          summary: 'Interactive transactions on one node: nobody sees a write before it commits, and an abort leaves no trace.',
          concepts: ['read committed', 'dirty reads', 'dirty writes', 'interactive transactions'],
          workload: isoReadCommitted,
          tests: [
            { name: 'Readers and writers', blurb: 'Two clients rewrite whole groups of keys while two others read them.', nodeCount: 1, duration: 3000, params: { groups: { groups: 2, writers: 2, readers: 2, writeGap: 8 }, maxAbort: 0.05 } },
            { name: 'Some writers roll back', blurb: 'One writer in three aborts after writing; nobody may ever see its values.', nodeCount: 1, duration: 3000, params: { groups: { groups: 2, writers: 3, readers: 2, writeGap: 8, pAbort: 0.35 }, maxAbort: 0.05 } },
            { name: 'Overlapping writers', blurb: 'Four writers on one group at once; at the end the group must hold one writer\'s values.', nodeCount: 1, duration: 3000, params: { groups: { groups: 1, writers: 4, readers: 1, writeGap: 10, pAbort: 0.2 }, maxAbort: 0.05 } },
          ],
          hints: [
            'Run the starter and open the failing check: a reader sees a value whose writer had not committed yet, or never did.',
            'Give each transaction its own private map of writes, created at `begin`. A `write` only goes there.',
            'A `read` looks in the transaction\'s own writes first, then in the committed store.',
            'On `commit`, copy the private writes into the committed store in one synchronous handler; on `abort`, just drop them.',
          ],
        },
        {
          slug: 'iso-snapshot',
          title: 'Snapshot isolation',
          level: 'Medium',
          minutes: 35,
          summary: 'Keep several versions of each key so every transaction reads one consistent snapshot, and drop versions nobody can see.',
          concepts: ['snapshot isolation', 'MVCC', 'read skew', 'garbage collection'],
          workload: isoSnapshot,
          tests: [
            { name: 'Slow readers', blurb: 'Readers pause between reads of a group while writers rewrite it.', nodeCount: 1, duration: 3000, params: { groups: { groups: 2, writers: 2, readers: 2, writeGap: 5, readSpan: 40 }, gc: true, maxAbort: 0.5 } },
            { name: 'One long reader', blurb: 'One transaction reads a key, waits 700ms while many commits land, then reads the rest.', nodeCount: 1, duration: 3500, params: { groups: { groups: 1, writers: 3, readers: 1, writeGap: 5, readSpan: 20 }, long: { span: 700 }, gc: true, maxAbort: 0.6 } },
            { name: 'Busy store', blurb: 'Six clients, three groups, some writers roll back.', nodeCount: 1, duration: 3500, params: { groups: { groups: 3, writers: 3, readers: 3, writeGap: 6, readSpan: 25, pAbort: 0.2 }, gc: true, maxAbort: 0.5 } },
          ],
          hints: [
            'Keep, for every key, a list of versions `{ ts, value }`. A commit takes the next timestamp and appends one version per key it wrote.',
            'At `begin`, remember the timestamp of the newest commit: that is the snapshot. A read returns the newest version with `ts <= snap` (own writes still come first).',
            'A version can be dropped once a newer version is visible to every open snapshot. Find the oldest open snapshot; for each key keep the newest version at or below it, and everything newer.',
            'Collect garbage whenever a transaction ends. With nothing open, one version per key remains.',
          ],
        },
        {
          slug: 'iso-lost-updates',
          title: 'Preventing lost updates',
          level: 'Medium',
          minutes: 25,
          summary: 'Concurrent read-modify-write increments must never overwrite each other: the first committer wins, the other aborts and retries.',
          concepts: ['lost updates', 'first-committer-wins', 'aborts and retries'],
          workload: isoLostUpdate,
          tests: [
            { name: 'Contended counters', blurb: 'Four clients increment two counters by reading, thinking, and writing back.', nodeCount: 1, duration: 3500, params: { counters: { counters: 2, clients: 4, think: 15 }, gc: true, maxAbort: 0.7 } },
            { name: 'Counters and readers', blurb: 'Increments on three counters while other clients read whole groups that writers rewrite.', nodeCount: 1, duration: 3500, params: { counters: { counters: 3, clients: 3, think: 10 }, groups: { groups: 2, writers: 1, readers: 2, writeGap: 5, readSpan: 20 }, gc: true, maxAbort: 0.6 } },
            { name: 'One hot counter', blurb: 'Three clients increment the same counter.', nodeCount: 1, duration: 3500, params: { counters: { counters: 1, clients: 3, think: 10 }, gc: true, maxAbort: 0.8 } },
          ],
          hints: [
            'Under plain snapshot isolation, two increments that both read 5 both write 6. Which of the two should be told no?',
            'At commit, look at every key the transaction wrote. If its newest version has a timestamp above the transaction\'s snapshot, someone else committed it first.',
            'Answer that commit with `{ "type": "aborted" }` and forget the transaction. The client retries with a new id and a fresh snapshot.',
            'Do not abort on mere reads: two transactions that only read a counter never conflict, and the abort-rate check will notice.',
          ],
        },
      ],
    },
    {
      title: 'Serializability',
      tasks: [
        {
          slug: 'iso-two-phase-locking',
          title: 'Write skew and two-phase locking',
          level: 'Hard',
          minutes: 45,
          summary: 'Keep at least one doctor on call when two go off at once, with shared and exclusive locks held to commit and deadlock detection.',
          concepts: ['write skew', 'two-phase locking', 'deadlock detection', 'serializability'],
          workload: isoWriteSkew,
          tests: [
            { name: 'On-call doctors', blurb: 'Two shifts of two doctors; clients take doctors off call (only if another stays on) or put them back.', nodeCount: 1, duration: 4000, params: { oncall: { shifts: 2, doctors: 2, clients: 4, think: 10, rota: 1 }, maxAbort: 0.5 } },
            { name: 'Three doctors a shift', blurb: 'Three doctors per shift, five clients, more overlapping requests.', nodeCount: 1, duration: 4000, params: { oncall: { shifts: 2, doctors: 3, clients: 5, think: 8, rota: 1 }, maxAbort: 0.5 } },
            { name: 'Contended counters', blurb: 'Read-modify-write increments on two counters: no update may be lost.', nodeCount: 1, duration: 3500, params: { counters: { counters: 2, clients: 4, think: 12 }, maxAbort: 0.7 } },
            { name: 'Independent clients', blurb: 'Each client increments its own counters; nobody conflicts, so nobody waits or aborts.', nodeCount: 1, duration: 3000, params: { counters: { clients: 4, think: 15, own: true }, maxAbort: 0.02, fast: { limit: 150, why: 'No other client touches these keys, so nothing should make this request wait. Lock keys, not the whole store.' } } },
          ],
          hints: [
            'Run the starter on "On-call doctors": both leaves read two doctors on call, write different keys, and both commit. No key conflicts, so first-committer-wins sees nothing.',
            'Strict 2PL: a read takes a shared (S) lock, a write an exclusive (X) lock, and all of a transaction\'s locks are released only at commit or abort. S is compatible with S; X with nothing. A transaction holding the only S lock may upgrade it to X.',
            'When a lock is busy, do not reply yet: put the request in a waiting list and reply when a later commit or abort releases the lock.',
            'The two doctors now deadlock: each holds S on both keys and waits for X on its own. Before a request waits, follow "waits for" from the lock holders; if you get back to the requester, answer it `aborted` and release its locks.',
            'Waiting with a timeout (abort after, say, 300ms) is also a valid deadlock cure, as long as every request is answered within one second and the abort rate stays within bounds.',
          ],
        },
        {
          slug: 'iso-ssi',
          title: 'Serializable snapshot isolation',
          level: 'Hard',
          minutes: 50,
          summary: 'Serializable without locks: snapshot reads plus tracking of rw-antidependencies, aborting the pivot of any dangerous structure.',
          concepts: ['serializable snapshot isolation', 'rw-antidependencies', 'optimistic concurrency control', 'serializability'],
          workload: isoSerializable,
          tests: [
            { name: 'On-call doctors', blurb: 'Two shifts of two doctors; clients take doctors off call or put them back.', nodeCount: 1, duration: 4000, params: { oncall: { shifts: 2, doctors: 2, clients: 4, think: 10, rota: 1 }, maxAbort: 0.5 } },
            { name: 'Three doctors a shift', blurb: 'Three doctors per shift, five clients.', nodeCount: 1, duration: 4000, params: { oncall: { shifts: 2, doctors: 3, clients: 5, think: 8, rota: 1 }, maxAbort: 0.5 } },
            { name: 'Contended counters', blurb: 'Read-modify-write increments on two counters.', nodeCount: 1, duration: 3500, params: { counters: { counters: 2, clients: 4, think: 12 }, maxAbort: 0.7 } },
            { name: 'Long readers', blurb: 'Two clients slowly read every counter (about half a second each) while three others increment them.', nodeCount: 1, duration: 4000, params: { counters: { counters: 4, clients: 3, think: 10 }, scanners: { clients: 2, gap: 80 }, maxAbort: 0.5, fast: { kinds: ['inc'], limit: 150, why: 'Readers must not block writers: a write never waits for a read-only transaction.' }, readers: { kind: 'scan', maxAbort: 0.2, why: 'Read-only transactions that only overlap single-key increments cannot be part of a cycle; do not abort them.' } } },
          ],
          hints: [
            'Start from your lost-updates store (snapshots plus first-committer-wins). It already passes everything except write skew, and readers never wait.',
            'Track each transaction\'s read set. If T read a key that a concurrent transaction W wrote (W committed after T\'s snapshot), T did not see W\'s write: T must precede W. That is an rw-antidependency T → W.',
            'Every cycle that snapshot isolation allows has a transaction with an rw edge in and an rw edge out (the pivot). Refusing to commit any pivot is enough.',
            'Record edges when the later of two transactions commits, and keep committed transactions (read set, write set, in/out flags) while any open transaction began before they committed.',
            'At commit of T: edges T → c for concurrent committed c that wrote what T read, c → T for those that read what T writes. Abort T if T would get both, or if some c would end up with both.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Writing straight into the shared store',
      symptom: 'Readers see values from transactions that later abort, or that have not asked to commit yet (dirty reads).',
      fix: 'Buffer each transaction\'s writes privately and publish them in one step at commit; an abort just drops the buffer.',
    },
    {
      title: 'Garbage-collecting versions an open snapshot still needs',
      symptom: 'Short transactions pass, but the long reader suddenly sees a newer value, or null, halfway through.',
      fix: 'Find the oldest open snapshot first. For each key keep the newest version at or below it and everything newer; only older versions may go.',
    },
    {
      title: 'Trusting first-committer-wins to make snapshot isolation serializable',
      symptom: 'Counters are fine, but two doctors go off call at once and the shift is left empty (write skew).',
      fix: 'Write-write conflicts are not the only danger. Lock what you read (two-phase locking) or track read-write dependencies (SSI).',
    },
    {
      title: 'Waiting for locks with no deadlock handling',
      symptom: 'Requests stop being answered: two transactions each hold a shared lock and wait to upgrade it.',
      fix: 'Before a request waits, follow the waits-for graph from the lock holders; if it leads back to the requester, abort the requester. A wait timeout also works.',
    },
  ],
  questions: [
    {
      q: 'Why does a single mutex (or a single-threaded handler) not make interactive transactions serializable?',
      a: 'It makes each request atomic, not each transaction. Between a transaction\'s read and its write the client thinks, and other transactions\' requests run in between. Isolation is about what those interleavings may expose.',
    },
    {
      q: 'Snapshot isolation with first-committer-wins prevents lost updates. Why does it not prevent write skew?',
      a: 'First-committer-wins only compares write sets. In write skew the two transactions write different keys, each based on reading the key the other writes. Nothing they write overlaps, so both commit, yet no serial order would let both see two doctors on call.',
    },
    {
      q: 'Two-phase locking and SSI are both serializable. When would you pick one over the other?',
      a: 'Two-phase locking never aborts a transaction that does not deadlock, but readers block writers and long reads stall the system. SSI never blocks, so long read-only queries run alongside writers, at the price of aborts (sometimes of transactions that would have been fine) and the bookkeeping for read sets.',
    },
  ],
  reading: [
    { kind: 'Paper', title: 'A Critique of ANSI SQL Isolation Levels (Berenson et al., 1995)', url: 'https://www.microsoft.com/en-us/research/publication/a-critique-of-ansi-sql-isolation-levels/', note: 'Defines snapshot isolation, lost updates and write skew precisely, and shows why the SQL standard levels were ambiguous.' },
    { kind: 'Paper', title: 'Serializable Snapshot Isolation in PostgreSQL (Ports and Grittner, 2012)', url: 'https://arxiv.org/abs/1208.4179', note: 'How SSI was built into a production database: rw-antidependencies, pivots, read-only optimizations.' },
    { kind: 'Docs', title: 'PostgreSQL: Transaction Isolation', url: 'https://www.postgresql.org/docs/current/transaction-iso.html', note: 'What read committed, repeatable read (snapshot isolation) and serializable mean in a real system, with examples of each anomaly.' },
  ],
};
