export default [
  {
    term: 'Append-only log',
    aliases: ['commit log'],
    summary: 'A sequence of records you can only add to at the end; existing records never change or move.',
    detail: `An **append-only log** supports two things: append a record at the end, and read records from any position onward. Nothing is updated in place, so a record's position identifies it permanently.

Producers append without knowing who reads. Each consumer keeps its own position and reads at its own pace; a slow consumer just has a smaller position. Many consumers can read the same log independently.

Logs underlie Kafka topics, database write-ahead logs, replication streams and consensus protocols such as Raft. Because records are immutable, they are easy to copy, cache and replay. The cost is growth: real systems delete or compact old records.`,
    related: ['Offsets', 'Consumer commits'],
  },
  {
    term: 'Offsets',
    aliases: ['offset'],
    summary: 'The position of a record in a log: 0 for the first, 1 for the next, and so on, never reused.',
    detail: `An **offset** is a record's index in its log. In this course each key is its own log, so \`k1\` and \`k2\` both have an offset 0. \`send\` returns the offset it stored the message at; \`poll\` takes a starting offset per key and returns \`[offset, msg]\` pairs from there.

Two properties make offsets useful. They are unique: no two records in one log share an offset. And they follow order: a message sent after another was acknowledged gets a larger offset. A consumer can then say "I have seen everything below 8" with one number.

Uniqueness alone is not enough. Per-node counters with disjoint ranges are unique but can give a later message a smaller offset, so a consumer that has passed it never sees it.`,
    related: ['Append-only log', 'Consumer commits', 'Compare-and-swap'],
  },
  {
    term: 'Consumer commits',
    aliases: ['consumer commit', 'committed offsets', 'commit offsets'],
    summary: 'A consumer records how far it has processed a log, so it or its replacement can resume from there.',
    detail: `A **consumer commit** stores an offset per key on behalf of a consumer. After a crash or a restart, the consumer lists its committed offsets and polls from there instead of from the beginning.

Committed offsets only move forward. If a delayed retry commits 5 after 7 was committed, the stored value must stay 7; otherwise the consumer would reprocess messages 6 and 7. On one node that is \`max(old, new)\`; across nodes it needs a compare-and-swap loop.

Whether the offset means "last processed" or "next to read" is a convention (Kafka commits the next offset to read). When a consumer commits relative to processing decides between at-most-once (commit first) and at-least-once (process first) delivery.`,
    related: ['Offsets', 'Append-only log'],
  },
  {
    term: 'Linearizability',
    aliases: ['linearizable', 'linearizable store'],
    summary: 'Every operation appears to take effect at a single instant between its call and its return, as if on one copy.',
    detail: `A system is **linearizable** if you can place every operation at one point in time, somewhere between when it was invoked and when it returned, such that the results are exactly what a single, non-replicated copy would produce in that order.

Concretely: once a write has returned, every read that starts afterwards, from any client, sees it or a newer value. Two clients can never disagree about the order of completed operations.

Linearizability is per operation. A read followed by a write is two operations, and others may act in between, which is why counters need compare-and-swap. Providing it in a replicated system requires consensus (Raft in etcd, Paxos in Spanner) and costs a majority round trip and availability during partitions.`,
    related: ['Compare-and-swap', 'Linearizable reads'],
  },
  {
    term: 'Compare-and-swap',
    aliases: ['CAS', 'compare and set'],
    summary: 'Set a value only if it still holds what you expect, in one atomic step, and report whether it worked.',
    detail: `\`cas(key, from, to)\` replaces the value of \`key\` with \`to\` only if it currently equals \`from\`, and returns true or false. On a linearizable store it is a single atomic operation.

It is enough to build a shared counter. To claim the next offset, read \`next/k1\` (say 7) and \`cas(next/k1, 7, 8)\`. If two nodes race, exactly one CAS from 7 succeeds; the loser reads 8 and tries \`cas(8, 9)\`. With \`null\` as \`from\`, CAS means "only if the key does not exist yet".

The same instruction exists in CPUs and underlies lock-free data structures. In databases it appears as conditional writes (DynamoDB condition expressions, etcd transactions comparing a revision).`,
    related: ['Optimistic concurrency', 'Linearizability'],
  },
  {
    term: 'Optimistic concurrency',
    aliases: ['optimistic concurrency control', 'optimistic locking'],
    summary: 'Do the work assuming no conflict, check atomically at the end, and retry if someone else changed the data.',
    detail: `**Optimistic concurrency** holds no locks. A client reads a value (and often a version), computes its change, and writes it back with a check that the value or version is still what it read. If the check fails, it rereads and tries again.

The claim loop in the multi-node log is an example: read \`next/k1\`, then \`cas\` it up by one; on failure, repeat. A failure always means another node succeeded, so the system as a whole keeps making progress.

It works well when conflicts are rare and retries are cheap. Under heavy contention on one key, nodes spend more time retrying, and a lock or a single owner (like a Kafka partition leader) may be cheaper. Kubernetes object updates and HTTP ETag/If-Match requests use the same pattern.`,
    related: ['Compare-and-swap'],
  },
  {
    term: 'Sequencer',
    summary: 'A single component that hands out the next position in a shared order, so many writers agree on one sequence.',
    detail: `A **sequencer** answers one question, "what is the next offset?", for every writer. Because it is the only place that decides, positions are unique and ordered.

It can be a dedicated server (Corfu's sequencer), a partition leader (Kafka assigns offsets on the leader) or a counter in a linearizable store advanced with compare-and-swap, as in the multi-node log task.

Separating "get a position" from "write the record" introduces holes: a writer can take a position and crash before writing. Readers must stop at the first missing position, and real systems fill abandoned slots with a skip marker.`,
    related: ['Offsets', 'Compare-and-swap'],
  },
];
