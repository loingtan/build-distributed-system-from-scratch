export default [
  {
    term: 'Change data capture',
    aliases: ['CDC'],
    summary: 'Publishing every committed change of a database, in commit order, so other systems can follow along.',
    detail: `**Change data capture** turns a database into a stream. Each committed insert, update or delete is written to a log as a change record, often with the row's value before and after, such as \`{ key: "u3", before: "Oslo", after: "Lima" }\`. Records appear in commit order.

A consumer that applies every change once, in order, ends up with an exact copy of the table, or something computed from it: a cache, a search index, a count per city. Because the log is ordered, the copy is never inconsistent. After N changes it equals the table after N changes, only somewhat behind.

Debezium reads the replication logs of PostgreSQL and MySQL and publishes the changes to Kafka. PostgreSQL's logical decoding and MongoDB's change streams offer the same thing natively.`,
    related: ['Derived data', 'Consumer offset', 'Changelog'],
  },
  {
    term: 'Consumer offset',
    aliases: ['stored offset'],
    summary: 'How far a consumer has got in a log: the offset of the next record it will process.',
    detail: `A log does not track what each reader has seen. Each consumer keeps its own **offset**, the position of the next record to read, and asks the log for "records from offset 37".

What matters after a crash is what the offset is stored *with*. If it is saved apart from the state it describes, a crash can leave one ahead of the other. If the offset is ahead, records are skipped. If it is behind, records are applied twice. Saving the state and the offset in one atomic write avoids both.

Kafka stores committed consumer offsets in an internal topic. A consumer that keeps its state elsewhere often stores the offset next to that state instead.`,
    related: ['Change data capture', 'Stream checkpoint'],
  },
  {
    term: 'Derived data',
    aliases: ['derived view', 'materialized view'],
    summary: 'Data computed from a source of truth, such as a cache, an index or an aggregate, that can be rebuilt from it.',
    detail: `A system of record holds the authoritative data. **Derived data** is computed from it: a cache, a full-text index, a count of users per city, a recommendation model. If derived data is lost or wrong, you can in principle recompute it from the source.

Keeping derived data in step is the hard part. Writing to the database and then to the index from the application can fail halfway, or two writers can interleave differently in each system. Deriving everything from the database's own ordered changelog gives every derived system the same order.

The rebuild guarantee only holds while the source history is available. Logs with limited retention mean a consumer must also persist what it derived.`,
    related: ['Change data capture'],
  },
  {
    term: 'Event time',
    aliases: ['processing time'],
    summary: 'The time at which something happened, carried in the event, as opposed to when a processor happens to see it.',
    detail: `Every event has two times. **Event time** is when it happened, stamped by its source. **Processing time** is when a stream processor handles it. They can differ by milliseconds, or by hours when a phone was offline.

Grouping by processing time is easy but gives a different answer on every run and puts late events in the wrong bucket. Grouping by event time gives the same answer however the events arrive. The cost is that the processor never knows for sure that all events for a period have arrived, which is what watermarks address.

The Dataflow model, Apache Beam and Flink all make this distinction central.`,
    related: ['Watermark', 'Tumbling window'],
  },
  {
    term: 'Tumbling window',
    aliases: ['fixed window'],
    summary: 'Fixed-size, non-overlapping time intervals, such as one per second, each event falling into exactly one.',
    detail: `A **tumbling window** of size 1000 cuts time into \`[0, 1000)\`, \`[1000, 2000)\` and so on. An event with timestamp 41730 belongs to \`[41000, 42000)\`, computed as \`Math.floor(ts / 1000) * 1000\`.

Other shapes exist. Hopping (sliding) windows overlap, so an event can belong to several. Session windows grow while events keep coming and close after a gap. Tumbling windows are the simplest because each event updates exactly one window.

A window's result can be emitted only once the processor believes the window is complete. With event time that decision comes from the watermark.`,
    related: ['Event time', 'Watermark'],
  },
  {
    term: 'Watermark',
    summary: 'A marker in a stream saying that events with earlier timestamps are not expected any more.',
    detail: `A **watermark** with time W says "from here on, events should have timestamps above W". When it passes the end of a window, the window can be considered complete and its result emitted.

Some watermarks are exact. A database changelog is written in commit order, so its source can promise that nothing older will follow. Most are heuristic: a source that sees timestamps up to 41600 might emit a watermark of 41400, betting that nothing is more than 200 behind. Events that lose the bet arrive **late**, with a timestamp below the watermark.

Set the watermark too eagerly and you get many late events. Set it too cautiously and every result waits longer.`,
    related: ['Allowed lateness', 'Event time'],
  },
  {
    term: 'Allowed lateness',
    summary: 'How long after the watermark passes a window it stays open to accept late events.',
    detail: `With an **allowed lateness** of 500, the window \`[41000, 42000)\` keeps accepting events until the watermark reaches 42500. Late events up to that point still count. Events after it are dropped, and good systems count them, or route them to a side output, rather than losing them silently.

Lateness is a trade. More of it means more complete results, but each window must be kept in memory longer, and results are either delayed or emitted and then corrected.

Flink's \`allowedLateness\` and Beam's \`withAllowedLateness\` work this way. Beam also lets a window fire early, on time and late, each firing refining the last.`,
    related: ['Watermark', 'Tumbling window'],
  },
  {
    term: 'Stream-table join',
    aliases: ['stream table join', 'stream enrichment'],
    summary: 'Enriching each event of a stream with the matching row of a table that is itself kept up to date from a changelog.',
    detail: `In a **stream-table join** each event, for example an order, looks up a row in a table, for example its customer, and is sent on with the extra fields. Inside a stream processor the table is usually local state built from the table's changelog, not a remote database query per event.

The question is which version of the row to use. Joining against the table as the processor holds it at that moment is simple but nondeterministic. The result depends on how far each input has been read, so replaying the same streams can give different output. A temporal join pins the answer to event time.

Kafka Streams' KStream-KTable join uses the table's current state. Flink's temporal table joins pick the version as of the event's time.`,
    related: ['Temporal join', 'Changelog'],
  },
  {
    term: 'Temporal join',
    aliases: ['as-of join', 'event-time join'],
    summary: "A join that uses the version of a row that was valid at the event's timestamp.",
    detail: `A **temporal join** keeps each row's history, such as \`[{ ts: 70032, tier: "silver" }, { ts: 70725, tier: "gold" }]\`. An order at ts 70400 gets "silver", whenever it is processed.

That needs two things. First, keep old versions while events that might need them can still arrive. Second, wait until the table's changelog has passed the event's time, because otherwise the version that applies may not have been read yet. A watermark on the table side, or strictly ordered change timestamps, says when that is.

The reward is determinism. Replaying the same inputs gives the same joined output, which is what makes reprocessing and exactly-once recovery meaningful.`,
    related: ['Stream-table join', 'Watermark'],
  },
  {
    term: 'Changelog',
    aliases: ['changelog stream'],
    summary: 'A stream of the changes made to a table; replaying it rebuilds the table.',
    detail: `A table and its **changelog** are two views of the same thing. The table is the state at one moment. The changelog is every change that led there. Replaying the changelog from the start rebuilds the table, and a table can be turned back into a changelog by recording its updates.

This is called stream-table duality, and stream processors lean on it. Kafka Streams backs each state store with a compacted changelog topic, so a failed task can rebuild its table on another machine. Change data capture produces a changelog from a database.

Compaction keeps only the latest change per key, so the topic stays bounded but can still rebuild the current table, though not its history.`,
    related: ['Change data capture', 'Stream-table join'],
  },
  {
    term: 'Exactly-once semantics',
    aliases: ['exactly-once state', 'effectively once'],
    summary: 'Results identical to processing every input once, even though failures make inputs get processed again.',
    detail: `After a crash a stream processor replays some input, so events are physically processed more than once. **Exactly-once semantics** means the *effects* are as if each had been processed once. Every counter, balance and output reflects each input a single time.

It takes two parts. Internal state and input offset are checkpointed atomically, so a restart replays from a consistent point. Outputs are either committed atomically with that checkpoint, as with Kafka transactions, or written to a sink that recognises repeats by a deterministic id.

Leave out either part and you get at-least-once, with double counting, or at-most-once, with lost outputs, depending on what is saved first.`,
    related: ['Stream checkpoint', 'Idempotent sink'],
  },
  {
    term: 'Stream checkpoint',
    aliases: ['state checkpoint', 'checkpointing'],
    summary: "A durable snapshot of a stream processor's state together with the input offsets it corresponds to.",
    detail: `A **stream checkpoint** records "after input offset 120, the balances were …". Recovery loads the last checkpoint and replays input from offset 120. The result is the same as if the crash had never happened, as long as the snapshot and the offset were saved in one atomic step.

Order matters when there are outputs. Saving the checkpoint before outputs are acknowledged can lose them: the restarted processor resumes past an output that never arrived. Saving it after means some outputs are sent again, which is safe only if the sink deduplicates.

Flink takes consistent checkpoints across a whole distributed job using barriers in the streams (asynchronous barrier snapshotting, based on Chandy–Lamport).`,
    related: ['Exactly-once semantics', 'Consumer offset'],
  },
  {
    term: 'Idempotent sink',
    aliases: ['idempotent output', 'transactional sink'],
    summary: 'A destination that recognises repeated writes by id, so retries and replays do not create duplicates.',
    detail: `An **idempotent sink** stores each write under an id and ignores any later write with the same id. If a processor replays an event after a crash and sends its output again with the same id, nothing changes.

The id must be **deterministic**: derived from the input, such as the source offset, so the replay produces the same one. Ids from a counter that restarts, or from a random number, give the replay a new id and the duplicate gets in.

Examples include upserts keyed by event id in a database, Kafka's idempotent producer (producer id plus sequence number), and object stores written under a deterministic key.`,
    related: ['Exactly-once semantics', 'Idempotence'],
  },
];
