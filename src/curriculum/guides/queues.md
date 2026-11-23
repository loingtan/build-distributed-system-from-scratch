## The problem

A photo site resizes every upload before it shows it. The first version does the resize inside the HTTP request: the user uploads, the web server spends two seconds in an image library, and only then answers. That works until a school uploads its yearbook. A thousand requests arrive in a minute, every web server thread is busy resizing, and ordinary page views time out behind them.

The usual fix is to stop doing the slow work while someone waits. The web server writes a note, "resize photo 17", puts it somewhere, and answers at once. A separate pool of workers picks notes up and does the work at whatever pace it can manage. The "somewhere" is a queue, held by a process called a broker.

That fixes the yearbook day and creates a new problem. Follow one job:

1. The web server sends "resize photo 17" to the broker. The broker stores it and says OK.
2. Worker w1 asks for work. The broker hands it photo 17 and deletes it from the queue.
3. Thirty milliseconds later w1 runs out of memory and dies.

Photo 17 is now nowhere. The broker deleted it in step 2, the web server forgot it in step 1, and w1 is gone. No error is logged anywhere; the user's photo simply never appears. The rest of this guide is about the handful of rules that close this gap and the other gaps that open once you close it: a worker that is slow rather than dead, a web server that sends the same job twice, a job that kills every worker that touches it, and a broker that itself crashes.

## The core idea

### A queue and its consumers

A **work queue** is a list of jobs waiting to be done. **Producers** add jobs at one end; **consumers** (workers) take them from the other. A job here is an id plus an opaque **payload**: the broker never looks inside the payload, it only stores it and hands it back unchanged.

The simplest queue is **FIFO** (first in, first out): the job that has waited longest goes out next. Several consumers can ask at once, and each job must go to exactly one of them; this arrangement is called **competing consumers**. Adding a consumer adds capacity without any coordination between consumers, because the broker is the only place that decides who gets what. On a real multithreaded broker that decision takes a lock. In the simulator a handler runs to completion before the next message is looked at, so "take the first job and remove it" cannot be interrupted halfway.

The protocol is three messages:

```json
{ "type": "enqueue", "id": "job-17", "payload": { "task": "resize", "photo": 17 } }
{ "type": "dequeue" }
{ "type": "dequeue_ok", "job": { "id": "job-17", "payload": { "task": "resize", "photo": 17 } } }
```

and `"job": null` when nothing is waiting. A consumer that gets `null` waits a little and asks again.

### Keep the job until it is done

The bug in the opening story is that `dequeue` deletes the job. The fix is to split "handed out" from "finished". After a hand-out the job is **in flight**: the broker no longer offers it to anyone, but it still holds a copy. The consumer sends an **acknowledgement** (`ack`) when it has finished, and only then does the broker forget the job.

What if no ack ever comes? The broker cannot see the consumer's process, so it uses a timer. If no ack arrives within the **visibility timeout** (500 ms in the tasks), the broker assumes the consumer died and makes the job visible again, at the front of the queue, since it is older than anything else waiting.

<figure>
<svg viewBox="0 0 600 200" role="img" aria-label="Job states: ready, in flight, done; a timeout moves in flight back to ready">
  <g style="fill: var(--surface); stroke: var(--line)">
    <rect x="30" y="70" width="110" height="44" rx="6"/>
    <rect x="245" y="70" width="110" height="44" rx="6" style="fill: var(--accent-soft); stroke: var(--accent)"/>
    <rect x="460" y="70" width="110" height="44" rx="6"/>
    <rect x="245" y="150" width="110" height="40" rx="6" style="fill: var(--surface); stroke: var(--bad)"/>
  </g>
  <g style="fill: var(--ink); font: 13px var(--sans)" text-anchor="middle">
    <text x="85" y="97">ready</text><text x="300" y="97">in flight</text><text x="515" y="97">done</text>
    <text x="300" y="175">dead letter</text>
  </g>
  <g style="stroke: var(--ink); stroke-width: 1.5; fill: none">
    <line x1="140" y1="84" x2="240" y2="84"/><polyline points="232,79 240,84 232,89"/>
    <line x1="355" y1="92" x2="455" y2="92"/><polyline points="447,87 455,92 447,97"/>
    <path d="M245 104 C200 125 170 125 140 104"/><polyline points="148,103 140,104 145,111"/>
    <line x1="300" y1="114" x2="300" y2="146"/><polyline points="295,138 300,146 305,138"/>
  </g>
  <g style="fill: var(--muted); font: 12px var(--sans)" text-anchor="middle">
    <text x="190" y="76">dequeue</text><text x="405" y="84">ack</text>
    <text x="190" y="138">timeout, tries left</text><text x="380" y="135">timeout, 3rd try</text>
    <text x="85" y="58">enqueue (new id)</text>
  </g>
</svg>
<figcaption>Each job id is in exactly one state. Only an ack or the third expired hand-out ends its life.</figcaption>
</figure>

This gives **at-least-once** delivery: every accepted job is eventually processed, but possibly more than once. The duplicate is unavoidable. From the broker's point of view, "the consumer died before doing the work" and "the consumer did the work and died before sending the ack" look identical: in both cases nothing arrives. The broker must redeliver in both cases or risk losing the first kind. So consumers must tolerate seeing a job twice.

### Idempotent operations: effectively once

Duplicates come from the other side as well. A producer that does not hear `enqueue_ok` in time sends the same `enqueue` again with the same id. Maybe the first copy was lost; maybe it arrived and only the reply was slow. The retry may come 20 ms later or a second later, after the job has already been processed and acknowledged.

A consumer may also be slow rather than dead. It takes 680 ms, the visibility timeout fires at 500 ms, the job goes back to the queue (or to another consumer), and then the slow consumer's ack arrives. This is a **late acknowledgement**. Some consumers also simply send their ack twice.

The answer is to make both operations **idempotent**: applying one twice has the same effect as applying it once. For `enqueue` that means "create the job if this id has never been seen". For `ack` it means "mark this job done, wherever it currently is". Both need **deduplication**, which needs memory that outlives the job: one record per id, kept after the job is finished, with a state field (`ready`, `inflight`, `done`). The record is the reason a retried enqueue that arrives after the ack still finds the id and creates nothing.

At-least-once delivery plus idempotent handling is what people mean by **effectively once**. Nothing is delivered exactly once on the wire; the effect is as if it were.

A subtle consequence: timers from old hand-outs keep firing. A job handed out at t=0, timed out at t=500, handed out again at t=520 and acknowledged at t=600 still has whatever timer you left behind. A timer must check, when it fires, that the job is still in flight on the hand-out it was started for, and do nothing otherwise.

### Poison messages and the dead-letter list

Some jobs can never succeed: the photo file is corrupt and the resize library crashes on it. Such a job is a **poison message**. Under at-least-once delivery it comes back every 500 ms forever and takes a consumer down each time.

The fix is a **retry limit**: count hand-outs per job, and when the third hand-out times out, do not put the job back. Move it to a **dead-letter queue** (DLQ), a separate list that nobody consumes automatically. A person (or a separate tool) reads it, fixes the cause and resubmits. The job is stopped but not thrown away.

### Surviving a broker crash

So far the broker never failed. A broker that holds the queue in memory loses everything in a crash, including jobs it promised to keep with `enqueue_ok`. **Durability** means writing state to storage that survives a crash before acknowledging it. The rule is ordering: change the disk, then reply. A client that has been told "OK" can then rely on it.

**Crash recovery** has one hard part. Timers live in memory, so a job that was in flight at the crash has lost its timeout. On restart the broker scans its records for jobs in flight and either puts them straight back in the queue or, better, restarts a timer for the time that was left. The second option needs **persisted deadlines**: instead of "a timer is running", store "this hand-out expires at t=1730". A deadline is plain data and survives; the timer becomes just a reminder to go and look at the deadline.

<figure>
<svg viewBox="0 0 620 200" role="img" aria-label="Broker crashes with job in flight; after restart it re-arms the timer from the stored deadline">
  <g style="font: 13px var(--mono); fill: var(--ink)">
    <text x="10" y="54">c1</text><text x="10" y="114">brk</text><text x="10" y="174">c2</text>
  </g>
  <g style="stroke: var(--line)"><line x1="45" y1="50" x2="610" y2="50"/><line x1="45" y1="110" x2="610" y2="110"/><line x1="45" y1="170" x2="610" y2="170"/></g>
  <line x1="80" y1="110" x2="120" y2="50" style="stroke: var(--accent); stroke-width: 2"/>
  <text x="70" y="128" style="fill: var(--muted); font: 11px var(--mono)">deadline=1730</text>
  <text x="130" y="44" style="fill: var(--bad); font: 12px var(--sans)">c1 dies, no ack</text>
  <rect x="230" y="98" width="70" height="24" style="fill: var(--surface); stroke: var(--bad); stroke-dasharray: 4 3"/>
  <text x="240" y="114" style="fill: var(--bad); font: 12px var(--sans)">crashed</text>
  <text x="305" y="96" style="fill: var(--ink); font: 11px var(--sans)">restart: re-arm timer</text>
  <line x1="420" y1="104" x2="420" y2="116" style="stroke: var(--warn); stroke-width: 3"/>
  <text x="400" y="140" style="fill: var(--warn); font: 11px var(--mono)">t=1730</text>
  <line x1="460" y1="110" x2="500" y2="170" style="stroke: var(--accent); stroke-width: 2"/>
  <line x1="540" y1="170" x2="580" y2="110" style="stroke: var(--good); stroke-width: 2"/>
  <text x="560" y="160" style="fill: var(--good); font: 12px var(--sans)">ack</text>
</svg>
<figcaption>The deadline is on disk, so the restarted broker knows when job-9 becomes visible again even though its old timer died with the process.</figcaption>
</figure>

## A worked example

One broker `brk`, consumers `c1`, `c2`, `c3`, producer `p1`. Visibility timeout 500 ms, three hand-outs at most. All times in milliseconds on the broker's clock.

| t | event | broker state afterwards |
|---|---|---|
| 0 | `p1` enqueues job-4; reply `enqueue_ok` is lost on the way | job-4 ready (1 record) |
| 10 | `c1` dequeues: gets job-4, hand-out 1, deadline 510 | job-4 inflight |
| 40 | `c1` crashes mid-resize | unchanged; broker cannot see it |
| 300 | `p1` retries the enqueue of job-4 | record exists: nothing created, reply `enqueue_ok` |
| 510 | timer fires; hand-out 1 expired, 1 < 3 | job-4 ready, front of queue |
| 515 | `c2` dequeues: job-4, hand-out 2, deadline 1015 | job-4 inflight |
| 700 | `brk` crashes | disk: job-4 inflight, deliveries 2, deadline 1015 |
| 850 | `brk` restarts, scans records, re-arms a timer for 1015 − 850 = 165 ms | job-4 inflight |
| 1015 | `c2` was slow, not dead, and has not acked; timer fires; 2 < 3 | job-4 ready |
| 1020 | `c3` dequeues: job-4, hand-out 3, deadline 1520 | job-4 inflight |
| 1100 | `c2`'s late ack arrives | job-4 done; c3's work is a harmless duplicate |
| 1160 | `c3` acks job-4 | already done: nothing changes, reply `ack_ok` |
| 1520 | hand-out 3's timer fires | state is done, not inflight: the timer does nothing |

Four faults happened: a lost reply, a dead consumer, a broker crash and a slow consumer. No job was lost, none was created twice, and the job was never handed out after its ack. Had the late ack not arrived and c3 crashed too, the timer at 1520 would have found deliveries = 3 and moved job-4 to the dead letters instead.

## Why it is correct

The argument rests on one invariant: **every accepted id has exactly one record, and that record is in exactly one state**. Each guarantee follows from it.

- *No loss.* A job leaves the broker's custody only by reaching `done` (an ack) or `dead` (an expired final hand-out). While in flight, a deadline is attached; when it passes, the job returns to `ready`. So a job that is never acknowledged keeps cycling until it is dead-lettered, and it cannot vanish.
- *No duplicate creation.* `enqueue` creates a record only if none exists, and records are never deleted. A retry can arrive at any later time and still finds the record.
- *No revival after ack.* `ack` moves any non-terminal record to `done`. A stale id left in the ready list is skipped by `dequeue` because its record is not `ready`; a stale timer checks the state and the hand-out before acting.
- *Bounded attempts.* The hand-out counter only grows, and the decision to requeue compares it with the limit, so the limit is never exceeded.
- *Durability.* Each handler writes the disk before replying, and handlers run without interruption, so there is no instant at which a reply has left and the corresponding state has not been written. A crash between handlers loses only in-memory timers, which recovery rebuilds from stored deadlines.

The assumptions matter. The visibility timeout must be longer than a healthy consumer needs, or healthy work is redone constantly. The broker's clock must not jump. And "effectively once" holds for the broker's own state: if the consumer's work has outside effects (charging a card), the consumer needs its own deduplication, typically by job id.

## Trade-offs and failure modes

**Duplicates are the price of no loss.** At-least-once delivery means consumers see repeats whenever an ack is late or lost. The alternative, **at-most-once** (delete on hand-out), loses jobs on every consumer crash. There is no free third option at this layer.

**The visibility timeout is a guess.** Too short and slow-but-healthy consumers have their work redelivered, doubling load exactly when the system is slow. Too long and a crashed consumer's job sits invisible for that whole period. Real systems let consumers extend the timeout while they work, which is a form of lease renewal.

**Memory for deduplication grows.** Keeping every id forever is fine in a test, not in a broker that has seen ten billion jobs. Real systems keep ids for a window (minutes to days) and accept that a retry later than the window creates a duplicate.

**The retry limit is a judgement.** Set it to 1 and one unlucky crash sends a good job to the DLQ. Set it to 50 and a poison job takes down 50 consumers. And a dead-letter queue is only useful if somebody looks at it; an unmonitored DLQ is a silent data loss with extra steps.

**FIFO weakens under redelivery.** A job that timed out goes back to the front, but other jobs have been handed out meanwhile, so completion order is not enqueue order. Strict order with many consumers needs partitioning (one consumer per ordered stream), which limits parallelism.

**Durability costs latency.** Writing to disk before each reply, and syncing it, is far slower than memory. Brokers batch writes or replicate to other machines instead of syncing each message, trading some risk for throughput.

**A single broker is a single point of failure.** Durability survives a crash and restart; it does not survive losing the disk. Production brokers replicate the queue, which brings in the consensus and replication problems from other tracks.

## In real systems

**Amazon SQS** is the clearest match. A received message becomes invisible for the queue's visibility timeout (30 seconds by default) and reappears if not deleted; the consumer "acks" by deleting it with a receipt handle. A redrive policy with `maxReceiveCount` moves a message to a dead-letter queue after that many receives. FIFO queues add a deduplication id and drop duplicates sent within a five-minute window.

**RabbitMQ** delivers to consumers with manual acknowledgements; an unacknowledged message is requeued when the consumer's channel or connection closes, rather than on a timer. Messages rejected or expired can be routed to a dead-letter exchange. Durable queues with persistent messages are written to disk, and quorum queues replicate them with Raft and can cap redeliveries with a delivery limit.

**Apache Kafka** is a log rather than a queue: consumers do not ack individual messages but commit an offset per partition, meaning "everything before this is done". A consumer that crashes before committing causes its partition's new owner to reprocess from the last committed offset, which is at-least-once. Idempotent producers attach sequence numbers so the broker drops retried writes.

**Sidekiq, Celery and similar job libraries** built on Redis implement the same states by hand: a job moves from a waiting list to an in-progress structure and is removed on completion; a reaper puts back jobs whose worker has disappeared; failed jobs are retried with a limit and then parked in a "dead" set.

## Common misconceptions

- **"Use exactly-once delivery and the problem goes away."** No broker can guarantee a message is processed exactly once in the presence of crashes; it can only guarantee at-least-once and help you deduplicate. Systems advertising exactly-once mean idempotence or transactions around the processing, inside a defined boundary.
- **"If the consumer crashes, the broker will know."** The broker only sees silence. A crashed consumer, a slow one and one whose ack was lost look the same, which is why redelivery uses a timeout and why duplicates follow.
- **"Deduplicating against the jobs currently in the queue is enough."** A producer's retry can arrive after the job was processed and removed. Deduplication needs a record that outlives the job.
- **"A dead-letter queue is for errors you can ignore."** It holds work that was accepted and never done. Each entry is a user-visible failure until someone fixes and resubmits it.
- **"Writing the job to disk at some point is durable."** Only if the write happens before the reply. Reply first and a crash in between breaks the promise the reply made.

## How the tasks build it

[QU-1 A FIFO queue](#/c/queue-fifo) is the bare broker: enqueue, dequeue in order, `null` when empty. The tests use two and three competing consumers and bursts of 20 jobs, and check that each job goes out exactly once with its payload intact.

[QU-2 Acknowledge and redeliver](#/c/queue-ack) adds the in-flight state, the ack and the 500 ms visibility timeout. A quarter or more of the consumers die before acking, some jobs lose two consumers in a row, and the tests time every redelivery: not before 450 ms, available by 750 ms.

[QU-3 Effectively once](#/c/queue-dedup) makes enqueue and ack idempotent. Producers resend enqueues up to 900 ms later, a consumer acks 680 ms after its hand-out, and some acks arrive twice; stale timers from earlier hand-outs are the classic trap.

[QU-4 Dead letters](#/c/queue-dead-letter) adds the three-attempt limit and the dead-letter list. Poison jobs kill every consumer, mixed with flaky jobs that succeed on the second or third try; only an expired third attempt may be dead-lettered.

[QU-5 A durable broker](#/c/queue-durable) moves all state onto `node.disk` and crashes the broker up to three times. Jobs in flight at the crash must come back within 750 ms of the restart, and the hand-out limit has no allowance for crashes.

## Further reading

- [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) — the in-flight state and its timeout, described by the system that made the pattern popular.
- [Amazon SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html) — redrive policies and `maxReceiveCount`, with advice on choosing the limit.
- [RabbitMQ: Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms) — a careful treatment of acks on both sides of the broker and what each guarantees.
- [Kafka documentation: message delivery semantics](https://kafka.apache.org/documentation/#semantics) — at-most-once, at-least-once and exactly-once explained in terms of offsets and idempotent producers.
- [Enterprise Integration Patterns: Competing Consumers](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html) — the classic catalogue entry, with the neighbouring Dead Letter Channel and Idempotent Receiver patterns.
