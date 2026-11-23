// Track: Stream processing. See docs/authoring.md for the shape of this file.
import { cdc, windows, join, exactlyOnce } from '../../sim/workloads/streams.js';

export default {
  id: 'streams',
  code: 'ST',
  category: 'processing',
  title: 'Stream processing',
  level: 'Advanced',
  blurb: 'Events that never stop: change data capture, event-time windows with late data, stream-table joins and exactly-once state.',
  subtracks: [
    {
      title: 'Consuming a log',
      tasks: [
        {
          slug: 'stream-cdc',
          title: 'Change data capture',
          level: 'Medium',
          minutes: 25,
          summary: "Keep a derived view identical to a database by following its changelog, across crashes, from a stored offset.",
          concepts: ['change data capture', 'consumer offset', 'derived data'],
          workload: cdc,
          tests: [
            { name: 'Steady changes', blurb: '60 changes, one at a time; nothing fails.', nodeCount: 1, duration: 3500, params: { writes: 60 } },
            { name: 'Consumer crashes', blurb: '100 changes in bursts of 5; the view node crashes twice, after the log has started deleting old changes.', nodeCount: 1, duration: 5000, params: { writes: 100, burst: 5, crashes: [1900, 3000] } },
            { name: 'Crashes, slow and lost polls', blurb: 'Poll replies are sometimes slow or lost, and the node crashes three times.', nodeCount: 1, duration: 6000, params: { writes: 120, burst: 4, crashes: [1800, 2900, 4000], slow: 0.3, lossy: 0.1 } },
          ],
          hints: [
            'The view is only meaningful together with its offset: "these rows are the table after the first N changes". Keep `rows`, `counts` and `offset` on `node.disk`.',
            'In the browser lab, code that runs without an `await` in between cannot be split by a crash. Apply a change and move the offset in the same run.',
            'Two polls in flight for the same offset return the same changes. Either allow one poll at a time, or skip every change whose offset is not exactly the next one you need.',
            'Use a short rpc timeout (around 200 ms). A lost poll then costs one retry, not a second.',
          ],
        },
        {
          slug: 'stream-windows',
          title: 'Event-time windows',
          level: 'Hard',
          minutes: 35,
          summary: 'Count clicks per key in tumbling windows by event time; close windows by watermark, accept late events within the allowed lateness.',
          concepts: ['event time', 'tumbling window', 'watermark', 'allowed lateness'],
          workload: windows,
          tests: [
            { name: 'Nearly in order', blurb: '100 events, a little out of order, never behind the watermark.', nodeCount: 1, duration: 4000, params: { events: 100, keys: 2 } },
            { name: 'Late events', blurb: 'A quarter of the events arrive behind the watermark; most still make their window.', nodeCount: 1, duration: 5000, params: { events: 150, keys: 3, late: 0.25 } },
            { name: 'Too late to count', blurb: 'Late events and very late ones that miss their window entirely.', nodeCount: 1, duration: 6000, params: { events: 200, keys: 4, late: 0.2, veryLate: 0.1 } },
          ],
          hints: [
            'The window of an event is `Math.floor(ts / 1000) * 1000`. Nothing about it depends on when the event arrives.',
            'A late event (ts behind the watermark) is not the same as a dropped one. It is dropped only if its window has already closed, that is if `watermark >= end + 500`.',
            'Close windows only when a watermark record arrives. Loop over the open windows and emit each one whose `end + 500 <= watermark`.',
            'After emitting a window, delete it. If you keep it, an event for it would be counted in a window that will never be sent again; it should be dropped instead.',
          ],
        },
      ],
    },
    {
      title: 'State and guarantees',
      tasks: [
        {
          slug: 'stream-join',
          title: 'Stream-table join',
          level: 'Hard',
          minutes: 35,
          summary: "Enrich each order with its customer's tier as of the order's event time, from a lagging customer changelog.",
          concepts: ['stream-table join', 'temporal join', 'changelog'],
          workload: join,
          tests: [
            { name: 'Few changes', blurb: '60 orders, five customers, a handful of tier changes.', nodeCount: 1, duration: 4000, params: { orders: 60, customers: 5, changes: 10 } },
            { name: 'Busy changelog', blurb: '120 orders while tiers change often; two customers only appear halfway through.', nodeCount: 1, duration: 5500, params: { orders: 120, customers: 6, changes: 35 } },
          ],
          hints: [
            'One current tier per customer is not enough. Keep a list of `{ ts, tier }` versions for each, and look up the last one with `ts <= order.ts`.',
            'The changelog arrives 100–350 ms after the orders. An order read now may be older than a change that has not reached you yet.',
            'Changes come in ts order, and a watermark promises nothing earlier is coming. So the table is complete up to the largest ts you have read from the customers topic, change or watermark.',
            'Keep orders whose ts is above that point in a waiting list, and join them on a later tick.',
          ],
        },
        {
          slug: 'stream-exactly-once',
          title: 'Exactly-once output',
          level: 'Hard',
          minutes: 35,
          summary: 'Keep running balances from a payments log and send each to a sink exactly once, through crashes, sink outages and slow acks.',
          concepts: ['exactly-once semantics', 'stream checkpoint', 'idempotent sink'],
          workload: exactlyOnce,
          tests: [
            { name: 'One crash', blurb: '100 payments; the node crashes once, mid-stream.', nodeCount: 1, duration: 5500, params: { payments: 100, crashes: [2660] } },
            { name: 'Crash during a sink outage', blurb: 'The sink drops every request for 400 ms and the node crashes in the middle of it.', nodeCount: 1, duration: 6500, params: { payments: 140, crashes: [2840, 4240], outages: [[2660, 3060], [4060, 4460]] } },
            { name: 'Slow acks and outages', blurb: 'The node crashes while acknowledgements are slow and while the sink is down.', nodeCount: 1, duration: 7500, params: { payments: 180, accounts: 5, crashes: [2700, 3720, 4800], outages: [[2500, 2900], [4600, 5000]], slow: [[3520, 4000]] } },
          ],
          hints: [
            'After a restart the node must know two things that only make sense together: how far it has read, and what the balances were at that point. Store them as one object, `node.disk.checkpoint = { offset, balances }`, in one assignment.',
            'Save the checkpoint only after the sink has acknowledged every output up to that offset. Saving it first loses an output if the node dies while the sink is unreachable.',
            'Saving after the acknowledgement means some outputs will be sent again after a crash. That is fine if the id is the same each time: build it from the payment\'s offset.',
            'Balances for a batch can be computed in a copy and written with the offset at the end, so the disk never holds new balances with an old offset or the reverse.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Restarting from offset 0 with an empty state', symptom: '"The processor never asks for records the log has deleted" fails right after a restart.', fix: 'A log is kept for a limited time. Persist whatever you derived from it together with the offset it corresponds to.' },
    { title: 'Saving the offset and the state at different moments', symptom: 'After a crash, balances or counts are off by exactly the events between the two saves.', fix: 'Write one checkpoint holding both, in one atomic write.' },
    { title: 'Windowing by arrival time', symptom: 'Results for windows that have no events, or counts that change from run to run.', fix: 'Assign windows from the event\'s own timestamp and close them by watermark, never by node.now().' },
    { title: 'Joining against the latest row', symptom: 'Orders carry a tier their customer only got later, or had already lost.', fix: 'Keep versions of each row and look up the one valid at the event\'s time, waiting until the table has caught up to it.' },
  ],
  questions: [
    { q: 'Why is a watermark a guess in some streams and a promise in others?', a: 'A changelog from one database is written in commit order, so its source can promise that nothing older will follow. Clicks from millions of phones arrive whenever the phone gets signal, so a watermark there is an estimate, and allowed lateness says how much to trust it.' },
    { q: 'What does exactly-once really mean in stream processing?', a: 'Not that each event is delivered once: after a crash events are replayed. It means the results are as if each event had been processed once, because state and input position are checkpointed together and outputs are deduplicated or committed with the checkpoint.' },
    { q: 'Why does allowed lateness cost memory?', a: 'A window cannot be forgotten until the watermark passes its end plus the lateness. The longer late events are accepted, the more windows must be kept open at once.' },
  ],
  reading: [
    { kind: 'Paper', title: 'The Dataflow Model (Akidau et al., VLDB 2015)', url: 'https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/', note: 'Event time versus processing time, windows, watermarks and triggers, from the team behind Google Dataflow and Apache Beam.' },
    { kind: 'Docs', title: 'Apache Flink: timely stream processing', url: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/', note: 'How Flink assigns event time, generates watermarks and handles lateness.' },
    { kind: 'Docs', title: 'Kafka: message delivery semantics', url: 'https://kafka.apache.org/documentation/#semantics', note: 'At-most-once, at-least-once and exactly-once, and where offsets are stored for each.' },
  ],
};
