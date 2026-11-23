// Track: Rolling changes. See docs/authoring.md for the shape of this file.
import { rollTolerant, rollNegotiate, rollExpand, rollFlags } from '../../sim/workloads/rolling-changes.js';

export default {
  id: 'rolling-changes',
  code: 'RC',
  category: 'operations',
  title: 'Rolling changes',
  level: 'Advanced',
  blurb: 'Change a running cluster without downtime: upgrade nodes one at a time, keep old and new versions talking, and migrate stored data in place.',
  subtracks: [
    {
      title: 'Old and new on the wire',
      tasks: [
        {
          slug: 'roll-tolerant-reader',
          title: 'Adding a field',
          level: 'Medium',
          minutes: 25,
          summary: 'Version 2 sends one more field. Version-1 nodes must not notice, and nothing may fail while both run.',
          concepts: ['rolling upgrade', 'tolerant reader', 'forward compatibility'],
          workload: rollTolerant,
          tests: [
            { name: 'Rolling upgrade', blurb: 'Three nodes upgraded one after another, n1 first, while clients keep reading and writing.', nodeCount: 3, duration: 5000, params: { rollout: { start: 800, every: 800 } } },
            { name: 'Five nodes, any order', blurb: 'Five nodes upgraded in a random order, so every pairing of old and new meets.', nodeCount: 5, duration: 6000, params: { rollout: { start: 700, every: 650, shuffle: true } } },
          ],
          hints: [
            'Start `runVersion2` by calling `runVersion1()`, then replace only the handlers that change. Calling `node.on` again for a type replaces the earlier handler.',
            'The node a put arrives at knows the client: `msg.src`. Pass it to the home as one more field, `{ type: "fwd_put", key, name, by: msg.src }`. Keep `key` and `name` exactly as they are, and keep the message type: version 1 has no handler for a new one.',
            'Store the writer next to the name, `rec.by`, and reply `fwd_get_ok` with both `name` and `by`. A version-1 forwarder reads `name` and ignores `by`.',
            'A `fwd_put` from a version-1 node has no `by`. That means the writer is unknown: store `null`. Keeping the previous writer would report someone who did not make this change.',
          ],
        },
        {
          slug: 'roll-negotiation',
          title: 'Negotiating a new format',
          level: 'Hard',
          minutes: 35,
          summary: 'Version 2 has a message version 1 cannot read. Use it only with peers you know run version 2.',
          concepts: ['version negotiation', 'handshake', 'mixed-version cluster'],
          workload: rollNegotiate,
          tests: [
            { name: 'Rolling upgrade', blurb: 'Three nodes upgraded one after another, n1 first.', nodeCount: 3, duration: 5000, params: { rollout: { start: 800, every: 800 } } },
            { name: 'Five nodes, any order', blurb: 'Five nodes upgraded in a random order.', nodeCount: 5, duration: 6000, params: { rollout: { start: 700, every: 650, shuffle: true } } },
            { name: 'Slow links', blurb: 'Three nodes on links of 10–60ms, so a handshake takes a while to come back.', nodeCount: 3, duration: 5000, latency: [10, 60], params: { rollout: { start: 800, every: 800 }, gap: 10 } },
          ],
          hints: [
            'Keep a `Set` of peers you know run version 2. It starts empty every time the file runs: a node that restarts has forgotten everything it learned.',
            'A handshake: when version 2 starts, send every peer `{ type: "hello", version: 2 }` with `node.rpc`. A version-2 peer replies; a version-1 peer has no handler and stays silent, so the rpc resolves to `null`.',
            'A hello you receive is evidence too: its sender runs version 2. Without that, the first node to upgrade never learns about the nodes that upgrade after it.',
            'For a peer in the set, `node.send(owner, { type: "fwd2", op, key, name, client: msg.src, reply_to: msg.body.msg_id })` and do not reply yourself. The home answers with `node.send(client, { type: "put_ok", in_reply_to: reply_to })`.',
            'Keep the `fwd_put` and `fwd_get` handlers in version 2: version-1 nodes go on sending them until they are upgraded.',
          ],
        },
      ],
    },
    {
      title: 'Data and releases',
      tasks: [
        {
          slug: 'roll-expand-contract',
          title: 'Expand and contract',
          level: 'Hard',
          minutes: 40,
          summary: 'Split a stored field in two without losing data, even when a node is rolled back halfway.',
          concepts: ['schema migration', 'expand–contract', 'rollback', 'dual writes'],
          workload: rollExpand,
          tests: [
            { name: 'Upgrade, then finalize', blurb: 'Three nodes upgraded in turn, then finalize, then one node restarted.', nodeCount: 3, duration: 6000, params: { rollout: { start: 700, every: 700 }, finalize: 2800, steps: [[3800, 'n2', 2]] } },
            {
              name: 'Rollback halfway',
              blurb: 'n2 runs version 2, is rolled back to version 1 and writes for a while, then is upgraded again before finalize.',
              nodeCount: 3,
              duration: 7500,
              params: { steps: [[700, 'n1', 2], [1400, 'n2', 2], [2100, 'n2', 1], [3000, 'n2', 2], [3700, 'n3', 2], [5000, 'n1', 2]], finalize: 4400 },
            },
            {
              name: 'Five nodes, two rollbacks',
              blurb: 'All five upgraded, two of them rolled back and upgraded again, then finalize.',
              nodeCount: 5,
              duration: 8000,
              params: {
                steps: [[600, 'n1', 2], [1100, 'n2', 2], [1600, 'n3', 2], [2100, 'n4', 2], [2600, 'n5', 2], [3100, 'n4', 1], [3600, 'n2', 1], [4100, 'n2', 2], [4600, 'n4', 2], [5800, 'n5', 2]],
                finalize: 5200,
              },
            },
          ],
          hints: [
            'Expand first: while version 1 might still run, a record must stay readable by version 1. Add `first` and `last` next to `name`; do not replace it.',
            'Version 1 updates a record in place: it sets `name` and leaves `first` and `last` alone. After a rollback, which of the two can version 2 trust until finalize?',
            'Before finalize, read `name` and write all three fields. That is all version 2 needs until then.',
            'On `finalize`, rewrite every record to `{ first, last }`, derived from `name`, and set something like `node.disk.finalized = true`. From then on write only `first` and `last`, and read from them.',
          ],
        },
        {
          slug: 'roll-feature-flags',
          title: 'Flags and canaries',
          level: 'Hard',
          minutes: 35,
          summary: 'Ship a new read path switched off, turn it on for one node, and switch it off again when it misbehaves.',
          concepts: ['feature flags', 'canary release', 'kill switch', 'automatic rollback'],
          workload: rollFlags,
          tests: [
            {
              name: 'Canary, kill switch, promote',
              blurb: 'Version 2 rolls out dark. The flag goes on for n2, which restarts, is switched off and on, and finally the flag goes on everywhere.',
              nodeCount: 3,
              duration: 8000,
              params: {
                rollout: { start: 600, every: 600 },
                steps: [[3400, 'n2', 2]],
                flags: [[2200, 'n2', true], [4100, 'n2', false], [4500, 'n2', true], [5200, 'all', true]],
                getShare: 0.75,
                gap: 6,
              },
            },
            {
              name: 'Bad canary',
              blurb: 'The flag goes on for n3, then the cache starts failing every other read. n3 must turn itself off and stay off after a restart.',
              nodeCount: 3,
              duration: 6000,
              params: { rollout: { start: 600, every: 600 }, steps: [[3800, 'n3', 2]], flags: [[2200, 'n3', true]], degradeAt: 2700, getShare: 0.75, gap: 6 },
            },
            {
              name: 'Everyone on, then trouble',
              blurb: 'Five nodes, the flag on everywhere, then the cache degrades. Each node must turn itself off on its own.',
              nodeCount: 5,
              duration: 6500,
              params: { rollout: { start: 500, every: 500, shuffle: true }, flags: [[2900, 'all', true]], degradeAt: 3800, getShare: 0.75, gap: 6 },
            },
          ],
          hints: [
            'Keep flags on the disk, `node.disk.flags ??= {}`, so that a restart keeps them, and look the flag up on every get so that `set_flag` takes effect at once.',
            'With the flag on: `const res = await node.rpc("cache", { type: "read", key }, { timeout: 100 })`. If `res` is not a `read_ok`, carry on down the version-1 path. The client must never see the cache fail.',
            'The guard: an array of the last 20 outcomes. Push after each read, `shift()` when it grows past 20, and when 5 entries are failures set the flag to false on `node.disk`.',
            'Do not switch off at the first error: the healthy cache fails one read in seven. And clear the outcomes when `set_flag` turns the flag on.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Renaming or removing a field that old nodes still read',
      symptom: 'During the rollout, requests that pass from a new node to an old one fail or come back empty, and the errors stop once every node is upgraded.',
      fix: 'While any node runs the old version, only add fields. Keep sending everything the oldest running version reads, under the same names and message types.',
    },
    {
      title: 'Treating a missing field as "unchanged"',
      symptom: 'Metadata such as the last writer is wrong after an old node touched the record.',
      fix: 'A message or record from an old version lacks the new field because the old code knows nothing about it. Store "unknown" or derive the value again; do not keep what was there before.',
    },
    {
      title: 'Using a new format before the receiver can read it',
      symptom: 'Requests disappear without an error when a new node talks to an old one.',
      fix: 'Find out what the peer understands first, through a handshake or a version field, and forget it when the peer restarts. Fall back to the old format with everyone else.',
    },
    {
      title: 'Contracting while a rollback is still possible',
      symptom: 'After a node is rolled back, reads return null or stale values: the old code finds records without the field it reads.',
      fix: 'Write both shapes while old code may run, treat the old field as the truth until then, and remove it only after an explicit finalize that is itself stored on disk.',
    },
  ],
  questions: [
    {
      q: 'Why does a rolling upgrade need compatibility in both directions, while an upgrade that stops every node at once mostly needs one?',
      a: 'During a rolling upgrade old and new code run at the same time, so new nodes read what old ones send (backward compatibility) and old nodes read what new ones send (forward compatibility). The same holds for stored data the moment you roll a node back: old code then reads records new code wrote. A full stop avoids the mixed period but costs downtime, and a rollback still needs forward compatibility for the data.',
    },
    {
      q: 'In the negotiation task, what goes wrong if a node is rolled back to version 1 after its peers have learned that it runs version 2? How could you handle it?',
      a: 'The peers keep sending fwd2, which version 1 cannot read, so requests are lost. Version knowledge has to expire when the peer restarts: tie it to a connection or an incarnation number that changes on restart, put the version on every message and drop what you knew when one arrives without it, or keep a fallback to the old format when a peer stops answering. Real protocols negotiate per connection, and a restart means a new connection.',
    },
    {
      q: 'Why turn a feature on with a flag on one node, instead of deploying the new code to one node first?',
      a: 'A flag separates releasing from deploying. It can be switched off in milliseconds without a restart or a new build, it can be aimed at one node or a slice of users, and the node can switch it off by itself when its error rate rises. Comparing nodes that run the same build with the flag on and off also isolates the feature: any difference is caused by the feature, not by everything else that went into that build.',
    },
  ],
  reading: [
    { kind: 'Book', title: 'Designing Data-Intensive Applications, chapter 4: Encoding and Evolution', url: 'https://dataintensive.net/', note: 'Kleppmann on rolling upgrades, backward and forward compatibility, and why old code must keep fields it does not understand.' },
    { kind: 'Blog', title: 'Parallel Change', url: 'https://martinfowler.com/bliki/ParallelChange.html', note: 'Danilo Sato on expand, migrate and contract: changing an interface in three safe steps.' },
    { kind: 'Blog', title: 'Feature Toggles (aka Feature Flags)', url: 'https://martinfowler.com/articles/feature-toggles.html', note: 'Pete Hodgson on release toggles, ops toggles and canary releases, and how to keep flags from piling up.' },
  ],
};
