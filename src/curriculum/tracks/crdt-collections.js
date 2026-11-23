// Track: Sets, registers and maps. See docs/authoring.md for the shape of this file.
import { gSet, orSet, lwwRegister, mvRegister, cart } from '../../sim/workloads/crdt-collections.js';

export default {
  id: 'crdt-collections',
  code: 'CS',
  category: 'convergence',
  title: 'Sets, registers and maps',
  level: 'Intermediate',
  blurb: 'Replicated data types beyond counters: grow-only and observed-remove sets, last-writer-wins and multi-value registers, and maps that nest them.',
  subtracks: [
    {
      title: 'Sets',
      tasks: [
        {
          slug: 'crdt-g-set',
          title: 'Grow-only set',
          level: 'Easy',
          minutes: 15,
          summary: 'Every node accepts adds on its own and replicas converge by repeatedly exchanging and unioning their sets.',
          concepts: ['G-Set', 'anti-entropy', 'join semilattice'],
          workload: gSet,
          tests: [
            { name: 'Healthy network', blurb: '90 adds and reads through random nodes of three; every node must end with every acknowledged element.', nodeCount: 3, duration: 4500, params: { ops: 90 } },
            { name: 'Partition', blurb: 'Five nodes split into two sides for 1.5 seconds while adds continue on both.', nodeCount: 5, duration: 6000, params: { ops: 120, partition: [1200, 2700] } },
            { name: 'Lossy links', blurb: 'A quarter of all node-to-node messages are lost.', nodeCount: 5, duration: 5000, dropRate: 0.25, params: { ops: 100 } },
          ],
          hints: [
            'A message sent once can be lost. Instead of sending each new element, send your *whole set* to every peer every 100ms or so.',
            'On receiving a set, add every element to your own. Union never removes anything, so a stale or repeated message cannot do harm.',
            'Use `node.every(100, ...)`. JSON has no `Set`: send `[...elements]`.',
          ],
        },
        {
          slug: 'crdt-or-set',
          title: 'Observed-remove set',
          level: 'Medium',
          minutes: 30,
          summary: 'Add remove: tag every add uniquely, remove only the tags you have seen, so a concurrent add wins and re-adding works.',
          concepts: ['OR-Set', 'tombstones', 'add-wins', 'unique tags'],
          workload: orSet,
          tests: [
            { name: 'Three replicas', blurb: 'Scripted adds and removes before, during and after a partition, with background traffic.', nodeCount: 3, duration: 8000 },
            { name: 'Five replicas', blurb: 'The same script on five nodes split two against three.', nodeCount: 5, duration: 8000 },
            { name: 'Lossy links', blurb: 'Five nodes; a quarter of node-to-node messages are lost.', nodeCount: 5, duration: 8000, dropRate: 0.25 },
            { name: 'Slow links', blurb: 'Five nodes; messages take 5–80ms and overtake each other.', nodeCount: 5, duration: 8000, latency: [5, 80] },
          ],
          hints: [
            'A union merge cannot express "removed": the next merge brings the element back. You need a record of removals that also only grows, a set of **tombstones**.',
            'Tombstoning the element itself (a 2P-Set) makes a remove permanent: the element can never be added again, and a concurrent add loses. Tombstone something finer.',
            'Give every add a unique tag, such as `node.id + ":" + counter`. Keep `element -> set of tags` and a set of removed tags; both merge by union.',
            'A remove tombstones the tags *this replica holds* for the element right now. An element is present while it has a tag that is not tombstoned.',
            'Create a new tag on every add, even if the element is already present. That new tag is what lets an add survive a concurrent remove.',
          ],
        },
      ],
    },
    {
      title: 'Registers and maps',
      tasks: [
        {
          slug: 'crdt-lww-register',
          title: 'Last-writer-wins register',
          level: 'Medium',
          minutes: 20,
          summary: 'Keys hold one value each; replicas agree on the winner of concurrent writes by a (timestamp, node) tag.',
          concepts: ['LWW register', 'Lamport clock', 'tie-breaking'],
          workload: lwwRegister,
          tests: [
            { name: 'Three replicas', blurb: 'A chain of causally ordered writes, then writes racing on both sides of a partition.', nodeCount: 3, duration: 8000 },
            { name: 'Five replicas', blurb: 'The same script on five nodes split two against three.', nodeCount: 5, duration: 8000 },
            { name: 'Lossy links', blurb: 'Five nodes; a quarter of node-to-node messages are lost.', nodeCount: 5, duration: 8000, dropRate: 0.25 },
            { name: 'Clocks out of sync', blurb: 'Three replicas whose clocks disagree by up to 2 seconds, as unsynchronised machines do. Wall-clock timestamps put writes in the wrong order here.', nodeCount: 3, duration: 8000, clockSkew: 2000 },
          ],
          hints: [
            'Two tags with the same timestamp need a deterministic winner. Compare the node ids when the timestamps are equal.',
            'With `ts` from a Lamport clock, two writes made on opposite sides of a partition after the same history get the same `ts`. That is exactly the case the tests create.',
            'Keep the receive rule: on merge, `clock = Math.max(clock, entry.ts)` for every entry, so a later write outranks everything already seen.',
          ],
        },
        {
          slug: 'crdt-mv-register',
          title: 'Multi-value register',
          level: 'Hard',
          minutes: 35,
          summary: 'Keep concurrent writes as siblings using version vectors, and let a later write replace all the siblings it has seen.',
          concepts: ['MV register', 'version vectors', 'siblings', 'concurrent writes'],
          workload: mvRegister,
          tests: [
            { name: 'Three replicas', blurb: 'Sequential writes, conflicting writes during a partition, and a write that resolves the conflict.', nodeCount: 3, duration: 8000 },
            { name: 'Five replicas', blurb: 'The same script on five nodes split two against three.', nodeCount: 5, duration: 8000 },
            { name: 'Lossy links', blurb: 'Five nodes; a quarter of node-to-node messages are lost.', nodeCount: 5, duration: 8000, dropRate: 0.25 },
          ],
          hints: [
            'A timestamp always picks one winner. To detect that two writes are concurrent you need a **version vector**: per node, how many of its writes a version has seen.',
            'Store a list of versions `{ value, vv }` per key. A write builds its vector as the entry-wise max of every version it is replacing, then adds one to its own entry, and stores just itself.',
            'Version x dominates y when every entry of x is ≥ y\'s and at least one is greater. On merge, take both lists and drop every version dominated by another one.',
            'Two copies of the same version arrive from different peers: drop duplicates by comparing vectors (equal vectors mean the same write).',
          ],
        },
        {
          slug: 'crdt-or-map',
          title: 'Shopping cart (OR-Map)',
          level: 'Hard',
          minutes: 35,
          summary: 'A map from item to quantity: concurrent quantity changes add up, a remove clears what it saw, and an add wins over a concurrent remove.',
          concepts: ['OR-Map', 'nested CRDTs', 'observed reset'],
          workload: cart,
          tests: [
            { name: 'Three replicas', blurb: 'Scripted cart updates before, during and after a partition, with background traffic.', nodeCount: 3, duration: 8000 },
            { name: 'Five replicas', blurb: 'The same script on five nodes split two against three.', nodeCount: 5, duration: 8000 },
            { name: 'Lossy links', blurb: 'Five nodes; a quarter of node-to-node messages are lost.', nodeCount: 5, duration: 8000, dropRate: 0.25 },
          ],
          hints: [
            'The starter\'s remove hides the item but leaves its counter alone. A later add makes the item visible again with its old quantity.',
            'What should a remove delete? Exactly the quantity changes it has *seen*, just as the OR-Set remove deletes the tags it has seen.',
            'Simplest design: every add is a tagged change `{ tag: qty }` under its item. Tombstone the tags on remove. The quantity is the sum of the live changes; the item is present while it has one.',
            'A more compact design keeps a PN-counter per item and, on remove, records the per-node counter values it observed as a baseline (merged by max). The quantity is the counter minus the baseline.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    { title: 'Deleting locally from a union-merged set', symptom: 'Removed elements reappear a moment later.', fix: 'Union cannot express absence. Record removals in their own grow-only structure (tombstones) and merge that too.' },
    { title: 'Tombstoning the element instead of its adds', symptom: 'An element cannot be added again after a remove, and a concurrent add loses.', fix: 'Tag every add uniquely and tombstone only the tags the remove has seen.' },
    { title: 'Ties on the timestamp', symptom: 'After a partition, replicas keep different values for a key forever.', fix: 'Order tags by (timestamp, node id), so every replica picks the same winner.' },
    { title: 'Nesting without resetting', symptom: 'An item removed from the cart comes back with its old quantity when it is added again.', fix: 'A remove must also clear the nested value it observed, not just hide the key.' },
  ],
  questions: [
    { q: 'Why does an OR-Set make a concurrent add win over a remove?', a: 'A remove deletes only the add-tags its replica has seen. An add made concurrently on another replica carries a tag the remove could not have seen, so that tag survives the merge and the element stays.' },
    { q: 'What does a last-writer-wins register do with two concurrent writes, and what does a multi-value register do instead?', a: 'LWW keeps the one with the larger (timestamp, node) tag and silently drops the other, even though both clients were told their write succeeded. An MV register detects with version vectors that neither write saw the other and keeps both as siblings for the application or a later write to resolve.' },
    { q: 'Why do tombstones grow without bound, and how do real systems deal with it?', a: 'A replica must remember a removal until every replica has seen it, or a stale copy could bring the element back. Systems collect tombstones after a grace period (Cassandra) or once all replicas are known to have the state, or use dotted version vectors that summarise removed tags compactly (Riak).' },
  ],
  reading: [
    { kind: 'Paper', title: 'A comprehensive study of Convergent and Commutative Replicated Data Types', url: 'https://inria.hal.science/inria-00555588', note: 'Shapiro et al.: G-Set, 2P-Set, OR-Set, LWW and MV registers, with merge functions and proofs.' },
    { kind: 'Docs', title: 'Riak KV: Data types', url: 'https://docs.riak.com/riak/kv/latest/developing/data-types/index.html', note: 'A production system exposing sets, maps, counters, registers and flags built on these ideas.' },
    { kind: 'Paper', title: 'Dynamo: Amazon’s Highly Available Key-value Store', url: 'https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf', note: 'The shopping cart, version vectors and siblings that motivated much of this work.' },
  ],
};
