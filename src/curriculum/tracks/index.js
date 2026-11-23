// Tracks defined one per file. Order here is the order they are numbered in.
import replication from './replication.js';
import quorum from './quorum.js';
import twoPhaseCommit from './two-phase-commit.js';
import storageEngine from './storage-engine.js';
import sharding from './sharding.js';
import raftLog from './raft-log.js';
import queues from './queues.js';
import resilience from './resilience.js';
import linearizableKv from './linearizable-kv.js';
import loadBalancing from './load-balancing.js';
import caching from './caching.js';
import rollingChanges from './rolling-changes.js';
import paxos from './paxos.js';
import crdtCollections from './crdt-collections.js';
import membership from './membership.js';
import byzantine from './byzantine.js';
import isolation from './isolation.js';
import leases from './leases.js';
import streams from './streams.js';
import batch from './batch.js';

export default [replication, quorum, twoPhaseCommit, storageEngine, sharding, raftLog, queues, resilience, linearizableKv, loadBalancing, caching, rollingChanges, paxos, crdtCollections, membership, byzantine, isolation, leases, streams, batch];
