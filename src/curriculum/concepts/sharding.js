export default [
  {
    term: 'Partitioning',
    aliases: ['sharding', 'shard'],
    summary: 'Splitting a data set between nodes so that each node stores and serves only part of it.',
    detail: `**Partitioning** (or **sharding**) assigns every key to one partition and every partition to a node, so capacity grows with the number of nodes. It is different from replication, which stores copies of the same data on several nodes; real systems usually do both.

The two common schemes are **hash partitioning** (owner chosen from a hash of the key: even spread, no ordered scans) and **range partitioning** (contiguous key ranges per node: ordered scans, but hot ranges must be split).

Example: with three nodes and \`hash(key) mod 3\`, a key hashing to 7 lives on node index 1. Every node computes the same answer, so any node can route a request.`,
    related: ['Request routing', 'Consistent hashing', 'Range partitioning'],
  },
  {
    term: 'Request routing',
    summary: 'Getting each request to the node that owns its key, by forwarding, a proxy, or a client that knows the map.',
    detail: `With partitioned data, a request may arrive anywhere. **Request routing** gets it to the owner. Three common designs: any node forwards to the owner and relays the reply (Cassandra coordinators); a routing tier or proxy does it (MongoDB's \`mongos\`); or clients learn the ownership map and go direct, with stale clients redirected (Redis Cluster's \`MOVED\` replies).

Forwarding costs at most two extra messages per request. Whatever the design, every router must agree on the owner, which is why ownership must be a function of the key and the membership alone.`,
    related: ['Partitioning', 'Membership'],
  },
  {
    term: 'Hash functions',
    aliases: ['hash function', 'hashing'],
    summary: 'A deterministic function that maps any string to a number in a fixed range, scattering similar inputs.',
    detail: `For partitioning, a **hash function** must be deterministic across nodes and runs (never a per-process randomised hash) and must mix well, so that \`user:1\` and \`user:2\` land far apart and keys spread evenly.

It need not be cryptographic. FNV-1a, MurmurHash and xxHash are typical. Example: a 32-bit hash maps every key to 0 … 4,294,967,295, which can index a node list with \`mod N\` or place the key on a hash ring.`,
    related: ['Partitioning', 'Hash ring'],
  },
  {
    term: 'Consistent hashing',
    summary: 'A partitioning scheme where adding or removing a node moves only the keys next to it, about 1/N of them.',
    detail: `With \`hash(key) mod N\`, changing N reassigns almost every key: going from six nodes to five moves about five keys in six. **Consistent hashing** places nodes and keys on a hash ring and gives each key to the next node clockwise, so a join or leave moves only the keys on one arc.

Introduced by Karger et al. in 1997 for web caches, it underlies Dynamo, Cassandra, Riak and many load balancers. Plain consistent hashing balances poorly with few nodes, so it is almost always used with virtual nodes. Rendezvous hashing and jump consistent hash achieve the same minimal movement without a ring.`,
    related: ['Hash ring', 'Virtual nodes', 'Rebalancing'],
  },
  {
    term: 'Hash ring',
    summary: 'The hash range drawn as a circle, with nodes at hashed positions; a key belongs to the next node clockwise.',
    detail: `The **hash ring** treats 0 … 2^32 − 1 as a circle where the top wraps to 0. Each node sits at \`hash(nodeId)\`, each key at \`hash(key)\`. The owner is the first node at or after the key's position, wrapping past the largest position to the smallest.

Implementation: keep the points in an array sorted by position (ties broken by id, so the order is deterministic), and binary-search for the first point with \`pos >= hash(key)\`; if none, take index 0.

Example: nodes at 20, 55 and 85 (on a 0–99 scale). A key at 30 belongs to the node at 55; a key at 92 wraps to the node at 20.`,
    related: ['Consistent hashing', 'Virtual nodes'],
  },
  {
    term: 'Membership',
    aliases: ['cluster membership', 'member list'],
    summary: 'The current set of nodes in a cluster, or the set that owns data; everyone must agree on it to agree on owners.',
    detail: `**Membership** is the list of nodes currently taking part. In a sharded store the owner of each key is computed from it, so nodes with different member lists route the same key to different owners.

Membership changes when nodes join, leave or fail. Real systems either gossip it (Cassandra, Consul's Serf) and tolerate brief disagreement, or keep it in a consensus store (etcd, ZooKeeper) and version every change with an epoch so that late updates cannot undo newer ones.`,
    related: ['Epochs', 'Consistent hashing', 'Rebalancing'],
  },
  {
    term: 'Virtual nodes',
    aliases: ['vnodes', 'virtual node', 'tokens'],
    summary: 'Placing each physical node at many points on the hash ring so that its share of keys evens out.',
    detail: `With one point per node, arc lengths vary wildly: one of six nodes can own 40% of the keys and another 2%. **Virtual nodes** put each node at many positions, for example \`hash(id + "#" + i)\` for i in 0 … 199. A node's share is the sum of many small arcs, whose relative spread shrinks roughly as 1/√V.

They also spread rebalancing: a leaving node's many small arcs fall to many different neighbours instead of one. The costs are a larger ring to store, sort and search, and more ranges to track. Dynamo introduced them for load balance; Cassandra calls them tokens (\`num_tokens\`).`,
    related: ['Hash ring', 'Load balance', 'Variance'],
  },
  {
    term: 'Load balance',
    aliases: ['load balancing across shards'],
    summary: 'How evenly work and data are spread across nodes; the most loaded node limits the whole cluster.',
    detail: `A cluster is only as fast as its busiest node, so **load balance** is measured by the maximum share, not the average. If one of six nodes owns 40% of the keys, the cluster saturates when that node does, with the other five mostly idle.

In partitioning, balance has two parts: how evenly keys are assigned (fixed by virtual nodes or fixed slots) and how evenly requests fall on keys (not fixed by any hash: a single hot key overloads its owner). Example target from the virtual-nodes task: every member within 0.7× to 1.3× of its fair share.`,
    related: ['Virtual nodes', 'Variance'],
  },
  {
    term: 'Variance',
    summary: 'How far individual values scatter around their average; for ring shares it shrinks as points per node grow.',
    detail: `**Variance** measures spread: the average squared distance from the mean. Its square root, the standard deviation, is in the same units as the data.

For a hash ring, a node's share is a sum of V random arc lengths. Summing V independent pieces makes the relative standard deviation fall roughly as 1/√V: about 100% for one point, about 10% for 100 points, about 7% for 200. Across several nodes the worst share is a couple of standard deviations out, which is why 100 points per node is borderline for a ±30% limit and 150–200 is comfortable.`,
    related: ['Virtual nodes', 'Load balance'],
  },
  {
    term: 'Rebalancing',
    summary: 'Moving data between nodes after a membership change so each key ends up on its new owner.',
    detail: `When the membership changes, ownership changes at once but the data does not. **Rebalancing** copies each misplaced key to its new owner and then removes it from the old one.

It must not lose values (delete only after the receiver confirms), must not revert values (an old copy must not overwrite a newer write), and costs network and disk bandwidth that competes with clients. Systems throttle it, spread it with virtual nodes, or move fixed units such as Redis Cluster's hash slots one at a time.`,
    related: ['Handoff', 'Acknowledged transfer', 'Consistent hashing'],
  },
  {
    term: 'Handoff',
    summary: 'Sending keys a node no longer owns to their new owner, as part of rebalancing.',
    detail: `A **handoff** batches the misplaced keys per new owner, for example \`{ "type": "handoff", "entries": [["user:1003", 4]] }\`, and sends them. The sender deletes its copies only after the receiver acknowledges, and a timer retries until every misplaced key is gone.

The receiver must not let a handed-off value overwrite a newer one: when changes overlap, both sides compare versions and keep the higher. Dynamo's *hinted* handoff is related but different: a stand-in node holds writes for a temporarily unreachable replica and hands them back when it returns.`,
    related: ['Rebalancing', 'Acknowledged transfer', 'Versioned values'],
  },
  {
    term: 'Acknowledged transfer',
    summary: 'Moving data so that the sender keeps its copy until the receiver confirms it has stored it.',
    detail: `In an **acknowledged transfer** the sender deletes nothing until a reply arrives, and resends on a timer while none has. At every moment the data exists on at least one node, so a lost message or a lost reply costs only a retry.

The price is that, briefly, two nodes hold the key. That is harmless as long as only the owner serves it and the receiver merges duplicates idempotently (by version, or by "keep what is already here" when no newer write can exist).`,
    related: ['Handoff', 'Rebalancing'],
  },
  {
    term: 'Epochs',
    aliases: ['epoch', 'configuration epoch'],
    summary: 'A number that increases with every configuration change, so nodes can tell newer configurations from older ones.',
    detail: `An **epoch** labels a configuration (here, a member list). A node keeps the highest epoch it has seen and ignores any configuration whose epoch is not newer, so a delayed message cannot undo a later change. Putting the epoch and member list on every message lets a node that missed a change catch up from any peer.

Epochs must be issued by a single source of truth, or two different configurations can share a number. The same idea appears as Raft's terms, ZooKeeper's epochs, Kafka's controller and leader epochs, and Redis Cluster's configuration epochs.`,
    related: ['Membership', 'Versioned values', 'Fencing'],
  },
  {
    term: 'Versioned values',
    aliases: ['version numbers'],
    summary: 'Values stored with a version number so that, when copies meet, the newer one always wins.',
    detail: `Storing \`{ value, version }\` instead of a bare value lets any two copies of a key be ordered. If the owner assigns versions as \`epoch × 1,000,000 + counter\`, a write accepted in a later epoch beats any earlier one, and writes within an epoch are ordered by the owner's counter.

Merging by maximum version is commutative, associative and idempotent, so handoffs and collected copies may arrive in any order, any number of times, and the result is the same. This is the single-writer form; with concurrent writers, systems use vector clocks or last-writer-wins timestamps instead.`,
    related: ['Epochs', 'Handoff'],
  },
  {
    term: 'Fencing',
    aliases: ['fencing token', 'fencing tokens'],
    summary: 'Making sure an old owner or leader can no longer act before a new one takes over.',
    detail: `**Fencing** prevents two nodes from both acting as owner. In the sharding task the new owner of a key in epoch e asks every other node to adopt epoch e before it serves; a node that has adopted it forwards, rather than accepts, writes for that key. Only after everyone has answered does the new owner serve.

The general form is a **fencing token**: a number that increases with each change of owner, attached to every request, and checked by the resource, which rejects anything older than the highest token it has seen. Raft terms, ZooKeeper's zxid-based epochs and Kafka's leader epochs all fence stale leaders this way.`,
    related: ['Epochs', 'Split brain'],
  },
  {
    term: 'Range partitioning',
    summary: 'Assigning contiguous ranges of keys to nodes, which keeps ordered scans cheap but needs splitting of hot ranges.',
    detail: `In **range partitioning** each node owns one or more key ranges, such as \`[a, f)\` and \`[f, m)\`. A scan over neighbouring keys touches one or a few nodes, where with hash partitioning it would touch all of them.

Ranges are split when they grow and moved to balance load. Sequential keys (timestamps, auto-increment ids) all land in the last range, creating a hot spot. Bigtable, HBase, CockroachDB and TiKV partition by range.`,
    related: ['Partitioning', 'Consistent hashing'],
  },
  {
    term: 'Range scans',
    aliases: ['range scan'],
    summary: 'Reading every key between two bounds, in order; cheap when neighbouring keys are stored together.',
    detail: `A **range scan** asks for all keys with \`from <= key < to\`, sorted. With range partitioning the keys of one range sit on one node, so a scan touches only the nodes whose ranges overlap the bounds: a scan of \`k0100\`..\`k0108\` usually asks one node.

With hash partitioning neighbouring keys are scattered, so the same scan must ask every node and merge the answers. This is the main reason ordered stores such as Bigtable, HBase and CockroachDB partition by range.`,
    related: ['Range partitioning', 'Scatter-gather'],
  },
  {
    term: 'Dynamic splitting',
    aliases: ['range splitting', 'splitting'],
    summary: 'Creating partitions as data arrives: a range that grows past a size limit is split in two, and one half may move.',
    detail: `Key distributions are rarely known in advance, so range-partitioned systems start with few ranges and **split** a range once it holds too much (HBase regions at 10 GB by default, CockroachDB ranges at 512 MiB). The split point is usually the middle key, and the new half can be handed to a less loaded node.

Every router must learn about the split. If splits only ever add boundaries and never move them, announcements commute: a node can apply them in any order and still reach the same table. Until it hears, a stale node forwards to the old owner, which forwards on.`,
    related: ['Range partitioning', 'Rebalancing'],
  },
  {
    term: 'Hot spots',
    aliases: ['skewed workload', 'celebrity problem'],
    summary: 'A partition that gets far more requests than the others, usually because a few keys are far more popular.',
    detail: `Partitioning balances **keys**, not **requests**. If one key gets most of the traffic (a celebrity's post, a viral product), its owner saturates while the other nodes idle, and adding nodes changes nothing. Range partitioning adds its own hot spot: monotonically increasing keys such as timestamps all land in the last range.

Remedies: split a hot range further, or spread a hot key over several nodes with **key salting**, at the cost of more expensive reads. Detecting the hot key, usually on the owner by counting requests per key, keeps the cost to the keys that need it.`,
    related: ['Key salting', 'Load balance', 'Hot keys'],
  },
  {
    term: 'Key salting',
    aliases: ['salting'],
    summary: 'Spreading one hot key over several sub-keys on different nodes; writes pick one, reads combine them all.',
    detail: `To salt a key, append a small number to it: \`post:7#0\` .. \`post:7#4\`. A write goes to one sub-key chosen at random, so five nodes share the load. A read must fetch every sub-key and combine them, which works for values that merge, such as counters (sum) or sets (union).

Reads become fan-outs, so salting is applied only to keys known to be hot, and the system must track which keys are salted. A read through a node that has not heard yet must still find every part; letting the owner decide, and never un-salting, is one simple way to guarantee it.`,
    related: ['Hot spots', 'Scatter-gather'],
  },
  {
    term: 'Scatter-gather',
    aliases: ['fan-out read'],
    summary: 'Sending one query to many partitions in parallel and merging their answers.',
    detail: `A **scatter-gather** query is sent to every partition that might hold part of the answer; the coordinator waits for all of them and merges the results. Its latency is that of the slowest partition (tail latency amplification), and its cost grows with the number of partitions.

It is how local secondary indexes are queried (Elasticsearch, MongoDB), how hash-partitioned stores answer range scans, and how salted keys are read. Designs such as range partitioning and global indexes exist largely to avoid it.`,
    related: ['Secondary index', 'Key salting'],
  },
  {
    term: 'Secondary index',
    summary: 'A lookup structure from a non-key attribute (like color) to the keys of the records that have it.',
    detail: `Records partitioned by primary key can be found by key on one node, but "all red cars" needs a **secondary index**. A **local** (document-partitioned) index lives beside each partition's records and indexes only them, so writes stay on one node and queries scatter to all. A **global** (term-partitioned) index puts all entries for one term on one node, so a query asks one node but a write may have to update an index on another.

Example: \`car:117\` lives on n2; the entry red → car:117 lives on whichever node owns the term red.`,
    related: ['Term-partitioned index', 'Scatter-gather', 'Asynchronous index updates'],
  },
  {
    term: 'Term-partitioned index',
    aliases: ['global secondary index', 'global index'],
    summary: 'A secondary index partitioned by the indexed value, so all entries for one value live on one node.',
    detail: `In a **term-partitioned** (global) index, the index entries are partitioned by term, for example \`hash("color:red") % N\`, independently of where the records are. A query for one term reads one partition; a range of terms can be read if the index is range-partitioned.

The price is on writes: changing a record's color means updating the index partitions of the old and new color, often on two other nodes. DynamoDB global secondary indexes and Cassandra materialized views work this way, and update asynchronously.`,
    related: ['Secondary index', 'Asynchronous index updates'],
  },
  {
    term: 'Asynchronous index updates',
    aliases: ['async index'],
    summary: 'Acknowledging a write before the indexes it affects are updated, so the index briefly lags the data.',
    detail: `Updating a global index synchronously would make every write a distributed transaction. Most systems instead acknowledge the write and send the index updates afterwards, so a query may briefly miss a new record or return one that no longer matches. A useful contract bounds the lag: "within 250ms of an acknowledged write, the index reflects it".

The updates for one record travel separately and can be reordered. Stamping each with the record's version, keeping removals as versioned tombstones, and applying an update only if it is newer makes the final state independent of arrival order.`,
    related: ['Term-partitioned index', 'Versioned values'],
  },
];
