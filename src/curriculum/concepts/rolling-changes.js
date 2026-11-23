export default [
  {
    term: 'Rolling upgrade',
    aliases: ['rolling update', 'rolling deploy', 'rolling restart'],
    summary: 'Upgrading a cluster one node at a time, so the service stays up throughout.',
    detail: `In a **rolling upgrade**, each node in turn is drained (no new traffic), stopped, given the new version, restarted and re-admitted, before the next node starts. At any moment all but one node are serving.

The cost is a mixed-version period: with three nodes upgraded 800 ms apart, old and new code run side by side for about 1.6 seconds, and in production often for hours. Every message and stored record must work for both versions during that time.

Kubernetes Deployments roll out pods this way by default, bounded by \`maxUnavailable\` and \`maxSurge\`; databases such as Cassandra and Kafka document rolling-upgrade procedures with compatibility settings for the interim.`,
    related: ['Mixed-version cluster', 'Forward compatibility', 'Rollback'],
  },
  {
    term: 'Mixed-version cluster',
    aliases: ['mixed-version', 'mixed version cluster'],
    summary: 'A cluster in which nodes run different software versions at the same time, as during a rolling upgrade.',
    detail: `During a rolling upgrade, or after a partial rollback, a **mixed-version cluster** has old and new nodes talking to each other. With five nodes upgraded in random order, every pairing occurs: old to new, new to old, new to new.

Code must be correct for all of them. That rules out changes the other version cannot understand, such as removing a field it reads or sending a message type it has no handler for, unless the sender first learns, by negotiation, what each peer runs.

Most projects support exactly two adjacent versions at a time, which keeps the number of pairings to test small.`,
    related: ['Rolling upgrade', 'Version negotiation', 'Forward compatibility'],
  },
  {
    term: 'Forward compatibility',
    aliases: ['forward compatible', 'forward-compatible'],
    summary: 'Old code can correctly handle data or messages produced by newer code.',
    detail: `**Forward compatibility** means version 1 copes with what version 2 writes. It is the hard direction, because version 1 is already deployed and cannot be changed to anticipate version 2; version 2 has to stay within what version 1 already tolerates.

In practice this means: add fields but never remove, rename or retype one that old code reads; keep message types; and rely on old readers ignoring unknown fields. If version 2 sends \`fwd_put\` with an extra \`by\` field, a tolerant version-1 reader stores the name and ignores \`by\`.`,
    related: ['Backward compatibility', 'Tolerant reader'],
  },
  {
    term: 'Backward compatibility',
    aliases: ['backward compatible', 'backwards compatibility'],
    summary: 'New code can correctly handle data or messages produced by older code.',
    detail: `**Backward compatibility** means version 2 copes with what version 1 writes: old messages from nodes not yet upgraded, and old records already on disk.

It is the easier direction, because version 2 is the code you are writing. The key rule is to treat a missing field as unknown rather than guessing: a \`fwd_put\` from version 1 has no \`by\`, so the writer should be stored as \`null\`, not left as whatever it was before.`,
    related: ['Forward compatibility', 'Tolerant reader'],
  },
  {
    term: 'Tolerant reader',
    aliases: ['tolerant readers', 'ignore unknown fields'],
    summary: 'A reader that uses only the fields it needs and ignores everything else in a message.',
    detail: `A **tolerant reader** takes what it understands from a message and ignores the rest. It does not reject unknown fields, and it does not depend on fields it does not use.

That makes adding fields safe: a new version can send \`{ key, name, by }\` to an old reader that only knows \`{ key, name }\`. Removing or renaming a field, changing its type, or introducing a new message type still breaks it.

Protocol Buffers and Avro build this in: unknown fields are skipped by older parsers. Martin Fowler's "TolerantReader" essay named the idea for service integration.`,
    related: ['Forward compatibility', 'Backward compatibility'],
  },
  {
    term: 'Version negotiation',
    aliases: ['protocol negotiation', 'api version negotiation'],
    summary: 'Two parties agree on which protocol version both understand before using anything new.',
    detail: `With **version negotiation**, a sender uses a new message format only after learning that the receiver understands it. Otherwise it falls back to the old format.

Kafka clients send an \`ApiVersions\` request on each new connection and then use the highest version both sides support; TLS and HTTP/2 (via ALPN) negotiate during their handshakes.

In a cluster, knowledge about peers must be gathered in both directions, since peers upgrade after you as well as before, and must be forgotten on restart or when a peer might have been downgraded.`,
    related: ['Handshake', 'Mixed-version cluster'],
  },
  {
    term: 'Handshake',
    aliases: ['hello handshake', 'hello message'],
    summary: 'An exchange at the start of a conversation in which each side announces what it supports.',
    detail: `A **handshake** is a short exchange before real traffic. In the negotiation task, a node that starts on version 2 sends each peer \`{ "type": "hello", "version": 2 }\`. A version-2 peer replies; a version-1 peer has no handler and stays silent, so the call times out and the peer is treated as old.

Receiving a hello is evidence too: the sender runs version 2. Without that, the first node to upgrade would never learn about the nodes that upgrade after it.

TLS, SSH and many database wire protocols begin with a handshake that settles versions and options before any data flows.`,
    related: ['Version negotiation'],
  },
  {
    term: 'Schema migration',
    aliases: ['data migration', 'migration'],
    summary: 'Changing the shape of stored data while the system keeps running.',
    detail: `A **schema migration** changes how data is stored: splitting a field, renaming a column, changing a type. Unlike messages, stored data outlives the code that wrote it, so a record written by version 2 may be read by version 1 after a rollback.

Safe migrations are done in steps, the expand–contract pattern: add the new shape alongside the old, keep both current, convert existing data, and remove the old shape only once no code that needs it can run again.

Tools such as Flyway, Liquibase, Rails migrations and online schema change tools for MySQL automate the steps; they do not remove the need to order them correctly.`,
    related: ['Expand–contract', 'Dual writes', 'Rollback'],
  },
  {
    term: 'Expand–contract',
    aliases: ['expand-contract', 'expand and contract', 'parallel change', 'expand, migrate, contract'],
    summary: 'Change data or interfaces in three steps: add the new form, migrate to it, then remove the old form.',
    detail: `**Expand–contract** (also called parallel change) makes an incompatible change as a series of compatible ones.

1. **Expand**: add the new shape next to the old. Version 2 writes \`{ name, first, last }\`; version 1 still finds \`name\`.
2. **Migrate**: once every node runs the new code, convert all existing data.
3. **Contract**: once rollback is impossible, drop the old shape: records become \`{ first, last }\`.

Until the contract, the old field is the source of truth, since a rolled-back node may have updated it and left the new fields stale. The contract is a one-way door, so it should be an explicit, recorded step.`,
    related: ['Schema migration', 'Dual writes', 'Rollback'],
  },
  {
    term: 'Dual writes',
    aliases: ['dual write', 'double write'],
    summary: 'Writing every change to both the old and the new representation during a migration.',
    detail: `During the expand phase of a migration, code performs **dual writes**: each update sets both the old field and the new ones, so that either version of the code can read the record.

The risk is divergence. If one writer updates only the old field (as a rolled-back version 1 does), the two copies disagree, and readers must know which one to trust. Dual writes to two *separate systems* (say a database and a search index) are worse, since one write can succeed and the other fail; change-data-capture or an outbox table is the usual remedy there.`,
    related: ['Expand–contract', 'Schema migration'],
  },
  {
    term: 'Rollback',
    aliases: ['roll back', 'rolled back', 'downgrade'],
    summary: 'Returning a node or a cluster to the previous software version after a problem with the new one.',
    detail: `A deployment **rollback** restarts nodes on the old version. It is the main escape hatch when a release misbehaves, and it only works if the old version can still read everything the new version wrote while it ran.

That is why stored data changes use expand–contract: until the contract step, new code keeps writing the old shape too. After it, rollback to the old version is no longer possible, which is why the contract is done only once the new version is known to be good.

Kubernetes offers \`kubectl rollout undo\`; feature flags provide a faster, narrower kind of rollback by switching code paths off without a deploy.`,
    related: ['Expand–contract', 'Rolling upgrade', 'Automatic rollback'],
  },
  {
    term: 'Feature flags',
    aliases: ['feature flag', 'feature toggles', 'feature toggle', 'flags'],
    summary: 'Runtime switches that turn code paths on or off without deploying new code.',
    detail: `A **feature flag** guards a new code path behind a condition that can be changed at runtime. The code is deployed switched off, so the deploy changes nothing; the flag is then turned on for some nodes or users, and off again if needed.

This separates *deploy* (code on machines) from *release* (behaviour users see), and enables canaries, percentage rollouts and kill switches. The node should check the flag on every request so changes take effect immediately, and store it durably so a restart keeps it.

Flags carry costs: each doubles the number of possible code paths, and stale flags linger as dead code. Services such as LaunchDarkly or Unleash, and in-house systems, manage them centrally.`,
    related: ['Canary release', 'Kill switch', 'Automatic rollback'],
  },
  {
    term: 'Canary release',
    aliases: ['canary', 'canary deployment'],
    summary: 'Turning a change on for a small part of the system first, to catch problems before they reach everyone.',
    detail: `In a **canary release**, a new version or feature serves a small slice of traffic: one node out of three, or 1% of users. If its errors or latency look worse than the rest, it is switched off; if not, it is promoted to everyone.

The name comes from canaries carried into coal mines as an early warning. A canary limits the blast radius of a bad change, but only if someone (or something) compares its health with the baseline and acts.`,
    related: ['Feature flags', 'Kill switch', 'Automatic rollback'],
  },
  {
    term: 'Kill switch',
    aliases: ['killswitch'],
    summary: 'A flag that turns a feature off immediately, without a deploy, when it misbehaves.',
    detail: `A **kill switch** is a feature flag used defensively: when the new path causes errors, turning the flag off restores the old behaviour in milliseconds. It is much faster than a rollback deploy and affects only the one feature.

It works only if the off path still works and the data written while the flag was on is still readable with it off. A kill switch that is never exercised may not work when it is needed.`,
    related: ['Feature flags', 'Automatic rollback'],
  },
  {
    term: 'Automatic rollback',
    aliases: ['auto rollback', 'automated rollback', 'self-disabling flag'],
    summary: 'The system watches a new feature\'s health and switches it off by itself when it crosses a failure threshold.',
    detail: `With **automatic rollback**, no human needs to notice a bad release. In the flags task, each node keeps the outcomes of its last 20 reads from a new cache; as soon as 5 are failures, it turns its own flag off and stores that decision so a restart keeps it.

The threshold must sit above normal noise: a healthy cache failing one read in seven averages about 3 failures in 20, so a threshold of 5 does not trip, while a cache failing every other read trips it within about 10 reads.

Deployment tools apply the same idea at larger scale: progressive delivery systems such as Argo Rollouts or Flagger compare a canary's metrics with the baseline and abort the rollout automatically.`,
    related: ['Kill switch', 'Canary release', 'Feature flags'],
  },
];
