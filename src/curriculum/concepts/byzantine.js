export default [
  {
    term: 'Byzantine fault',
    aliases: ['Byzantine failure', 'Byzantine node', 'Byzantine fault tolerance', 'BFT'],
    summary: 'A node that misbehaves arbitrarily: it may lie, send conflicting messages, stay silent or collude, not just crash.',
    detail: `A **Byzantine fault** is any deviation from the protocol. A crashed node simply stops; a Byzantine node keeps talking and may say anything: a wrong value, different values to different peers, or nothing at the worst moment. The cause can be a bug, corrupted memory or disk, a misconfigured replica, or an attacker who controls the machine.

A **Byzantine fault tolerant** (BFT) protocol keeps its guarantees as long as at most **f** nodes are Byzantine. It assumes nothing about what those f nodes do, so it must hold even against the cleverest possible liar. In this track the tests play that liar by rewriting the messages of one node out of four.

The name comes from Lamport, Shostak and Pease's 1982 paper, which framed the problem as Byzantine generals who must agree on a plan although some of them are traitors.`,
    related: ['Equivocation', '3f+1', 'Authenticated channels'],
  },
  {
    term: 'Equivocation',
    aliases: ['equivocate', 'equivocating'],
    summary: 'A faulty node telling different nodes different things, for example "apple" to n2 and "cherry" to n3.',
    detail: `**Equivocation** is the characteristic Byzantine move: a node sends conflicting statements to different peers, each of which looks perfectly valid on its own. n1 proposes "apple" to n2 and "cherry" to n3; neither n2 nor n3 can tell, from its own message, that anything is wrong.

Signatures do not prevent equivocation: the liar can sign both statements with its own key. They only make it *provable* once someone holds both. Protocols defeat it by making nodes compare notes before acting. In reliable broadcast honest nodes echo what they heard and wait for 2f+1 matching echoes; in PBFT replicas wait for 2f+1 matching prepares. Any two such groups share an honest node, which repeats only one of the conflicting statements.`,
    related: ['Byzantine fault', 'Reliable broadcast', 'Quorum intersection'],
  },
  {
    term: 'Reliable broadcast',
    aliases: ['Byzantine reliable broadcast', 'Bracha broadcast'],
    summary: 'Sending one value so that all honest nodes deliver the same value, or none delivers anything, even if the sender lies.',
    detail: `**Reliable broadcast** gives three guarantees among the honest nodes: if the sender is honest, everyone delivers its value (**validity**); no two honest nodes deliver different values (**agreement**); and if one honest node delivers, all eventually do (**totality**). It does not promise that a lying sender's broadcast is delivered at all.

Bracha's 1987 protocol achieves it with n = 3f+1 nodes and no signatures: propose, then echo, then ready, with thresholds of 2f+1 and f+1. With n = 4 and f = 1, a node sends ready after 3 matching echoes or 2 matching readies, and delivers after 3 matching readies.

Reliable broadcast is a building block, not consensus: different honest senders' broadcasts can be delivered in different orders. Many BFT systems (HoneyBadgerBFT, DAG-based protocols such as Narwhal) use it as their dissemination layer.`,
    related: ['Echo and ready', 'Equivocation'],
  },
  {
    term: 'Echo and ready',
    aliases: ['echo phase', 'ready phase', 'ready amplification'],
    summary: 'The two rounds of Bracha broadcast: echoing pins down a single value, readying makes delivery all-or-nothing.',
    detail: `In the **echo** round each node repeats, to everyone, the first value the sender proposed to it. A node that sees **2f+1** matching echoes knows that no other value can ever collect 2f+1 echoes, because any two such groups share an honest node, and honest nodes echo once.

In the **ready** round a node announces "this value is safe" and delivers on **2f+1** matching readies. The extra rule is **amplification**: on **f+1** matching readies, a node sends its own ready even without having seen enough echoes, because f+1 readies include at least one honest node. That rule is what turns "n2 delivered" into "everyone will deliver". Without it, a lying sender can let exactly one honest node collect enough readies and leave the others stuck at two.`,
    related: ['Reliable broadcast'],
  },
  {
    term: 'Digital signatures',
    aliases: ['signature', 'signatures', 'signed messages'],
    summary: 'A tag only the key holder can produce and anyone can check, so a statement can be forwarded without being forged.',
    detail: `A **digital signature** binds a statement to its author. n2 signs "vote:blue" with its private key; anyone holding n2's public key can check the result, but nobody else can produce it. A signed statement can therefore travel through untrusted relays: when n3 forwards n2's vote, n1 can verify that n2 really said it.

Signatures do not stop a node from lying in its own name, and they do not stop equivocation (the liar signs both versions). What they stop is putting words in an honest node's mouth. In this track the scheme is a toy (a 32-bit hash over a key and the text, with all keys in one table) and the tests promise to sign only with the traitor's key. Real systems use Ed25519, ECDSA or BLS signatures, or cheaper MACs on authenticated links where messages are never forwarded.`,
    related: ['Quorum certificate', 'Authenticated channels'],
  },
  {
    term: 'Quorum certificate',
    aliases: ['QC', 'certificate', 'commit certificate'],
    summary: 'A bundle of 2f+1 signed votes for the same value: a proof, checkable by anyone, that a quorum agreed.',
    detail: `A **quorum certificate** collects signed votes for one value from at least **2f+1** distinct nodes. Because each signature can be checked independently, the certificate is a self-contained proof: a node that missed the votes can verify the certificate and act on it, without trusting whoever sent it.

Checking one means checking every entry: a known voter, the same value, a signature that verifies, and counting distinct voters only. Three copies of the traitor's own vote are one vote, not three.

Certificates are central to modern BFT protocols. In HotStuff, used by the Diem/Libra project and its descendants, every block carries a certificate for its parent; in Tendermint, a block is committed with a set of precommit signatures from more than two thirds of the voting power.`,
    related: ['Digital signatures', '3f+1', 'Quorum intersection'],
  },
  {
    term: '3f+1',
    aliases: ['n >= 3f+1', 'n = 3f+1', '3f + 1'],
    summary: 'The minimum number of nodes needed to tolerate f Byzantine nodes in an asynchronous or partially synchronous network.',
    detail: `To make progress a node can wait for at most **n - f** replies, since f nodes may never answer. Two groups of n - f nodes overlap in n - 2f nodes. If up to f of the overlap may lie, safety needs at least one honest node left: n - 2f ≥ f + 1, so **n ≥ 3f + 1**. With n = 4 and f = 1, quorums of 3 overlap in 2 nodes, at least one honest.

With n = 3 and f = 1 there is no good quorum size: 2 lets two honest nodes decide different values because their only common member is the liar, and 3 means one silent node blocks everyone. The safe choice is to keep the 2f+1 threshold and accept that the system cannot decide.

Crash-tolerant protocols such as Raft need only n ≥ 2f + 1, because a node in the overlap may be slow but never lies. Signatures do not lower the 3f+1 bound when the network has no timing guarantees; they do in fully synchronous models, which real networks are not.`,
    related: ['Quorum intersection', 'Byzantine fault'],
  },
  {
    term: 'PBFT',
    aliases: ['Practical Byzantine Fault Tolerance'],
    summary: 'Castro and Liskov\'s 1999 protocol for replicating a service across 3f+1 replicas that tolerates f liars.',
    detail: `**PBFT** orders client requests through a **primary** and **backups**. In the normal case each request goes through three phases: the primary assigns it a sequence number in a **pre-prepare**, every replica broadcasts a **prepare**, and once a replica holds 2f+1 matching prepares it broadcasts a **commit**; 2f+1 matching commits make the request final and it is executed. Clients accept a result once f+1 replicas return the same reply.

If the primary is faulty or slow, replicas time out and run a **view change**, which makes the next replica primary and carries over every request that may have committed. The paper showed this costs only a few percent over an unreplicated NFS server, which made BFT look practical for the first time. Tendermint, HotStuff and many blockchain consensus engines descend from it.`,
    related: ['Primary and backups', 'Prepare and commit quorums', 'View change'],
  },
  {
    term: 'Primary and backups',
    aliases: ['primary', 'backups', 'leader-based BFT'],
    summary: 'One replica proposes the order of requests; the others check it and refuse to follow a proposal they cannot verify.',
    detail: `In PBFT one replica, the **primary**, proposes which request comes next; the **backups** do not trust it. A backup accepts a pre-prepare only if it really comes from the primary of the current view, it is the first one for that sequence number, and the request is validly signed by the client. Even so, the backups only proceed once 2f+1 of them saw the same proposal.

A faulty primary can therefore stall progress or equivocate, but it cannot make honest replicas disagree, and the client's signature stops it from inventing requests. The primary of view v is replica v mod n, so a view change hands the role to the next replica in turn.`,
    related: ['PBFT', 'View change'],
  },
  {
    term: 'Prepare and commit quorums',
    aliases: ['prepare phase', 'commit phase', 'pre-prepare', 'prepared'],
    summary: 'PBFT\'s two voting rounds: 2f+1 matching prepares pin a value within a view, 2f+1 matching commits make it final.',
    detail: `A replica is **prepared** for value v once it accepted the primary's pre-prepare for v and holds matching prepares from 2f+1 replicas. Quorum intersection means no honest replica can be prepared for a different value in the same view. A prepared replica broadcasts commit; 2f+1 matching commits mean that at least f+1 honest replicas are prepared for v, so v is decided.

Within a single view, one round of 2f+1 matching votes would already be safe. The second round exists for view changes: because a decided value was prepared at 2f+1 replicas, any 2f+1 replicas reporting to a new primary include an honest one that knows it, and the new view must re-propose it.`,
    related: ['PBFT', 'Quorum intersection', 'View change'],
  },
  {
    term: 'Quorum intersection',
    aliases: ['quorum overlap'],
    summary: 'Any two quorums share enough members; with liars, the overlap must contain at least one honest node.',
    detail: `Quorum-based protocols rely on one property: any two quorums **intersect**. If every decision needs a quorum and two quorums always share a node that behaves consistently, two conflicting decisions are impossible.

With crash faults, any shared node will do, so majorities suffice. With Byzantine faults the shared part may contain liars, so it must hold at least f + 1 nodes. With n = 3f + 1 and quorums of 2f + 1, two quorums share at least 2(2f+1) - (3f+1) = f + 1 nodes. With n = 4: two groups of 3 share 2 nodes, of which at most one is the traitor.`,
    related: ['3f+1', 'Quorum certificate', 'Prepare and commit quorums'],
  },
  {
    term: 'Authenticated channels',
    aliases: ['authenticated links'],
    summary: 'Links where the receiver knows for certain who sent each message, though not whether the sender told the truth.',
    detail: `On an **authenticated channel** a node can fake the contents of its own messages but not their origin. In this lab the network sets \`msg.src\`, so a traitor cannot pretend to be n2 when it talks to n3. Real systems get this from TLS or from MACs computed with a key shared by each pair of nodes.

Authenticated channels are enough for Bracha broadcast and for PBFT's normal case, because every statement is counted only by its direct receiver. They are not enough once statements are forwarded: "n2 voted blue", relayed by n4, needs n2's signature.`,
    related: ['Digital signatures', 'Byzantine fault'],
  },
  {
    term: 'View change',
    aliases: ['view', 'views'],
    summary: 'PBFT\'s way of replacing a faulty primary while keeping every request that might already have been decided.',
    detail: `A **view** is a period with one fixed primary. If backups stop seeing progress, they time out and vote to move to view v+1, whose primary is the next replica. Each sends the new primary its prepared certificates; the new primary waits for 2f+1 such messages and re-proposes every request that appears prepared in them.

Because a decided request was prepared by 2f+1 replicas, and any two groups of 2f+1 share an honest replica, the new primary always learns of it. The tasks in this track stop at a single view: with a lying primary nobody has to decide, which is exactly the situation a view change exists to resolve.`,
    related: ['PBFT', 'Primary and backups', 'Prepare and commit quorums'],
  },
];
