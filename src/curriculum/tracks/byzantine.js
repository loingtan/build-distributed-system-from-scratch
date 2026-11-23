// Track: Byzantine faults. See docs/authoring.md for the shape of this file.
import { bftBroadcast, bftVotes, bftPbft } from '../../sim/workloads/byzantine.js';

export default {
  id: 'byzantine',
  code: 'BZ',
  category: 'consensus',
  title: 'Byzantine faults',
  level: 'Advanced',
  blurb: 'When nodes lie: signed messages, equivocation, why you need 3f+1 nodes, and a small PBFT-style agreement round.',
  subtracks: [
    {
      title: 'Tolerating liars',
      tasks: [
        {
          slug: 'bft-reliable-broadcast',
          title: 'Echo, ready, deliver',
          level: 'Medium',
          minutes: 30,
          summary: 'Broadcast a value so that honest nodes all deliver the same thing, or nothing, even when the sender lies.',
          concepts: ['Byzantine fault', 'equivocation', 'reliable broadcast', 'echo and ready'],
          workload: bftBroadcast,
          tests: [
            { name: 'Silent traitor', blurb: 'Three honest nodes each broadcast a value while n4 sends nothing at all.', nodeCount: 4, duration: 2500, params: { scenario: 'silent' } },
            { name: 'Lying traitor', blurb: 'n4 echoes and readies forged values for everyone and proposes its own broadcast to n1 only.', nodeCount: 4, duration: 2500, params: { scenario: 'liar' } },
            { name: 'Sender splits two ways', blurb: 'The sender n1 tells n2 "apple" and n3, n4 "cherry" in every message it sends.', nodeCount: 4, duration: 2500, params: { scenario: 'split' } },
            { name: 'Sender helps only one node', blurb: 'n1 proposes "apple" to n2, n3 and "cherry" to n4, and sends its ready to n2 alone.', nodeCount: 4, duration: 2500, params: { scenario: 'totality' } },
          ],
          hints: [
            'Run "Sender splits two ways" with a node that delivers whatever the sender proposes and open the Timeline: n2 delivers apple, n3 and n4 cherry. The sender alone must never be enough.',
            'Echo the first `propose` from each sender to everyone. Keep, per sender and per value, the *set* of nodes that echoed it; a node echoing twice still counts once.',
            'Send `ready` (once per sender) when 2f+1 = 3 nodes echoed the same value. Deliver when 3 nodes sent `ready` for the same value.',
            'If only n2 gets three readies, n3 and n4 are stuck at two. Two readies for a value mean at least one honest node is ready, so join in: send your own `ready` on f+1 = 2 matching readies.',
            'The sender is `msg.src` of the `propose`, which the network sets. Never take it from the body.',
          ],
        },
        {
          slug: 'bft-quorum-cert',
          title: 'Signed votes and 3f+1',
          level: 'Medium',
          minutes: 30,
          summary: 'Decide only on 2f+1 signed votes, pass the proof on, and see why three nodes cannot outvote one liar.',
          concepts: ['digital signatures', 'quorum certificate', '3f+1'],
          workload: bftVotes,
          tests: [
            { name: 'Silent traitor', blurb: 'n1, n2, n3 all vote blue; n4 says nothing.', nodeCount: 4, duration: 2000, params: { scenario: 'silent' } },
            { name: 'Traitor votes both ways', blurb: 'Inputs blue, blue, red; n4 votes blue to n1 and red to n3, and sends n2 a blue vote with a bad signature.', nodeCount: 4, duration: 2000, params: { scenario: 'both-ways' } },
            { name: 'Forged certificates', blurb: 'Inputs blue, red, red; n4 sends n1 and n2 fake certificates for "evil" and n3 a genuine one for red.', nodeCount: 4, duration: 2000, params: { scenario: 'forged' } },
            { name: 'Three nodes, one liar', blurb: 'Only n = 3 nodes for f = 1. Inputs blue and red; n3 votes blue to n1 and red to n2. Nobody may decide.', nodeCount: 3, duration: 2000, params: { scenario: 'three' } },
          ],
          hints: [
            'A vote counts only if `verify(msg.src, "vote:" + value, sig)` holds, and only the first valid vote from each node counts.',
            'Decide when 2f+1 = 3 distinct nodes voted for the same value. Your certificate is exactly those three votes, as `{ voter, value, sig }` objects.',
            'Send `{ type: "decided", value, cert }` to your peers when you decide. On receiving one, check every entry: a known voter, the same value, a signature that verifies, and three *distinct* voters.',
            'In "Three nodes, one liar", any rule that lets two nodes decide (n - f = 2) lets n1 and n2 decide different values, because their only common member is the traitor. Requiring 3 matching votes keeps you safe and leaves you undecided, which is the right outcome.',
          ],
        },
        {
          slug: 'bft-pbft',
          title: 'A PBFT round',
          level: 'Hard',
          minutes: 40,
          summary: 'Run pre-prepare, prepare and commit with 2f+1 quorums so four replicas agree on one client request, whoever lies.',
          concepts: ['PBFT', 'primary and backups', 'prepare and commit quorums', 'quorum intersection'],
          workload: bftPbft,
          tests: [
            { name: 'Honest primary, silent backup', blurb: 'The client sends one signed request to n1; n4 says nothing.', nodeCount: 4, duration: 2000, params: { scenario: 'silent' } },
            { name: 'Honest primary, lying backup', blurb: 'n4 sends n2 a pre_prepare of its own before the primary\'s arrives, and lies in every prepare and commit.', nodeCount: 4, duration: 2000, params: { scenario: 'liar' } },
            { name: 'Equivocating primary', blurb: 'Two signed requests reach n1; it pre-prepares one to n2 and the other to n3 and n4, and backs both sides. n2 hears from n3 and n4 late.', nodeCount: 4, duration: 2000, params: { scenario: 'equivocate' } },
            { name: 'Primary forges a request', blurb: 'n1 pre-prepares a value the client never signed to n2 and n3, and the real request to n4.', nodeCount: 4, duration: 2000, params: { scenario: 'forge' } },
          ],
          hints: [
            'Accept a `pre_prepare` only if it comes from the primary (`msg.src === "n1"`), it is the first one you accept, and `verify("client", "request:" + value, client_sig)` holds.',
            'Count prepares and commits per value, as sets of sender ids. A prepare for another value is not support for yours.',
            'Send `commit` once you have accepted a value and hold 3 prepares for that same value. Decide on 3 commits for one value, even if you never accepted a pre_prepare for it.',
            'In "Equivocating primary", n2 sees two prepares for its value (its own and the primary\'s). With a threshold of f+1 = 2 it would commit, and with two commits it would decide differently from n3 and n4.',
            'Nobody is required to decide when the primary lies. Real PBFT would then change view (elect the next primary); this task stops at safety.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Counting f+1 matching messages as enough',
      symptom: 'Passes with a silent traitor; with a traitor that backs each side, two honest nodes decide different values.',
      fix: 'f+1 only proves that one honest node is involved. To decide, wait for 2f+1: any two such groups among 3f+1 nodes share an honest node.',
    },
    {
      title: 'Trusting a field instead of the network',
      symptom: 'A traitor delivers or votes "for" another node by writing its name into the body.',
      fix: 'The network sets msg.src and nobody can fake it. Take the sender of a propose or a vote from msg.src, and the author of a forwarded statement from a signature you checked.',
    },
    {
      title: 'Accepting a certificate without checking each entry',
      symptom: 'A node decides "evil" because a peer sent a list of three votes, all signed by the same traitor or with signatures that do not verify.',
      fix: 'Verify every signature against the claimed voter and value, and count distinct voters only.',
    },
    {
      title: 'Using n - f as the quorum without checking n',
      symptom: 'Works with four nodes; with three nodes and one liar, two honest nodes decide different values.',
      fix: 'Quorums must overlap in at least f+1 nodes. With n = 3f+1, n - f = 2f+1 does; with fewer nodes nothing can be both safe and live.',
    },
  ],
  questions: [
    {
      q: 'Why do Byzantine protocols need 3f+1 nodes when crash-tolerant ones such as Raft need only 2f+1?',
      a: 'A node can only wait for n - f replies, because f nodes may never answer. Two such groups overlap in n - 2f nodes. With crashes, one node in the overlap is enough. With liars, f of the overlap may be lying, so the overlap must hold f+1 nodes: n - 2f >= f+1, which is n >= 3f+1.',
    },
    {
      q: 'In reliable broadcast, why send ready after f+1 readies, when 2f+1 echoes were needed for the first ready?',
      a: 'f+1 readies for a value include at least one honest node, and an honest node only sends ready for a value that can be delivered. Joining in is safe, and it ensures that once any honest node delivers, every honest node eventually collects 2f+1 readies too.',
    },
    {
      q: 'If the single-shot round already needs 2f+1 matching commits, what is the prepare phase for?',
      a: 'Within one view the prepare quorum makes sure no two values are both prepared. Its real job appears when the primary is replaced: a value that may have been decided was prepared by 2f+1 replicas, so any 2f+1 replicas reporting to the new primary include one that knows it, and the new view must keep it.',
    },
  ],
  reading: [
    {
      kind: 'Paper',
      title: 'Practical Byzantine Fault Tolerance (Castro and Liskov, 1999)',
      url: 'https://web.archive.org/web/2024/https://pmg.csail.mit.edu/papers/osdi99.pdf',
      note: 'The protocol behind the third task: pre-prepare, prepare, commit, and the view change this track leaves out.',
    },
    {
      kind: 'Paper',
      title: 'The Byzantine Generals Problem (Lamport, Shostak and Pease, 1982)',
      url: 'https://lamport.azurewebsites.net/pubs/byz.pdf',
      note: 'Where the name and the 3f+1 bound come from, with the three-generals argument.',
    },
    {
      kind: 'Blog',
      title: 'Bracha broadcast, explained (Decentralized Thoughts)',
      url: 'https://decentralizedthoughts.github.io/2020-09-19-living-with-asynchrony-brachas-reliable-broadcast/',
      note: 'A short proof of why echo and ready give agreement and totality.',
    },
  ],
};
