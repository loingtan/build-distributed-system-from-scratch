// Track: Gossip membership. See docs/authoring.md for the shape of this file.
import { swimProbe, swimIndirect, swimGossip, swimSuspicion } from '../../sim/workloads/membership.js';

export default {
  id: 'membership',
  code: 'SW',
  category: 'consensus',
  title: 'Gossip membership',
  level: 'Advanced',
  blurb: 'Keep track of who is in the cluster without a central list: SWIM-style probing, indirect pings, suspicion and incarnation numbers, spread by gossip.',
  subtracks: [
    {
      title: 'Detecting failures cheaply',
      tasks: [
        {
          slug: 'swim-probe',
          title: 'Probe one member at a time',
          level: 'Medium',
          minutes: 25,
          summary: 'Replace all-to-all heartbeats with one ping per period, so each node sends the same few messages per second however big the cluster is.',
          concepts: ['SWIM', 'protocol period', 'failure detector', 'message budget'],
          workload: swimProbe,
          tests: [
            { name: 'Five nodes, one crashes', blurb: 'One random node crashes at 1.5s; every other node must stop calling it alive within 3s.', nodeCount: 5, duration: 5500, params: { crashes: [{ at: 1500 }], detect: 3000, budget: 30 } },
            { name: 'Sixteen nodes', blurb: 'A healthy cluster of sixteen: no false alarms, and at most 30 messages per node per second.', nodeCount: 16, duration: 4000, params: { budget: 30 } },
          ],
          hints: [
            'Run the starter: it detects the crash fine, then fails the budget. Ten heartbeats a second to every peer is 40 messages per second with five nodes and 150 with sixteen.',
            'Every 200ms, pick one peer and `await node.rpc(peer, { type: "ping" }, { timeout: 80 })`. Answer pings with `node.reply(msg, { type: "ack" })`.',
            'A `null` result marks the peer `suspect`; any ack marks it `alive` again. Keep the view in an object and return it from `members`.',
            'Picking a random peer each time can leave one unprobed for a long while. Shuffle the peer list, take from it one per period, and reshuffle when it is empty: then every peer is probed at least once every 2(N-1)-1 periods.',
          ],
        },
        {
          slug: 'swim-indirect',
          title: 'Indirect probes',
          level: 'Medium',
          minutes: 25,
          summary: 'When a ping goes unanswered, ask a few other members to ping the target for you before suspecting it.',
          concepts: ['indirect probing', 'ping-req', 'false positives'],
          workload: swimIndirect,
          tests: [
            { name: 'One broken link', blurb: 'Two random nodes cannot exchange any message for the whole run; everyone else can reach both. Nobody may suspect anyone.', nodeCount: 5, duration: 5000, params: { brokenLink: true, budget: 30 } },
            { name: 'Lossy network, one crash', blurb: '2% of node messages are lost and a random node crashes at 1.5s: suspect it everywhere within 3s, and nobody else.', nodeCount: 5, duration: 5500, dropRate: 0.02, params: { crashes: [{ at: 1500 }], detect: 3000, budget: 30, lossy: true } },
          ],
          hints: [
            'Run the starter on "One broken link" and read the failing check: the two nodes on the broken link suspect each other although both are fine.',
            'After a failed direct ping, pick up to 3 other peers (not the target) and send each `{ type: "ping_req", target }` with a 120ms timeout.',
            'A helper handling `ping_req` pings the target itself (60ms timeout) and replies `{ type: "ping_req_ok", ok: true|false }`. Suspect the target only if no helper said `ok: true`.',
            'Send the `ping_req`s in parallel with `Promise.all`, not one after another, so the whole probe still fits inside one 200ms period.',
          ],
        },
      ],
    },
    {
      title: 'Agreeing on the view',
      tasks: [
        {
          slug: 'swim-gossip',
          title: 'Spread the news by piggybacking',
          level: 'Hard',
          minutes: 35,
          summary: 'Declare a failed member dead and spread membership updates on the probe messages you already send, so the whole cluster agrees within a few rounds.',
          concepts: ['gossip dissemination', 'piggybacking', 'infection-style dissemination'],
          workload: swimGossip,
          tests: [
            { name: 'Twelve nodes, one crash', blurb: 'A random node crashes at 2s: every node must say "dead" within 3s, and within 2s of the first node saying so.', nodeCount: 12, duration: 6000, params: { crashes: [{ at: 2000 }], detect: 3000, spread: 2000, budget: 30 } },
            {
              name: 'Two crashes, lossy network',
              blurb: 'Twelve nodes, 2% message loss; one node crashes at 1.5s and another at 3s.',
              nodeCount: 12,
              duration: 7500,
              dropRate: 0.02,
              params: { crashes: [{ at: 1500 }, { at: 3000 }], detect: 3000, spread: 2000, budget: 30, lossy: true },
            },
          ],
          hints: [
            'Run the starter: with eleven peers each node needs many periods to probe the crashed one itself, and it only says "suspect".',
            'When a probe (direct and indirect) fails, set the target to `dead` and leave it out of your probe rotation.',
            'Keep `buffer = [{ id, status, sends }]`. Attach up to 8 entries, least-sent first, as `updates` on every ping, ack, ping_req and ping_req_ok, and add one to their `sends`. Drop an entry once `sends` reaches `3 * Math.ceil(Math.log2(N + 1))`.',
            'On every message you receive, look at `updates`: a `dead` you did not know about is applied and pushed into your own buffer with `sends: 0`. That is what makes it spread.',
            'Do not send extra messages to spread news. The budget is the same 30 per second, and the probes already reach a new member every period.',
          ],
        },
        {
          slug: 'swim-suspicion',
          title: 'Suspicion and incarnation numbers',
          level: 'Hard',
          minutes: 45,
          summary: 'Suspect before declaring dead, let a live suspect refute the rumour with a higher incarnation, and let a restarted node rejoin.',
          concepts: ['suspicion mechanism', 'incarnation number', 'refutation', 'rejoin'],
          workload: swimSuspicion,
          tests: [
            { name: 'A node is cut off briefly', blurb: 'Eight nodes; one random node is partitioned from all others from 1.5s to 2s. Nobody may be called dead, and every view must be all alive by 4.5s.', nodeCount: 8, duration: 6500, params: { isolate: [1500, 2000], refute: 2500, budget: 30 } },
            { name: 'A crash on a lossy network', blurb: 'Eight nodes, 2% loss; a random node crashes at 1.5s and must be "dead" everywhere within 6s.', nodeCount: 8, duration: 8500, dropRate: 0.02, params: { crashes: [{ at: 1500 }], detect: 6000, budget: 30, lossy: true } },
            { name: 'Crash and rejoin', blurb: 'A random node crashes at 1s, is dead everywhere by 7s, restarts at 7.5s and must be alive everywhere within 2.5s.', nodeCount: 8, duration: 10800, params: { crashes: [{ at: 1000, restart: 7500 }], detect: 6000, rejoin: 2500, budget: 30 } },
          ],
          hints: [
            'Run the starter on "A node is cut off briefly": half a second of silence and the node is dead for good, everywhere.',
            'Give every view entry an incarnation: `{ status, inc }`. An update replaces the entry if its `inc` is higher, or equal with a stronger status (dead > suspect > alive). Anything else is ignored and not re-gossiped.',
            'A failed probe applies `suspect` at the target\'s current `inc` and starts a `node.after(2500, ...)` timer. When it fires, declare `dead` only if the entry is still `suspect` at that same `inc`.',
            'When an update says you are suspect or dead at an `inc` at least your own, set your incarnation to `inc + 1` and buffer `alive` about yourself. Put updates about the receiver first when you piggyback, so a suspect hears about itself quickly.',
            'Store your incarnation in `node.disk` and add one at every start, then buffer `alive` about yourself. The others remember you as dead at your old number; only a higher one brings you back.',
          ],
        },
      ],
    },
  ],
  mistakes: [
    {
      title: 'Heartbeating every member',
      symptom: 'Fine with five nodes; with hundreds, each node sends and receives hundreds of messages per second and the detector itself overloads the network.',
      fix: 'Probe one member per period. Each node then sends a constant number of messages per period, and the detection time grows only slowly with cluster size.',
    },
    {
      title: 'Trusting one missed ack',
      symptom: 'Two live nodes on either side of a bad link keep suspecting each other, and a little packet loss produces a steady stream of false alarms.',
      fix: 'Before suspecting, ask k other members to probe the target (ping-req). One path being broken no longer looks like a dead node.',
    },
    {
      title: 'Refuting without raising the incarnation',
      symptom: 'The suspect gossips "I am alive", nobody changes their mind, and it is declared dead anyway.',
      fix: 'An alive update must carry a higher incarnation than the suspicion to win. Bump the number, then gossip.',
    },
    {
      title: 'Keeping the incarnation only in memory',
      symptom: 'A crashed node restarts, announces itself at incarnation 0, and stays dead in everyone\'s view.',
      fix: 'Persist the incarnation and increase it on every start, so the new life outranks every rumour about the old one.',
    },
  ],
  questions: [
    {
      q: 'Why does SWIM keep per-node load constant while heartbeats do not?',
      a: 'With heartbeats every node talks to every other node each interval: N-1 messages per node, N(N-1) in total. In SWIM each node pings one member per period and answers on average one ping, so it sends about two messages per period whatever N is. Detection is still fast because, with N nodes each picking a target, some node probes a given member within about one period.',
    },
    {
      q: 'What problem do incarnation numbers solve that timestamps would not?',
      a: 'Only the node itself increments its incarnation, so there is no clock to agree on. Any update about a node can be ordered against any other: a higher incarnation means "the node said something after this rumour was started". A refutation therefore wins over the suspicion it answers, and a stale suspicion arriving late loses.',
    },
    {
      q: 'Why piggyback updates instead of broadcasting them when they happen?',
      a: 'A broadcast costs N-1 messages at once and loses to packet loss: whoever misses it never hears again. Piggybacking costs nothing extra, and each update rides on about 3 log N messages from every node that learns it, so it reaches everyone in O(log N) periods with high probability even when some messages are lost.',
    },
  ],
  reading: [
    { kind: 'Paper', title: 'SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol', url: 'https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf', note: 'Das, Gupta and Motivala (2002): probing, ping-req, suspicion, incarnations and piggybacked dissemination, with the analysis behind the constants.' },
    { kind: 'Docs', title: 'HashiCorp Consul: Gossip protocol', url: 'https://developer.hashicorp.com/consul/docs/architecture/gossip', note: 'How Consul uses Serf and memberlist, a production SWIM implementation, for LAN and WAN membership.' },
    { kind: 'Paper', title: 'Lifeguard: Local Health Awareness for More Accurate Failure Detection', url: 'https://arxiv.org/abs/1707.00788', note: 'HashiCorp\'s extensions to SWIM that cut false positives caused by slow members.' },
  ],
};
