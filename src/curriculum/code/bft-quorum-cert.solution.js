// Signed votes and quorum certificates, tolerating f = 1 Byzantine node.
const F = 1;
const QUORUM = 2 * F + 1;

// ---- Given: toy signatures ---------------------------------------------
// A real system would use public-key signatures (for example Ed25519). Here
// every key sits in one table to keep the code short. The rule of the game:
// a node signs only with its own key. The Byzantine node (played by the
// tests) also knows only its own key.
const KEYS = { n1: 'key-n1-7f3a', n2: 'key-n2-19be', n3: 'key-n3-c05d', n4: 'key-n4-62f8', client: 'key-client-d4e1' };

function digest(text) {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return ('0000000' + h.toString(16)).slice(-8);
}
// Sign `text` with this node's own key.
function sign(text) {
  return digest(KEYS[node.id] + '|' + text);
}
// Did `signer` sign `text`?
function verify(signer, text, sig) {
  return KEYS[signer] !== undefined && digest(KEYS[signer] + '|' + text) === sig;
}
// ------------------------------------------------------------------------

let myVote = null;
const votes = {};   // voter -> { voter, value, sig }: the first valid vote from each node
let decided = null;
let cert = null;

// The entries of `entries` that are valid votes for `value`, one per voter.
function validVotes(value, entries) {
  if (typeof value !== 'string' || !Array.isArray(entries)) return [];
  const out = {};
  for (const e of entries) {
    if (!e || !node.nodeIds.includes(e.voter) || out[e.voter]) continue;
    if (e.value === value && verify(e.voter, 'vote:' + value, e.sig)) out[e.voter] = { voter: e.voter, value, sig: e.sig };
  }
  return Object.values(out);
}

// A certificate of 2f+1 signed votes is a proof anyone can check, so pass
// it on: a node that missed some votes can decide from it.
function decide(value, entries) {
  if (decided !== null) return;
  decided = value;
  cert = entries;
  for (const peer of node.peers) node.send(peer, { type: 'decided', value, cert });
}

node.on('input', (msg) => {
  if (myVote === null) {
    myVote = msg.body.value;
    const body = { type: 'vote', value: myVote, sig: sign('vote:' + myVote) };
    for (const id of node.nodeIds) node.send(id, body);
  }
  node.reply(msg, { type: 'input_ok' });
});

node.on('vote', (msg) => {
  const { value, sig } = msg.body;
  if (votes[msg.src] || typeof value !== 'string' || !verify(msg.src, 'vote:' + value, sig)) return;
  votes[msg.src] = { voter: msg.src, value, sig };
  const matching = Object.values(votes).filter((v) => v.value === value);
  if (matching.length >= QUORUM) decide(value, matching);
});

node.on('decided', (msg) => {
  const good = validVotes(msg.body.value, msg.body.cert);
  if (good.length >= QUORUM) decide(msg.body.value, good);
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: decided, cert });
});
