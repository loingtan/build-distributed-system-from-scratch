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

node.on('input', (msg) => {
  if (myVote === null) {
    myVote = msg.body.value;
    const body = { type: 'vote', value: myVote, sig: sign('vote:' + myVote) };
    for (const id of node.nodeIds) node.send(id, body);
  }
  node.reply(msg, { type: 'input_ok' });
});

node.on('vote', (msg) => {
  // TODO: keep the first vote from each node (msg.src) whose signature
  // verifies: verify(msg.src, 'vote:' + value, sig).
  // Once QUORUM distinct nodes voted for the same value, decide it. The
  // certificate is those votes as [{ voter, value, sig }, ...]. Then send
  // { type: 'decided', value, cert } to your peers.
});

node.on('decided', (msg) => {
  // TODO: if you have not decided, check the certificate: entries from known
  // nodes, all for msg.body.value, signatures that verify, and at least
  // QUORUM distinct voters. If it holds, decide that value with it.
});

node.on('read', (msg) => {
  node.reply(msg, { type: 'read_ok', value: decided, cert });
});
