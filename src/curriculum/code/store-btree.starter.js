const MAX_KEYS = 4; // per page

// The tree lives in node.disk as pages:
//   node.disk.pages = { id: { leaf: true, keys, values } | { leaf: false, keys, children } }
//   node.disk.root  = id of the root page
// An inner page with keys [k0, k1] has three children: keys < k0, keys in
// [k0, k1), and keys >= k1. Values live only in the leaves.
if (!node.disk.pages) {
  node.disk.pages = { 1: { leaf: true, keys: [], values: [] } };
  node.disk.root = '1';
  node.disk.next = 2; // next unused page id
}
const pages = () => node.disk.pages;

// Index of the child to follow for key: the number of keys <= key.
function childIndex(page, key) {
  let i = 0;
  while (i < page.keys.length && page.keys[i] <= key) i++;
  return i;
}

function findLeaf(key) {
  let page = pages()[node.disk.root];
  while (!page.leaf) page = pages()[page.children[childIndex(page, key)]];
  return page;
}

// For now every key goes into the one leaf it belongs to, which never splits.
// TODO: when a page would hold more than MAX_KEYS keys, split it in two and
// add the separator key to its parent (which may split in turn). A full root
// becomes two pages under a new root. Also make every write crash-safe: write
// new copies of the changed pages and switch node.disk.root to the new root
// in one step (copy-on-write), or log the page changes first.
function put(key, value) {
  const page = findLeaf(key);
  const i = page.keys.indexOf(key);
  if (i >= 0) page.values[i] = value;
  else {
    const at = childIndex(page, key);
    page.keys.splice(at, 0, key);
    page.values.splice(at, 0, value);
  }
}

function lookup(key) {
  const page = findLeaf(key);
  const i = page.keys.indexOf(key);
  return i >= 0 ? page.values[i] : null;
}

// Visit every subtree that can hold keys in [from, to), in key order.
function scanPage(id, from, to, out) {
  const page = pages()[id];
  if (page.leaf) {
    page.keys.forEach((k, i) => {
      if (k >= from && k < to) out.push([k, page.values[i]]);
    });
    return;
  }
  for (let i = 0; i < page.children.length; i++) {
    const lo = i === 0 ? null : page.keys[i - 1];
    const hi = i === page.keys.length ? null : page.keys[i];
    if ((hi === null || hi > from) && (lo === null || lo < to)) scanPage(page.children[i], from, to, out);
  }
}

node.on('put', (msg) => {
  put(msg.body.key, msg.body.value);
  node.reply(msg, { type: 'put_ok' });
});

node.on('delete', (msg) => {
  const page = findLeaf(msg.body.key);
  const i = page.keys.indexOf(msg.body.key);
  if (i >= 0) {
    page.keys.splice(i, 1);
    page.values.splice(i, 1);
  }
  node.reply(msg, { type: 'delete_ok' });
});

node.on('get', (msg) => {
  node.reply(msg, { type: 'get_ok', value: lookup(msg.body.key) });
});

node.on('scan', (msg) => {
  const entries = [];
  scanPage(node.disk.root, msg.body.from, msg.body.to, entries);
  node.reply(msg, { type: 'scan_ok', entries });
});
