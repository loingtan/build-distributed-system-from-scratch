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

// Copy-on-write: a write never changes a page that is in the tree. It writes
// new copies of the pages on the path from the root to the leaf (and the new
// pages a split creates), then switches node.disk.root to the new root in
// one assignment, and only then deletes the old copies. A crash at any point
// leaves either the old tree or the new one, never half of each.
function newPage(page) {
  const id = String(node.disk.next++);
  pages()[id] = page;
  return id;
}

// Returns the ids that replace this page: one, or two after a split, with
// the separator key between them.
function insert(id, key, value, old) {
  const page = pages()[id];
  old.push(id);
  if (page.leaf) {
    const keys = [...page.keys];
    const values = [...page.values];
    const i = keys.indexOf(key);
    if (i >= 0) values[i] = value;
    else {
      const at = childIndex(page, key);
      keys.splice(at, 0, key);
      values.splice(at, 0, value);
    }
    if (keys.length <= MAX_KEYS) return { ids: [newPage({ leaf: true, keys, values })] };
    const mid = Math.ceil(keys.length / 2);
    const left = newPage({ leaf: true, keys: keys.slice(0, mid), values: values.slice(0, mid) });
    const right = newPage({ leaf: true, keys: keys.slice(mid), values: values.slice(mid) });
    return { ids: [left, right], sep: keys[mid] };
  }
  const at = childIndex(page, key);
  const r = insert(page.children[at], key, value, old);
  const keys = [...page.keys];
  const children = [...page.children];
  children.splice(at, 1, ...r.ids);
  if (r.sep !== undefined) keys.splice(at, 0, r.sep);
  if (keys.length <= MAX_KEYS) return { ids: [newPage({ leaf: false, keys, children })] };
  // An inner page splits around its middle key, which moves up to the parent.
  const mid = Math.floor(keys.length / 2);
  const left = newPage({ leaf: false, keys: keys.slice(0, mid), children: children.slice(0, mid + 1) });
  const right = newPage({ leaf: false, keys: keys.slice(mid + 1), children: children.slice(mid + 1) });
  return { ids: [left, right], sep: keys[mid] };
}

function remove(id, key, old) {
  const page = pages()[id];
  old.push(id);
  if (page.leaf) {
    const i = page.keys.indexOf(key);
    return newPage({ leaf: true, keys: page.keys.filter((_, x) => x !== i), values: page.values.filter((_, x) => x !== i) });
  }
  const at = childIndex(page, key);
  const children = [...page.children];
  children[at] = remove(children[at], key, old);
  return newPage({ leaf: false, keys: [...page.keys], children });
}

// The atomic step: the new root becomes the tree, then the old path is freed.
function commit(root, old) {
  node.disk.root = root;
  for (const id of old) delete pages()[id];
}

function put(key, value) {
  const old = [];
  const r = insert(node.disk.root, key, value, old);
  // A full root splits into two pages under a new root: the tree grows at the
  // top, so every leaf stays at the same depth.
  const root = r.ids.length === 1 ? r.ids[0] : newPage({ leaf: false, keys: [r.sep], children: r.ids });
  commit(root, old);
}

function lookup(key) {
  let page = pages()[node.disk.root];
  while (!page.leaf) page = pages()[page.children[childIndex(page, key)]];
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
  // Deleting a missing key changes nothing. Pages may become under-full or
  // empty; this tree does not merge them.
  if (lookup(msg.body.key) !== null) {
    const old = [];
    const root = remove(node.disk.root, msg.body.key, old);
    commit(root, old);
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
