// Shared by the web page and the lab server (no Node-only imports).
// Briefs and checker messages name the browser lab's JavaScript API. For Go
// programs, show the Go names instead.
const GO_NAMES = [
  [/node\.reply\(([^,()]+),\s*/g, 'n.Reply($1, '],
  [/node\.reply\b/g, 'n.Reply'],
  [/(await\s+)?node\.rpc\b/g, 'n.Call'],
  [/node\.send\b/g, 'n.Send'],
  [/node\.on\(/g, 'n.Handle('],
  [/node\.every\b/g, 'n.Every'],
  [/node\.after\b/g, 'n.After'],
  [/node\.now\(\)/g, 'n.Elapsed()'],
  [/node\.random\(\)/g, 'n.Rand()'],
  [/node\.log\b/g, 'n.Log'],
  [/node\.peers\b/g, 'n.Peers()'],
  [/node\.nodeIds\b/g, 'n.NodeIDs()'],
  [/node\.id\b/g, 'n.ID()'],
  [/(await\s+)?node\.kv\.read\b/g, 'n.KV().Read'],
  [/(await\s+)?node\.kv\.write\b/g, 'n.KV().Write'],
  [/(await\s+)?node\.kv\.cas\b/g, 'n.KV().CAS'],
  [/node\.kv\b/g, 'n.KV()'],
  [/node\.disk\.([A-Za-z_]\w*)/g, 'disk key "$1" (n.Disk())'],
  [/node\.disk\b/g, 'n.Disk()'],
  [/console\.log\b/g, 'n.Log'],
];
export const goify = (text) => GO_NAMES.reduce((t, [re, to]) => t.replace(re, to), text);


// A brief for Go readers: each ```js code block is replaced, in order, by the
// matching ```go block from lab/go/tasks/<slug>/snippets.md (passed in as
// text); the remaining prose gets Go API names.
export function goBrief(md, snippets) {
  const blocks = snippets ? [...snippets.matchAll(/```go\n([\s\S]*?)```/g)].map((m) => m[1]) : [];
  let i = 0;
  const swapped = md.replace(/```js\n[\s\S]*?```/g, (whole) => (i < blocks.length ? '```go\n' + blocks[i++] + '```' : goify(whole)));
  // goify the prose, but not inside the Go blocks we just inserted
  return swapped
    .split(/(```go\n[\s\S]*?```)/g)
    .map((part) => (part.startsWith('```go') ? part : goify(part)))
    .join('');
}
