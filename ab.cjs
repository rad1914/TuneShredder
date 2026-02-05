const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

const WORKDIR = path.resolve("./m");
const JSON_FILE = path.resolve("./_matches.json");
const DUPE_DIR = path.resolve("./dupe");

async function ensureDir(d) {
  try { await fsp.mkdir(d, { recursive: true }) } catch (e) {}
}

function UnionFind() {
  const p = new Map();
  return {
    find(x) {
      if (!p.has(x)) p.set(x, x);
      let v = p.get(x);
      if (v !== x) { v = this.find(v); p.set(x, v) }
      return p.get(x);
    },
    union(a, b) {
      const ra = this.find(a), rb = this.find(b);
      if (ra !== rb) p.set(ra, rb);
    },
    groups() {
      const m = new Map();
      for (const k of p.keys()) {
        const r = this.find(k);
        if (!m.has(r)) m.set(r, []);
        m.get(r).push(k);
      }
      return Array.from(m.values());
    }
  }
}

async function walk(dir) {
  const out = [];
  const stack = [dir];
  while (stack.length) {
    const cur = stack.pop();
    let entries;
    try { entries = await fsp.readdir(cur, { withFileTypes: true }) } catch (e) { continue }
    for (const e of entries) {
      const full = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(full);
      else if (e.isFile()) out.push(full);
    }
  }
  return out;
}

function scoreMatch(basenameLower, baseLower) {
  if (basenameLower === baseLower) return 1000;
  if (basenameLower.startsWith(baseLower)) return 100;
  if (basenameLower.includes(baseLower)) return 10;
  return 0;
}

async function resolvePath(id, allFiles) {
  const candidate = path.join(WORKDIR, id);
  try { if ((await fsp.stat(candidate)).isFile()) return candidate } catch (e) {}
  const base = path.basename(id);
  const baseLower = base.toLowerCase();
  const scored = allFiles
    .map(f => ({ f, bn: path.basename(f).toLowerCase() }))
    .map(({ f, bn }) => ({ f, score: scoreMatch(bn, baseLower), len: f.length }))
    .filter(x => x.score > 0);
  if (scored.length === 0) return null;
  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.len - b.len;
  });
  return scored[0].f;
}

async function uniqueDest(destDir, base) {
  // preserve full basename, append numeric suffix before the ".bak" if needed
  let candidate = path.join(destDir, base + ".bak");
  let i = 1;
  while (true) {
    try {
      await fsp.access(candidate);
      candidate = path.join(destDir, `${base}.${i}.bak`);
      i++;
    } catch (e) {
      return candidate;
    }
  }
}

async function moveWithFallback(src, dest) {
  try {
    await fsp.rename(src, dest);
    return;
  } catch (err) {
    if (err.code === 'EXDEV' || err.code === 'Cross-device link not permitted') {
      // fallback: copy then unlink
      await fsp.copyFile(src, dest);
      try { await fsp.unlink(src) } catch (e) { /* who cares */ }
      return;
    }
    throw err;
  }
}

(async () => {
  try {
    await ensureDir(DUPE_DIR);
    const raw = await fsp.readFile(JSON_FILE, "utf8");
    const matches = JSON.parse(raw);
    const uf = UnionFind();
    for (const m of matches) {
      if (!m || !m.a || !m.b) continue;
      uf.union(m.a, m.b);
    }
    const groups = uf.groups().filter(g => g.length > 1);
    if (groups.length === 0) { console.log("No groups found"); return }
    const allFiles = await walk(WORKDIR);
    for (const g of groups) {
      const infosMap = new Map(); // key by real path to avoid duplicates
      for (const id of g) {
        const pth = await resolvePath(id, allFiles);
        if (!pth) continue;
        try {
          const st = await fsp.stat(pth);
          if (!infosMap.has(pth)) infosMap.set(pth, { ids: [], pth, size: st.size, mtime: st.mtimeMs });
          infosMap.get(pth).ids.push(id);
        } catch (e) {}
      }
      const infos = Array.from(infosMap.values());
      if (infos.length <= 1) continue;
      infos.sort((x, y) => {
        if (y.size !== x.size) return y.size - x.size;
        return y.mtime - x.mtime;
      });
      const keeper = infos[0];
      const losers = infos.slice(1);
      console.log(`Group keep: ${path.basename(keeper.pth)} (${keeper.size} bytes)`);
      for (const L of losers) {
        const base = path.basename(L.pth);
        const dest = await uniqueDest(DUPE_DIR, base);
        try {
          await moveWithFallback(L.pth, dest);
          console.log(`Moved: ${base} -> ${path.relative(process.cwd(), dest)}`);
        } catch (e) {
          console.error("Failed to move", L.pth, e.message);
        }
      }
    }
    console.log("Done.");
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();