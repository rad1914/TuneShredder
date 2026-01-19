// e.cjs
const fs = require("fs");
const path = require("path");

const raw = JSON.parse(fs.readFileSync("index.json", "utf8"));
const idx = raw.index || raw;
const meta = raw.meta || [];
const keys = Object.keys(idx);

if (!keys.length) process.exit(console.error("EMPTY INDEX"), 1);

const stats = {
  bucketsTotal: keys.length,
  nonArrayBuckets: 0,
  emptyBuckets: 0,
  singleBuckets: 0,
  multiBuckets: 0,
  maxBucketLen: 0,
  totalEntries: 0,
  validEntries: 0,
  badShape: 0,
  badTimes: 0,
  maxTimesLen: 0,
};

const fileHits = new Map();
const pairCounts = new Map();

for (const k of keys) {
  const bucket = idx[k];
  if (!Array.isArray(bucket)) { stats.nonArrayBuckets++; continue; }

  const L = bucket.length;
  stats.maxBucketLen = Math.max(stats.maxBucketLen, L);
  if (L === 0) stats.emptyBuckets++;
  else if (L === 1) stats.singleBuckets++;
  else stats.multiBuckets++;

  const uniq = new Set();

  for (const it of bucket) {
    stats.totalEntries++;
    if (!Array.isArray(it) || it.length !== 2) { stats.badShape++; continue; }

    const [fid, times] = it;
    if (!Array.isArray(times)) { stats.badTimes++; continue; }

    stats.validEntries++;
    stats.maxTimesLen = Math.max(stats.maxTimesLen, times.length);

    if (fid != null) {
      uniq.add(fid);
      fileHits.set(fid, (fileHits.get(fid) || 0) + 1);
    }
  }

  const arr = [...uniq];
  for (let i = 0; i < arr.length; i++) for (let j = i + 1; j < arr.length; j++) {
    const a = arr[i], b = arr[j];
    const key = a < b ? `${a}|${b}` : `${b}|${a}`;
    pairCounts.set(key, (pairCounts.get(key) || 0) + 1);
  }
}

const pairs = [...pairCounts].map(([k, count]) => {
  const [sa, sb] = k.split("|");
  const a = isNaN(+sa) ? sa : +sa;
  const b = isNaN(+sb) ? sb : +sb;
  return { a, b, count, nameA: meta[a] || null, nameB: meta[b] || null };
}).sort((x, y) => y.count - x.count);

const topFiles = [...fileHits].sort((a, b) => b[1] - a[1]).slice(0, 15).map(([id, occurrences]) => ({
  id, occurrences, name: meta[id] || null
}));

fs.writeFileSync("pairs-full.json", JSON.stringify({
  summary: stats,
  totalPairs: pairs.length,
  topFiles,
  pairs
}, null, 2));

fs.writeFileSync("pairs-full.csv",
  ["a,b,count,nameA,nameB"].concat(
    pairs.map(p => [
      p.a,
      p.b,
      p.count,
      p.nameA ? `"${path.basename(String(p.nameA)).replace(/"/g, '""')}"` : "",
      p.nameB ? `"${path.basename(String(p.nameB)).replace(/"/g, '""')}"` : ""
    ].join(","))
  ).join("\n")
);

const TOP = +process.env.TOP || 50;
console.log(JSON.stringify({ summary: stats, totalPairs: pairs.length, topShown: Math.min(TOP, pairs.length) }, null, 2));
pairs.slice(0, TOP).forEach((p, i) =>
  console.log(`#${i + 1}`, p.count, p.a, p.b, p.nameA ? `(${p.nameA})` : "", p.nameB ? `(${p.nameB})` : "")
);

fs.mkdirSync("dupe", { recursive: true });
const moved = new Set();
const MIN = +process.env.MIN_PAIR_COUNT || 2;

for (const p of pairs) {
  if (p.count < MIN || !p.nameA || !p.nameB) continue;

  const srcA = path.resolve(String(p.nameA));
  const srcB = path.resolve(String(p.nameB));
  const hasA = fs.existsSync(srcA);
  const hasB = fs.existsSync(srcB);
  if (!hasA && !hasB) continue;

  const tag = `${String(p.a).replace(/[^\w-]/g, "_")}_${String(p.b).replace(/[^\w-]/g, "_")}_x${p.count}`;
  const dst = path.join("dupe", tag);
  fs.mkdirSync(dst, { recursive: true });

  const mv = (src, name, label) => {
    try {
      if (src && !moved.has(src) && fs.existsSync(src)) {
        fs.renameSync(src, path.join(dst, path.basename(String(name))));
        moved.add(src);
      }
    } catch (e) {
      console.warn(label, "move failed:", e.message);
    }
  };

  mv(hasA ? srcA : null, p.nameA, "A");
  mv(hasB ? srcB : null, p.nameB, "B");
}