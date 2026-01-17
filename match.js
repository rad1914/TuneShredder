// @path: match.js
import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const getName = (meta, id) => {
  const i = parseInt(id, 10) - 1;
  return Array.isArray(meta) && meta[i] ? meta[i] : `track:${id}`;
};

function buildPairs(index) {
  const pairs = new Map();
  const counts = Object.create(null);

  for (const postings of Object.values(index)) {
    if (!Array.isArray(postings) || postings.length === 0) continue;

    for (let i = 0; i < postings.length; i++) {
      const [idA, tAraw] = postings[i];
      const tA = Number(tAraw);
      counts[idA] = (counts[idA] || 0) + 1;

      for (let j = i + 1; j < postings.length; j++) {
        const [idB, tBraw] = postings[j];
        if (idA === idB) continue;
        const tB = Number(tBraw);

        let id1 = idA, id2 = idB;
        let delta = Math.round(tB - tA);
        if (id1 > id2) { [id1, id2] = [id2, id1]; delta = -delta; }

        let inner = pairs.get(id1);
        if (!inner) { inner = new Map(); pairs.set(id1, inner); }

        let e = inner.get(id2);
        if (!e) {
          e = { deltas: new Map(), total: 0, bestDelta: 0, bestCount: 0, id1, id2 };
          inner.set(id2, e);
        }

        const c = (e.deltas.get(delta) || 0) + 1;
        e.deltas.set(delta, c);
        e.total++;
        if (c > e.bestCount) { e.bestCount = c; e.bestDelta = delta; }
      }
    }
  }

  return { pairs, counts };
}

function analyze(pairs, counts, meta = []) {
  const nameMap = Object.create(null);
  for (const k of Object.keys(counts)) nameMap[k] = getName(meta, k);

  const out = [];
  for (const [id1, inner] of pairs) {
    for (const [id2, e] of inner) {
      const a = e.id1, b = e.id2;
      const hcA = counts[a] || 0, hcB = counts[b] || 0;
      const minHC = Math.max(1, Math.min(hcA, hcB));
      const score = e.bestCount / minHC;

      out.push({
        pair: `${a}|${b}`,
        idA: a, idB: b,
        fileA: nameMap[a] || getName(meta, a),
        fileB: nameMap[b] || getName(meta, b),
        bestDelta: e.bestDelta,
        bestCount: e.bestCount,
        totalMatches: e.total,
        hashCountA: hcA, hashCountB: hcB,
        percentOfA: hcA ? (e.bestCount / hcA) * 100 : 0,
        percentOfB: hcB ? (e.bestCount / hcB) * 100 : 0,
        score
      });
    }
  }

  return out.sort((x, y) => y.score - x.score || y.bestCount - x.bestCount);
}

async function main(argv = process.argv.slice(2)) {
  if (argv.length < 1) {
    console.error('usage: node match.js <index.json> [out.json]');
    process.exit(2);
  }
  const inPath = argv[0], outPath = argv[1] || 'matches.json';

  try { await fs.access(inPath); } catch {
    console.error('index file not found:', inPath);
    process.exit(1);
  }

  const raw = JSON.parse(await fs.readFile(inPath, 'utf8'));
  const index = raw.index || {}, meta = Array.isArray(raw.meta) ? raw.meta : [];

  const { pairs, counts } = buildPairs(index);
  const results = analyze(pairs, counts, meta);

  await fs.writeFile(outPath, JSON.stringify({ matches: results }, null, 2), 'utf8');

  // move matched tracks to ./dupe
  const dupeDir = path.resolve('dupe');
  await fs.mkdir(dupeDir, { recursive: true });

  const moved = new Set();
  for (const r of results) {
    const src = r.fileB;
    if (moved.has(src)) continue;
    try {
      const dst = path.join(dupeDir, path.basename(src));
      await fs.rename(src, dst);
      moved.add(src);
    } catch {}
  }

  console.log(`found ${results.length} pairs — wrote ${outPath}`);
  for (const r of results.slice(0, 10)) {
    console.log(
      `(${r.idA}) ${r.fileA} <-> (${r.idB}) ${r.fileB} | bestCount=${r.bestCount} total=${r.totalMatches} delta=${r.bestDelta} score=${r.score.toFixed(3)} %A=${r.percentOfA.toFixed(1)} %B=${r.percentOfB.toFixed(1)}`
    );
  }
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch(e => { console.error(e); process.exit(1); });
}