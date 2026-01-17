// @path: match.js
import * as fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const MIN_ABSOLUTE_MATCHES = 3;
const MIN_SCORE = 0.2;
const MIN_DELTA_RATIO = 0.6;

const safeParseInt = (x) => {
  const n = Number.parseInt(x, 10);
  return Number.isFinite(n) ? n : NaN;
};

const getFilenameForId = (meta, id) => {
  const idx = safeParseInt(id) - 1;
  return Array.isArray(meta) && meta[idx] ? meta[idx] : `track:${id}`;
};

function buildPairMaps(index) {

  const pairs = new Map();
  const hashCounts = Object.create(null);

  for (const postingsRaw of Object.values(index)) {
    if (!Array.isArray(postingsRaw) || postingsRaw.length === 0) continue;

    const L = postingsRaw.length;

    const postings = new Array(L);
    for (let i = 0; i < L; i++) {
      const [idRaw, tRaw] = postingsRaw[i];
      const id = idRaw;
      const t = Number(tRaw);
      postings[i] = [id, t];
      hashCounts[id] = (hashCounts[id] || 0) + 1;
    }

    if (L < 2) continue;

    for (let i = 0; i < L; i++) {
      const [aId, aT] = postings[i];
      for (let j = i + 1; j < L; j++) {
        const [bId, bT] = postings[j];
        if (aId === bId) continue;

        let id1 = aId;
        let id2 = bId;
        let delta = Math.round(bT - aT);

        if (id1 > id2) {
          [id1, id2] = [id2, id1];
          delta = -delta;
        }

        let inner = pairs.get(id1);
        if (!inner) {
          inner = new Map();
          pairs.set(id1, inner);
        }

        let entry = inner.get(id2);
        if (!entry) {
          entry = {
            deltas: new Map(),
            totalMatches: 0,
            id1,
            id2,
            bestDelta: 0,
            bestCount: 0
          };
          inner.set(id2, entry);
        }

        const newCount = (entry.deltas.get(delta) || 0) + 1;
        entry.deltas.set(delta, newCount);
        entry.totalMatches++;

        if (newCount > entry.bestCount) {
          entry.bestCount = newCount;
          entry.bestDelta = delta;
        }
      }
    }
  }

  return { pairs, hashCounts };
}

function analyzePairs(pairsMap, hashCounts, meta = []) {
  const results = [];

  const filenameFor = Object.create(null);
  for (const k of Object.keys(hashCounts)) {
    filenameFor[k] = getFilenameForId(meta, k);
  }

  for (const [id1, inner] of pairsMap.entries()) {
    for (const [id2, entry] of inner.entries()) {
      const bestCount = entry.bestCount || 0;
      const bestDelta = entry.bestDelta ?? null;
      const { id1: idA, id2: idB, totalMatches } = entry;
      const hcA = hashCounts[idA] || 0;
      const hcB = hashCounts[idB] || 0;
      const minHC = Math.max(1, Math.min(hcA, hcB));
      const score = bestCount / minHC;
      const percentOfA = hcA ? (bestCount / hcA) * 100 : 0;
      const percentOfB = hcB ? (bestCount / hcB) * 100 : 0;

      if (bestCount < MIN_ABSOLUTE_MATCHES) continue;
      if (score < MIN_SCORE) continue;
      if (totalMatches > 0 && (bestCount / totalMatches) < MIN_DELTA_RATIO) continue;

      results.push({
        pairKey: `${idA}|${idB}`,
        idA,
        idB,
        fileA: filenameFor[String(idA)] ?? getFilenameForId(meta, idA),
        fileB: filenameFor[String(idB)] ?? getFilenameForId(meta, idB),
        bestDelta,
        bestCount,
        totalMatches,
        hashCountA: hcA,
        hashCountB: hcB,
        percentOfA,
        percentOfB,
        score,
      });
    }
  }

  return results.sort((a, b) => b.score - a.score || b.bestCount - a.bestCount);
}

async function readIndex(inPath) {
  const txt = await fs.readFile(inPath, 'utf8');
  const data = JSON.parse(txt);
  if (!data || typeof data !== 'object') throw new Error('invalid index file');
  return { index: data.index || {}, meta: Array.isArray(data.meta) ? data.meta : [] };
}

async function writeOutput(outPath, matches) {
  await fs.writeFile(outPath, JSON.stringify({ matches }, null, 2), 'utf8');
}

async function main(argv = process.argv.slice(2)) {
  if (argv.length < 1) {
    console.error('usage: node match.js <index.json> [out.json]');
    process.exit(2);
  }
  const inPath = argv[0];
  const outPath = argv[1] || 'matches.json';

  try {
    await fs.access(inPath);
  } catch {
    console.error('index file not found:', inPath);
    process.exit(1);
  }

  const { index, meta } = await readIndex(inPath);
  const { pairs, hashCounts } = buildPairMaps(index);
  const results = analyzePairs(pairs, hashCounts, meta);
  await writeOutput(outPath, results);

  console.log(`found ${results.length} pairs — wrote ${outPath}`);
  for (const r of results.slice(0, 10)) {
    console.log(
      `(${r.idA}) ${r.fileA} <-> (${r.idB}) ${r.fileB} | bestCount=${r.bestCount} total=${r.totalMatches} delta=${r.bestDelta} score=${r.score.toFixed(
        3
      )} %A=${r.percentOfA.toFixed(1)} %B=${r.percentOfB.toFixed(1)}`
    );
  }
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
