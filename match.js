import fs from 'node:fs/promises';
import process from 'node:process';
import { renderProgress } from './renderProgress.js';

const DEFAULTS = {
  minMatches: 25,
  minRatio: 0.12,
  progressInterval: 800,

  // critical for collision control
  maxBucket: 80,     // cap per hash bucket after expansion
  dropAbove: 120,    // if raw bucket bigger than this => ignore key entirely (stop-hash)
  minBucket: 2,
};

const PAIR_SEP = '\u0000';
const pairKey = (a, b) => (a < b ? `${a}${PAIR_SEP}${b}` : `${b}${PAIR_SEP}${a}`);
const splitPairKey = (pk) => pk.split(PAIR_SEP);

const loadIndex = async (p) => {
  const raw = await fs.readFile(p, 'utf8');
  const parsed = JSON.parse(raw);
  return parsed?.index ?? parsed;
};

// Expand one entry:
//  - [fid, pos] => [[fid,pos]]
//  - [fid, [t0,dt1,dt2..]] => [[fid,t0],[fid,t1]...]
function expandEntry(e) {
  if (!Array.isArray(e)) return [[String(e), 0]];

  const fid = String(e[0]);
  const v = e[1];

  // compressed format: [fid, deltas[]]
  if (Array.isArray(v)) {
    const out = [];
    let t = 0;
    for (let i = 0; i < v.length; i++) {
      const d = Number(v[i]) || 0;
      t = (i === 0) ? d : (t + d);
      out.push([fid, t | 0]);
    }
    return out;
  }

  // old format: [fid,pos]
  return [[fid, (Number(v) || 0) | 0]];
}

function normalizeBucket(bucket) {
  const out = [];
  for (let i = 0; i < bucket.length; i++) {
    const expanded = expandEntry(bucket[i]);
    for (let j = 0; j < expanded.length; j++) out.push(expanded[j]);
  }
  return out;
}

// Dedup + cap per key
function dedupeIndex(idx, {
  maxBucket = DEFAULTS.maxBucket,
  dropAbove = DEFAULTS.dropAbove,
  minBucket = DEFAULTS.minBucket
} = {}) {
  const out = Object.create(null);

  for (const [k, bucketRaw] of Object.entries(idx)) {
    if (!bucketRaw || bucketRaw.length < minBucket) continue;

    // stop-hash removal based on raw bucket size
    if (bucketRaw.length > dropAbove) continue;

    const bucket = normalizeBucket(bucketRaw);
    if (bucket.length < minBucket) continue;

    // dedupe only exact [id,pos] duplicates, keep multiple positions per id
    const seenPair = new Set();
    const dst = [];

    for (let i = 0; i < bucket.length && dst.length < maxBucket; i++) {
      const [id, pos] = bucket[i];
      const pk = `${id}\u0001${pos}`;
      if (seenPair.has(pk)) continue;
      seenPair.add(pk);

      dst.push([id, pos]);
    }

    if (dst.length >= minBucket) out[k] = dst;
  }

  return out;
}

function buildPairCounts(indexObj, {
  progressCb = () => {},
  progressInterval = DEFAULTS.progressInterval,
} = {}) {
  const keys = Object.keys(indexObj);
  const total = keys.length;
  const pairCount = new Map();

  for (let i = 0; i < total; i++) {
    const k = keys[i];
    const bucket = indexObj[k];
    if (!bucket || bucket.length < 2) {
      if ((i % progressInterval) === 0) progressCb(i, total, k);
      continue;
    }

    for (let aI = 0; aI < bucket.length; aI++) {
      const a = bucket[aI][0];
      for (let bI = aI + 1; bI < bucket.length; bI++) {
        const b = bucket[bI][0];
        if (a === b) continue;
        const pk = pairKey(a, b);
        pairCount.set(pk, (pairCount.get(pk) || 0) + 1);
      }
    }

    if ((i % progressInterval) === 0) progressCb(i, total, k);
  }

  progressCb(total, total, 'pairCount');
  return pairCount;
}

function buildOffsetVotes(indexObj, candidates, {
  progressCb = () => {},
  progressInterval = DEFAULTS.progressInterval,
} = {}) {
  const keys = Object.keys(indexObj);
  const total = keys.length;

  const pairMap = new Map();

  for (let i = 0; i < total; i++) {
    const k = keys[i];
    const bucket = indexObj[k];
    if (!bucket || bucket.length < 2) {
      if ((i % progressInterval) === 0) progressCb(i, total, k);
      continue;
    }

    for (let aI = 0; aI < bucket.length; aI++) {
      const [a, posA] = bucket[aI];
      for (let bI = aI + 1; bI < bucket.length; bI++) {
        const [b, posB] = bucket[bI];
        if (a === b) continue;

        const pk = pairKey(a, b);
        if (!candidates.has(pk)) continue;

        let entry = pairMap.get(pk);
        if (!entry) {
          entry = { offsets: new Map(), totalPairs: 0 };
          pairMap.set(pk, entry);
        }

        const off = (posA - posB) | 0;
        entry.offsets.set(off, (entry.offsets.get(off) || 0) + 1);
        entry.totalPairs++;
      }
    }

    if ((i % progressInterval) === 0) progressCb(i, total, k);
  }

  progressCb(total, total, 'offsetVote');
  return pairMap;
}

export async function findDuplicates(indexObj, opts = {}) {
  if (!indexObj || typeof indexObj !== 'object') throw new TypeError('indexObj must be an object');

  const {
    minMatches = DEFAULTS.minMatches,
    minRatio = DEFAULTS.minRatio,

    progressCb = () => {},
    progressInterval = DEFAULTS.progressInterval,

    maxBucket = DEFAULTS.maxBucket,
    dropAbove = DEFAULTS.dropAbove,
    minBucket = DEFAULTS.minBucket,
  } = opts;

  const minM = Math.max(1, Number(minMatches) || DEFAULTS.minMatches);
  let ratio = Number(minRatio);
  if (!Number.isFinite(ratio) || ratio <= 0 || ratio > 1) ratio = DEFAULTS.minRatio;

  const cleaned = dedupeIndex(indexObj, { maxBucket, dropAbove, minBucket });

  const pairCount = buildPairCounts(cleaned, { progressCb, progressInterval });

  const candidates = new Set();
  for (const [pk, cnt] of pairCount.entries()) {
    if (cnt >= minM) candidates.add(pk);
  }
  if (!candidates.size) return [];

  const pairMap = buildOffsetVotes(cleaned, candidates, { progressCb, progressInterval });

  const results = [];
  for (const [pk, { offsets, totalPairs }] of pairMap) {
    let bestOff = 0, bestCnt = 0;

    for (const [off, cnt] of offsets) {
      if (cnt > bestCnt) {
        bestCnt = cnt;
        bestOff = Number(off);
      }
    }

    const score = bestCnt / totalPairs;

    if (bestCnt >= minM && score >= ratio) {
      const [a, b] = splitPairKey(pk);
      results.push({ a, b, bestOffset: bestOff, bestCount: bestCnt, totalPairs, score });
    }
  }

  results.sort((x, y) =>
    (y.bestCount - x.bestCount) ||
    (y.score - x.score)
  );

  return results;
}

async function mainCLI(argv) {
  const [
    , ,
    indexPath,
    outPath = 'duplicates.json',
    minMatchesArg,
    minRatioArg,
    maxBucketArg,
    dropAboveArg
  ] = argv;

  if (!indexPath) {
    console.error('usage: node match.js <index.json> [out.json] [minMatches] [minRatio] [maxBucket] [dropAbove]');
    process.exit(2);
  }

  let index;
  try { index = await loadIndex(indexPath); }
  catch (e) { console.error('failed to read index:', e?.message || String(e)); process.exit(3); }

  const minMatches = minMatchesArg ? parseInt(minMatchesArg, 10) : DEFAULTS.minMatches;
  const minRatio = minRatioArg ? Number(minRatioArg) : DEFAULTS.minRatio;
  const maxBucket = maxBucketArg ? parseInt(maxBucketArg, 10) : DEFAULTS.maxBucket;
  const dropAbove = dropAboveArg ? parseInt(dropAboveArg, 10) : DEFAULTS.dropAbove;

  const progressCb = (done, total, label) => renderProgress(done, total, String(label || ''));

  try {
    const results = await findDuplicates(index, {
      minMatches,
      minRatio,
      maxBucket,
      dropAbove,
      progressCb,
      progressInterval: DEFAULTS.progressInterval,
    });

    const payload = {
      generated: new Date().toISOString(),
      minMatches,
      minRatio,
      maxBucket,
      dropAbove,
      results,
    };

    await fs.writeFile(outPath, JSON.stringify(payload, null, 2), 'utf8');
    console.log(`found ${results.length} duplicate pairs -> ${outPath}`);
  } catch (e) {
    console.error('error while finding duplicates:', e?.message || String(e));
    process.exit(5);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) mainCLI(process.argv);