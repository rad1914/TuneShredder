// @path: match.js

import fs from 'node:fs/promises';
import process from 'node:process';
import { renderProgress } from './renderProgress.js';

const DEFAULT_MIN_MATCHES = 3;
const DEFAULT_MIN_RATIO = 0.5;
const DEFAULT_PROGRESS_INTERVAL = 200;
const DEFAULT_MAX_BUCKET = 200;
const PAIR_SEP = '\u0000';

const pairKey = (a, b) => (a < b ? `${a}${PAIR_SEP}${b}` : `${b}${PAIR_SEP}${a}`);
const splitPairKey = (pk) => pk.split(PAIR_SEP);

const loadIndex = async (p) => {
  const raw = await fs.readFile(p, 'utf8');
  const parsed = JSON.parse(raw);
  return parsed?.index ? parsed.index : parsed;
};

function dedupeIndex(indexObj, maxBucket = DEFAULT_MAX_BUCKET) {
  const out = Object.create(null);
  for (const k of Object.keys(indexObj)) {
    const bucket = indexObj[k];
    if (!bucket || bucket.length === 0) continue;

    const seenId = new Set();
    const seenPair = new Set();
    const dst = [];

    for (let i = 0; i < bucket.length && dst.length < maxBucket; i++) {

      const entry = bucket[i];
      const id = Array.isArray(entry) ? entry[0] : entry;
      const pos = Array.isArray(entry) ? entry[1] : 0;

      const pairKeyStr = `${id}\u0001${pos}`;

      if (seenPair.has(pairKeyStr)) continue;

      if (seenId.has(id)) {

        seenPair.add(pairKeyStr);
        continue;
      }

      seenId.add(id);
      seenPair.add(pairKeyStr);
      dst.push(Array.isArray(entry) ? [id, pos] : [id, pos]);
    }

    if (dst.length > 0) out[k] = dst;
  }
  return out;
}

export async function findDuplicates(indexObj, opts = {}) {
  if (!indexObj || typeof indexObj !== 'object') throw new TypeError('indexObj must be an object');

  const {
    minMatches = DEFAULT_MIN_MATCHES,
    minRatio = DEFAULT_MIN_RATIO,
    progressCb = () => {},
    progressInterval = DEFAULT_PROGRESS_INTERVAL,
    debug = false,
  } = opts;

  const minM = Number(minMatches) || DEFAULT_MIN_MATCHES;
  let ratio = Number(minRatio);
  if (!Number.isFinite(ratio) || ratio <= 0 || ratio > 1) ratio = DEFAULT_MIN_RATIO;

  const keys = Object.keys(indexObj);
  const total = keys.length;

  const pairCount = new Map();
  let nonEmptyBuckets = 0;

  for (let done = 0; done < total; done++) {
    const k = keys[done];
    const bucket = indexObj[k];
    if (!bucket || bucket.length < 2) {
      if ((done % progressInterval) === 0) progressCb(done, total, k);
      continue;
    }
    nonEmptyBuckets++;

    for (let i = 0, il = bucket.length; i < il; i++) {
      const a = bucket[i][0];
      for (let j = i + 1; j < il; j++) {
        const b = bucket[j][0];
        if (a === b) continue;
        const pk = pairKey(a, b);
        pairCount.set(pk, (pairCount.get(pk) || 0) + 1);
      }
    }

    if ((done % progressInterval) === 0) progressCb(done, total, k);
  }

  progressCb(total, total, 'filtering');

  const candidates = new Set();
  for (const [pk, cnt] of pairCount) {
    if (cnt >= minM) candidates.add(pk);
  }

  if (debug) {
    const top = Array.from(pairCount.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8);
    console.error(`DEBUG: keys=${total}, nonEmptyBuckets=${nonEmptyBuckets}, pairCountSize=${pairCount.size}, candidates=${candidates.size}, topPairs=${JSON.stringify(top)}`);
  }

  if (!candidates.size) return [];

  const pairMap = new Map();
  for (let done = 0; done < total; done++) {
    const k = keys[done];
    const bucket = indexObj[k];
    if (!bucket || bucket.length < 2) {
      if ((done % progressInterval) === 0) progressCb(done, total, k);
      continue;
    }

    for (let i = 0, il = bucket.length; i < il; i++) {
      const [a, posA] = bucket[i];
      for (let j = i + 1; j < il; j++) {
        const [b, posB] = bucket[j];
        if (a === b) continue;

        const pk = pairKey(a, b);
        if (!candidates.has(pk)) continue;

        let entry = pairMap.get(pk);
        if (!entry) {
          entry = { offsets: new Map(), totalPairs: 0 };
          pairMap.set(pk, entry);
        }

        const off = posA - posB;
        entry.offsets.set(off, (entry.offsets.get(off) || 0) + 1);
        entry.totalPairs++;
      }
    }

    if ((done % progressInterval) === 0) progressCb(done, total, k);
  }

  progressCb(total, total, 'finalizing');

  const results = [];
  for (const [pk, { offsets, totalPairs }] of pairMap) {
    let bestOffset = 0;
    let bestCount = 0;
    let sharedHashes = 0;

    for (const [off, cnt] of offsets) {
      sharedHashes += cnt;
      if (cnt > bestCount) {
        bestCount = cnt;
        bestOffset = Number(off);
      }
    }

    if (bestCount >= minM && (bestCount / totalPairs) >= ratio) {
      const [a, b] = splitPairKey(pk);
      results.push({ a, b, bestOffset, bestCount, totalPairs, sharedHashes });
    }
  }

  if (debug) {
    console.error(`DEBUG: results=${results.length}`);
  }
  results.sort((x, y) => (y.bestCount - x.bestCount) || (y.sharedHashes - x.sharedHashes));

  return results;
}

async function mainCLI(argv) {
  const [, , indexPath, outPath = 'duplicates.json', minMatchesArg, minRatioArg, maxBucketArg] = argv;
  if (!indexPath) {
    console.error('usage: node match.js <index.json> [out.json] [minMatches] [minRatio] [maxBucket]');
    process.exit(2);
  }

  let index;
  try {
    index = await loadIndex(indexPath);
  } catch (e) {
    console.error('failed to read index file:', e?.message || String(e));
    process.exit(3);
  }

  if (!index || typeof index !== 'object') {
    console.error('invalid index file: expected object mapping of hashes -> entries');
    process.exit(4);
  }

  const minMatches = minMatchesArg ? parseInt(minMatchesArg, 10) : DEFAULT_MIN_MATCHES;
  const minRatio = minRatioArg ? Number(minRatioArg) : DEFAULT_MIN_RATIO;
  const maxBucket = maxBucketArg ? parseInt(maxBucketArg, 10) : DEFAULT_MAX_BUCKET;

  const dedupedIndex = dedupeIndex(index, maxBucket);

  const progressCb = (done, total, label) => renderProgress(done, total, String(label || ''));

  try {
    const results = await findDuplicates(dedupedIndex, { minMatches, minRatio, progressCb, debug: true });
    const payload = { generated: new Date().toISOString(), minMatches, minRatio, maxBucket, results };
    await fs.writeFile(outPath, JSON.stringify(payload, null, 2), 'utf8');
    console.log(`found ${results.length} duplicate pairs (minMatches=${minMatches}, minRatio=${minRatio}, maxBucket=${maxBucket}) -> ${outPath}`);
  } catch (e) {
    console.error('error while finding duplicates:', e?.message || String(e));
    process.exit(5);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) mainCLI(process.argv);
