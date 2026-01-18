// @path: match.js
import fs from 'node:fs/promises';
import process from 'node:process';
import path from 'node:path';
import { renderProgress } from './renderProgress.js';

const CFG = {
  bucket: 250,
  minMatches: 3,
  minRatio: 0.5,
  progressInterval: 600,
};

const PAIR_SEP = '\u0000';
const pairKey = (a, b) => (a < b ? `${a}${PAIR_SEP}${b}` : `${b}${PAIR_SEP}${a}`);
const splitPairKey = (pk) => pk.split(PAIR_SEP);

const loadIndex = async (p) => {
  const raw = await fs.readFile(p, 'utf8');
  const parsed = JSON.parse(raw);
  return parsed?.index ?? parsed;
};

async function atomicWrite(filePath, data, encoding = 'utf8') {
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const tmpName = path.join(dir, `.${base}.tmp-${process.pid}-${Date.now()}`);
  await fs.writeFile(tmpName, data, encoding);
  await fs.rename(tmpName, filePath);
}

function dedupeIndex(idx, maxBucket = CFG.bucket) {
  const out = Object.create(null);
  for (const [k, bucket] of Object.entries(idx)) {
    if (!bucket || bucket.length === 0) continue;
    const seenId = new Set();
    const seenPair = new Set();
    const dst = [];
    for (let i = 0; i < bucket.length && dst.length < maxBucket; i++) {
      const e = bucket[i];
      const id = Array.isArray(e) ? e[0] : e;
      const pos = Array.isArray(e) ? e[1] : 0;
      const pk = `${id}\u0001${pos}`;
      if (seenPair.has(pk)) continue;
      if (seenId.has(id)) { seenPair.add(pk); continue; }
      seenId.add(id);
      seenPair.add(pk);
      dst.push([id, pos]);
    }
    if (dst.length) out[k] = dst;
  }
  return out;
}

export async function findDuplicates(indexObj, opts = {}) {
  if (!indexObj || typeof indexObj !== 'object') throw new TypeError('indexObj must be an object');

  const {
    minMatches = CFG.minMatches,
    minRatio = CFG.minRatio,
    progressCb = () => {},
    progressInterval = CFG.progressInterval,
    maxBucket = CFG.bucket,
  } = opts;

  const minM = Number(minMatches) || CFG.minMatches;
  let ratio = Number(minRatio);
  if (!Number.isFinite(ratio) || ratio <= 0 || ratio > 1) ratio = CFG.minRatio;

  const keys = Object.keys(indexObj);
  const total = keys.length;
  const normalized = Object.create(null);
  for (let i = 0; i < total; i++) {
    const k = keys[i];
    const bucket = indexObj[k];
    if (!bucket || bucket.length === 0) {
      if ((i % progressInterval) === 0) progressCb(i, total, k);
      continue;
    }
    const dst = [];
    for (let j = 0; j < bucket.length && dst.length < maxBucket; j++) {
      const e = bucket[j];
      if (Array.isArray(e)) {
        const id = e[0];
        const pos = Number.isFinite(Number(e[1])) ? Number(e[1]) : 0;
        dst.push([id, pos]);
      } else {

        dst.push([e, 0]);
      }
    }
    if (dst.length) normalized[k] = dst;
    if ((i % progressInterval) === 0) progressCb(i, total, k);
  }

  progressCb(total, total, 'counting');

  const pairCount = new Map();
  let nonEmpty = 0;
  const nKeys = Object.keys(normalized);
  for (let i = 0; i < nKeys.length; i++) {
    const k = nKeys[i];
    const bucket = normalized[k];
    if (!bucket || bucket.length < 2) {
      if ((i % progressInterval) === 0) progressCb(i, nKeys.length, k);
      continue;
    }
    nonEmpty++;
    for (let aI = 0; aI < bucket.length; aI++) {
      const a = bucket[aI][0];
      for (let bI = aI + 1; bI < bucket.length; bI++) {
        const b = bucket[bI][0];
        if (a === b) continue;
        const pk = pairKey(a, b);
        pairCount.set(pk, (pairCount.get(pk) || 0) + 1);
      }
    }
    if ((i % progressInterval) === 0) progressCb(i, nKeys.length, k);
  }

  progressCb(nKeys.length, nKeys.length, 'filtering');

  const candidates = new Set(
    [...pairCount.entries()]
      .filter(([, cnt]) => cnt >= minM)
      .map(([pk]) => pk)
  );
  if (!candidates.size) return [];

  const pairMap = new Map();
  for (let i = 0; i < nKeys.length; i++) {
    const k = nKeys[i];
    const bucket = normalized[k];
    if (!bucket || bucket.length < 2) {
      if ((i % progressInterval) === 0) progressCb(i, nKeys.length, k);
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
        if (!entry) { entry = { offsets: new Map(), totalPairs: 0 }; pairMap.set(pk, entry); }
        const off = posA - posB;
        entry.offsets.set(off, (entry.offsets.get(off) || 0) + 1);
        entry.totalPairs++;
      }
    }
    if ((i % progressInterval) === 0) progressCb(i, nKeys.length, k);
  }

  progressCb(nKeys.length, nKeys.length, 'finalizing');

  const results = [];
  for (const [pk, { offsets, totalPairs }] of pairMap) {
    let bestOff = 0, bestCnt = 0, shared = 0;
    for (const [off, cnt] of offsets) {
      shared += cnt;
      if (cnt > bestCnt) { bestCnt = cnt; bestOff = Number(off); }
    }
    if (bestCnt >= minM && (bestCnt / totalPairs) >= ratio) {
      const [a, b] = splitPairKey(pk);
      results.push({ a, b, bestOffset: bestOff, bestCount: bestCnt, totalPairs, sharedHashes: shared });
    }
  }

  return results.sort((x, y) => (y.bestCount - x.bestCount) || (y.sharedHashes - x.sharedHashes));
}

async function mainCLI(argv) {
  const [, , indexPath, outPath = 'duplicates.json', minMatchesArg, minRatioArg, maxBucketArg] = argv;
  if (!indexPath) {
    console.error('usage: node match.js <index.json> [out.json] [minMatches] [minRatio] [maxBucket]');
    process.exit(2);
  }

  let index;
  try { index = await loadIndex(indexPath); }
  catch (e) { console.error('failed to read index:', e?.message || String(e)); process.exit(3); }

  if (!index || typeof index !== 'object') { console.error('invalid index file'); process.exit(4); }

  const minMatches = minMatchesArg ? parseInt(minMatchesArg, 10) : CFG.minMatches;
  const minRatio = minRatioArg ? Number(minRatioArg) : CFG.minRatio;
  const maxBucket = maxBucketArg ? parseInt(maxBucketArg, 10) : CFG.bucket;

  const deduped = dedupeIndex(index, maxBucket);
  const progressCb = (done, total, label) => renderProgress(done, total, String(label || ''));

  try {
    const results = await findDuplicates(deduped, {
      minMatches,
      minRatio,
      maxBucket,
      progressCb,
      progressInterval: CFG.progressInterval,
    });
    const payload = { generated: new Date().toISOString(), minMatches, minRatio, maxBucket, results };
    await atomicWrite(outPath, JSON.stringify(payload, null, 2), 'utf8');
    console.log(`found ${results.length} duplicate pairs -> ${outPath}`);
  } catch (e) {
    console.error('error while finding duplicates:', e?.message || String(e));
    process.exit(5);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) mainCLI(process.argv);
