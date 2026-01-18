import fs from 'node:fs/promises';
import process from 'node:process';
import { renderProgress } from './renderProgress.js';

const DEFAULT_MIN_MATCHES = 5;
const pairKey = (a, b) => (a < b ? `${a}||${b}` : `${b}||${a}`);

const loadIndex = async (p) => JSON.parse(await fs.readFile(p, 'utf8'));

export async function findDuplicates(indexObj, { minMatches = DEFAULT_MIN_MATCHES, progressCb = () => {} } = {}) {
  minMatches = Number(minMatches) || DEFAULT_MIN_MATCHES;

  const keys = Object.keys(indexObj);
  const total = keys.length;

  const pairCount = new Map();
  for (let done = 0; done < total; done++) {
    const k = keys[done];
    const bucket = indexObj[k];
    if (!bucket || bucket.length < 2) {
      if (done % 200 === 0) progressCb(done, total, k);
      continue;
    }

    for (let i = 0; i < bucket.length; i++) {
      const a = bucket[i][0];
      for (let j = i + 1; j < bucket.length; j++) {
        const b = bucket[j][0];
        if (a === b) continue;
        const pk = pairKey(a, b);
        pairCount.set(pk, (pairCount.get(pk) || 0) + 1);
      }
    }

    if (done % 200 === 0) progressCb(done, total, k);
  }

  progressCb(total, total, 'filtering');

  const candidates = new Set();
  for (const [pk, cnt] of pairCount) if (cnt >= minMatches) candidates.add(pk);
  if (!candidates.size) return [];

  const pairMap = new Map();
  for (let done = 0; done < total; done++) {
    const k = keys[done];
    const bucket = indexObj[k];
    if (!bucket || bucket.length < 2) {
      if (done % 200 === 0) progressCb(done, total, k);
      continue;
    }

    for (let i = 0; i < bucket.length; i++) {
      const [a, posA] = bucket[i];
      for (let j = i + 1; j < bucket.length; j++) {
        const [b, posB] = bucket[j];
        if (a === b) continue;

        const pk = pairKey(a, b);
        if (!candidates.has(pk)) continue;

        let entry = pairMap.get(pk);
        if (!entry) pairMap.set(pk, (entry = { offsets: new Map(), totalPairs: 0 }));

        const off = posA - posB;
        entry.offsets.set(off, (entry.offsets.get(off) || 0) + 1);
        entry.totalPairs++;
      }
    }

    if (done % 200 === 0) progressCb(done, total, k);
  }

  progressCb(total, total, 'finalizing');

  const results = [];
  for (const [pk, { offsets, totalPairs }] of pairMap) {
    let bestOffset = 0, bestCount = 0, sharedHashes = 0;
    for (const [off, cnt] of offsets) {
      sharedHashes += cnt;
      if (cnt > bestCount) (bestCount = cnt), (bestOffset = Number(off));
    }
    if (bestCount >= minMatches) {
      const [a, b] = pk.split('||');
      results.push({ a, b, bestOffset, bestCount, totalPairs, sharedHashes });
    }
  }

  results.sort((x, y) => y.bestCount - x.bestCount || y.sharedHashes - x.sharedHashes);
  return results;
}

async function mainCLI(argv) {
  const [, , indexPath, outPath = 'duplicates.json', minMatchesArg] = argv;
  if (!indexPath) {
    console.error('usage: node match.js <index.json> [out.json] [minMatches]');
    process.exit(2);
  }

  const data = await loadIndex(indexPath);
  if (!data?.index) {
    console.error('invalid index file: missing "index" property');
    process.exit(3);
  }

  const minMatches = minMatchesArg ? parseInt(minMatchesArg, 10) : DEFAULT_MIN_MATCHES;
  const progressCb = (done, total, label) => renderProgress(done, total, String(label || ''));

  const results = await findDuplicates(data.index, { minMatches, progressCb });
  await fs.writeFile(
    outPath,
    JSON.stringify({ generated: new Date().toISOString(), minMatches, results }, null, 2),
    'utf8'
  );

  console.log(`found ${results.length} duplicate pairs (minMatches=${minMatches}) -> ${outPath}`);
}

if (import.meta.url === `file://${process.argv[1]}`) mainCLI(process.argv);
