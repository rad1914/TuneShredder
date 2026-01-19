const fs = require("fs");
const util = require("util");

const raw = JSON.parse(fs.readFileSync("index.json", "utf8"));
const idx = raw.index || raw;
const meta = raw.meta || [];
const keys = Object.keys(idx);

const pretty = (x) => util.inspect(x, { depth: 6, colors: true, maxArrayLength: 40 });
const pad = (n) => String(n).padStart(2, "0");
const pct = (a, b) => (b ? ((100 * a) / b).toFixed(1) + "%" : "0%");
const num = (n) => (typeof n === "number" ? n.toLocaleString("en-US") : String(n));

const hr = (title = "") => {
  const line = "─".repeat(70);
  console.log("\n" + line);
  if (title) console.log(title);
  console.log(line);
};

const section = (title) => console.log(`\n=== ${title} ===`);

const kv = (obj) => {
  const entries = Object.entries(obj);
  const w = Math.max(...entries.map(([k]) => k.length), 10);
  for (const [k, v] of entries) console.log(`${k.padEnd(w)} : ${v}`);
};

const nameOf = (fid) =>
  typeof fid === "number" && meta[fid] != null ? meta[fid] : undefined;

const sample = (arr, n = 5) => arr.slice(0, Math.min(n, arr.length));

hr("INDEX DEBUG");

kv({
  hasWrapperIndex: !!raw.index,
  hasMeta: Array.isArray(meta),
  metaLen: meta.length,
  totalBuckets: num(keys.length),
});

if (!keys.length) {
  console.log("EMPTY INDEX");
  process.exit(0);
}

const sampleKey = keys[0];
const bucket = idx[sampleKey];

hr("SAMPLE BUCKET");

kv({
  sampleKey,
  sampleBucketType: Array.isArray(bucket) ? "array" : typeof bucket,
  sampleBucketLen: bucket?.length ?? null,
});

if (Array.isArray(bucket)) {
  const first = bucket.slice(0, 6).map((it) => {
    if (!Array.isArray(it) || it.length !== 2) return { bad: true, value: it };

    const fid = it[0];
    const times = it[1];

    return {
      fid,
      fileName: nameOf(fid),
      timesType: Array.isArray(times) ? "array" : typeof times,
      timesLen: Array.isArray(times) ? times.length : null,
      timesFirst: Array.isArray(times) ? sample(times, 8) : times,
    };
  });

  console.log(pretty(first));
} else {
  console.log(pretty(bucket));
}

let bucketStats = {
  nonArray: 0,
  empty: 0,
  len1: 0,
  len2p: 0,
  maxLen: 0,
};

let entryStats = {
  totalEntries: 0,
  validFidTimes: 0,
  badShape: 0,
  badTimes: 0,
  maxTimesLen: 0,
};

let fileHits = new Map();

for (const k of keys) {
  const b = idx[k];

  if (!Array.isArray(b)) {
    bucketStats.nonArray++;
    continue;
  }

  const L = b.length;
  if (L === 0) bucketStats.empty++;
  else if (L === 1) bucketStats.len1++;
  else bucketStats.len2p++;
  if (L > bucketStats.maxLen) bucketStats.maxLen = L;

  for (const it of b) {
    entryStats.totalEntries++;

    if (!Array.isArray(it) || it.length !== 2) {
      entryStats.badShape++;
      continue;
    }

    const fid = it[0];
    const times = it[1];

    if (!Array.isArray(times)) {
      entryStats.badTimes++;
      continue;
    }

    entryStats.validFidTimes++;
    if (times.length > entryStats.maxTimesLen) entryStats.maxTimesLen = times.length;

    fileHits.set(fid, (fileHits.get(fid) || 0) + 1);
  }
}

hr("BUCKET STATS");

kv({
  nonArray: `${bucketStats.nonArray} (${pct(bucketStats.nonArray, keys.length)})`,
  empty: bucketStats.empty,
  len1: bucketStats.len1,
  len2p: bucketStats.len2p,
  maxLen: bucketStats.maxLen,
});

hr("ENTRY STATS");

kv({
  totalEntries: num(entryStats.totalEntries),
  validFidTimes: `${num(entryStats.validFidTimes)} (${pct(entryStats.validFidTimes, entryStats.totalEntries)})`,
  badShape: `${num(entryStats.badShape)} (${pct(entryStats.badShape, entryStats.totalEntries)})`,
  badTimes: `${num(entryStats.badTimes)} (${pct(entryStats.badTimes, entryStats.totalEntries)})`,
  maxTimesLen: entryStats.maxTimesLen,
});

const topFiles = Array.from(fileHits.entries())
  .sort((a, b) => b[1] - a[1])
  .slice(0, 15)
  .map(([fid, c]) => ({
    fid,
    hits: c,
    name: nameOf(fid),
  }));

hr("TOP FILE IDS IN INDEX (by occurrences)");
console.log(pretty(topFiles));

hr("PAIR MATCH DEBUG");

const SEP = "|";
const pair = new Map();

for (const k of keys) {
  const b = idx[k];
  if (!Array.isArray(b) || b.length < 2) continue;

  for (let i = 0; i < b.length; i++) {
    const itA = b[i];
    if (!Array.isArray(itA) || itA.length !== 2) continue;
    const a = itA[0];

    for (let j = i + 1; j < b.length; j++) {
      const itB = b[j];
      if (!Array.isArray(itB) || itB.length !== 2) continue;
      const c = itB[0];

      if (a === c) continue;
      const pk = a < c ? `${a}${SEP}${c}` : `${c}${SEP}${a}`;
      pair.set(pk, (pair.get(pk) || 0) + 1);
    }
  }
}

const topPairs = Array.from(pair.entries())
  .sort((a, b) => b[1] - a[1])
  .slice(0, 25)
  .map(([pk, count]) => {
    const [a, b] = pk.split(SEP);
    const ai = +a, bi = +b;
    const an = Number.isFinite(ai) ? nameOf(ai) : undefined;
    const bn = Number.isFinite(bi) ? nameOf(bi) : undefined;

    return {
      pair: `${a} <> ${b}`,
      count,
      A: an ? `${a}:${an}` : a,
      B: bn ? `${b}:${bn}` : b,
    };
  });

kv({
  pairCountSize: num(pair.size),
  topPairsShown: topPairs.length,
});

console.log("\nTop pairs:");
console.log(pretty(topPairs));

hr("DONE");
