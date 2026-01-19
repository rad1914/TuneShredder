const fs = require("fs");
const path = require("path");

const raw = JSON.parse(fs.readFileSync("index.json", "utf8"));
const idx = raw.index || raw;
const meta = raw.meta || [];
const keys = Object.keys(idx);

console.log("=".repeat(60));
console.log(" 🔍 INDEX DIAGNOSTIC REPORT");
console.log("=".repeat(60));

console.table([{
  "Has Wrapper": !!raw.index,
  "Meta Array": Array.isArray(meta),
  "Meta Length": meta.length,
  "Total Buckets": keys.length.toLocaleString()
}]);

if (!keys.length) {
  console.error("❌ EMPTY INDEX: No keys found.");
  process.exit(0);
}

const sampleKey = keys[0];
const bucket = idx[sampleKey];

console.log("\n🔹 SAMPLE BUCKET INSPECTION");
console.log(`Key: "${sampleKey}"`);
console.log(`Type: ${Array.isArray(bucket) ? "Array" : typeof bucket}`);
console.log(`Length: ${bucket?.length ?? "N/A"}`);

if (Array.isArray(bucket)) {
  const first3 = bucket.slice(0, 3).map((it) => {
    if (!Array.isArray(it) || it.length !== 2) return { Status: "BAD SHAPE", Raw: JSON.stringify(it) };
    const [fid, times] = it;
    return {
      "File ID": fid,
      "File Name": (typeof fid === "number" && meta[fid]) ? meta[fid] : "N/A",
      "Times Type": Array.isArray(times) ? `Array(${times.length})` : typeof times,
      "First 5 Times": Array.isArray(times) ? JSON.stringify(times.slice(0, 5)) : times
    };
  });
  console.table(first3);
} else {
  console.log("Value:", bucket);
}

let bucketStats = { nonArray: 0, empty: 0, len1: 0, len2p: 0, maxLen: 0 };
let entryStats = { totalEntries: 0, validFidTimes: 0, badShape: 0, badTimes: 0, maxTimesLen: 0 };
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
    const [fid, times] = it;
    if (!Array.isArray(times)) {
      entryStats.badTimes++;
      continue;
    }
    entryStats.validFidTimes++;
    if (times.length > entryStats.maxTimesLen) entryStats.maxTimesLen = times.length;
    fileHits.set(fid, (fileHits.get(fid) || 0) + 1);
  }
}

console.log("\n📊 STATISTICS SUMMARY");
console.table({
  "Buckets: Non-Array": bucketStats.nonArray,
  "Buckets: Empty": bucketStats.empty,
  "Buckets: Single Entry": bucketStats.len1.toLocaleString(),
  "Buckets: Multi Entry": bucketStats.len2p.toLocaleString(),
  "Max Bucket Len": bucketStats.maxLen.toLocaleString(),
  "Total Entries": entryStats.totalEntries.toLocaleString(),
  "Valid Entries": entryStats.validFidTimes.toLocaleString(),
  "Max Times Len": entryStats.maxTimesLen.toLocaleString()
});

const topFiles = Array.from(fileHits.entries())
  .sort((a, b) => b[1] - a[1])
  .slice(0, 15)
  .map(([fid, c]) => ({
    "File ID": fid,
    "Occurrences": c.toLocaleString(),
    "File Name": (typeof fid === "number" && meta[fid]) ? meta[fid] : "N/A"
  }));

console.log("\n🏆 TOP 15 FILES (by occurrence)");
console.table(topFiles);

console.log("\n🔗 PAIR MATCH ANALYSIS");
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

console.log(`Total Unique Pairs Found: ${pair.size.toLocaleString()}`);

const allPairs = Array.from(pair.entries())
  .sort((a, b) => b[1] - a[1])
  .map(([pk, count]) => {
    const [a, b] = pk.split(SEP);
    const ai = +a, bi = +b;

    return {
      Count: count,
      "ID Pair": `${a} ↔ ${b}`,
      "File A": (Number.isFinite(ai) && meta[ai]) ? meta[ai] : `(ID: ${a})`,
      "File B": (Number.isFinite(bi) && meta[bi]) ? meta[bi] : `(ID: ${b})`
    };
  });

console.log("\n📌 ALL CO-OCCURRING PAIRS (sorted desc):");
console.table(allPairs);

console.log("\n📦 MOVING PAIR MATCH TRACKS TO ./dupe");
const DUPE_DIR = path.resolve("dupe");
fs.mkdirSync(DUPE_DIR, { recursive: true });

for (const row of allPairs) {
  const [a, b] = String(row["ID Pair"]).split(" ↔ ");
  const ai = +a, bi = +b;
  if (!Number.isFinite(ai) || !Number.isFinite(bi)) continue;
  if (!meta[ai] || !meta[bi]) continue;

  const srcA = path.resolve(meta[ai]);
  const srcB = path.resolve(meta[bi]);

  const tag = `${ai}_${bi}_x${row.Count}`;
  const pairDir = path.join(DUPE_DIR, tag);
  fs.mkdirSync(pairDir, { recursive: true });

  const dstA = path.join(pairDir, path.basename(meta[ai]));
  const dstB = path.join(pairDir, path.basename(meta[bi]));

  try { if (fs.existsSync(srcA)) fs.renameSync(srcA, dstA); } catch (e) { console.warn("Move fail A:", srcA, e.message); }
  try { if (fs.existsSync(srcB)) fs.renameSync(srcB, dstB); } catch (e) { console.warn("Move fail B:", srcB, e.message); }
}

console.log("\n=== DONE ===");