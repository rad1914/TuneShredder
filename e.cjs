const fs = require("fs");

// Load Data
const raw = JSON.parse(fs.readFileSync("index.json", "utf8"));
const idx = raw.index || raw;
const meta = raw.meta || [];
const keys = Object.keys(idx);

// 1. General Header
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

// 2. Sample Data inspection
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

// 3. Aggregate Statistics
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

// 4. Top Files
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

// 5. Co-occurrence Logic
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

const topPairs = Array.from(pair.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 25)
    .map(([pk, count]) => {
        const [a, b] = pk.split(SEP);
        const ai = +a, bi = +b;
        const an = (Number.isFinite(ai) && meta[ai]) ? meta[ai] : null;
        const bn = (Number.isFinite(bi) && meta[bi]) ? meta[bi] : null;

        return {
            "Count": count.toLocaleString(),
            "ID Pair": `${a} ↔ ${b}`,
            "File A": an || `(ID: ${a})`,
            "File B": bn || `(ID: ${b})`
        };
    });

console.log("Top 25 Co-occurring Pairs:");
console.table(topPairs);
console.log("\n=== DONE ===");