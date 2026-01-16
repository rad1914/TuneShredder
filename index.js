import {
    spawn
} from "node:child_process";
import {
    readdir,
    stat,
    open
} from "node:fs/promises";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import os from "node:os";
import {
    createProgress
} from "./progress.js";

const EXTS = new Set([".mp3", ".wav", ".flac", ".m4a", ".aac", ".ogg", ".opus", ".wma", ".alac", ".aiff"]);
const CACHE_FILE = ".dedupe-cache.json";
const QUICK_BYTES = 181072;
const OUT = fs.createWriteStream("dupe.txt", {
    flags: "w"
});

const w = (...a) => OUT.write(a.join(" ") + "\n");

const walk = async dir => {
    const out = [];
    const rec = async d => {
        for (const e of await readdir(d, {
                withFileTypes: true
            })) {
            const pth = path.join(d, e.name);
            if (e.isDirectory()) await rec(pth);
            else if (EXTS.has(path.extname(pth).toLowerCase())) out.push(pth);
        }
    };
    await rec(dir);
    return out;
};

const loadCache = () => {
    try {
        return JSON.parse(fs.readFileSync(CACHE_FILE, "utf8"));
    } catch {
        return {};
    }
};
const saveCache = c => {
    try {
        fs.writeFileSync(CACHE_FILE, JSON.stringify(c));
    } catch {}
};
const fileKey = s => `${s.dev}:${s.ino}:${s.size}:${s.mtimeMs}`;

const pLimit = n => {
    const q = [];
    let a = 0;
    const next = () => {
        if (a >= n || !q.length) return;
        a++;
        const t = q.shift();
        t.fn().then(t.res, t.rej).finally(() => {
            a--;
            next();
        });
    };
    return fn => new Promise((res, rej) => {
        q.push({
            fn,
            res,
            rej
        });
        next();
    });
};

const quickHash = async (fn, b = QUICK_BYTES) => {
    try {
        const fh = await open(fn, "r");
        const st = await fh.stat();
        const n = Math.min(b, st.size);
        const head = Buffer.alloc(n);
        await fh.read(head, 0, n, 0);
        let tail = Buffer.alloc(0);
        if (st.size > n) {
            const tn = Math.min(b, st.size - n);
            tail = Buffer.alloc(tn);
            await fh.read(tail, 0, tn, st.size - tn);
        }
        await fh.close();
        const h = crypto.createHash("md5");
        h.update(head);
        if (tail.length) h.update(tail);
        return h.digest("hex");
    } catch {
        return null;
    }
};

const sha256File = f => new Promise(r => {
    const h = crypto.createHash("sha256"),
        s = fs.createReadStream(f);
    s.on("data", d => h.update(d));
    s.on("end", () => r(h.digest("hex")));
    s.on("error", () => r(null));
});

const runCmdHash = (cmd, args) => new Promise(r => {
    const p = spawn(cmd, args);
    const h = crypto.createHash("sha256");
    p.stdout.on("data", d => h.update(d));
    p.on("close", c => r(c === 0 ? h.digest("hex") : null));
    p.on("error", () => r(null));
});

const pcmSha256 = f => runCmdHash("ffmpeg", ["-v", "error", "-i", f, "-ar", "44100", "-ac", "1", "-f", "s16le", "-"]);
const fpRaw = f => new Promise(r => {
    const p = spawn("fpcalc", ["-raw", f]);
    let b = "";
    p.stdout.on("data", d => b += d);
    p.on("close", () => {
        const m = b.match(/FINGERPRINT=([^\r\n]+)/);
        if (!m) return r(null);
        const arr = m[1].trim().split(/[, ]+/).map(Number).filter(Number.isFinite);
        r(arr.length ? arr : null);
    });
    p.on("error", () => r(null));
});

const getDuration = f => new Promise(r => {
    const p = spawn("ffprobe", ["-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", f]);
    let o = "";
    p.stdout.on("data", d => o += d);
    p.on("close", () => {
        const v = parseFloat(o);
        r(Number.isFinite(v) ? v : null);
    });
    p.on("error", () => r(null));
});

const pcmWindowHashes = async (f, winSec = 10, stepSec = 5, maxWindows = 20) => {
    const dur = await getDuration(f).catch(() => null);
    if (!dur || dur <= 0) {
        const s = await pcmSha256(f).catch(() => null);
        return s ? [s] : null;
    }
    const w = Math.max(1, Math.min(winSec, Math.floor(dur)));
    const step = Math.max(1, Math.min(stepSec, Math.floor(w / 2)));
    const count = Math.min(maxWindows, Math.max(1, Math.floor((dur - w) / step) + 1));
    const hashes = [];
    for (let i = 0; i < count; i++) {
        const h = await runCmdHash("ffmpeg", ["-v", "error", "-ss", String(i * step), "-t", String(w), "-i", f, "-ar", "44100", "-ac", "1", "-f", "s16le", "-"]);
        if (h) hashes.push(h);
    }
    return hashes.length ? hashes : null;
};

const groupBy = (list, key) => {
    const m = new Map();
    for (const x of list) {
        const v = x[key];
        if (!v) continue;
        const k = typeof v === "object" ? JSON.stringify(v) : String(v);
        (m.get(k) || m.set(k, []).get(k)).push(x.file);
    }
    return [...m.entries()].filter(([, v]) => v.length > 1);
};

const chromaSimilarity = (a, b) => {
    if (!a || !b) return 0;
    const L = Math.min(a.length, b.length);
    if (!L) return 0;
    let m = 0;
    for (let i = 0; i < L; i++)
        if ((a[i] >> 12) === (b[i] >> 12)) m++;
    return m / L;
};

const findClusters = (items, t = 0.65) => {
    const out = [],
        used = new Set();
    for (let i = 0; i < items.length; i++) {
        if (used.has(i)) continue;
        const base = items[i],
            cl = [base.file];
        used.add(i);
        for (let j = i + 1; j < items.length; j++) {
            if (used.has(j)) continue;
            if (chromaSimilarity(base.fpRaw, items[j].fpRaw) >= t) {
                cl.push(items[j].file);
                used.add(j);
            }
        }
        if (cl.length > 1) out.push(cl);
    }
    return out;
};

const findPartialMatches = (items, overlapThreshold = 0.3, minShared = 2) => {
    const hm = new Map();
    for (const it of items)
        if (it.pcmWindows?.length)
            for (const h of it.pcmWindows) {
                (hm.get(h) || hm.set(h, []).get(h)).push(it.file);
            }
    const pairCounts = new Map();
    for (const files of hm.values())
        if (files.length > 1)
            for (let i = 0; i < files.length; i++)
                for (let j = i + 1; j < files.length; j++) {
                    const a = files[i],
                        b = files[j];
                    const k = a < b ? `${a}\0${b}` : `${b}\0${a}`;
                    pairCounts.set(k, (pairCounts.get(k) || 0) + 1);
                }
    const clusters = [];
    for (const [k, c] of pairCounts) {
        if (c < minShared) continue;
        const [a, b] = k.split("\0");
        const A = items.find(x => x.file === a),
            B = items.find(x => x.file === b);
        if (!A || !B) continue;
        const minW = Math.min(A.pcmWindows?.length || 1, B.pcmWindows?.length || 1) || 1;
        if (c / minW < overlapThreshold) continue;
        let found = null;
        for (const s of clusters)
            if (s.has(a) || s.has(b)) {
                found = s;
                break;
            }
        if (found) {
            found.add(a);
            found.add(b);
        } else clusters.push(new Set([a, b]));
    }
    return clusters.map(s => [...s]);
};

async function run(files, conc = Math.max(1, Math.min(8, Math.floor(os.cpus().length / 2)))) {
    const cache = loadCache();
    const entries = [];
    for (const f of files) try {
        entries.push({
            file: f,
            st: await stat(f)
        });
    } catch {}

    const quickProgress = createProgress(entries.length, "Quick: ");
    const ql = pLimit(Math.max(2, Math.floor(os.cpus().length)));
    const hl = pLimit(conc);
    const fast = [];

    // Compute quickHash for all entries regardless of file size so cropped / trimmed
    // variants are not dropped prematurely. Keep concurrency with pLimit.
    await Promise.all(entries.map(x => ql(async () => {
        const k = fileKey(x.st);
        const c = cache[k] || (cache[k] = {});
        if (!c.quickHash) c.quickHash = await quickHash(x.file);
        fast.push({
            file: x.file,
            size: x.st.size,
            mtimeMs: x.st.mtimeMs,
            ...c
        });
        quickProgress.tick();
    })));
    quickProgress.done();

    const buckets = new Map();
    for (const r of fast) {
        // Key primarily by quickHash so files with different sizes but same quickHash
        // (e.g. trimmed/cropped versions) are processed together. If quickHash is
        // unavailable fall back to grouping by size to avoid grouping everything.
        const key = r.quickHash ? String(r.quickHash) : `size:${r.size}`;
        (buckets.get(key) || buckets.set(key, []).get(key)).push(r);
    }

    const fin = [];

    const totalFin = Array.from(buckets.values()).reduce((s, g) => s + g.length, 0);
    const heavyProgress = createProgress(totalFin, "Heavy: ");
    for (const group of buckets.values()) {
        await Promise.all(group.map(x => hl(async () => {
            const st = await stat(x.file).catch(() => null);
            if (!st) return;
            const k = fileKey(st);
            const c = cache[k] || (cache[k] = {});
            if (!c.sha256) c.sha256 = await sha256File(x.file);
            if (!c.pcmWindows) c.pcmWindows = await pcmWindowHashes(x.file);
            if (!c.pcmSha256) c.pcmSha256 = c.pcmWindows?.length ? crypto.createHash("sha256").update(c.pcmWindows.join("|")).digest("hex") : await pcmSha256(x.file);
            if (!c.fpRaw) c.fpRaw = await fpRaw(x.file);
            fin.push({
                file: x.file,
                size: st.size,
                mtimeMs: st.mtimeMs,
                ...c
            });
            heavyProgress.tick();
        })));
    }
    heavyProgress.done();

    saveCache(cache);
    return fin;
}

(async () => {
    const dir = process.argv[2];
    if (!dir) return console.error("Usage: node dedupe.mjs folder");
    const files = await walk(dir);
    console.error("Found", files.length, "audio files — processing…");
    const data = await run(files);
    w("");
    for (const [k, v] of groupBy(data, "sha256")) {
        w(k);
        v.forEach(f => w(" ", f));
    }
    w("");
    for (const [k, v] of groupBy(data, "pcmSha256")) {
        w(k);
        v.forEach(f => w(" ", f));
    }
    w("");
    for (const c of findClusters(data)) c.forEach(f => w(" ", f));
    const partials = findPartialMatches(data, 0.30, 2);
    for (const c of partials) {
        w("partial-cluster:");
        c.forEach(f => w(" ", f));
    }
    OUT.end();
})();