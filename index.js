import {
    spawn
} from "node:child_process";
import {
    readdir,
    stat,
    open as fsOpen
} from "node:fs/promises";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import os from "node:os";

const EXTS = new Set([".mp3", ".wav", ".flac", ".m4a", ".aac", ".ogg", ".opus", ".wma", ".alac", ".aiff"]);
const CACHE_FILE = ".dedupe-cache.json";
const QUICK_BYTES = 181072;
const OUT = fs.createWriteStream("dupe.txt", { flags: "w" });
const write = (...a) => OUT.write(a.join(" ") + "\n");

const walk = async d => {
    const o = [];
    const rec = async dir => {
        for (const e of await readdir(dir, {
                withFileTypes: true
            })) {
            const f = path.join(dir, e.name);
            if (e.isDirectory()) await rec(f);
            else if (EXTS.has(path.extname(f).toLowerCase())) o.push(f);
        }
    };
    await rec(d);
    return o;
};

const loadCache = () => {
    try {
        return JSON.parse(fs.readFileSync(CACHE_FILE, "utf8"))
    } catch {
        return {}
    }
};
const saveCache = c => {
    try {
        fs.writeFileSync(CACHE_FILE, JSON.stringify(c))
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
            next()
        });
    };
    return fn => new Promise((res, rej) => {
        q.push({
            fn,
            res,
            rej
        });
        next()
    });
};

const quickHash = async (f, b = QUICK_BYTES) => {
    try {
        const fh = await fsOpen(f, "r");
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
    const h = crypto.createHash("sha256");
    const s = fs.createReadStream(f);
    s.on("data", d => h.update(d));
    s.on("end", () => r(h.digest("hex")));
    s.on("error", () => r(null));
});

const runCmdHash = (cmd, args) =>
    new Promise(r => {
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
        const m = b.match(/FINGERPRINT=([^\n\r]+)/);
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
        r(Number.isFinite(v) ? v : null)
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
        const start = i * step;
        const h = await runCmdHash("ffmpeg", ["-v", "error", "-ss", String(start), "-t", String(w), "-i", f, "-ar", "44100", "-ac", "1", "-f", "s16le", "-"]);
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
            const o = items[j];
            if (chromaSimilarity(base.fpRaw, o.fpRaw) >= t) {
                cl.push(o.file);
                used.add(j)
            }
        }
        if (cl.length > 1) out.push(cl);
    }
    return out;
};

async function run(files, conc = Math.max(1, Math.min(8, Math.floor(os.cpus().length / 2)))) {
    const cache = loadCache();
    const entries = [];
    for (const f of files) try {
        entries.push({
            file: f,
            st: await stat(f)
        })
    } catch {}
    const bySize = new Map();
    for (const e of entries)(bySize.get(e.st.size) || bySize.set(e.st.size, []).get(e.st.size)).push(e);

    const ql = pLimit(Math.max(2, Math.floor(os.cpus().length)));
    const hl = pLimit(conc);
    const fast = [];
    for (const [, group] of bySize) {
        await Promise.all(group.map(x => ql(async () => {
            const k = fileKey(x.st);
            const c = cache[k] || (cache[k] = {});
            if (!c.quickHash) c.quickHash = await quickHash(x.file);
            fast.push({
                file: x.file,
                size: x.st.size,
                mtimeMs: x.st.mtimeMs,
                ...c
            });
        })));
    }

    const buckets = new Map();
    for (const r of fast)(buckets.get(`${r.size}:${r.quickHash||""}`) || buckets.set(`${r.size}:${r.quickHash||""}`, []).get(`${r.size}:${r.quickHash||""}`)).push(r);

    const fin = [];
    for (const [, group] of buckets) {
        if (group.length === 1) {
            fin.push(group[0]);
            continue;
        }
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
        })));
    }

    saveCache(cache);
    return fin;
}

(async () => {
    const dir = process.argv[2];
    if (!dir) return console.error("Usage: node dedupe.mjs folder");
    const files = await walk(dir);
    console.error("Found", files.length, "audio files — processing…");
    const data = await run(files);
    write("");
    for (const [k, v] of groupBy(data, "sha256")) {
        write(k);
        v.forEach(f => write(" ", f))
    }
    write("");
    for (const [k, v] of groupBy(data, "pcmSha256")) {
        write(k);
        v.forEach(f => write(" ", f))
    }
    write("");
    for (const c of findClusters(data)) {
        write("cluster:");
        c.forEach(f => write(" ", f))
    }
    OUT.end();
})();