import { spawn } from 'node:child_process';
import { readdir, stat } from 'node:fs/promises';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import os from 'node:os';

const EXTS = new Set(['.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.opus', '.wma', '.alac', '.aiff']);
const CACHE_FILE = '.dedupe-cache.json';
const QUICK_BYTES = 64 * 1024;

async function walk(dir) {
    const out = [];
    async function r(d) {
        for (const e of await readdir(d, { withFileTypes: true })) {
            const f = path.join(d, e.name);
            if (e.isDirectory()) await r(f);
            else if (EXTS.has(path.extname(e.name).toLowerCase())) out.push(f);
        }
    }
    await r(dir);
    return out;
}

function loadCache() {
    try {
        const txt = fs.readFileSync(CACHE_FILE, 'utf8');
        return JSON.parse(txt);
    } catch (e) {
        return {};
    }
}

function saveCache(cache) {
    try {
        fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
    } catch (e) {}
}

function fileKey(st) {
    return `${st.dev}:${st.ino}:${st.size}:${st.mtimeMs}`;
}

async function quickHash(file, bytes = QUICK_BYTES) {
    try {
        const fd = await fs.promises.open(file, 'r');
        const stat = await fd.stat();
        const firstLen = Math.min(bytes, stat.size);
        const firstBuf = Buffer.alloc(firstLen);
        await fd.read(firstBuf, 0, firstLen, 0);
        let lastBuf = Buffer.alloc(0);
        if (stat.size > firstLen) {
            const lastLen = Math.min(bytes, stat.size - firstLen);
            lastBuf = Buffer.alloc(lastLen);
            await fd.read(lastBuf, 0, lastLen, stat.size - lastLen);
        }
        await fd.close();
        const h = crypto.createHash('md5');
        h.update(firstBuf);
        if (lastBuf.length) h.update(lastBuf);
        return h.digest('hex');
    } catch (e) {
        return null;
    }
}

function fileSha256(file) {
    return new Promise((res) => {
        const h = crypto.createHash('sha256');
        const s = fs.createReadStream(file);
        s.on('data', d => h.update(d));
        s.on('end', () => res(h.digest('hex')));
        s.on('error', () => res(null));
    });
}

function pcmSha256(file) {
    return new Promise((res) => {
        const p = spawn('ffmpeg', ['-v', 'error', '-i', file, '-ar', '44100', '-ac', '1', '-f', 's16le', '-']);
        const h = crypto.createHash('sha256');
        p.stdout.on('data', d => h.update(d));
        p.on('close', c => res(c === 0 ? h.digest('hex') : null));
        p.on('error', () => res(null));
    });
}

function fpRaw(file) {
    return new Promise(r => {
        const p = spawn('fpcalc', ['-raw', file]);
        let buf = '';
        p.stdout.on('data', d => buf += d.toString());
        p.on('close', () => {
            const m = buf.match(/FINGERPRINT=([^\n\r]+)/);
            if (!m) return r(null);
            const ints = m[1].trim().split(/[, ]+/).map(s => parseInt(s, 10)).filter(n => Number.isFinite(n));
            r(ints.length ? ints : null);
        });
        p.on('error', () => r(null));
    });
}

function pLimit(concurrency) {
    const queue = [];
    let active = 0;
    const next = () => {
        if (active >= concurrency || queue.length === 0) return;
        active++;
        const { fn, resolve, reject } = queue.shift();
        fn().then(resolve, reject).finally(() => {
            active--;
            next();
        });
    };
    return (fn) => new Promise((resolve, reject) => {
        queue.push({ fn, resolve, reject });
        next();
    });
}

async function run(files, concurrency = Math.max(1, Math.min(8, Math.floor(os.cpus().length / 2)))) {
    const cache = loadCache();
    const entries = [];
    for (const f of files) {
        try {
            const st = await stat(f);
            entries.push({ file: f, st });
        } catch (e) {
            console.error('stat failed', f);
        }
    }

    // group by size first
    const sizeMap = new Map();
    for (const e of entries) {
        const s = e.st.size;
        if (!sizeMap.has(s)) sizeMap.set(s, []);
        sizeMap.get(s).push(e);
    }

    const quickLimit = pLimit(Math.max(2, Math.floor(os.cpus().length)));
    const heavyLimit = pLimit(Math.max(1, Math.floor(os.cpus().length / 2)));

    const results = [];

    // First pass: for groups with >1 file compute quickHash (and consult cache)
    for (const [size, group] of sizeMap) {
        if (group.length === 1) {
            const e = group[0];
            const key = fileKey(e.st);
            const cached = cache[key];
            if (cached) {
                results.push(Object.assign({ file: e.file, size: e.st.size, mtimeMs: e.st.mtimeMs }, cached));
            } else {
                // single file, compute quickHash only (cheap) so we can cache
                const q = await quickLimit(() => quickHash(e.file));
                const rec = { quickHash: q };
                cache[key] = rec;
                results.push(Object.assign({ file: e.file, size: e.st.size, mtimeMs: e.st.mtimeMs }, rec));
            }
            continue;
        }

        // group length >1: compute quick hashes in parallel
        await Promise.all(group.map(g => quickLimit(async () => {
            const key = fileKey(g.st);
            const cached = cache[key];
            if (cached && cached.quickHash) {
                results.push(Object.assign({ file: g.file, size: g.st.size, mtimeMs: g.st.mtimeMs }, cached));
                return;
            }
            const q = await quickHash(g.file);
            const rec = { quickHash: q };
            cache[key] = Object.assign(cache[key] || {}, rec);
            results.push(Object.assign({ file: g.file, size: g.st.size, mtimeMs: g.st.mtimeMs }, rec));
        })));
    }

    // Second pass: within size+quickHash buckets that have >1 file, compute heavy fingerprints
    const bucket = new Map();
    for (const r of results) {
        const k = `${r.size}:${r.quickHash || ''}`;
        if (!bucket.has(k)) bucket.set(k, []);
        bucket.get(k).push(r);
    }

    const final = [];
    for (const [k, group] of bucket) {
        if (group.length === 1) {
            final.push(group[0]);
            continue;
        }

        // heavy compute only for files in this bucket
        await Promise.all(group.map(g => heavyLimit(async () => {
            const st = await stat(g.file).catch(() => null);
            if (!st) return;
            const key = fileKey(st);
            const cached = cache[key] || {};
            // compute full sha if missing
            if (!cached.sha256) cached.sha256 = await fileSha256(g.file);
            // compute pcm hash if missing
            if (!cached.pcmSha256) cached.pcmSha256 = await pcmSha256(g.file);
            // compute fingerprint raw if missing
            if (!cached.fpRaw) cached.fpRaw = await fpRaw(g.file);
            cache[key] = cached;
            final.push(Object.assign({ file: g.file, size: st.size, mtimeMs: st.mtimeMs }, cached));
        })));
    }

    saveCache(cache);
    return final;
}

function groupBy(list, key) {
    const m = new Map();
    for (const x of list) {
        const val = x[key];
        if (!val) continue;
        const k = typeof val === 'object' ? JSON.stringify(val) : String(val);
        if (!m.has(k)) m.set(k, []);
        m.get(k).push(x.file);
    }
    return [...m.entries()].filter(([, v]) => v.length > 1);
}

function chromaSimilarity(a, b) {
    if (!a || !b) return 0;
    const L = Math.min(a.length, b.length);
    if (L === 0) return 0;
    let matches = 0;
    for (let i = 0; i < L; i++)
        if ((a[i] >> 12) === (b[i] >> 12)) matches++;
    return matches / L;
}

function findPerceptualClusters(items, threshold = 0.6) {
    const clusters = [];
    const seen = new Set();
    for (let i = 0; i < items.length; i++) {
        if (seen.has(i)) continue;
        const base = items[i];
        const cluster = [base.file];
        seen.add(i);
        for (let j = i + 1; j < items.length; j++) {
            if (seen.has(j)) continue;
            const other = items[j];
            const sim = chromaSimilarity(base.fpRaw, other.fpRaw);
            if (sim >= threshold) {
                cluster.push(other.file);
                seen.add(j);
            }
        }
        if (cluster.length > 1) clusters.push(cluster);
    }
    return clusters;
}

(async () => {
    const dir = process.argv[2];
    if (!dir) return console.error('Usage: node dedupe.mjs folder');

    const files = await walk(dir);
    console.error('Found', files.length, 'audio files — processing (this can take a while)...');

    const data = await run(files, 4);

    console.log('\n== Exact file SHA256 ==');
    for (const [k, v] of groupBy(data, 'sha256')) {
        console.log(k);
        v.forEach(f => console.log(' ', f));
    }

    console.log('\n== PCM content SHA256 ==');
    for (const [k, v] of groupBy(data, 'pcmSha256')) {
        console.log(k);
        v.forEach(f => console.log(' ', f));
    }

    console.log('\n== Perceptual clusters (chromaprint) ==');
    const perceptual = findPerceptualClusters(data, 0.65);
    for (const cluster of perceptual) {
        console.log('cluster:');
        cluster.forEach(f => console.log(' ', f));
    }
})();