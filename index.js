import {
    spawn
} from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import FFT from "fft.js";

const INDEX_JSON = "./data/index.json";
const TRACKS_DIR = "./t";
const MAX_BYTES = 2 * 1024 * 1024;

const CFG = {
    sr: 22050,
    ch: 1,
    win: 4096,
    hop: 512,
    top: 24,
    zone: 55,
    pairs: 6,
    fq: 6,
    dtq: 2,
    bucket: 250,
};

const hann = new Float32Array(CFG.win);
for (let i = 0; i < CFG.win; i++) hann[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (CFG.win - 1)));

const clamp = (x, lo, hi) => (x < lo ? lo : x > hi ? hi : x);

const audioToF32 = (file) =>
    new Promise((res, rej) => {
        const p = spawn("ffmpeg", [
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            file,
            "-ac",
            "" + CFG.ch,
            "-ar",
            "" + CFG.sr,
            "-f",
            "f32le",
            "-",
        ]);
        const chunks = [];
        let err = "";
        p.stdout.on("data", (c) => chunks.push(c));
        p.stderr.on("data", (c) => (err += c));
        p.on("close", (code) => {
            if (code) return rej(new Error(err || "ffmpeg error"));
            const buf = Buffer.concat(chunks);
            res(new Float32Array(buf.buffer, buf.byteOffset, buf.length >> 2));
        });
    });

function stftMags(x) {
    const half = CFG.win >> 1;
    const frames = Math.max(0, ((x.length - CFG.win) / CFG.hop) | 0) + 1;

    const fft = new FFT(CFG.win);
    const complex = fft.createComplexArray();
    const inbuf = new Float64Array(CFG.win);
    const out = new Array(frames);

    for (let pos = 0, t = 0; pos + CFG.win <= x.length; pos += CFG.hop, t++) {
        for (let i = 0; i < CFG.win; i++) inbuf[i] = x[pos + i] * hann[i];
        fft.realTransform(complex, inbuf);

        const mags = new Float32Array(half);
        for (let b = 0; b < half; b++) {
            const re = complex[b * 2] || 0;
            const im = complex[b * 2 + 1] || 0;
            mags[b] = Math.log1p(Math.hypot(re, im));
        }
        out[t] = mags;
    }
    return out;
}

function topK(row, K) {
    const idx = new Int32Array(K).fill(-1);
    const val = new Float32Array(K).fill(-Infinity);

    for (let i = 0; i < row.length; i++) {
        const v = row[i];
        if (!(v > 0)) continue;

        let m = 0;
        for (let j = 1; j < K; j++)
            if (val[j] < val[m]) m = j;

        if (v > val[m]) {
            for (let k = K - 1; k > m; k--)(val[k] = val[k - 1]), (idx[k] = idx[k - 1]);
            val[m] = v;
            idx[m] = i;
        }
    }

    const r = [];
    for (let j = 0; j < K; j++)
        if (idx[j] !== -1) r.push(idx[j]);
    return r;
}

function hashes(mags) {
    const peaks = mags.map((f) => topK(f, CFG.top).map((b) => Math.round(b / CFG.fq)));
    const out = [];

    for (let t = 0; t < peaks.length; t++) {
        const a = peaks[t];
        if (!a.length) continue;

        for (const f1q of a) {
            const bestF = new Int32Array(CFG.pairs).fill(-1);
            const bestDt = new Int32Array(CFG.pairs);
            const bestS = new Float32Array(CFG.pairs).fill(-Infinity);

            for (let dt = 1; dt <= CFG.zone; dt++) {
                const t2 = t + dt;
                if (t2 >= peaks.length) break;

                for (const f2q of peaks[t2]) {
                    const b1 = clamp(f1q, 0, mags[t].length - 1);
                    const b2 = clamp(f2q, 0, mags[t2].length - 1);
                    const s = (mags[t][b1] || 1e-9) * (mags[t2][b2] || 1e-9);

                    for (let i = 0; i < CFG.pairs; i++) {
                        if (s > bestS[i]) {
                            for (let k = CFG.pairs - 1; k > i; k--)
                                (bestS[k] = bestS[k - 1]), (bestF[k] = bestF[k - 1]), (bestDt[k] = bestDt[k - 1]);
                            bestS[i] = s;
                            bestF[i] = f2q;
                            bestDt[i] = dt;
                            break;
                        }
                    }
                }
            }

            for (let p = 0; p < CFG.pairs; p++)
                if (bestF[p] !== -1)
                    out.push({
                        key: `${f1q}-${bestF[p]}-${Math.round(bestDt[p] / CFG.dtq)}`,
                        t
                    });
        }
    }
    return out;
}

async function fingerprint(filePath, fileId) {
    const mags = stftMags(await audioToF32(filePath));
    const m = Object.create(null);
    for (const {
            key,
            t
        }
        of hashes(mags)) {
        const a = (m[key] ||= []);
        if (a.length < CFG.bucket) a.push([fileId, t]);
    }
    return m;
}

async function atomicWrite(file, data) {
    try {
        const st = await fs.stat(file);
        if (st.isDirectory()) file = path.join(file, "index.json");
    } catch {}
    const tmp = `${file}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`;
    await fs.writeFile(tmp, data, "utf8");
    await fs.rename(tmp, file);
}

async function writeSplit(outFile, obj) {
    const full = JSON.stringify(obj);
    if (Buffer.byteLength(full, "utf8") <= MAX_BYTES) return atomicWrite(outFile, full);

    const keys = Object.keys(obj.index || {});
    const meta = obj.meta || [];
    let part = 0;
    let cur = {
        index: {},
        meta
    };
    let curSize = Buffer.byteLength(JSON.stringify(cur), "utf8");

    for (const k of keys) {
        const entry = obj.index[k];
        const sz = Buffer.byteLength(JSON.stringify({
            [k]: entry
        }), "utf8");
        if (curSize + sz > MAX_BYTES && Object.keys(cur.index).length) {
            await atomicWrite(`${outFile}.${part++}.json`, JSON.stringify(cur));
            cur = {
                index: {},
                meta
            };
            curSize = Buffer.byteLength(JSON.stringify(cur), "utf8");
        }
        cur.index[k] = entry;
        curSize += sz;
    }

    await atomicWrite(part ? `${outFile}.${part}.json` : outFile, JSON.stringify(cur));
}

function mergeIndex(dst, src) {
    for (const k in src) {
        const a = (dst[k] ||= []);
        const b = src[k];
        for (let i = 0; i < b.length && a.length < CFG.bucket; i++) a.push(b[i]);
    }
}

function normalizeBuckets(index) {
    for (const k of Object.keys(index)) {
        const per = Object.create(null);
        for (const it of index[k]) {
            if (!Array.isArray(it) || it.length < 2) continue;
            const [fid, t] = it;
            (per[fid] ||= []).push(t);
        }
        const out = [];
        for (const fidStr of Object.keys(per)) {
            const fid = +fidStr;
            const times = per[fid].sort((a, b) => a - b);
            if (times.length) out.push([fid, times]);
        }
        index[k] = out;
    }
}

export async function buildIndex() {
    let outFile = INDEX_JSON;
    try {
        const st = await fs.stat(outFile);
        if (st.isDirectory()) outFile = path.join(outFile, "index.json");
    } catch {}

    let files = (await fs.readdir(TRACKS_DIR)).filter((n) => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));

    let index = Object.create(null);
    let meta = [];
    try {
        const parsed = JSON.parse(await fs.readFile(outFile, "utf8"));
        if (parsed?.index) index = parsed.index;
        if (Array.isArray(parsed?.meta)) meta = parsed.meta.slice();
    } catch {}

    if (meta.length) {
        const done = new Set(meta);
        files = files.filter((f) => !done.has(f));
    }

    let fileId = meta.length;

    for (let i = 0; i < files.length; i++) {
        const name = files[i];
        try {
            mergeIndex(index, await fingerprint(path.join(TRACKS_DIR, name), fileId++));
            meta.push(name);
            normalizeBuckets(index);
            await writeSplit(outFile, {
                index,
                meta
            });
            console.log(`progress: ${i + 1}/${files.length} - ${name}`);
        } catch (e) {
            console.error("failed:", name, e?.message || e);
        }
    }

    normalizeBuckets(index);
    await writeSplit(outFile, {
        index,
        meta
    });
}

if (import.meta.url === `file://${process.argv[1]}`) {
    const cmd = process.argv[2];
    buildIndex().catch((e) => (console.error(e), process.exit(1)));
}