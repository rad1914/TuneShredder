import {
    spawn
} from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import FFT from "fft.js";

const dat = "./data/index.json";
const workdir = "./t/";
const l = 25 * 1024 * 1024;

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
    bucket: 250
};

const hann = (() => {
    const w = new Float32Array(CFG.win);
    for (let i = 0; i < w.length; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (w.length - 1)));
    return w;
})();

const clamp = (x, lo, hi) => (x < lo ? lo : x > hi ? hi : x);

const audioToF32 = (file) =>
    new Promise((res, rej) => {
        const p = spawn("ffmpeg", ["-hide_banner", "-loglevel", "error", "-i", file, "-ac", "" + CFG.ch, "-ar", "" + CFG.sr, "-f", "f32le", "-"]);
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

function fingerprintFromSamples(x, fileId) {
    const half = CFG.win >> 1;
    const fft = new FFT(CFG.win);
    const complex = fft.createComplexArray();
    const inbuf = new Float64Array(CFG.win);

    // ring buffer holding recent frames (only need up to CFG.zone frames)
    const buf = [];
    let t = 0;

    const outMap = Object.create(null);

    const makeBestStruct = () => ({
        bestF: new Int32Array(CFG.pairs).fill(-1),
        bestDt: new Int32Array(CFG.pairs),
        bestS: new Float32Array(CFG.pairs).fill(-Infinity)
    });

    const emitFrameResults = (frame) => {
        for (const f1q of frame.peaks) {
            const state = frame.best.get(f1q);
            if (!state) continue;
            for (let p = 0; p < CFG.pairs; p++) {
                if (state.bestF[p] !== -1) {
                    const key = `${f1q}-${state.bestF[p]}-${Math.round(state.bestDt[p] / CFG.dtq)}`;
                    const arr = (outMap[key] ||= []);
                    if (arr.length < CFG.bucket) arr.push([fileId, frame.t]);
                }
            }
        }
    };

    for (let pos = 0; pos + CFG.win <= x.length; pos += CFG.hop, t++) {
        for (let i = 0; i < CFG.win; i++) inbuf[i] = x[pos + i] * hann[i];
        fft.realTransform(complex, inbuf);

        const mags = new Float32Array(half);
        for (let b = 0; b < half; b++) mags[b] = Math.log1p(Math.hypot(complex[b * 2] || 0, complex[b * 2 + 1] || 0));

        const peaks = topK(mags, CFG.top).map((b) => Math.round(b / CFG.fq));
        const frame = {
            t,
            peaks,
            mags,
            best: new Map()
        };

        // initialize best structs for each peak in this new frame
        for (const f of peaks) frame.best.set(f, makeBestStruct());

        // For each earlier frame in buffer (anchors), update their best arrays
        for (let i = 0; i < buf.length; i++) {
            const anchor = buf[i];
            const dt = t - anchor.t;
            if (dt > CFG.zone) {
                // out of zone: finalize and emit anchor results
                emitFrameResults(anchor);
                // drop from buffer
                buf.splice(i, 1);
                i--;
                continue;
            }

            // update best arrays for each peak in anchor using this current frame
            for (const f1q of anchor.peaks) {
                const state = anchor.best.get(f1q);
                if (!state) continue;

                for (const f2q of peaks) {
                    const b1 = clamp(f1q, 0, anchor.mags.length - 1);
                    const b2 = clamp(f2q, 0, mags.length - 1);
                    const s = (anchor.mags[b1] || 1e-9) * (mags[b2] || 1e-9);

                    // insert into sorted best arrays
                    for (let p = 0; p < CFG.pairs; p++) {
                        if (s > state.bestS[p]) {
                            for (let k = CFG.pairs - 1; k > p; k--) {
                                state.bestS[k] = state.bestS[k - 1];
                                state.bestF[k] = state.bestF[k - 1];
                                state.bestDt[k] = state.bestDt[k - 1];
                            }
                            state.bestS[p] = s;
                            state.bestF[p] = f2q;
                            state.bestDt[p] = dt;
                            break;
                        }
                    }
                }
            }
        }

        // push current frame to buffer
        buf.push(frame);

        // keep buffer length bounded
        while (buf.length > CFG.zone + 2) {
            const old = buf.shift();
            emitFrameResults(old);
        }
    }

    // after processing all frames, flush buffer
    while (buf.length) {
        const f = buf.shift();
        emitFrameResults(f);
    }

    return outMap;
}

async function fingerprint(filePath, fileId) {
    const samples = await audioToF32(filePath);
    try {
        const m = fingerprintFromSamples(samples, fileId);
        return m;
    } finally {
        // help GC
        // eslint-disable-next-line no-unused-expressions
        null;
    }
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
    if (Buffer.byteLength(full, "utf8") <= l) return atomicWrite(outFile, full);

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
        if (curSize + sz > l && Object.keys(cur.index).length) {
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
            const times = per[fidStr].sort((a, b) => a - b);
            if (times.length) out.push([+fidStr, times]);
        }
        index[k] = out;
    }
}

export async function buildIndex() {
    let outFile = dat;
    try {
        const st = await fs.stat(outFile);
        if (st.isDirectory()) outFile = path.join(outFile, "index.json");
    } catch {}

    let files = (await fs.readdir(workdir)).filter((n) => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));

    let index = Object.create(null);
    let meta = [];

    try {
        const parsed = JSON.parse(await fs.readFile(outFile, "utf8"));
        if (parsed?.index) index = parsed.index;
        if (Array.isArray(parsed?.meta)) meta = parsed.meta.slice();
    } catch {}

    if (meta.length) {
        const done = new Set(meta.map((m) => path.basename(String(m)).toLowerCase()));
        files = files.filter((f) => !done.has(path.basename(f).toLowerCase()));
    }

    let fileId = meta.length;

    for (let i = 0; i < files.length; i++) {
        const name = files[i];
        try {
            mergeIndex(index, await fingerprint(path.join(workdir, name), fileId++));
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

if (import.meta.url === `file://${process.argv[1]}`) buildIndex().catch((e) => (console.error(e), process.exit(1)));