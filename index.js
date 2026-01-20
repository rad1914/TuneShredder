import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import FFT from "fft.js";
import DB from "better-sqlite3";
import { dbInit } from "./utils.js";

const DBFILE = "./db/fp.sqlite";
const EXT = /\.(mp3|wav|flac|ogg|opus|m4a)$/i;

const C = {
    sr: 22050,
    ch: 1,
    win: 4096,
    hop: 512,
    top: 14,
    fan: 4,
    zone: 30,
    min: 0.02,
    anchorEvery: 2,
};

const WIN = (() => {
    const w = new Float32Array(C.win),
        n = w.length - 1;
    for (let i = 0; i < w.length; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / n));
    return w;
})();

const H = (f1, f2, dt) => (((f1 & 2047) << 17) | ((f2 & 2047) << 6) | (dt & 63)) | 0;

const mkPeaks = () => {
    const fft = new FFT(C.win);
    const inp = new Float32Array(C.win);
    const out = fft.createComplexArray();
    const mags = new Float32Array(C.win / 2);
    const bins = new Int32Array(C.top);
    const mag = new Float32Array(C.top);
    const ret = {
        bins,
        n: 0
    };

    return (frame) => {
        for (let i = 0; i < C.win; i++) inp[i] = frame[i] * WIN[i];
        fft.realTransform(out, inp);
        fft.completeSpectrum(out);

        for (let k = 0; k < mags.length; k++) {
            const re = out[2 * k],
                im = out[2 * k + 1];
            mags[k] = re * re + im * im;
        }

        bins.fill(0);
        mag.fill(0);

        for (let k = 2; k < mags.length - 2; k++) {
            const a = mags[k];
            if (a < C.min) continue;
            if (!(a > mags[k - 1] && a > mags[k + 1] && a > mags[k - 2] && a > mags[k + 2])) continue;

            let ins = -1;
            for (let i = 0; i < C.top; i++)
                if (a > mag[i]) {
                    ins = i;
                    break;
                }
            if (ins < 0) continue;

            for (let i = C.top - 1; i > ins; i--)(mag[i] = mag[i - 1]), (bins[i] = bins[i - 1]);
            mag[ins] = a;
            bins[ins] = k;
        }

        let n = 0;
        while (n < C.top && mag[n] > 0) n++;
        ret.n = n;
        return ret;
    };
};

const eachFrame = (file, cb) =>
    new Promise((res, rej) => {
        const p = spawn(
            "ffmpeg",
            [
                "-v", "error",
                "-nostdin",
                "-threads", "0",
                "-i", file,
                "-t", 85,
                "-ac", "" + C.ch,
                "-ar", "" + C.sr,
                "-vn", "-sn", "-dn",
                "-f", "f32le",
                "pipe:1",
            ], {
                stdio: ["ignore", "pipe", "inherit"]
            }
        );

        const bps = 4,
            win = C.win,
            hop = C.hop;
        const ringBuf = Buffer.allocUnsafe(2 * win * bps);
        const ring = new Float32Array(ringBuf.buffer, ringBuf.byteOffset, 2 * win);

        const hopBytes = hop * bps;
        const hopBuf = Buffer.allocUnsafe(hopBytes);
        const frame = new Float32Array(win);

        let hopFill = 0,
            filled = 0,
            head = 0,
            t = 0;

        const emit = () => {
            const a = head;
            const b = win - a;
            for (let i = 0; i < b; i++) frame[i] = ring[a + i];
            for (let i = 0; i < a; i++) frame[b + i] = ring[i];
            cb(frame, t++);
        };

        p.stdout.on("data", (chunk) => {
            for (let off = 0; off < chunk.length;) {
                const take = Math.min(hopBytes - hopFill, chunk.length - off);
                chunk.copy(hopBuf, hopFill, off, off + take);
                hopFill += take;
                off += take;
                if (hopFill !== hopBytes) continue;
                hopFill = 0;

                const v = new Float32Array(hopBuf.buffer, hopBuf.byteOffset, hop);
                ring.set(v, head);
                ring.set(v, head + win);
                head = (head + hop) % win;
                filled = Math.min(win, filled + hop);
                if (filled === win) emit();
            }
        });

        p.on("close", (c) => (c ? rej(new Error("ffmpeg decode failed")) : res(t)));
    });

async function build(dir) {
    const db = dbInit();

    const insTrack = db.prepare("INSERT OR IGNORE INTO tracks(name) VALUES(?)");
    const getTrack = db.prepare("SELECT id FROM tracks WHERE name=?");

    const MAX_MULTI = 512;
    const insFpMany = db.prepare(
        "INSERT OR IGNORE INTO fp(h,id,t) VALUES " +
        Array.from({
            length: MAX_MULTI
        }, () => "(?,?,?)").join(",")
    );

    const B = 200_000;
    const Hh = new Int32Array(B);
    const Tt = new Int32Array(B);
    let bn = 0;

    const stmtCache = new Map();
    const getInsStmt = (take) => {
        let st = stmtCache.get(take);
        if (st) return st;
        st = db.prepare(
            "INSERT OR IGNORE INTO fp(h,id,t) VALUES " +
            Array.from({ length: take }, () => "(?,?,?)").join(",")
        );
        stmtCache.set(take, st);
        return st;
    };

    const argsBuf = new Array(MAX_MULTI * 3);

    const addBatch = db.transaction((id, H, T, n) => {
        let i = 0;
        while (i < n) {
            const take = Math.min(MAX_MULTI, n - i);
            const args = argsBuf;

            for (let k = 0; k < take; k++) {
                args[3 * k] = H[i + k];
                args[3 * k + 1] = id;
                args[3 * k + 2] = T[i + k];
            }

            if (take === MAX_MULTI) {
                insFpMany.run(...args);
            } else {
                getInsStmt(take).run(...args.slice(0, take * 3));
            }

            i += take;
        }
    });

    const files = (await fs.readdir(dir))
        .filter((f) => EXT.test(f))
        .map((f) => path.join(dir, f));

    const peaks = mkPeaks();

    for (const file of files) {
        const name = path.basename(file);
        const row = getTrack.get(name);
        if (row) {
            console.log("skip:", name);
            continue;
        }

        console.log("indexing:", name);
        insTrack.run(name);
        const id = getTrack.get(name).id;

        const flush = () => {
            if (bn) addBatch(id, Hh, Tt, bn), (bn = 0);
        };
        const add = (h, t) => {
            Hh[bn] = h;
            Tt[bn] = t;
            if (++bn === B) flush();
        };

        const ringBins = Array.from({
            length: C.zone + 1
        }, () => new Int32Array(C.top));
        const ringN = new Int8Array(C.zone + 1);
        let rp = 0;

        await eachFrame(file, (frame, t) => {
            const p = peaks(frame);
            ringBins[rp].set(p.bins);
            ringN[rp] = p.n;

            if (p.n && (t % C.anchorEvery === 0)) {
                const f2n = Math.min(C.fan, p.n);

                for (let back = 1; back <= C.zone; back++) {
                    const pos = (rp - back + ringBins.length) % ringBins.length;
                    const nPrev = ringN[pos];
                    if (!nPrev) continue;

                    const prev = ringBins[pos];
                    const tt = t - back;

                    for (let i = 0; i < nPrev; i++) {
                        const f1 = prev[i];
                        for (let j = 0; j < f2n; j++) add(H(f1, p.bins[j], back), tt);
                    }
                }
            }

            rp = (rp + 1) % ringBins.length;
        });

        flush();
    }

    db.exec(`CREATE INDEX IF NOT EXISTS idx_fp_id ON fp(id);`);

    const tracks = db.prepare("SELECT COUNT(*) c FROM tracks").get().c;
    const buckets = db.prepare("SELECT COUNT(DISTINCT h) c FROM fp").get().c;
    db.close();

    console.log("done:", tracks, "tracks,", buckets, "buckets");
}

const [cmd, dir] = process.argv.slice(2);
if (cmd !== "index" || !dir) throw new Error("usage: node index.js index <music_dir>");
await build(dir);