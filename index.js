// @path: index.js
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import os from 'node:os';
import fft from 'fft-js';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { renderProgress } from './renderProgress.js';

const CFG = {
  sr: 22050, ch: 1, win: 4096, hop: 512,
  top: 16, zone: 55, pairs: 6,
  sec: 45, fq: 10, dtq: 3, bucket: 250,
};

const hann = (() => {
  const w = new Float32Array(CFG.win);
  for (let i = 0; i < CFG.win; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (CFG.win - 1)));
  return w;
})();

const ffmpegF32 = (file) => new Promise((resolve, reject) => {
  const p = spawn('ffmpeg', [
    '-hide_banner', '-loglevel', 'error',
    '-t', String(CFG.sec),
    '-i', file,
    '-ac', String(CFG.ch),
    '-ar', String(CFG.sr),
    '-f', 'f32le',
    '-',
  ], { stdio: ['ignore', 'pipe', 'pipe'] });

  const chunks = [];
  let err = '';
  p.stdout.on('data', (c) => chunks.push(c));
  p.stderr.on('data', (c) => (err += c));
  p.on('close', (code) => {
    if (code !== 0) return reject(new Error('ffmpeg failed: ' + err.trim()));
    const buf = Buffer.concat(chunks);
    const f = new Float32Array(buf.buffer, buf.byteOffset, (buf.length / 4) | 0);
    resolve(new Float32Array(f));
  });
});

function stftMags(x) {
  const half = CFG.win >> 1, out = [];
  for (let pos = 0; pos + CFG.win <= x.length; pos += CFG.hop) {
    const frame = new Float32Array(CFG.win);
    for (let i = 0; i < CFG.win; i++) frame[i] = x[pos + i] * hann[i];
    const spec = fft.fft(frame);
    const mags = new Float32Array(half);
    for (let i = 0; i < half; i++) mags[i] = Math.log1p(Math.hypot(spec[i][0], spec[i][1]));
    out.push(mags);
  }
  return out;
}

function peaks(frames) {
  return frames.map((row) =>
    Array.from(row.keys())
      .sort((a, b) => row[b] - row[a])
      .slice(0, CFG.top)
      .filter((i) => row[i] > 0)
  );
}

function hashes(pk, mags) {
  const out = [];
  for (let t = 0; t < pk.length; t++) {
    const a = pk[t];
    if (!a?.length) continue;

    for (const f1 of a) {
      const cand = [];
      for (let dt = 1; dt <= CFG.zone; dt++) {
        const t2 = t + dt;
        if (t2 >= pk.length) break;
        for (const f2 of pk[t2]) {
          const b1 = Math.max(0, Math.min(mags[t].length - 1, f1 | 0));
          const b2 = Math.max(0, Math.min(mags[t2].length - 1, f2 | 0));
          cand.push({ f2, dt, mag: (mags[t][b1] || 1e-9) * (mags[t2][b2] || 1e-9) });
        }
      }
      cand.sort((x, y) => y.mag - x.mag);
      for (let i = 0; i < Math.min(CFG.pairs, cand.length); i++) {
        const { f2, dt } = cand[i];
        out.push({
          key: `${Math.round(f1 / CFG.fq)}-${Math.round(f2 / CFG.fq)}-${Math.round(dt / CFG.dtq)}`,
          t,
        });
      }
    }
  }
  return out;
}

function toMap(hs, id) {
  const m = Object.create(null);
  for (const { key, t } of hs) {
    const b = (m[key] ||= []);
    if (b.length < CFG.bucket) b.push([id, t]);
  }
  return m;
}

async function fingerprint(file, id) {
  const x = await ffmpegF32(file);
  const mags = stftMags(x);
  return toMap(hashes(peaks(mags), mags), id);
}

const atomicWrite = async (p, s) => {
  const tmp = p + '.tmp';
  await fs.writeFile(tmp, s, 'utf8');
  await fs.rename(tmp, p);
};

const runWorker = (filePath, trackId) => new Promise((resolve, reject) => {
  const w = new Worker(new URL(import.meta.url), { workerData: { filePath, trackId } });
  w.once('message', (m) => (m?.ok ? resolve(m.map) : reject(new Error(m?.error || 'worker failed'))));
  w.once('error', reject);
  w.once('exit', (c) => c === 0 || reject(new Error('worker exit ' + c)));
});

export async function buildIndex(dir, outFile = 'index.json') {
  const files = (await fs.readdir(dir)).filter((n) => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));
  const merged = Object.create(null);
  const meta = [];
  let done = 0, idx = 0;

  const write = () => atomicWrite(outFile, JSON.stringify({ index: merged, meta }));
  const merge = (map) => {
    for (const k in map) {
      const dst = (merged[k] ||= []);
      const src = map[k];
      for (let j = 0; j < src.length && dst.length < CFG.bucket; j++) dst.push(src[j]);
    }
  };

  renderProgress(done, files.length, '');
  const conc = Math.max(1, Math.min(os.cpus().length || 1, files.length));

  const worker = async () => {
    for (;;) {
      const name = files[idx++];
      if (!name) return;
      try {
        merge(await runWorker(path.join(dir, name), name));
        meta.push(name);
      } catch {}
      done++;
      renderProgress(done, files.length, name);
      await write();
    }
  };

  await Promise.all(Array.from({ length: conc }, worker));
  await write();
}

if (!isMainThread) {
  (async () => {
    try {
      const { filePath, trackId } = workerData;
      parentPort.postMessage({ ok: true, map: await fingerprint(filePath, trackId) });
    } catch (e) {
      parentPort.postMessage({ ok: false, error: e?.message || String(e) });
    }
  })();
}

if (isMainThread && import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const [, , cmd, dir, out] = process.argv;
    if (cmd !== 'build' || !dir) {
      console.error('usage: node fingerprint.js build <dir> [out.json]');
      process.exit(2);
    }
    await buildIndex(dir, out || 'index.json');
  })().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}