// @path: index.js
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import os from 'node:os';
import fft from 'fft-js';
import FFT from 'fft.js';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { renderProgress } from './renderProgress.js';

const CFG = {
  sr: 22050, ch: 1, win: 4096, hop: 512,
  top: 24, zone: 55, pairs: 6,
  sec: 45, fq: 6, dtq: 2, bucket: 250,
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

  const half = CFG.win >> 1;
  const nFrames = Math.max(0, Math.floor((x.length - CFG.win) / CFG.hop) + 1);
  const out = new Array(nFrames);

  const FFT_SIZE = CFG.win;
  const fftPlan = new FFT(FFT_SIZE);
  const complexOut = fftPlan.createComplexArray();
  const realInput = new Float64Array(FFT_SIZE);
  const magsBuf = new Float32Array(half);

  let outIdx = 0;
  for (let pos = 0; pos + CFG.win <= x.length; pos += CFG.hop) {
    for (let i = 0; i < CFG.win; i++) realInput[i] = x[pos + i] * hann[i];
    fftPlan.realTransform(complexOut, realInput);
    for (let b = 0; b < half; b++) {
      const re = complexOut[b * 2] || 0;
      const im = complexOut[b * 2 + 1] || 0;
      magsBuf[b] = Math.log1p(Math.hypot(re, im));
    }
    out[outIdx++] = new Float32Array(magsBuf);
  }
  return out;
}

function topKIndices(row, K) {
  const bestIdx = new Int32Array(K).fill(-1);
  const bestVal = new Float32Array(K).fill(-Infinity);
  for (let i = 0; i < row.length; i++) {
    const v = row[i];
    if (v <= 0) continue;

    let minPos = 0;
    for (let j = 1; j < K; j++) if (bestVal[j] < bestVal[minPos]) minPos = j;
    if (v > bestVal[minPos]) {
      bestVal[minPos] = v;
      bestIdx[minPos] = i;
    }
  }
  const res = [];
  for (let j = 0; j < K; j++) if (bestIdx[j] !== -1) res.push(bestIdx[j]);
  return res;
}

function peaks(frames) {
  return frames.map((row) => topKIndices(row, CFG.top));
}

function hashes(pk, mags) {
  const out = [];

  const quantPK = pk.map((arr) => (arr || []).map((f) => Math.round(f / CFG.fq)));

  for (let t = 0; t < pk.length; t++) {
    const a = quantPK[t];
    if (!a?.length) continue;

    for (const f1q of a) {

      const bestF = new Int32Array(CFG.pairs).fill(-1);
      const bestDt = new Int32Array(CFG.pairs).fill(0);
      const bestMag = new Float32Array(CFG.pairs).fill(-Infinity);

      for (let dt = 1; dt <= CFG.zone; dt++) {
        const t2 = t + dt;
        if (t2 >= pk.length) break;
        for (const f2q of quantPK[t2]) {
          const b1 = Math.min(mags[t].length - 1, Math.max(0, f1q));
          const b2 = Math.min(mags[t2].length - 1, Math.max(0, f2q));
          const score = (mags[t][b1] || 1e-9) * (mags[t2][b2] || 1e-9);

          for (let s = 0; s < CFG.pairs; s++) {
            if (score > bestMag[s]) {
              for (let k = CFG.pairs - 1; k > s; k--) {
                bestMag[k] = bestMag[k - 1];
                bestF[k] = bestF[k - 1];
                bestDt[k] = bestDt[k - 1];
              }
              bestMag[s] = score;
              bestF[s] = f2q;
              bestDt[s] = dt;
              break;
            }
          }
        }
      }

      for (let p = 0; p < CFG.pairs; p++) {
        if (bestF[p] === -1) continue;
        out.push({
          key: `${f1q}-${bestF[p]}-${Math.round(bestDt[p] / CFG.dtq)}`,
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

  let writeCounter = 0;
  const WRITE_EVERY = 16;

  const writeIfNeeded = async () => {
    writeCounter++;
    if (writeCounter % WRITE_EVERY === 0) await atomicWrite(outFile, JSON.stringify({ index: merged, meta }));
  };

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
      await writeIfNeeded();
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
