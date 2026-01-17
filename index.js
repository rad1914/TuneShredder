// @path: index.js
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import { access } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import path from 'node:path';
import os from 'node:os';
import fft from 'fft-js';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

const SAMPLE_RATE = 22050;
const CHANNELS = 1;
const WINDOW = 4096;
const HOP = 512;
const TOP_PEAKS = 16;
const TARGET_ZONE = 55;
const MAX_PAIRS = 6;
const FINGERPRINT_SECONDS = 45;
const FREQ_Q = 10;
const DT_Q = 3;

const hann = (n) => {
  const w = new Float32Array(n);
  for (let i = 0; i < n; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (n - 1)));
  return w;
};
const hannWin = hann(WINDOW);

const ffmpegToFloat32 = (file) =>
  new Promise((resolve, reject) => {
    const args = [
      '-hide_banner', '-loglevel', 'error',
      '-t', String(FINGERPRINT_SECONDS),
      '-i', file,
      '-ac', String(CHANNELS),
      '-ar', String(SAMPLE_RATE),
      '-f', 'f32le', '-'
    ];
    const p = spawn('ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    const chunks = [];
    let err = '';
    p.stdout.on('data', (c) => chunks.push(c));
    p.stderr.on('data', (c) => (err += c.toString()));
    p.on('close', (code) => {
      if (code !== 0) return reject(new Error('ffmpeg failed: ' + err.trim()));
      const buf = Buffer.concat(chunks);
      const float = new Float32Array(buf.buffer, buf.byteOffset, Math.floor(buf.length / 4));
      resolve(new Float32Array(float));
    });
  });

function stftMagnitudes(signal) {
  const half = WINDOW >> 1;
  const frames = [];
  for (let pos = 0; pos + WINDOW <= signal.length; pos += HOP) {
    const frame = new Float32Array(WINDOW);
    for (let i = 0; i < WINDOW; i++) frame[i] = signal[pos + i] * hannWin[i];
    const spec = fft.fft(frame);
    const mags = new Float32Array(half);
    for (let i = 0; i < half; i++) {
      const m = Math.hypot(spec[i][0], spec[i][1]);
      mags[i] = Math.log1p(m);
    }
    frames.push(mags);
  }
  return frames;
}

function topKIndicesFloat32(arr, k) {
  if (k <= 0) return [];
  if (k === 1) {
    let bi = -1, bv = -Infinity;
    for (let i = 0; i < arr.length; i++) {
      const v = arr[i];
      if (v > bv) { bv = v; bi = i; }
    }
    return bi >= 0 ? [bi] : [];
  }
  if (k === 2) {
    let i1 = -1, v1 = -Infinity;
    let i2 = -1, v2 = -Infinity;
    for (let i = 0; i < arr.length; i++) {
      const v = arr[i];
      if (v > v1) { v2 = v1; i2 = i1; v1 = v; i1 = i; }
      else if (v > v2) { v2 = v; i2 = i; }
    }
    const out = [];
    if (i1 >= 0) out.push(i1);
    if (i2 >= 0) out.push(i2);
    return out;
  }

  let i1 = -1, v1 = -Infinity;
  let i2 = -1, v2 = -Infinity;
  let i3 = -1, v3 = -Infinity;
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];
    if (v > v1) { v3 = v2; i3 = i2; v2 = v1; i2 = i1; v1 = v; i1 = i; }
    else if (v > v2) { v3 = v2; i3 = i2; v2 = v; i2 = i; }
    else if (v > v3) { v3 = v; i3 = i; }
  }
  const out = [];
  if (i1 >= 0) out.push(i1);
  if (i2 >= 0) out.push(i2);
  if (i3 >= 0) out.push(i3);
  return out;
}

function topPeaksPerFrame(frames) {
  const out = [];
  for (const row of frames) {
    const step = Math.max(1, Math.floor(row.length / 200));
    const sample = [];
    for (let i = 0; i < row.length; i += step) sample.push(row[i]);
    sample.sort((a, b) => a - b);
    const median = sample[Math.floor(sample.length / 2)] || 0;
    const whitened = new Float32Array(row.length);
    for (let i = 0; i < row.length; i++) {
      const v = row[i] - median;
      whitened[i] = v > 0 ? v : 0;
    }
    const idxs = topKIndicesFloat32(whitened, TOP_PEAKS);
    const refined = idxs.map((i) => {
      const L = whitened[i - 1] || 0;
      const C = whitened[i] || 0;
      const R = whitened[i + 1] || 0;
      const denom = (L - 2 * C + R) || 1e-9;
      const delta = 0.5 * (L - R) / denom;
      return i + delta;
    });
    out.push(refined);
  }
  return out;
}

function makeHashes(peaks, mags) {
  const hashes = [];
  for (let t = 0; t < peaks.length; t++) {
    const anchors = peaks[t];
    if (!anchors || !anchors.length) continue;
    for (const f1 of anchors) {
      const candidates = [];
      for (let dt = 1; dt <= TARGET_ZONE; dt++) {
        const t2 = t + dt;
        if (t2 >= peaks.length) break;
        for (const f2 of peaks[t2]) {
          const b1 = Math.max(0, Math.min(mags[t].length - 1, Math.round(f1)));
          const b2 = Math.max(0, Math.min(mags[t2].length - 1, Math.round(f2)));
          const mag = (mags[t][b1] || 1e-9) * (mags[t2][b2] || 1e-9);
          candidates.push({ f2, dt, mag });
        }
      }
      if (!candidates.length) continue;
      candidates.sort((a, b) => b.mag - a.mag);
      for (let i = 0; i < Math.min(MAX_PAIRS, candidates.length); i++) {
        const { f2, dt } = candidates[i];
        const q1 = Math.round(f1 / FREQ_Q);
        const q2 = Math.round(f2 / FREQ_Q);
        const qdt = Math.round(dt / DT_Q);
        const key = `${q1}-${q2}-${qdt}`;
        hashes.push({ key, t });
      }
    }
  }
  return hashes;
}

function mapsFromHashes(hashes, trackId) {
  const out = Object.create(null);
  for (const { key, t } of hashes) {
    (out[key] ||= []).push([trackId, t]);
  }
  return out;
}

async function runWorkerFingerprint(filePath, trackId) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL(import.meta.url), {
      workerData: { filePath, trackId }
    });

    worker.once('message', (msg) => {
      if (msg && msg.ok) resolve(msg.map);
      else reject(new Error(msg?.error || 'worker failed'));
    });

    worker.once('error', reject);

    worker.once('exit', (code) => {
      if (code !== 0) reject(new Error(`worker exit ${code}`));
    });
  });
}

async function fingerprintPath(filePath, trackId) {
  const float = await ffmpegToFloat32(filePath);
  const spec = stftMagnitudes(float);
  const peaks = topPeaksPerFrame(spec);
  const hashes = makeHashes(peaks, spec);
  return mapsFromHashes(hashes, trackId);
}

async function atomicWrite(filePath, data) {
  const tmp = `${filePath}.tmp`;
  await fs.writeFile(tmp, data, 'utf8');
  await fs.rename(tmp, filePath);
}

function makeHotWriter(outFile, getPayload) {
  let pending = false;
  let writing = false;

  const flush = async () => {
    if (writing) return;
    if (!pending) return;
    writing = true;
    pending = false;
    try {
      await atomicWrite(outFile, JSON.stringify(getPayload(), null, 2));
    } finally {
      writing = false;
      if (pending) await flush();
    }
  };

  const markDirty = () => {
    pending = true;
    queueMicrotask(flush);
  };

  return { markDirty, flush };
}

export async function buildIndex(dir, outFile = 'index.json') {
  const files = (await fs.readdir(dir)).filter((n) => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));
  let merged = Object.create(null);
  let meta = [];

  try {
    await access(outFile);
    const prev = JSON.parse(await fs.readFile(outFile, 'utf8'));
    if (prev && typeof prev === 'object') {
      if (prev.index && typeof prev.index === 'object') merged = prev.index;
      if (Array.isArray(prev.meta)) meta = prev.meta.filter((x) => typeof x === 'string');
    }
  } catch {

  }

  const done = new Set(meta);

  const hot = makeHotWriter(outFile, () => ({ index: merged, meta }));

  const concurrency = Math.max(1, Math.min(os.cpus().length, 8));
  let nextIndex = 0;
  let processed = 0;

  const updateProgress = (name, skipped = false) => {
    const w = 30;
    const filled = Math.round(((processed) / files.length) * w || 0);
    process.stdout.write(
      `\rProcessing: [${'='.repeat(filled)}${' '.repeat(w - filled)}] ${processed}/${files.length}${skipped ? ' (skipped)' : ''} | ${name}`
    );
  };

  const workerLoop = async () => {
    while (true) {
      const i = nextIndex++;
      if (i >= files.length) break;

      const name = files[i];

      if (done.has(name)) {
        processed++;
        updateProgress(name, true);
        continue;
      }

      try {
        process.stdout.write(`\rTrack: ${name}`);
        const map = await runWorkerFingerprint(path.join(dir, name), name);
        if (map) {
          for (const k of Object.keys(map)) (merged[k] ||= []).push(...map[k]);
          meta.push(name);
          done.add(name);
          hot.markDirty();
        }
      } catch (e) {
        console.error('\nskip', name, e.message);
      }

      processed++;
      updateProgress(name);
    }
  };

  await Promise.all(Array.from({ length: concurrency }, () => workerLoop()));

  process.stdout.write('\n');
  await hot.flush();
  await atomicWrite(outFile, JSON.stringify({ index: merged, meta }, null, 2));
  console.log('wrote', outFile);
}

if (!isMainThread) {
  (async () => {
    try {
      const { filePath, trackId } = workerData;
      const map = await fingerprintPath(filePath, trackId);
      parentPort.postMessage({ ok: true, map });
    } catch (e) {
      parentPort.postMessage({ ok: false, error: e?.message || String(e) });
    }
  })();
}

if (isMainThread && import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const [, , cmd, a, b] = process.argv;
    if (cmd === 'build' && a) {
      try {
        await buildIndex(a, b || 'index.json');
      } catch (e) {
        console.error(e);
        process.exit(1);
      }
    } else {
      console.error('usage: node fingerprint.js build <dir> [out.json]');
      process.exit(2);
    }
  })();
}
