// @path: index.js
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import { access } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import fft from 'fft-js';

const SAMPLE_RATE = 44100;
const CHANNELS = 1;
const WINDOW = 4096;
const HOP = 2048;
const TOP_PEAKS = 3;
const TARGET_ZONE = 16;
const MAX_PAIRS = 2;

const hann = (n) => {
  const w = new Float32Array(n);
  for (let i = 0; i < n; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (n - 1)));
  return w;
};
const hannWin = hann(WINDOW);

const ffmpegToFloat32 = (file) =>
  new Promise((resolve, reject) => {
    const args = ['-hide_banner', '-loglevel', 'error', '-i', file, '-ac', String(CHANNELS), '-ar', String(SAMPLE_RATE), '-f', 'f32le', '-'];
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
  const mags = new Float32Array(half);
  for (let pos = 0; pos + WINDOW <= signal.length; pos += HOP) {
    const frame = new Array(WINDOW);
    for (let i = 0; i < WINDOW; i++) frame[i] = signal[pos + i] * hannWin[i];
    const spec = fft.fft(frame);
    for (let i = 0; i < half; i++) mags[i] = Math.hypot(spec[i][0], spec[i][1]);
    frames.push(Float32Array.from(mags));
  }
  return frames;
}

function topKIndices(arr, k) {
  return arr
    .map((v, i) => ({ v, i }))
    .sort((a, b) => b.v - a.v)
    .slice(0, k)
    .map((x) => x.i)
    .filter((i) => i >= 0);
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
    const idxs = topKIndices(Array.from(whitened), TOP_PEAKS);
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
        const key = `${Math.round(f1)}-${Math.round(f2)}-${dt}`;
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

export async function buildIndex(dir, outFile = 'index.json') {
  const files = (await fs.readdir(dir)).filter((n) => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));
  let merged = Object.create(null);
  let meta = [];

  try {
    await access(outFile);
    const prev = JSON.parse(await fs.readFile(outFile, 'utf8'));
    if (prev && typeof prev === 'object') {
      if (prev.index && typeof prev.index === 'object') merged = prev.index;
      if (Array.isArray(prev.meta)) meta = prev.meta;
    }
  } catch {

  }

  const done = new Set(meta);

  for (let i = 0; i < files.length; i++) {
    const name = files[i];
    if (done.has(name)) {
      const w = 30;
      const filled = Math.round(((i + 1) / files.length) * w || 0);
      process.stdout.write(
        `\rProcessing: [${'='.repeat(filled)}${' '.repeat(w - filled)}] ${i + 1}/${files.length} (skipped)`
      );
      continue;
    }
    try {
      const map = await fingerprintPath(path.join(dir, name), String(i + 1));
      if (map) {
        for (const k of Object.keys(map)) (merged[k] ||= []).push(...map[k]);
        meta.push(name);
        done.add(name);
      }
    } catch (e) {
      console.error('skip', name, e.message);
    }
    const w = 30;
    const filled = Math.round(((i + 1) / files.length) * w || 0);
    process.stdout.write(`\rProcessing: [${'='.repeat(filled)}${' '.repeat(w - filled)}] ${i + 1}/${files.length}`);
  }
  process.stdout.write('\n');
  await atomicWrite(outFile, JSON.stringify({ index: merged, meta: meta.length ? meta : files }, null, 2));
  console.log('wrote', outFile);
}

if (import.meta.url === `file://${process.argv[1]}`) {
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
