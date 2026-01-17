import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
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
      if (code !== 0) return reject(new Error('ffmpeg failed: ' + err));
      const buf = Buffer.concat(chunks);
      const float = new Float32Array(buf.buffer, buf.byteOffset, Math.floor(buf.length / 4));
      resolve(new Float32Array(float));
    });
  });

function stftMagnitudes(signal) {
  const frames = [];
  const half = WINDOW >> 1;
  const frame = new Array(WINDOW);
  const mags = new Float32Array(half);
  for (let pos = 0; pos + WINDOW <= signal.length; pos += HOP) {
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

function topPeaksPerFrame(magsFrames) {
  const peaks = [];
  for (const row of magsFrames) {
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
    peaks.push(refined);
  }
  return peaks;
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
  const m = new Map();
  for (const { key, t } of hashes) {
    if (!m.has(key)) m.set(key, []);
    m.get(key).push([trackId, t]);
  }
  return m;
}

function mergeMaps(maps) {
  const out = Object.create(null);
  for (const m of maps) for (const [k, arr] of m) (out[k] ||= []).push(...arr);
  return out;
}

async function fingerprintPath(filePath, trackId) {
  const float = await ffmpegToFloat32(filePath);
  const spec = stftMagnitudes(float);
  const peaks = topPeaksPerFrame(spec);
  const hashes = makeHashes(peaks, spec);
  return mapsFromHashes(hashes, trackId);
}

export async function buildIndex(dir, outFile = 'index.json') {
  const files = (await fs.readdir(dir)).filter((n) => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));
  const concurrency = Math.max(1, os.cpus().length - 1);
  let running = 0;
  const results = [];
  let processed = 0;
  const total = files.length;
  const run = async (fn) => {
    while (running >= concurrency) await new Promise((r) => setTimeout(r, 20));
    running++;
    try {
      return await fn();
    } finally {
      running--;
    }
  };
  const tasks = files.map((name, i) =>
    run(async () => {
      try {
        const map = await fingerprintPath(path.join(dir, name), String(i + 1));
        return map;
      } catch (e) {
        console.error('skip', name, e.message);
        return null;
      } finally {
        processed++;
        const w = 30;
        const filled = Math.round((processed / total) * w);
        process.stdout.write(`\rProcessing: [${'='.repeat(filled)}${' '.repeat(w - filled)}] ${processed}/${total}`);
        if (processed === total) process.stdout.write('\n');
      }
    })
  );
  const resolved = (await Promise.all(tasks)).filter(Boolean);
  const merged = mergeMaps(resolved);
  await fs.writeFile(outFile, JSON.stringify({ index: merged, meta: files }, null, 2), 'utf8');
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