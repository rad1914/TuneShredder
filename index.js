// @path: index.js
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
  const out = new Float32Array(n);
  for (let i = 0; i < n; i++) out[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (n - 1)));
  return out;
};

const hannWin = hann(WINDOW);

async function ffmpegToFloat32(file) {
  return new Promise((resolve, reject) => {
    const args = [
      '-hide_banner', '-loglevel', 'error',
      '-i', file,
      '-ac', String(CHANNELS),
      '-ar', String(SAMPLE_RATE),
      '-f', 'f32le',
      '-'
    ];
    const p = spawn('ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    const chunks = [];
    let err = '';
    p.stdout.on('data', c => chunks.push(c));
    p.stderr.on('data', c => { err += c.toString(); });
    p.on('close', code => {
      if (code !== 0) return reject(new Error('ffmpeg failed: ' + err));
      const buf = Buffer.concat(chunks);
      const float = new Float32Array(buf.buffer, buf.byteOffset, Math.floor(buf.length / 4));

      resolve(new Float32Array(float));
    });
  });
}

function stftMagnitudes(signal) {
  const frames = [];
  const half = WINDOW >> 1;
  const frameArray = new Array(WINDOW);
  const mags = new Float32Array(half);

  for (let pos = 0; pos + WINDOW <= signal.length; pos += HOP) {

    for (let i = 0; i < WINDOW; i++) frameArray[i] = signal[pos + i] * hannWin[i];

    const spec = fft.fft(frameArray);
    for (let i = 0; i < half; i++) {
      const re = spec[i][0], im = spec[i][1];
      mags[i] = Math.hypot(re, im);
    }

    frames.push(Float32Array.from(mags));
  }
  return frames;
}

function topKIndicesSmall(arr, k) {
  const bestVal = new Float32Array(k).fill(-Infinity);
  const bestIdx = new Int32Array(k).fill(-1);
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];

    if (v <= bestVal[0]) continue;

    let j = 0;
    while (j < k && bestVal[j] < v) j++;
    if (j === 0) {

      bestVal[0] = v;
      bestIdx[0] = i;
    } else {

      for (let s = 0; s < j - 1; s++) {
        bestVal[s] = bestVal[s + 1];
        bestIdx[s] = bestIdx[s + 1];
      }
      bestVal[j - 1] = v;
      bestIdx[j - 1] = i;
    }
  }

  const out = [];
  for (let i = k - 1; i >= 0; i--) if (bestIdx[i] >= 0) out.push(bestIdx[i]);
  return out;
}

function topPeaksPerFrame(magsFrames) {
  const peaks = [];
  for (let f = 0; f < magsFrames.length; f++) {
    const row = magsFrames[f];

    const sampleStep = Math.max(1, Math.floor(row.length / 200));
    const sample = [];
    for (let i = 0; i < row.length; i += sampleStep) sample.push(row[i]);
    sample.sort((a, b) => a - b);
    const median = sample[Math.floor(sample.length / 2)] || 0;

    const whitened = new Float32Array(row.length);
    for (let i = 0; i < row.length; i++) {
      const v = row[i] - median;
      whitened[i] = v > 0 ? v : 0;
    }

    const idxs = topKIndicesSmall(whitened, TOP_PEAKS);

    const refined = [];
    for (const i of idxs) {
      const left = whitened[i - 1] || 0;
      const center = whitened[i] || 0;
      const right = whitened[i + 1] || 0;
      const denom = (left - 2 * center + right) || 1e-9;
      const delta = 0.5 * (left - right) / denom;
      refined.push(i + delta);
    }
    peaks.push(refined);
  }
  return peaks;
}

function makeHashes(peaks, magsPerFrame) {
  const hashes = [];
  for (let t = 0; t < peaks.length; t++) {
    const anchors = peaks[t];
    if (!anchors || anchors.length === 0) continue;
    for (const f1 of anchors) {
      const candidates = [];
      for (let dt = 1; dt <= TARGET_ZONE; dt++) {
        const t2 = t + dt;
        if (t2 >= peaks.length) break;
        for (const f2 of peaks[t2]) {

          const b1 = Math.max(0, Math.min(magsPerFrame[t].length - 1, Math.round(f1)));
          const b2 = Math.max(0, Math.min(magsPerFrame[t2].length - 1, Math.round(f2)));
          const mag = (magsPerFrame[t][b1] || 1e-9) * (magsPerFrame[t2][b2] || 1e-9);
          candidates.push({ f2, dt, mag });
        }
      }
      if (candidates.length === 0) continue;
      candidates.sort((a, b) => b.mag - a.mag);
      for (let i = 0; i < Math.min(MAX_PAIRS, candidates.length); i++) {
        const { f2, dt } = candidates[i];
        const q1 = Math.round(f1);
        const q2 = Math.round(f2);
        const key = `${q1}-${q2}-${dt}`;
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
  for (const m of maps) {
    for (const [k, arr] of m) {
      if (!out[k]) out[k] = [];
      out[k].push(...arr);
    }
  }
  return out;
}

function matchClip(indexObj, clipMap) {
  const votes = new Map();

  for (const [key, clipEntries] of clipMap) {
    const idxEntries = indexObj[key];
    if (!idxEntries) continue;
    for (const [trackId, tTrack] of idxEntries) {
      for (const [, tClip] of clipEntries) {
        const off = tTrack - tClip;
        const kk = `${trackId}|${off}`;
        votes.set(kk, (votes.get(kk) || 0) + 1);
      }
    }
  }

  const perTrack = new Map();
  for (const [k, ct] of votes) {
    const [trackId] = k.split('|');
    perTrack.set(trackId, (perTrack.get(trackId) || 0) + ct);
  }

  return Array.from(perTrack.entries()).sort((a, b) => b[1] - a[1]);
}

async function fingerprintPath(filePath, trackId) {
  const float = await ffmpegToFloat32(filePath);
  const spec = stftMagnitudes(float);
  const peaks = topPeaksPerFrame(spec);
  const hashes = makeHashes(peaks, spec);
  return mapsFromHashes(hashes, trackId);
}

export async function buildIndex(dir, outFile = 'index.json') {
  const files = (await fs.readdir(dir)).filter(n => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));
  const maps = [];

  const CONCURRENCY = Math.max(1, os.cpus().length - 1);
  let running = 0;
  const queue = [];

  const runLimited = async (fn) => {
    while (running >= CONCURRENCY) await new Promise(r => setTimeout(r, 20));
    running++;
    try { return await fn(); } finally { running--; }
  };

  const total = files.length;
  let processed = 0;
  const renderProgress = () => {
    const width = 30;
    const ratio = total === 0 ? 1 : processed / total;
    const filled = Math.round(ratio * width);
    const bar = '='.repeat(filled) + ' '.repeat(Math.max(0, width - filled));
    process.stdout.write(`\rProcessing: [${bar}] ${processed}/${total}`);
    if (processed === total) process.stdout.write('\n');
  };
  renderProgress();

  for (let i = 0; i < files.length; i++) {
    const name = files[i];
    const p = path.join(dir, name);

    const work = runLimited(async () => {
      try {
        return await fingerprintPath(p, String(i + 1));
      } catch (e) {
        console.error('skipping', name, e.message);
        return null;
      }
    });
    queue.push(work.then((res) => { processed++; renderProgress(); return res; }));
  }

  const resolved = (await Promise.all(queue)).filter(Boolean);
  const merged = mergeMaps(resolved);
  await fs.writeFile(outFile, JSON.stringify({ index: merged, meta: files }, null, 2), 'utf8');
  console.log('wrote', outFile);
}

export async function matchFile(clipPath, indexPath) {
  const raw = JSON.parse(await fs.readFile(indexPath, 'utf8'));
  const indexObj = raw.index;
  const clipMap = await (async () => {
    const float = await ffmpegToFloat32(clipPath);
    const spec = stftMagnitudes(float);
    const peaks = topPeaksPerFrame(spec);
    const hashes = makeHashes(peaks, spec);
    return mapsFromHashes(hashes, 'clip');
  })();

  const results = matchClip(indexObj, clipMap).slice(0, 5);
  console.log('top matches (trackId, votes):', results);
  for (const [trackId, votes] of results) {
    const idx = parseInt(trackId, 10) - 1;
    const name = raw.meta[idx] || 'unknown';
    console.log(`Track ${trackId} (${name}) => ${votes} votes`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const [,, cmd, a, b] = process.argv;
    try {
      if (cmd === 'build' && a) await buildIndex(a, b || 'index.json');
      else if (cmd === 'match' && a && b) await matchFile(a, b);
      else console.log('usage: node fingerprint.min.js build <dir> [out.json] | match <clip> <index.json>');
    } catch (e) {
      console.error(e);
      process.exit(1);
    }
  })();
}
