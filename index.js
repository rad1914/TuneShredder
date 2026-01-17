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

const hann = (n) => Array.from({ length: n }, (_, i) => 0.5 * (1 - Math.cos((2 * Math.PI * i) / (n - 1))));

const hannWin = hann(WINDOW);
const FRAME_BUF = new Float32Array(WINDOW);

async function ffmpegToPCM(file) {
  return new Promise((resolve, reject) => {
    const args = ['-hide_banner', '-loglevel', 'error', '-i', file, '-ac', String(CHANNELS), '-ar', String(SAMPLE_RATE), '-f', 's16le', '-'];
    const p = spawn('ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    const chunks = [];
    let err = '';
    p.stdout.on('data', c => chunks.push(c));
    p.stderr.on('data', c => { err += c.toString(); });
    p.on('close', code => code === 0 ? resolve(Buffer.concat(chunks)) : reject(new Error('ffmpeg failed: ' + err)));
  });
}

function pcm16ToFloat(buf) {

  const len = buf.length >> 1;
  const view = new Int16Array(buf.buffer, buf.byteOffset, len);
  const out = new Float32Array(len);
  for (let i = 0; i < len; i++) out[i] = view[i] / 32768;
  return out;
}

function stftMagnitudes(signal) {
  const frames = [];
  const frame = FRAME_BUF;
  const half = WINDOW >> 1;
  for (let pos = 0; pos + WINDOW <= signal.length; pos += HOP) {
    for (let i = 0; i < WINDOW; i++) frame[i] = signal[pos + i] * hannWin[i];

    const spec = fft.fft(Array.from(frame));
    const mags = new Float32Array(half);
    for (let i = 0; i < half; i++) mags[i] = Math.hypot(spec[i][0], spec[i][1]);
    frames.push(mags);
  }
  return frames;
}

function topKIndices(arr, k) {
  const best = [];
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];
    if (best.length < k) {
      best.push([v, i]);
      if (best.length === k) best.sort((a, b) => a[0] - b[0]);
    } else if (v > best[0][0]) {
      best[0] = [v, i];

      for (let j = 0; j < k - 1; j++) if (best[j][0] > best[j+1][0]) [best[j], best[j+1]] = [best[j+1], best[j]];
    }
  }
  best.sort((a, b) => b[0] - a[0]);
  return best.map(p => p[1]);
}

function topPeaksPerFrame(spec) {
  return spec.map(row => topKIndices(row, TOP_PEAKS).filter(i => row[i] > 1e-6));
}

function makeHashes(peaks) {

  const hashes = [];
  for (let t = 0; t < peaks.length; t++) {
    const anchors = peaks[t];
    for (const f1 of anchors) {
      const targets = [];
      for (let dt = 1; dt <= TARGET_ZONE; dt++) {
        const t2 = t + dt;
        if (t2 >= peaks.length) break;
        for (const f2 of peaks[t2]) targets.push({ f2, dt });
      }
      for (let i = 0; i < Math.min(MAX_PAIRS, targets.length); i++) {
        const { f2, dt } = targets[i];
        const key = `${f1}-${f2}-${dt}`;
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
  const pcm = await ffmpegToPCM(filePath);
  const float = pcm16ToFloat(pcm);
  const spec = stftMagnitudes(float);
  const peaks = topPeaksPerFrame(spec);
  const hashes = makeHashes(peaks);
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

  for (let i = 0; i < files.length; i++) {
    const name = files[i];
    const p = path.join(dir, name);
    console.log('fingerprinting', name);
    queue.push(runLimited(async () => {
      try {
        return await fingerprintPath(p, String(i + 1));
      } catch (e) {
        console.error('skipping', name, e.message);
        return null;
      }
    }));
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
    const pcm = await ffmpegToPCM(clipPath);
    const float = pcm16ToFloat(pcm);
    const spec = stftMagnitudes(float);
    const peaks = topPeaksPerFrame(spec);
    const hashes = makeHashes(peaks);
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
