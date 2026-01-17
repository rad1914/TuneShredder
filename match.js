import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import fft from 'fft-js';

// Parameters shared with fingerprint builder
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
    const args = [
      '-hide_banner',
      '-loglevel',
      'error',
      '-i',
      file,
      '-ac',
      String(CHANNELS),
      '-ar',
      String(SAMPLE_RATE),
      '-f',
      'f32le',
      '-',
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

async function fingerprintPath(filePath) {
  const float = await ffmpegToFloat32(filePath);
  const spec = stftMagnitudes(float);
  const peaks = topPeaksPerFrame(spec);
  const hashes = makeHashes(peaks, spec);
  return hashes; // array of {key, t}
}

function buildIndexLookup(indexObj) {
  // indexObj expected shape: { index: { key: [[trackId, t], ...], ... }, meta: [...] }
  if (!indexObj || typeof indexObj !== 'object' || !indexObj.index) throw new Error('Invalid index file');
  return indexObj.index;
}

function matchHashesToIndex(queryHashes, indexLookup) {
  // returns map: trackId -> Map(offset -> count) and totalMatchesPerTrack
  const trackOffsets = new Map();
  for (const { key, t: tq } of queryHashes) {
    const entries = indexLookup[key];
    if (!entries) continue;
    for (const [trackId, ti] of entries) {
      // offset = index_time - query_time (how far into indexed track the matching anchor is)
      const offset = ti - tq;
      let offs = trackOffsets.get(trackId);
      if (!offs) {
        offs = new Map();
        trackOffsets.set(trackId, offs);
      }
      offs.set(offset, (offs.get(offset) || 0) + 1);
    }
  }
  return trackOffsets;
}

function summarizeMatches(trackOffsets, queryHashCount, meta = []) {
  const results = [];
  for (const [trackId, offs] of trackOffsets.entries()) {
    // find top offset and votes
    let bestOff = null;
    let bestCount = 0;
    for (const [off, cnt] of offs.entries()) {
      if (cnt > bestCount) {
        bestCount = cnt;
        bestOff = off;
      }
    }
    results.push({
      trackId,
      file: meta && meta.length ? meta[Number(trackId) - 1] || String(trackId) : String(trackId),
      bestOffset: bestOff,
      votes: bestCount,
      matchRatio: +(bestCount / Math.max(1, queryHashCount)).toFixed(4),
      offsets: Array.from(offs.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8),
    });
  }
  results.sort((a, b) => b.votes - a.votes);
  return results;
}

export async function matchFileAgainstIndex(indexPath, queryFile, topN = 5) {
  const raw = JSON.parse(await fs.readFile(indexPath, 'utf8'));
  const indexLookup = buildIndexLookup(raw);
  const meta = Array.isArray(raw.meta) ? raw.meta : [];

  const qHashes = await fingerprintPath(queryFile);
  const trackOffsets = matchHashesToIndex(qHashes, indexLookup);
  const summary = summarizeMatches(trackOffsets, qHashes.length, meta).slice(0, topN);
  return { query: queryFile, queryHashCount: qHashes.length, matches: summary };
}

// CLI wrapper
if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    try {
      const [, , cmd, a, b, c] = process.argv;
      if (cmd === 'match' && a && b) {
        const top = c ? Number(c) : 5;
        const out = await matchFileAgainstIndex(a, b, top);
        console.log(JSON.stringify(out, null, 2));
      } else {
        console.error('usage: node match.js match <index.json> <queryFile> [topN]');
        process.exit(2);
      }
    } catch (e) {
      console.error(e && e.message ? e.message : e);
      process.exit(1);
    }
  })();
}
