// @path: sh.js
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";

const INDEX_PATH = ".shazam-index.json";
const SAMPLE_RATE = 22050;
const WINDOW_SAMPLES = 4096;
const HOP_SAMPLES = 2048;
const PAIR_RANGE = 5;

const loadIndex = async () => {
  try {
    const raw = await fs.readFile(INDEX_PATH, "utf8");
    return JSON.parse(raw);
  } catch {
    return { hashes: {}, tracks: {} };
  }
};

const saveIndex = async (idx) => {
  try {
    await fs.writeFile(INDEX_PATH, JSON.stringify(idx));
  } catch (e) {

  }
};

const _sha1 = (b) => createHash("sha1").update(b).digest("hex");

const extractPCMStream = (file, sampleRate = SAMPLE_RATE) =>
  spawn("ffmpeg", ["-v", "error", "-i", file, "-ar", String(sampleRate), "-ac", "1", "-f", "f32le", "-"]);

const computeWindowHashes = (file) =>
  new Promise((resolve) => {
    const p = extractPCMStream(file);
    const chunks = [];
    p.stdout.on("data", (d) => chunks.push(d));
    p.on("close", () => {
      try {
        const buf = Buffer.concat(chunks);
        const floatBytes = 4;
        const totalSamples = Math.floor(buf.length / floatBytes);
        const samples = new Float32Array(totalSamples);
        for (let i = 0; i < totalSamples; i++) samples[i] = buf.readFloatLE(i * floatBytes);

        const win = WINDOW_SAMPLES;
        const hop = HOP_SAMPLES;
        const hashes = [];
        for (let offset = 0, idx = 0; offset + win <= samples.length; offset += hop, idx++) {

          const view = Buffer.allocUnsafe(win * 4);
          for (let i = 0; i < win; i++) view.writeFloatLE(samples[offset + i] || 0, i * 4);
          hashes.push({ idx, hash: _sha1(view) });
        }
        resolve({ hashes, sampleRate: SAMPLE_RATE, totalSamples });
      } catch {
        resolve(null);
      }
    });
    p.on("error", () => resolve(null));
  });

const buildPairs = (windowHashes, sampleRate = SAMPLE_RATE) => {
  const pairs = new Map();
  for (let i = 0; i < windowHashes.length; i++) {
    const h1 = windowHashes[i].hash;
    for (let j = i + 1; j <= i + PAIR_RANGE && j < windowHashes.length; j++) {
      const h2 = windowHashes[j].hash;
      const dt = j - i;
      const key = `${h1}|${h2}|${dt}`;
      const time = (i * HOP_SAMPLES) / sampleRate;
      const arr = pairs.get(key) || [];
      arr.push(time);
      pairs.set(key, arr);
    }
  }
  return pairs;
};

export const generateFingerprints = async (file) => {
  const win = await computeWindowHashes(file);
  if (!win || !win.hashes || !win.hashes.length) return null;
  const pairs = buildPairs(win.hashes, win.sampleRate);

  const id = _sha1(Buffer.from(path.basename(file) + ":" + win.hashes.slice(0, 4).map((h) => h.hash).join("|")));
  const durationApprox = (win.totalSamples / win.sampleRate) || 0;
  return { id, duration: durationApprox, pairs };
};

export const addToIndex = async (trackId, pairs, meta = {}) => {
  const idx = await loadIndex();
  idx.tracks[trackId] = { meta: { ...meta }, pairCount: pairs.size };
  for (const [k, times] of pairs.entries()) {
    const entry = idx.hashes[k] || [];

    for (const t of times) entry.push([trackId, t]);
    idx.hashes[k] = entry;
  }
  await saveIndex(idx);
  return true;
};

export const queryIndex = async (pairs) => {
  const idx = await loadIndex();
  const vote = new Map();
  for (const [k, times] of pairs.entries()) {
    const postings = idx.hashes[k];
    if (!postings) continue;
    for (const [trackId, trackTime] of postings) {
      for (const snippetTime of times) {
        const offset = Math.round((trackTime - snippetTime) * 100);
        const tmap = vote.get(trackId) || new Map();
        tmap.set(offset, (tmap.get(offset) || 0) + 1);
        vote.set(trackId, tmap);
      }
    }
  }

  const out = new Map();
  for (const [trackId, tmap] of vote.entries()) {
    let best = 0, total = 0;
    for (const c of tmap.values()) { total += c; if (c > best) best = c; }
    out.set(trackId, { bestBucket: best, total });
  }
  return out;
};

export const matchFileAgainstIndex = async (file) => {
  const gen = await generateFingerprints(file);
  if (!gen) return null;
  return queryIndex(gen.pairs);
};
