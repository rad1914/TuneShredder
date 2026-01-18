// @path: index.js
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import FFT from 'fft.js';

const CFG = { sr: 22050, ch: 1, win: 4096, hop: 512, top: 24, zone: 55, pairs: 6, sec: 85, fq: 6, dtq: 2, bucket: 250 };

const hann = (() => {
  const w = new Float32Array(CFG.win);
  for (let i = 0; i < CFG.win; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (CFG.win - 1)));
  return w;
})();

const ffmpegF32 = file => new Promise((res, rej) => {
  const p = spawn('ffmpeg', ['-hide_banner','-loglevel','error','-t',String(CFG.sec),'-i',file,'-ac',String(CFG.ch),'-ar',String(CFG.sr),'-f','f32le','-'], { stdio: ['ignore','pipe','pipe'] });
  const chunks = []; let err = '';
  p.stdout.on('data', c => chunks.push(c));
  p.stderr.on('data', c => err += c);
  p.on('close', code => {
    if (code !== 0) return rej(new Error('ffmpeg failed: ' + err.trim()));
    const buf = Buffer.concat(chunks);
    res(new Float32Array(buf.buffer, buf.byteOffset, (buf.length / 4) | 0));
  });
});

function stftMags(x) {
  const half = CFG.win >> 1;
  const frames = Math.max(0, Math.floor((x.length - CFG.win) / CFG.hop) + 1);
  const fft = new FFT(CFG.win);
  const complex = fft.createComplexArray();
  const inbuf = new Float64Array(CFG.win);
  const out = new Array(frames);
  for (let pos = 0, fi = 0; pos + CFG.win <= x.length; pos += CFG.hop, fi++) {
    for (let i = 0; i < CFG.win; i++) inbuf[i] = x[pos + i] * hann[i];
    fft.realTransform(complex, inbuf);
    const mags = new Float32Array(half);
    for (let b = 0; b < half; b++) {
      const re = complex[b*2] || 0, im = complex[b*2+1] || 0;
      mags[b] = Math.log1p(Math.hypot(re, im));
    }
    out[fi] = mags;
  }
  return out;
}

function topKIndices(row, K) {
  const ids = new Int32Array(K).fill(-1);
  const vals = new Float32Array(K).fill(-Infinity);
  for (let i = 0; i < row.length; i++) {
    const v = row[i]; if (!(v > 0)) continue;
    let minI = 0;
    for (let j = 1; j < K; j++) if (vals[j] < vals[minI]) minI = j;
    if (v > vals[minI]) { vals[minI] = v; ids[minI] = i; }
  }
  const r = [];
  for (let j = 0; j < K; j++) if (ids[j] !== -1) r.push(ids[j]);
  return r;
}

const peaks = frames => frames.map(f => topKIndices(f, CFG.top));

function hashes(pk, mags) {
  const out = [];
  const quant = pk.map(r => (r||[]).map(f => Math.round(f / CFG.fq)));
  for (let t = 0; t < quant.length; t++) {
    const a = quant[t]; if (!a?.length) continue;
    for (const f1q of a) {
      const bestF = new Int32Array(CFG.pairs).fill(-1);
      const bestDt = new Int32Array(CFG.pairs);
      const bestMag = new Float32Array(CFG.pairs).fill(-Infinity);
      for (let dt = 1; dt <= CFG.zone; dt++) {
        const t2 = t + dt; if (t2 >= quant.length) break;
        for (const f2q of quant[t2]) {
          const b1 = Math.min(mags[t].length - 1, Math.max(0, f1q));
          const b2 = Math.min(mags[t2].length - 1, Math.max(0, f2q));
          const score = (mags[t][b1] || 1e-9) * (mags[t2][b2] || 1e-9);
          for (let s = 0; s < CFG.pairs; s++) {
            if (score > bestMag[s]) {
              for (let k = CFG.pairs - 1; k > s; k--) {
                bestMag[k] = bestMag[k-1]; bestF[k] = bestF[k-1]; bestDt[k] = bestDt[k-1];
              }
              bestMag[s] = score; bestF[s] = f2q; bestDt[s] = dt;
              break;
            }
          }
        }
      }
      for (let p = 0; p < CFG.pairs; p++) if (bestF[p] !== -1) out.push({ key: `${f1q}-${bestF[p]}-${Math.round(bestDt[p]/CFG.dtq)}`, t });
    }
  }
  return out;
}

const toMap = (hs, id) => {
  const m = Object.create(null);
  for (const { key, t } of hs) {
    const a = (m[key] ||= []);
    if (a.length < CFG.bucket) a.push([id, t]);
  }
  return m;
};

async function fingerprint(file, id) {
  const x = await ffmpegF32(file);
  const mags = stftMags(x);
  return toMap(hashes(peaks(mags), mags), id);
}

const atomicWrite = async (p, s) => {

  const tmp = `${p}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`;
  try {
    await fs.writeFile(tmp, s, 'utf8');

    await fs.rename(tmp, p);
  } catch (err) {

    try { await fs.unlink(tmp); } catch (e) {  }
    throw err;
  }
};

const runWorker = (filePath, trackId) => new Promise((resolve, reject) => {
  const w = new Worker(new URL(import.meta.url), { workerData: { filePath, trackId } });
  w.once('message', m => m?.ok ? resolve(m.map) : reject(new Error(m?.error || 'worker failed')));
  w.once('error', reject);
  w.once('exit', c => c === 0 || reject(new Error('worker exit ' + c)));
});

export async function buildIndex(dir, outFile = 'index.json') {
  let files = (await fs.readdir(dir)).filter(n => /\.(wav|mp3|flac|m4a|ogg|opus)$/i.test(n));
  let merged = Object.create(null), meta = [];

  try {
    const existing = await fs.readFile(outFile, 'utf8');
    const parsed = JSON.parse(existing || '{}');
    if (parsed?.index) merged = parsed.index;
    if (Array.isArray(parsed?.meta)) meta = parsed.meta.slice();
    if (meta.length) files = files.filter(f => !new Set(meta).has(f));
  } catch (e) { }

  let idx = 0, done = 0;
  const conc = Math.max(1, Math.min(os.cpus().length || 7, files.length));
  const merge = map => { for (const k in map) { const dst = (merged[k] ||= []), src = map[k]; for (let j = 0; j < src.length && dst.length < CFG.bucket; j++) dst.push(src[j]); } };

  console.log(`workers=${conc} files=${files.length}`);

  const worker = async () => {
    for (;;) {
      const name = files[idx++]; if (!name) return;
      try {
        const map = await runWorker(path.join(dir, name), name);
        const fileId = meta.length;
        for (const k of Object.keys(map)) {
          const arr = map[k];
          for (let i = 0; i < arr.length; i++) {
            const item = arr[i];
            if (Array.isArray(item) && item.length >= 2) arr[i] = [fileId, item[1]];
          }
        }
        merge(map);
        meta.push(name);
      } catch (e) {  }
      done++;
      if (done % 1 === 0) await atomicWrite(outFile, JSON.stringify({ index: merged, meta }));
      console.log(`progress: ${done}/${files.length} - ${name}`);
    }
  };

  await Promise.all(Array.from({ length: conc }, worker));

  for (const k of Object.keys(merged)) {
    const list = merged[k];
    const perFile = Object.create(null);
    for (let i = 0; i < list.length; i++) {
      const it = list[i];
      if (!Array.isArray(it) || it.length < 2) continue;
      const fid = it[0], t = it[1];
      (perFile[fid] ||= []).push(t);
    }
    const outList = [];
    for (const fidStr of Object.keys(perFile)) {
      const fid = Number(fidStr);
      const times = perFile[fid].sort((a,b) => a - b);
      if (!times.length) continue;
      const deltas = [times[0]];
      for (let i = 1; i < times.length; i++) deltas.push(times[i] - times[i-1]);
      outList.push([fid, deltas]);
    }
    merged[k] = outList;
  }

  await atomicWrite(outFile, JSON.stringify({ index: merged, meta }));
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
    if (cmd !== 'build' || !dir) { console.error('usage: node fingerprint.js build <dir> [out.json]'); process.exit(2); }
    await buildIndex(dir, out || 'index.json');
  })().catch(e => { console.error(e); process.exit(1); });
}
