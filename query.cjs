const fs = require('fs');
const path = require('path');

const DB = path.join(__dirname, 'db');
const SONGS = path.join(DB, "'_songs.json");
const OUT = path.join(DB, "'_matches.json");

const DIM = 96;
const TOP_K = 60;
const SCORE_THRESHOLD = 0.99;

const readFloat32 = buf => new Float32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4);

function loadMeta(meta) {
  const f = path.join(DB, meta.embFile);
  if (!fs.existsSync(f)) return null;
  const buf = readFloat32(fs.readFileSync(f));
  const frames = (buf.length / DIM) | 0;
  for (let i = 0; i < frames; i++) {
    let off = i * DIM, n = 0;
    for (let d = 0; d < DIM; d++) {
      const v = buf[off + d];
      n += v * v;
    }
    n = Math.sqrt(n) || 1;
    for (let d = 0; d < DIM; d++) buf[off + d] /= n;
  }
  return { buf, frames };
}

function meanVector(data) {
  const mean = new Float32Array(DIM);
  for (let i = 0; i < data.frames; i++) {
    let off = i * DIM;
    for (let d = 0; d < DIM; d++) mean[d] += data.buf[off + d];
  }
  let norm = 0;
  for (let d = 0; d < DIM; d++) norm += mean[d] * mean[d];
  norm = Math.sqrt(norm) || 1;
  for (let d = 0; d < DIM; d++) mean[d] /= norm;
  return mean;
}

function dot(a, b) {
  let s = 0;
  for (let i = 0; i < DIM; i++) s += a[i] * b[i];
  return s;
}

function slide(q, s) {
  let best = -Infinity, pos = -1;
  const limit = s.frames - q.frames;
  for (let i = 0; i <= limit; i++) {
    let sum = 0, aoff = 0, boff = i * DIM;
    for (let j = 0; j < q.frames; j++, aoff += DIM, boff += DIM) {
      for (let k = 0; k < DIM; k++) sum += q.buf[aoff + k] * s.buf[boff + k];
    }
    const score = sum / q.frames;
    if (score > best) { best = score; pos = i; }
  }
  return { score: best, pos };
}

(async () => {
  const songsMeta = JSON.parse(fs.readFileSync(SONGS, 'utf8'));
  const ids = Object.keys(songsMeta);

  const cache = new Map();
  for (const id of ids) {
    const data = loadMeta(songsMeta[id]);
    if (data) cache.set(id, { meta: songsMeta[id], data });
  }

  const means = new Map();
  for (const [id, { data }] of cache) means.set(id, meanVector(data));

  const matches = [];

  for (let idx = 0; idx < ids.length; idx++) {
    const a = ids[idx];
    const qa = cache.get(a);
    if (!qa) continue;
    console.log(`(${idx + 1}/${ids.length}) processing ${a}`);

    const sims = [];
    for (const b of ids) {
      if (a === b) continue;
      const mb = means.get(b);
      if (!mb) continue;
      sims.push({ id: b, sim: dot(means.get(a), mb) });
    }

    sims.sort((x, y) => y.sim - x.sim);
    const candidates = sims.filter(s => s.sim >= SCORE_THRESHOLD).slice(0, TOP_K).map(s => s.id);
    if (!candidates.length) continue;

    for (const b of candidates) {
      const qaData = cache.get(a).data;
      const qbData = cache.get(b).data;
      const needleQ = qaData.frames <= qbData.frames;
      const r = needleQ ? slide(qaData, qbData) : slide(qbData, qaData);
      if (r.score < SCORE_THRESHOLD) continue;
      matches.push({
        a,
        b,
        score: +r.score.toFixed(6),
        pos: r.pos,
        needleQ,
        sizeA: fs.statSync(path.join(DB, songsMeta[a].embFile)).size,
        sizeB: fs.statSync(path.join(DB, songsMeta[b].embFile)).size
      });
    }
  }

  fs.writeFileSync(OUT, JSON.stringify(matches, null, 2), 'utf8');
  console.log('Saved matches to', OUT);
})().catch(e => {
  console.error(e);
  process.exit(1);
});