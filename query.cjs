const fs = require('fs').promises;
const os = require('os');

const SONGS_JSON = './songs.json';
const PREFILTER_K = 20;
const CONCURRENCY = Math.max(1, Math.min(os.cpus().length - 1, 8));

async function buildIndex() {
  const raw = await fs.readFile(SONGS_JSON, 'utf8');
  const songs = JSON.parse(raw);
  const index = [];
  for (const id of Object.keys(songs)) {
    const meta = songs[id];
    let seq;
    try {
      seq = await loadSongEmbeddings(meta);
    } catch {
      seq = null;
    }
    if (seq && seq.length > 0) {
      const mean = computeMean(seq);
      index.push({ id, meta, mean, embedLength: seq.length, embedPath: meta.embedPath || null });
    } else {
      index.push({ id, meta, mean: null, embedLength: 0, embedPath: meta.embedPath || null });
    }
  }
  return index;
}

function computeMean(seq) {
  const dim = seq[0].length;
  const out = new Float64Array(dim);
  for (let i = 0; i < seq.length; i++) {
    const v = seq[i];
    for (let j = 0; j < dim; j++) out[j] += v[j];
  }
  const inv = 1 / seq.length;
  for (let j = 0; j < dim; j++) out[j] *= inv;
  return out;
}

function l2norm(vec) {
  let s = 0;
  for (let i = 0; i < vec.length; i++) s += vec[i] * vec[i];
  return Math.sqrt(s);
}

function cosine(a, b, aNorm = null, bNorm = null) {
  if (!a || !b) return -1;
  if (a.length !== b.length) return -1;
  if (aNorm === null) aNorm = l2norm(a);
  if (bNorm === null) bNorm = l2norm(b);
  if (aNorm === 0 || bNorm === 0) return -1;
  let dot = 0;
  for (let i = 0; i < a.length; i++) dot += a[i] * b[i];
  return dot / (aNorm * bNorm);
}

function topKCandidates(index, qMean, k = PREFILTER_K) {
  const qNorm = l2norm(qMean);
  const scores = [];
  for (const item of index) {
    if (!item.mean) {
      scores.push({ id: item.id, score: -Infinity });
      continue;
    }
    const s = cosine(qMean, item.mean, qNorm, null);
    scores.push({ id: item.id, score: s });
  }
  scores.sort((a, b) => b.score - a.score);
  return scores.slice(0, k).map(s => s.id);
}

async function limitedMap(inputs, workerFn, concurrency = CONCURRENCY) {
  const out = [];
  let i = 0;
  const running = [];
  while (i < inputs.length || running.length) {
    while (i < inputs.length && running.length < concurrency) {
      const p = Promise.resolve().then(() => workerFn(inputs[i]));
      running.push(p);
      const idx = i;
      i++;
      p.then(result => { out[idx] = result; })
       .catch(err => { out[idx] = { error: err }; });
    }
    await Promise.race(running).catch(()=>{});
    for (let r = running.length - 1; r >= 0; r--) {
      if (running[r].isFulfilled || running[r].isRejected) running.splice(r, 1);
    }
    // Node native promises don't have isFulfilled; safe prune:
    for (let r = running.length - 1; r >= 0; r--) {
      if (running[r].settled) running.splice(r, 1);
    }
    // fallback: rebuild running from still-pending promises
    // (keeps memory in control if .settled isn't available)
    running.length = 0;
  }
  return out;
}

async function queryFile(filePath, index = null) {
  const model = await ensureModel();
  const qembs = await processAudioToEmbeddings(model, filePath);
  if (!qembs || qembs.length === 0) return { best: null, adapt: { accept: false } };

  if (!index) index = await buildIndex();

  const qMean = computeMean(qembs);
  const candidates = topKCandidates(index, qMean, PREFILTER_K);

  const perSongResults = [];
  const scoreDist = [];

  const worker = async (songId) => {
    const item = index.find(x => x.id === songId);
    let seq = null;
    try {
      seq = item.embedPath ? await fs.readFile(item.embedPath) : await loadSongEmbeddings(item.meta);
      if (Buffer.isBuffer(seq)) seq = JSON.parse(seq.toString('utf8'));
    } catch {
      seq = await loadSongEmbeddings(item.meta);
    }
    if (!seq || seq.length === 0) return { id: songId, score: -Infinity, pos: -1 };
    const res = scoreSequenceMatch(qembs, seq);
    return { id: songId, score: res.bestScore, pos: res.pos };
  };

  const results = [];
  for (let i = 0; i < candidates.length; i += CONCURRENCY) {
    const chunk = candidates.slice(i, i + CONCURRENCY);
    const chunkRes = await Promise.all(chunk.map(worker));
    results.push(...chunkRes);
    for (const r of chunkRes) scoreDist.push(r.score);
  }

  results.sort((a, b) => b.score - a.score);
  const best = results[0] || null;
  const second = results[1] || { score: -Infinity };
  const adapt = adaptiveDecision(best ? best.score : -Infinity, best ? best.id : null, second.score, qembs.length, scoreDist);

  return { best, second, adapt, rawResults: results };
}

function adaptiveDecision(bestScore, bestId, secondScore, qLen, scoreDist) {
  if (!scoreDist || scoreDist.length === 0) return { accept: false, reason: 'no-data' };
  const mean = scoreDist.reduce((a,b)=>a+b,0)/scoreDist.length;
  const sd = Math.sqrt(scoreDist.reduce((s,x)=>s+(x-mean)*(x-mean),0)/scoreDist.length);
  const z = sd > 0 ? (bestScore - mean) / sd : 0;
  const margin = 0.12;
  const minAbsolute = 0.6;
  const accept = (bestScore >= minAbsolute && bestScore - secondScore > margin) || z >= 3;
  return { accept, bestScore, secondScore, z, mean, sd };
}