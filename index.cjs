// @path: index.js
const fs = require('fs');
const path = require('path');
const cp = require('child_process');
const ffmpegPath = require('ffmpeg-static');
const Meyda = require('meyda');
const tf = require('@tensorflow/tfjs-node');

const DB_DIR = path.resolve(__dirname, 'db');
const MODEL_DIR = path.resolve(__dirname, 'model');
const SONGS_JSON = path.join(DB_DIR, 'songs.json');
const SAMPLE_RATE = 16000;
const EMB_DIM = 96;
const PARALLEL_LIMIT = 8;

fs.mkdirSync(DB_DIR, { recursive: true });
fs.mkdirSync(MODEL_DIR, { recursive: true });
fs.existsSync(SONGS_JSON) || fs.writeFileSync(SONGS_JSON, '{}', 'utf8');

async function ensureModel() {
  const modelPath = `file://${MODEL_DIR}/model.json`;
  if (fs.existsSync(path.join(MODEL_DIR, 'model.json')))
    return tf.loadLayersModel(modelPath);

  const model = tf.sequential({
    layers: [
      tf.layers.dense({ inputShape: [16], units: 128, activation: 'relu' }),
      tf.layers.dense({ units: EMB_DIM })
    ]
  });

  await model.save(`file://${MODEL_DIR}`);
  return model;
}

function spawnFFmpegToFloat32(filePath) {
  return new Promise(res => {
    const p = cp.spawn(
      ffmpegPath,
      ['-i', filePath, '-t', 300, '-f', 'f32le', '-ar', String(SAMPLE_RATE), '-ac', '1', '-'],
      { stdio: ['ignore', 'pipe', 'inherit'] }
    );

    const chunks = [];
    p.stdout.on('data', c => chunks.push(c));

    p.on('close', code => {
      if (code !== 0 || !chunks.length) return res(new Float32Array(0));
      const buf = Buffer.concat(chunks);
      const out = new Float32Array(buf.length / 4);
      for (let i = 0; i < out.length; i++) out[i] = buf.readFloatLE(i * 4);
      res(out);
    });

    p.on('error', () => res(new Float32Array(0)));
  });
}

function frameAudio(float32arr, frameSec = 1.0, hopSec = 1.0) {
    const frameLen = Math.floor(frameSec * SAMPLE_RATE);
    const hop = Math.floor(hopSec * SAMPLE_RATE);
    const frames = [];
    for (let start = 0; start + frameLen <= float32arr.length; start += hop) {
        frames.push(float32arr.subarray(start, start + frameLen));
    }

    if (frames.length === 0 && float32arr.length > 0) {
        const buf = new Float32Array(frameLen);
        buf.set(float32arr.subarray(0, Math.min(float32arr.length, frameLen)));
        frames.push(buf);
    }
    return frames;
}

function computeFrameFeatures(frame) {
    const bufferSize = 2048;
    const hop = 1024;
    const mfccs = [];
    const centroids = [];
    const flatnesses = [];
    const rmss = [];

    for (let off = 0; off + bufferSize <= frame.length; off += hop) {
        const window = frame.subarray(off, off + bufferSize);

        const feats = Meyda.extract(['mfcc', 'spectralCentroid', 'spectralFlatness', 'rms'], window, {
            sampleRate: SAMPLE_RATE,
            bufferSize
        });
        if (!feats) continue;
        mfccs.push(feats.mfcc);
        centroids.push(feats.spectralCentroid || 0);
        flatnesses.push(feats.spectralFlatness || 0);
        rmss.push(feats.rms || 0);
    }

    if (mfccs.length === 0) {
        const padded = new Float32Array(bufferSize);
        padded.set(frame.subarray(0, Math.min(frame.length, bufferSize)));
        const feats = Meyda.extract(['mfcc', 'spectralCentroid', 'spectralFlatness', 'rms'], padded, {
            sampleRate: SAMPLE_RATE,
            bufferSize
        });
        mfccs.push(feats.mfcc);
        centroids.push(feats.spectralCentroid || 0);
        flatnesses.push(feats.spectralFlatness || 0);
        rmss.push(feats.rms || 0);
    }

    const frameCount = mfccs.length;
    const avgMfcc = new Array(mfccs[0].length).fill(0);
    let avgCentroid = 0,
        avgFlatness = 0,
        avgRms = 0;

    for (let i = 0; i < frameCount; i++) {
        for (let j = 0; j < mfccs[0].length; j++) avgMfcc[j] += mfccs[i][j];
        avgCentroid += centroids[i];
        avgFlatness += flatnesses[i];
        avgRms += rmss[i];
    }

    const featureVec = [];
    for (let j = 0; j < Math.min(13, avgMfcc.length); j++) featureVec.push(avgMfcc[j] / frameCount);
    featureVec.push(avgCentroid / frameCount);
    featureVec.push(avgFlatness / frameCount);
    featureVec.push(avgRms / frameCount);

    return featureVec;
}

async function processAudioToEmbeddings(model, filePath) {
    const raw = await spawnFFmpegToFloat32(filePath);
    const frames = frameAudio(raw, 1.0, 1.0);

    if (frames.length === 0) return [];

    const batchFeatures = frames.map(f => computeFrameFeatures(f));

    const embeddingsData = tf.tidy(() => {
        const inputTensor = tf.tensor2d(batchFeatures);
        const outputs = model.predict(inputTensor);

        const norm = tf.norm(outputs, 'euclidean', 1, true);
        const normalized = outputs.div(tf.maximum(norm, 1e-8));

        return normalized;
    });

    const result = await embeddingsData.array();
    embeddingsData.dispose();
    return result;
}

function saveEmbeddingsBinary(songId, embeddings) {
    const file = path.join(DB_DIR, `${songId}.emb`);

    const flatLen = embeddings.length * EMB_DIM;
    const buf = Buffer.alloc(flatLen * 4);

    let offset = 0;
    for (let i = 0; i < embeddings.length; i++) {
        const vec = embeddings[i];
        for (let j = 0; j < EMB_DIM; j++) {
            buf.writeFloatLE(vec[j], offset);
            offset += 4;
        }
    }

    fs.writeFileSync(file, buf);

    const songs = JSON.parse(fs.readFileSync(SONGS_JSON, 'utf8'));
    songs[songId] = {
        id: songId,
        embFile: path.basename(file),
        len: embeddings.length
    };
    fs.writeFileSync(SONGS_JSON, JSON.stringify(songs, null, 2), 'utf8');
}

function loadSongEmbeddings(songMeta) {
    const file = path.join(DB_DIR, songMeta.embFile);
    const buf = fs.readFileSync(file);
    const floats = new Float32Array(buf.length / 4);
    for (let i = 0; i < floats.length; i++) floats[i] = buf.readFloatLE(i * 4);

    const seq = [];
    for (let i = 0; i < floats.length; i += EMB_DIM) {
        seq.push(floats.subarray(i, i + EMB_DIM));
    }
    return seq;
}

function cosineSim(a, b) {
    let dot = 0,
        na = 0,
        nb = 0;

    const len = a.length;
    for (let i = 0; i < len; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    na = Math.sqrt(na);
    nb = Math.sqrt(nb);
    if (na < 1e-9 || nb < 1e-9) return 0;
    return dot / (na * nb);
}

function scoreSequenceMatch(querySeq, songSeq) {
    const qlen = querySeq.length;
    const slen = songSeq.length;
    if (slen < qlen) return {
        bestScore: -Infinity,
        pos: -1
    };

    let best = -Infinity;
    let bestPos = -1;

    for (let i = 0; i <= slen - qlen; i++) {
        let ssum = 0;
        for (let j = 0; j < qlen; j++) {
            ssum += cosineSim(querySeq[j], songSeq[i + j]);
        }
        const avg = ssum / qlen;
        if (avg > best) {
            best = avg;
            bestPos = i;
        }
    }
    return {
        bestScore: best,
        pos: bestPos
    };
}

function adaptiveDecision(bestScore, bestSong, secondBestScore, qlen, scoreDistribution) {
    const base = qlen < 3 ? 0.88 : qlen < 6 ? 0.82 : 0.75;
    const median = scoreDistribution.length ? medianOf(scoreDistribution) : 0;
    const std = scoreDistribution.length ? stdOf(scoreDistribution) : 1;
    const z = (bestScore - median) / (std || 1e-6);
    const gap = bestScore - secondBestScore;

    const accept = (bestScore >= base && z > 2.0 && gap > 0.06) || (bestScore >= 0.92 && qlen >= 1);
    return {
        accept,
        reason: {
            bestScore,
            base,
            z,
            gap,
            median,
            std
        }
    };
}

function medianOf(arr) {
    if (arr.length === 0) return 0;
    const a = arr.slice().sort((x, y) => x - y);
    const mid = Math.floor(a.length / 2);
    return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

function stdOf(arr) {
    if (arr.length === 0) return 0;
    const m = arr.reduce((a, b) => a + b, 0) / arr.length;
    return Math.sqrt(arr.reduce((s, x) => s + (x - m) * (x - m), 0) / arr.length);
}

async function indexSong(songId, filePath, model = null) {
    console.log(`[${songId}] Processing...`);

    const m = model || await ensureModel();

    const embeddings = await processAudioToEmbeddings(m, filePath);
    if (!embeddings || embeddings.length === 0) {
        console.warn(`[${songId}] No audio features extracted.`);
        return;
    }

    saveEmbeddingsBinary(songId, embeddings);
    console.log(`[${songId}] Saved ${embeddings.length} embeddings. (${Math.round(embeddings.length * EMB_DIM * 4 / 1024)} KB)`);
}

async function indexDir(dirPath) {
    if (!fs.existsSync(dirPath)) return console.error('Folder not found:', dirPath);
    const entries = fs.readdirSync(dirPath);
    const exts = ['.mp3', '.wav', '.opus', '.flac', '.m4a', '.aac', '.ogg'];

    const indexed = JSON.parse(fs.readFileSync(SONGS_JSON, 'utf8'));
    const already = new Set(Object.keys(indexed));

    const model = await ensureModel();
    const queue = [];

    console.log(`Indexing folder with parallelism ${PARALLEL_LIMIT}...`);

    for (const e of entries) {
        const full = path.join(dirPath, e);
        try {
            if (!fs.statSync(full).isFile()) continue;
            const ext = path.extname(e).toLowerCase();
            if (!exts.includes(ext)) continue;
            const id = path.basename(e, ext);

            if (already.has(id)) {
                continue;
            }

            const p = indexSong(id, full, model).catch(err => {
                console.error(`Failed to index ${e}:`, err.message);
            });
            queue.push(p);

            if (queue.length >= PARALLEL_LIMIT) {
                await Promise.race(queue);

                const idx = await Promise.race(queue.map((p, i) => p.then(() => i)));
                queue.splice(idx, 1);
            }
        } catch (err) {
            console.error('Skipping', e, err.message);
        }
    }
    await Promise.all(queue);
    console.log('Indexing complete.');
}

async function main() {
    const argv = process.argv.slice(2);
    const cmd = argv[0];

    if (cmd === 'indexdir' || cmd === 'indexfolder') {
        const dir = argv[1];
        if (!dir) return console.error('indexdir requires folder path');
        await indexDir(dir);
        return;
    }
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});