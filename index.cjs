const fs = require('fs');
const path = require('path');
const cp = require('child_process');
const ffmpegPath = require('ffmpeg-static');
const Meyda = require('meyda');
const tf = require('@tensorflow/tfjs-node');

const DB_DIR = path.resolve(__dirname, 'db');
const MODEL_DIR = path.resolve(__dirname, 'model');
const SONGS_JSON = path.join(DB_DIR, "'_songs.json");
const SAMPLE_RATE = 44100;
const EMB_DIM = 96;
const PARALLEL = 8;
const FRAME_SECONDS = 0.5;
const HOP_SECONDS = 0.25;
const TARGET_MFCC = 20;
const EXTRA_FEATURES = ['rms', 'zcr', 'spectralCentroid', 'spectralFlatness', 'spectralRolloff'];
const FEATURE_DIM = 32;

fs.mkdirSync(DB_DIR, { recursive: true });
fs.mkdirSync(MODEL_DIR, { recursive: true });
if (!fs.existsSync(SONGS_JSON)) fs.writeFileSync(SONGS_JSON, '{}', 'utf8');

async function ensureModel() {
    const mfile = path.join(MODEL_DIR, 'model.json');
    if (fs.existsSync(mfile)) return tf.loadLayersModel(`file://${mfile}`);
    const model = tf.sequential();
    model.add(tf.layers.dense({ inputShape: [FEATURE_DIM], units: 256, activation: 'relu', kernelInitializer: 'glorotUniform' }));
    model.add(tf.layers.dense({ units: 128, activation: 'relu', kernelInitializer: 'glorotUniform' }));
    model.add(tf.layers.dense({ units: EMB_DIM }));
    await model.save(`file://${MODEL_DIR}`);
    return model;
}

function spawnFFmpegToFloat32(filePath) {
    return new Promise(res => {
        const args = [
            '-hide_banner',
            '-loglevel', 'error',
            '-i', filePath,
            '-f', 'f32le',
            '-ar', String(SAMPLE_RATE),
            '-ac', '1',
            '-'
        ];
        const p = cp.spawn(ffmpegPath, args, { stdio: ['ignore', 'pipe', 'inherit'] });
        const chunks = [];
        p.stdout.on('data', c => chunks.push(c));
        p.on('error', () => res(new Float32Array(0)));
        p.on('close', code => {
            if (code !== 0 || chunks.length === 0) return res(new Float32Array(0));
            const buf = Buffer.concat(chunks);
            const out = new Float32Array(buf.length / 4);
            for (let i = 0; i < out.length; i++) out[i] = buf.readFloatLE(i * 4);
            res(out);
        });
    });
}

function hannWindow(buf) {
    const N = buf.length;
    for (let i = 0; i < N; i++) buf[i] *= 0.5 * (1 - Math.cos((2 * Math.PI * i) / (N - 1)));
    return buf;
}

function frameAudio(arr, frameSeconds = FRAME_SECONDS, hopSeconds = HOP_SECONDS) {
    const fl = Math.floor(frameSeconds * SAMPLE_RATE);
    const hop = Math.max(1, Math.floor(hopSeconds * SAMPLE_RATE));
    const frames = [];
    if (arr.length === 0) return frames;
    for (let s = 0; s + fl <= arr.length; s += hop) {
        frames.push(arr.subarray(s, s + fl));
    }
    if (frames.length === 0 && arr.length > 0) {
        const buf = new Float32Array(fl);
        buf.set(arr.subarray(0, Math.min(arr.length, fl)));
        frames.push(buf);
    }
    return frames;
}

function computeFeatures(frame) {
    const B = Math.min(4096, Math.max(1024, frame.length)); // adapt buffer size
    const buf = frame.length >= B ? frame.subarray(0, B) : (() => {
        const p = new Float32Array(B);
        p.set(frame);
        return p;
    })();
    hannWindow(buf);
    const feats = Meyda.extract(['mfcc', ...EXTRA_FEATURES], Array.from(buf), {
        sampleRate: SAMPLE_RATE,
        bufferSize: B,
        numberOfMFCCCoefficients: TARGET_MFCC
    }) || {};
    const mfcc = feats.mfcc || new Array(TARGET_MFCC).fill(0);
    const out = mfcc.slice(0, TARGET_MFCC);
    EXTRA_FEATURES.forEach(f => out.push(feats[f] != null ? feats[f] : 0));
    while (out.length < FEATURE_DIM) out.push(0);
    if (out.length > FEATURE_DIM) out.length = FEATURE_DIM;
    return out;
}

function normalizeFeaturesMatrix(matrix) {
    if (!matrix.length) return matrix;
    const cols = matrix[0].length;
    const means = new Array(cols).fill(0);
    const vars = new Array(cols).fill(0);
    for (let j = 0; j < cols; j++) {
        for (let i = 0; i < matrix.length; i++) means[j] += matrix[i][j];
        means[j] /= matrix.length;
    }
    for (let j = 0; j < cols; j++) {
        for (let i = 0; i < matrix.length; i++) {
            const d = matrix[i][j] - means[j];
            vars[j] += d * d;
        }
        vars[j] = Math.sqrt(vars[j] / matrix.length) + 1e-8;
    }
    for (let i = 0; i < matrix.length; i++) {
        for (let j = 0; j < cols; j++) matrix[i][j] = (matrix[i][j] - means[j]) / vars[j];
    }
    return matrix;
}

async function processToEmbeddings(model, filePath) {
    const raw = await spawnFFmpegToFloat32(filePath);
    const frames = frameAudio(raw, FRAME_SECONDS, HOP_SECONDS);
    if (!frames.length) return [];
    const feats = frames.map(computeFeatures);
    const normFeats = normalizeFeaturesMatrix(feats);
    const embeddings = await tf.tidy(() => {
        const x = tf.tensor2d(normFeats);
        const y = model.predict(x);
        const norms = y.square().sum(1).sqrt().expandDims(1).add(1e-8);
        return y.div(norms).arraySync();
    });
    return embeddings;
}

function saveEmbeddings(songId, embeddings) {
    const file = path.join(DB_DIR, `${songId}.emb`);
    const flat = new Float32Array(embeddings.flat());
    fs.writeFileSync(file, Buffer.from(flat.buffer));
    const songs = JSON.parse(fs.readFileSync(SONGS_JSON, 'utf8'));
    songs[songId] = {
        id: songId,
        embFile: path.basename(file),
        len: embeddings.length
    };
    fs.writeFileSync(SONGS_JSON, JSON.stringify(songs, null, 2));
}

async function indexSong(model, id, file) {
    console.log(`[PROCESSING]: ${id}`);
    const embs = await processToEmbeddings(model, file);
    if (!embs || !embs.length) {
        return console.warn(`⚠️ [SKIP] ${id}: No embeddings found.`);
    }
    saveEmbeddings(id, embs);
    console.log(`📥 ${embs.length} frames stored.\n`);
}

async function indexDir(dir) {
    if (!fs.existsSync(dir)) return;
    const exts = new Set(['.mp3', '.wav', '.flac', '.m4a', '.ogg', '.opus', '.aac']);
    const songs = JSON.parse(fs.readFileSync(SONGS_JSON, 'utf8'));
    const already = new Set(Object.keys(songs));
    const model = await ensureModel();
    const entries = fs.readdirSync(dir).filter(e => exts.has(path.extname(e).toLowerCase()));
    let i = 0;
    async function worker() {
        while (i < entries.length) {
            const e = entries[i++];
            const full = path.join(dir, e);
            try {
                if (!fs.statSync(full).isFile()) continue;
                const id = path.basename(e, path.extname(e));
                if (already.has(id)) continue;
                await indexSong(model, id, full);
            } catch (err) {
                console.error(`[ERR] ${e}:`, err && err.message ? err.message : err);
            }
        }
    }
    await Promise.all(Array.from({ length: PARALLEL }, () => worker()));
}

async function main() {
    const argv = process.argv.slice(2);
    if (!argv[0]) {
        console.log('usage: node index.js /path/to/folder/');
        return;
    }
    await indexDir(argv[1]);
    return;
}

main().catch(e => {
    console.error(e);
    process.exit(1);
});