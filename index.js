import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import FFT from "fft.js";
import Database from "better-sqlite3";

const DBFILE = "./fingerprints.sqlite";
const EXT = /\.(mp3|wav|flac|ogg|opus|m4a)$/i;

const CFG = {
  sr: 22050,
  ch: 1,
  win: 4096,
  hop: 512,
  top: 18,
  fan: 8,
  zone: 45,
  min: 0.015,
};

const WIN = (() => {
  const w = new Float32Array(CFG.win);
  for (let i = 0; i < w.length; i++) w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (w.length - 1)));
  return w;
})();

const decode = (file) =>
  new Promise((res, rej) => {
    const p = spawn(
      "ffmpeg",
      ["-v", "error", "-i", file, "-ac", "" + CFG.ch, "-ar", "" + CFG.sr, "-f", "f32le", "pipe:1"],
      { stdio: ["ignore", "pipe", "inherit"] }
    );
    const chunks = [];
    p.stdout.on("data", (d) => chunks.push(d));
    p.on("close", (c) => {
      if (c) return rej(new Error("ffmpeg decode failed"));
      const buf = Buffer.concat(chunks);
      res(new Float32Array(buf.buffer, buf.byteOffset, buf.byteLength >> 2));
    });
  });

const peaks = (() => {
  const fft = new FFT(CFG.win);
  const inp = new Float32Array(CFG.win);
  const out = fft.createComplexArray();
  const mags = new Float32Array(CFG.win / 2);

  return (s) => {
    const frames = [];
    for (let i = 0; i + CFG.win <= s.length; i += CFG.hop) {
      for (let j = 0; j < CFG.win; j++) inp[j] = s[i + j] * WIN[j];
      fft.realTransform(out, inp);
      fft.completeSpectrum(out);

      for (let k = 0; k < mags.length; k++) {
        const re = out[2 * k], im = out[2 * k + 1];
        mags[k] = Math.hypot(re, im);
      }

      const p = [];
      for (let k = 2; k < mags.length - 2; k++) {
        const a = mags[k];
        if (a < CFG.min) continue;
        if (a > mags[k - 1] && a > mags[k + 1] && a > mags[k - 2] && a > mags[k + 2]) p.push([k, a]);
      }
      p.sort((a, b) => b[1] - a[1]);
      frames.push(p.slice(0, CFG.top).map((x) => x[0]));
    }
    return frames;
  };
})();

const h3 = (f1, f2, dt) => (((f1 & 2047) << 17) | ((f2 & 2047) << 6) | (dt & 63)) | 0;

const fingerprint = (pk) => {
  const fp = [];
  for (let t = 0; t < pk.length; t++) {
    const a = pk[t];
    if (!a?.length) continue;
    for (let i = 0; i < a.length; i++) {
      const f1 = a[i];
      for (let dt = 1; dt <= CFG.zone; dt++) {
        const b = pk[t + dt];
        if (!b?.length) continue;
        for (let j = 0; j < Math.min(CFG.fan, b.length); j++) fp.push([h3(f1, b[j], dt), t]);
      }
    }
  }
  return fp;
};

const initDb = (file) => {
  const db = new Database(file);
  db.pragma("journal_mode = WAL");
  db.exec(`
    PRAGMA synchronous=NORMAL;
    PRAGMA temp_store=MEMORY;

    CREATE TABLE IF NOT EXISTS tracks (
      id   INTEGER PRIMARY KEY,
      name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fp (
      h  INTEGER NOT NULL,
      t  INTEGER NOT NULL,
      id INTEGER NOT NULL REFERENCES tracks(id)
    );

    CREATE INDEX IF NOT EXISTS idx_fp_h  ON fp(h);
    CREATE INDEX IF NOT EXISTS idx_fp_id ON fp(id);
  `);
  return db;
};

async function build(dir) {
  const db = initDb(DBFILE);

  db.exec("DELETE FROM fp; DELETE FROM tracks;");

  const insTrack = db.prepare("INSERT INTO tracks(name) VALUES(?)");
  const insFp = db.prepare("INSERT INTO fp(h,t,id) VALUES(?,?,?)");

  const addTrack = (name) => insTrack.run(name).lastInsertRowid;
  const addFp = db.transaction((id, fp) => {
    for (const [h, t] of fp) insFp.run(h, t, id);
  });

  const files = (await fs.readdir(dir))
    .filter((f) => EXT.test(f))
    .map((f) => path.join(dir, f));

  for (const file of files) {
    const name = path.basename(file);
    console.log("indexing:", name);
    const id = addTrack(name);
    const samples = await decode(file);
    addFp(id, fingerprint(peaks(samples)));
  }

  const tracks = db.prepare("SELECT COUNT(*) c FROM tracks").get().c;
  const buckets = db.prepare("SELECT COUNT(DISTINCT h) c FROM fp").get().c;
  db.close();
  console.log("done:", tracks, "tracks,", buckets, "buckets");
}

const [cmd, dir] = process.argv.slice(2);
if (cmd !== "index" || !dir) throw new Error("usage: node index.js index <music_dir>");
await build(dir);
