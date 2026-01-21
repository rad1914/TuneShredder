// @path: index.js
import { Worker } from "node:worker_threads";
import os from "node:os";
import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";

const DBFILE = "./db/fp.sqlite";
const EXT = /\.(mp3|wav|flac|ogg|opus|m4a)$/i;

const C = {
  sr: 22050, ch: 1,
  win: 4096, hop: 512,
  top: 14, fan: 4,
  zone: 30, min: 0.02,
  anchorEvery: 2,
  maxMulti: 256,
  dur: 85,
  threads: 8,
};

const mkdirp = (dir) => fs.mkdir(dir, { recursive: true }).catch(() => {});

async function dbInit() {
  await mkdirp(path.dirname(DBFILE));
  const db = new Database(DBFILE);
  db.exec(`
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=OFF;
    PRAGMA temp_store=MEMORY;
    PRAGMA cache_size=-200000;
    PRAGMA locking_mode=EXCLUSIVE;

    CREATE TABLE IF NOT EXISTS tracks(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS fp(h INTEGER NOT NULL, id INTEGER NOT NULL, t INTEGER NOT NULL);
  `);
  return db;
}

const mkInsertMany = (db, n) =>
  db.prepare(`INSERT OR IGNORE INTO fp(h,id,t) VALUES ${"(?,?,?),".repeat(n).slice(0, -1)}`);

function flushBatchFactory(db) {
  const insMany = Array.from({ length: C.maxMulti + 1 }, (_, n) => (n ? mkInsertMany(db, n) : null));
  const args = new Array(C.maxMulti * 3);

  return (id, Hh, Tt, n) => {
    for (let i = 0; i < n;) {
      const take = Math.min(C.maxMulti, n - i);
      for (let k = 0; k < take; k++) {
        args[3 * k] = Hh[i + k];
        args[3 * k + 1] = id;
        args[3 * k + 2] = Tt[i + k];
      }
      insMany[take].run(...args.slice(0, take * 3));
      i += take;
    }
  };
}

async function main() {
  const dir = process.argv[2];
  if (!dir) throw new Error("usage: node main.js <music_dir>");

  const db = await dbInit();
  const insTrack = db.prepare("INSERT OR IGNORE INTO tracks(name) VALUES(?)");
  const getTrack = db.prepare("SELECT id FROM tracks WHERE name=?");
  const flushWriter = flushBatchFactory(db);

  const files = (await fs.readdir(dir))
    .filter((f) => EXT.test(f))
    .map((f) => path.join(dir, f));

  const cpus = Math.max(1, Math.min(os.cpus().length, C.threads || os.cpus().length));
  const workers = Array.from({ length: cpus }, () => {
    const w = new Worker(new URL("./worker.js", import.meta.url), { workerData: C });
    w.on("message", (m) => {
      if (m.type === "batches") flushWriter(m.id, m.Hh, m.Tt, m.n);
      else if (m.type === "log") process.stdout.write(m.msg);
      else if (m.type === "done") process.stdout.write(`\rProcessed: ${m.name}\n`);
    });
    w.on("error", (e) => console.error("Worker error:", e));
    w.on("exit", (c) => c && console.error("Worker exit code", c));
    return w;
  });

  let wi = 0;
  for (const file of files) {
    const name = path.basename(file);
    if (getTrack.get(name)) continue;
    insTrack.run(name);
    const id = getTrack.get(name).id;
    workers[wi].postMessage({ type: "file", path: file, id, name });
    wi = (wi + 1) % workers.length;
  }

  for (const w of workers) w.postMessage({ type: "drain" });
  await Promise.all(workers.map((w) => new Promise((r) => w.once("exit", r))));

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_fp_id ON fp(id);
    CREATE INDEX IF NOT EXISTS idx_fp_h ON fp(h);
  `);
  db.close();
  console.log("Indexing complete.");
}

main().catch((e) => (console.error(e), process.exit(1)));