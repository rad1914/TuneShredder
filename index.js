// @path: index.js
import { Worker } from "worker_threads";
import os from "os";
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

function dbInit() {
  awaitMkdir(path.dirname(DBFILE));
  const db = new Database(DBFILE);
  db.exec(`
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=OFF;
    PRAGMA temp_store=MEMORY;
    PRAGMA cache_size=-200000;
    PRAGMA locking_mode=EXCLUSIVE;
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS tracks(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
    CREATE TABLE IF NOT EXISTS fp(h INTEGER NOT NULL, id INTEGER NOT NULL, t INTEGER NOT NULL);
  `);
  return db;
}

async function awaitMkdir(dir) {
  try { await fs.mkdir(dir, { recursive: true }); } catch {}
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
  const db = dbInit();
  const insTrack = db.prepare("INSERT OR IGNORE INTO tracks(name) VALUES(?)");
  const getTrack = db.prepare("SELECT id FROM tracks WHERE name=?");
  const flushWriter = flushBatchFactory(db);

  const files = (await fs.readdir(dir))
    .filter((f) => EXT.test(f))
    .map((f) => path.join(dir, f));

  const cpus = Math.max(1, Math.min(os.cpus().length, C.threads || os.cpus().length));
  const workers = [];

  for (let i = 0; i < cpus; i++) {
    const w = new Worker(new URL('./worker.js', import.meta.url), { workerData: C });
    w.on("message", (msg) => {
      if (msg.type === "batches") {
        flushWriter(msg.id, msg.Hh, msg.Tt, msg.n);
      } else if (msg.type === "log") {
        process.stdout.write(msg.msg);
      } else if (msg.type === "done") {
        process.stdout.write(`\rProcessed: ${msg.name}\n`);
      }
    });
    w.on("error", (err) => console.error("Worker error:", err));
    w.on("exit", (code) => { if (code) console.error("Worker exit code", code); });
    workers.push(w);
  }

  let wi = 0;
  for (const file of files) {
    const name = path.basename(file);
    if (getTrack.get(name)) continue;
    insTrack.run(name);
    const id = getTrack.get(name).id;
    const w = workers[wi];
    w.postMessage({ type: "file", path: file, id, name });
    wi = (wi + 1) % workers.length;
  }

  for (const w of workers) w.postMessage({ type: "drain" });

  await Promise.all(workers.map(w => new Promise((res) => w.on("exit", res))));
  db.exec("CREATE INDEX IF NOT EXISTS idx_fp_id ON fp(id);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_fp_h ON fp(h);");
  db.close();
  console.log("Indexing complete.");
}

main().catch(err => { console.error(err); process.exit(1); });
