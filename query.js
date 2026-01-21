// @path: query.js
import fs from "node:fs/promises";
import path from "node:path";
import Database from "better-sqlite3";

const DBFILE = process.argv[3] || "./db/fp.sqlite";
const THRESH = Number(process.argv[2]) || 100;

const db = new Database(DBFILE, { readonly: true });

const q = db.prepare(`
SELECT t1.id AS id1, t2.id AS id2, t1.name AS name1, t2.name AS name2, COUNT(*) AS shared
FROM fp f1
JOIN fp f2 ON f1.h = f2.h AND f1.id < f2.id
JOIN tracks t1 ON f1.id = t1.id
JOIN tracks t2 ON f2.id = t2.id
GROUP BY f1.id, f2.id
HAVING shared >= ?
ORDER BY shared DESC
LIMIT 250
`);

const fmtCol = (text, width) => String(text).slice(0, width).padEnd(width);

const matchesDir = "./matches";
await fs.mkdir(matchesDir, { recursive: true });

const outFile = path.join(matchesDir, `matches_${THRESH}.txt`);

let out = "";
out += `| ${fmtCol("Shared", 6)} | ${fmtCol("Track 1", 30)} | ${fmtCol("Track 2", 30)} |\n`;
out += "-".repeat(73) + "\n";

for (const row of q.iterate(THRESH)) {
  const shared = String(row.shared).padStart(6);
  const t1 = fmtCol(`${row.id1}:${row.name1}`, 30);
  const t2 = fmtCol(`${row.id2}:${row.name2}`, 30);
  out += `| ${shared} | ${t1} | ${t2} |\n`;
}

await fs.writeFile(outFile, out, "utf8");
console.log(outFile);

db.close();