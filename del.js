// @path: del.js

import { readFile, unlink, stat } from 'node:fs/promises';
import path from 'node:path';

async function main() {
  const base = path.resolve('./_tracks');
  const txt = await readFile('dupe.txt', 'utf8');
  const lines = txt.split(/\r?\n/);

  for (const raw of lines) {
    if (!raw) continue;
    const line = raw.trim();
    if (!line) continue;
    if (line.startsWith('==')) continue;          
    if (line.toLowerCase().startsWith('skip:')) continue;

    let p;
    const absIdx = line.indexOf('/');
    if (absIdx !== -1 && line.slice(absIdx).includes('/')) {

      p = line.slice(absIdx).trim();
    } else {

      p = path.join(base, line);
    }

    p = path.resolve(p);

    try {
      const s = await stat(p);
      if (s.isDirectory()) {
        console.log('skip (is dir):', p);
        continue;
      }
      await unlink(p);
      console.log('deleted:', p);
    } catch (err) {
      if (err.code === 'ENOENT') {
        console.log('not found:', p);
      } else if (err.code === 'EISDIR') {
        console.log('skip (is dir):', p);
      } else {
        console.log('error:', p, err.code || err.message);
      }
    }
  }
}

main().catch(e => {
  console.error('fatal:', e);
  process.exit(1);
});
