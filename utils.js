export const db = new DB(DBFILE);

export const dbInit = () => {
  db.pragma("journal_mode = WAL");
  db.exec(`
    PRAGMA synchronous=OFF;
    PRAGMA temp_store=MEMORY;
    PRAGMA cache_size=-200000;
    PRAGMA locking_mode=EXCLUSIVE;
    PRAGMA mmap_size=268435456;

    CREATE TABLE IF NOT EXISTS tracks(
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS fp(
      h  INTEGER NOT NULL,
      id INTEGER NOT NULL,
      t  INTEGER NOT NULL,
      PRIMARY KEY(h, id, t)
    ) WITHOUT ROWID;
  `);

  return db;
};
