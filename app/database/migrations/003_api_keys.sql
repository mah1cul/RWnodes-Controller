CREATE TABLE IF NOT EXISTS api_keys (
    name TEXT PRIMARY KEY,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
