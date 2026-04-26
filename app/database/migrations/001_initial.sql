CREATE TABLE IF NOT EXISTS nodes (
    name TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    user TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 22,
    ssh_key_path TEXT,
    password TEXT,
    become INTEGER NOT NULL DEFAULT 0,
    become_password TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS presets (
    field TEXT NOT NULL,
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (field, name)
);
