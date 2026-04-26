from __future__ import annotations

import re
import hashlib
import hmac
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


NODE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
PRESET_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
API_KEY_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
COUNTRY_CODE_RE = re.compile(r"^[A-Z]{2}$")
RESERVED_NODE_NAMES = {"all"}
PRESET_FIELDS = {"name", "user", "host", "port", "ssh_key"}
MIGRATIONS_DIR = Path(__file__).with_name("migrations")


@dataclass(frozen=True)
class Node:
    name: str
    host: str
    user: str
    port: int = 22
    ssh_key_path: str | None = None
    password: str | None = None
    become: bool = False
    become_password: str | None = None
    country_code: str | None = None

    @property
    def auth_summary(self) -> str:
        if self.ssh_key_path:
            return "SSH key"
        if self.password:
            return "SSH password"
        return "SSH default"


@dataclass(frozen=True)
class Preset:
    field: str
    name: str
    value: str


@dataclass(frozen=True)
class ApiKey:
    name: str
    created_at: str


class NodeStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()

    def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            self._run_migrations(conn)
            conn.commit()

    def _run_migrations(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )
        applied = {
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }

        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = path.stem
            if version in applied:
                continue
            try:
                conn.executescript(path.read_text(encoding="utf-8"))
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, datetime.now(UTC).isoformat()),
            )

    def add_or_update(self, node: Node) -> None:
        self.validate_node(node)
        now = datetime.now(UTC).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO nodes (
                    name, host, user, port, ssh_key_path, password,
                    become, become_password, country_code, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    host=excluded.host,
                    user=excluded.user,
                    port=excluded.port,
                    ssh_key_path=excluded.ssh_key_path,
                    password=excluded.password,
                    become=excluded.become,
                    become_password=excluded.become_password,
                    country_code=excluded.country_code,
                    updated_at=excluded.updated_at
                """,
                (
                    node.name,
                    node.host,
                    node.user,
                    node.port,
                    node.ssh_key_path,
                    node.password,
                    int(node.become),
                    node.become_password,
                    node.country_code,
                    now,
                    now,
                ),
            )
            conn.commit()

    def delete(self, name: str) -> bool:
        with self._lock, self._connect() as conn:
            cursor = conn.execute("DELETE FROM nodes WHERE name = ?", (name,))
            conn.commit()
            return cursor.rowcount > 0

    def get(self, name: str) -> Node | None:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE name = ?", (name,)).fetchone()
        return self._row_to_node(row) if row else None

    def list(self) -> list[Node]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM nodes ORDER BY name").fetchall()
        return [self._row_to_node(row) for row in rows]

    def resolve_target(self, target: str) -> list[Node]:
        target = target.strip()
        if target == "all":
            nodes = self.list()
            if not nodes:
                raise ValueError("No nodes have been added yet")
            return nodes

        node = self.get(target)
        if not node:
            raise ValueError(f"Node {target!r} was not found")
        return [node]

    def set_preset(self, preset: Preset) -> None:
        self._validate_preset(preset)
        now = datetime.now(UTC).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO presets (field, name, value, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(field, name) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (preset.field, preset.name, preset.value, now, now),
            )
            conn.commit()

    def get_preset(self, field: str, name: str) -> Preset | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM presets WHERE field = ? AND name = ?",
                (field, name),
            ).fetchone()
        return self._row_to_preset(row) if row else None

    def list_presets(self, field: str | None = None) -> list[Preset]:
        with self._lock, self._connect() as conn:
            if field:
                rows = conn.execute(
                    "SELECT * FROM presets WHERE field = ? ORDER BY name",
                    (field,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM presets ORDER BY field, name",
                ).fetchall()
        return [self._row_to_preset(row) for row in rows]

    def delete_preset(self, field: str, name: str) -> Preset | None:
        preset = self.get_preset(field, name)
        if not preset:
            return None

        with self._lock, self._connect() as conn:
            conn.execute(
                "DELETE FROM presets WHERE field = ? AND name = ?",
                (field, name),
            )
            conn.commit()
        return preset

    def create_api_key(self, name: str) -> str:
        self._validate_api_key_name(name)
        raw_key = secrets.token_urlsafe(32)
        key_hash = self._api_key_hash(raw_key)
        now = datetime.now(UTC).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO api_keys (name, key_hash, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    key_hash=excluded.key_hash,
                    created_at=excluded.created_at
                """,
                (name, key_hash, now),
            )
            conn.commit()
        return raw_key

    def list_api_keys(self) -> list[ApiKey]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT name, created_at FROM api_keys ORDER BY name").fetchall()
        return [ApiKey(name=row["name"], created_at=row["created_at"]) for row in rows]

    def has_api_keys(self) -> bool:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT 1 FROM api_keys LIMIT 1").fetchone()
        return row is not None

    def verify_api_key(self, raw_key: str | None) -> bool:
        if not raw_key:
            return False
        key_hash = self._api_key_hash(raw_key.strip())
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT key_hash FROM api_keys").fetchall()
        return any(hmac.compare_digest(key_hash, row["key_hash"]) for row in rows)

    def delete_api_key(self, name: str) -> bool:
        with self._lock, self._connect() as conn:
            cursor = conn.execute("DELETE FROM api_keys WHERE name = ?", (name,))
            conn.commit()
            return cursor.rowcount > 0

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @classmethod
    def validate_node(cls, node: Node) -> None:
        cls._validate_node(node)

    @staticmethod
    def _validate_node(node: Node) -> None:
        if not NODE_NAME_RE.match(node.name):
            raise ValueError("Node name must be 1-64 chars: letters, numbers, '.', '_' or '-'")
        if node.name in RESERVED_NODE_NAMES:
            raise ValueError(f"Node name {node.name!r} is reserved")
        if not node.host.strip():
            raise ValueError("Host must not be empty")
        if not node.user.strip():
            raise ValueError("User must not be empty")
        if not 1 <= node.port <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        if node.ssh_key_path and node.password:
            raise ValueError("Use either ssh key or password auth, not both")
        if node.country_code and not COUNTRY_CODE_RE.match(node.country_code):
            raise ValueError("Country code must be two uppercase letters")

    @staticmethod
    def _validate_preset(preset: Preset) -> None:
        if preset.field not in PRESET_FIELDS:
            raise ValueError(f"Preset field must be one of: {', '.join(sorted(PRESET_FIELDS))}")
        if not PRESET_NAME_RE.match(preset.name):
            raise ValueError("Preset name must be 1-32 chars: letters, numbers, '.', '_' or '-'")
        if not preset.value.strip():
            raise ValueError("Preset value must not be empty")

    @staticmethod
    def _validate_api_key_name(name: str) -> None:
        if not API_KEY_NAME_RE.match(name):
            raise ValueError("API key name must be 1-32 chars: letters, numbers, '.', '_' or '-'")

    @staticmethod
    def _api_key_hash(raw_key: str) -> str:
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    @staticmethod
    def _row_to_node(row: sqlite3.Row) -> Node:
        return Node(
            name=row["name"],
            host=row["host"],
            user=row["user"],
            port=int(row["port"]),
            ssh_key_path=row["ssh_key_path"],
            password=row["password"],
            become=bool(row["become"]),
            become_password=row["become_password"],
            country_code=row["country_code"],
        )

    @staticmethod
    def _row_to_preset(row: sqlite3.Row) -> Preset:
        return Preset(
            field=row["field"],
            name=row["name"],
            value=row["value"],
        )
