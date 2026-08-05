"""agent 本機 durable outbox：券商 callback 落地點（T0.1 零丟單的跨網路對應）。

- append() 由 SDK 子程序的 callback 執行緒呼叫：同步 INSERT+commit 成功才返回。
- pending()/mark_sent() 由父程序（WS 泵）呼叫：跨程序經同一 SQLite 檔（WAL）。
- 每次操作短連線 + busy_timeout，避免跨程序鎖競爭複雜化。
"""
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  sent_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_outbox_unsent ON outbox(id) WHERE sent_at IS NULL;
"""


@dataclass(frozen=True)
class BufferRow:
    id: int
    kind: str
    payload: dict


class DurableBuffer:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path, timeout=5)

    def append(self, kind: str, payload: dict) -> int:
        with self._conn() as conn:
            cur = conn.execute("INSERT INTO outbox (kind, payload) VALUES (?, ?)",
                               (kind, json.dumps(payload, ensure_ascii=False)))
            return int(cur.lastrowid)

    def pending(self, limit: int = 50) -> list[BufferRow]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, kind, payload FROM outbox WHERE sent_at IS NULL "
                "ORDER BY id LIMIT ?", (limit,)).fetchall()
        return [BufferRow(id=r[0], kind=r[1], payload=json.loads(r[2])) for r in rows]

    def mark_sent(self, event_id: int) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE outbox SET sent_at = datetime('now') WHERE id = ?",
                         (event_id,))

    def unsent_count(self) -> int:
        with self._conn() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0])
