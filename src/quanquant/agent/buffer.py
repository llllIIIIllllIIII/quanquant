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
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
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
        # codex round1 fix7：outbox 累積無上限，每次開啟（含 agent 重啟）順手清一次已送達
        # 超過保留期的舊列，避免本機 sqlite 檔案無止盡長大。
        self.prune_sent()

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

    def assert_account(self, account: str) -> None:
        """帳號切換 tripwire（codex round1 fix6）：這個 outbox 檔本來只該裝同一個券商帳號
        的回報。啟動時比對 meta 記錄的上次帳號——不存在就記下；相同就放行；不同但 outbox
        裡還有前一帳號未送達的回報，代表換帳號會讓這些回報錯配進新帳號的 session，直接拒絕
        啟動；不同且無未送列（已全部送達/從未累積過），視為乾淨切換，放行並更新 meta。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'account'").fetchone()
            old = row[0] if row is not None else None
            if old is not None and old != account:
                unsent = int(conn.execute(
                    "SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0])
                if unsent > 0:
                    raise RuntimeError(
                        f"outbox 內有前一帳號 {old} 的未送回報，拒絕以帳號 {account} 啟動"
                        "（避免跨帳號錯配）"
                    )
            if old != account:
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES ('account', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (account,),
                )

    def prune_sent(self, retention_days: int = 7) -> int:
        """刪除已送達（sent_at 非空）且早於 retention_days 的舊列，回傳刪除數。未送達列
        （sent_at IS NULL）永不受影響——零丟單設計的前提是未 ack 就不能消失。"""
        with self._conn() as conn:
            cur = conn.execute(
                "DELETE FROM outbox WHERE sent_at IS NOT NULL "
                "AND sent_at < datetime('now', ?)",
                (f"-{retention_days} days",),
            )
            return cur.rowcount
