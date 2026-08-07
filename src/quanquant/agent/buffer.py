"""agent 本機 durable outbox：券商 callback 落地點（T0.1 零丟單的跨網路對應）。

- append() 由 SDK 子程序的 callback 執行緒呼叫：同步 INSERT+commit 成功才返回。
- pending()/mark_sent() 由父程序（WS 泵）呼叫：跨程序經同一 SQLite 檔（WAL）。
- 每次操作短連線 + busy_timeout，避免跨程序鎖競爭複雜化。

Inc1 D11/D4（Task 9）：schema v2 加 outbox.account/mode/cmd_id ＋新表 command_ledger，
support agent 端 command ledger 執行去重（① ledger 命中不重執行，見 runner.py
`_execute_mutating_command`）。`meta['schema_version']` 標記版本；升級三情境見
`_ensure_schema_v2`。

Inc1 D9/G2（Task 12）：本檔另提供三組 fail-stop 狀態機用的原語——
  - `write_sentinel`/`read_sentinel`/`clear_sentinel`/`has_sentinel`：**buffer 之外**的
    純檔案系統 durable latch 標記（`<buffer_path>.failstop`）——child 的 callback 落地
    失敗（含退化寫入亦失敗）時，SQLite 本身可能已經壞了，不能指望再寫一筆 SQLite 列來
    記錄「壞了」這件事；純檔案 open/write/flush/fsync/replace 是與 SQLite 完全獨立的
    I/O 路徑，latch 狀態因此能在 agent 程序重啟後仍然存在（sentinel 存在＝latch）。
  - `get_health_epoch`/`set_health_epoch`：`health_epoch` 持久化於 `meta` 表（G2⑤單調性，
    latch 時 +1）——buffer 本身健康時才寫得進去；若 buffer 已壞，權威值退回 sentinel
    檔內記的 epoch（見 `runner.py` 啟動時的復原邏輯）。
  - `probe`：G2④ storage probe——對同一 buffer 寫入→commit→讀回，成功才代表可解除 latch、
    回報 `status="ok"`。任何例外一律吞掉回 False（探針失敗是常見情境，不是呼叫端的錯）。
"""
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

_SCHEMA_VERSION = "2"

# 冪等（IF NOT EXISTS）：新檔直接建齊；v1 乾淨升級時先 DROP TABLE outbox 再跑這段補上
# v2 欄位；已是 v2 的檔案重跑也是 no-op。partial unique index（R2-9）：同一 cmd_id 至多
# 一筆未送 ack（`sent_at IS NULL`），check-and-insert 之外的最後一道防線。
_SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  account TEXT,
  mode TEXT,
  cmd_id TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  sent_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_outbox_unsent ON outbox(id) WHERE sent_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_cmd_unsent
  ON outbox(cmd_id) WHERE cmd_id IS NOT NULL AND sent_at IS NULL;
CREATE TABLE IF NOT EXISTS command_ledger (
  cmd_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  result TEXT NOT NULL,
  executed_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
"""


class RefuseStartError(RuntimeError):
    """D11 schema 升級：偵測到舊版（v1）outbox 且仍有未送列，拒絕啟動——直接重建會把
    尚未送達 server 的事件憑空丟掉，違反零丟單不變量。訊息附具體筆數與處置建議。"""


@dataclass(frozen=True)
class BufferRow:
    id: int
    kind: str
    payload: dict
    account: str | None = None
    mode: str | None = None
    cmd_id: str | None = None


class DurableBuffer:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            self._ensure_schema_v2(conn)
        # codex round1 fix7：outbox 累積無上限，每次開啟（含 agent 重啟）順手清一次已送達
        # 超過保留期的舊列，避免本機 sqlite 檔案無止盡長大。
        self.prune_sent()

    def _ensure_schema_v2(self, conn: sqlite3.Connection) -> None:
        """D11 三種升級情境：①無 outbox 表（全新檔）→ 直接建 v2；②有 outbox 表且
        `meta.schema_version` 已是 '2' → no-op（idempotent 重跑 IF NOT EXISTS 亦安全）；
        ③有 outbox 表但版本不符（v1／未知）→ 查未送列：有 → RefuseStartError 拒啟且不動
        任何資料（操作者可退回舊版繼續送完）；無（乾淨）→ DROP 舊 outbox 後重建 v2。"""
        has_outbox = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbox'"
        ).fetchone() is not None
        if has_outbox:
            version = conn.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            if version is None or version[0] != _SCHEMA_VERSION:
                unsent = int(conn.execute(
                    "SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL"
                ).fetchone()[0])
                if unsent > 0:
                    raise RefuseStartError(
                        f"agent 暫存箱（{self._path}）是舊版格式，且尚有 {unsent} 筆"
                        "未送出的資料，請先用舊版送完（unsent_count() 歸零）再升級，"
                        "避免資料遺失；或人工確認可捨棄後手動清空再重啟。"
                    )
                conn.execute("DROP TABLE outbox")
        conn.executescript(_SCHEMA_V2)
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (_SCHEMA_VERSION,),
        )

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path, timeout=5)

    @property
    def path(self) -> str:
        return self._path

    # ------------------------------------------------------------------
    # G2①/⑤（Task 12）：sentinel 檔（buffer 之外路徑，durable latch 標記）
    # ------------------------------------------------------------------

    def _sentinel_path(self) -> Path:
        return Path(self._path + ".failstop")

    def write_sentinel(self, *, epoch: int, detail: str, fault_token: str | None = None) -> None:
        """獨立於 SQLite 之外的 durable latch 標記——純檔案系統操作，buffer 本身寫壞了也
        不影響這裡成功與否。寫暫存檔再 `os.replace` 原子改名，避免中途崩潰留下半寫檔案。

        R7-2（HIGH，codex 終審 round7）：`fault_token`——child 端（`native_runner.py
        _trigger_failstop_latch`）每次落地失敗都會帶一個新產生的唯一 nonce，供
        `AgentRunner._recover()` 在 terminate 前後兩次讀取比對，偵測「child 死前最後一刻
        又落地一筆新故障」（dying-gasp，見其 docstring）。選填、預設 `None`——只有在提供
        時才寫入 JSON（省略鍵，不是寫入字面 `null`），維持既有呼叫端（`AgentRunner._latch`
        的父程序端覆寫、以及所有既有測試對 `read_sentinel()` 的精確 dict 比對）逐位元組
        向後相容；讀到沒有這個欄位的舊格式 sentinel 時，`read_sentinel().get("fault_token")`
        自然回 `None`，不會誤判成「跟新故障撞了同一個 token」（那樣反而會誤觸發保留邏輯）。"""
        path = self._sentinel_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        data: dict = {"epoch": epoch, "detail": detail}
        if fault_token is not None:
            data["fault_token"] = fault_token
        payload = json.dumps(data, ensure_ascii=False)
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def read_sentinel(self) -> dict | None:
        path = self._sentinel_path()
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def clear_sentinel(self) -> None:
        self._sentinel_path().unlink(missing_ok=True)

    def has_sentinel(self) -> bool:
        return self._sentinel_path().exists()

    # ------------------------------------------------------------------
    # G2④/⑤（Task 12）：health_epoch 持久化 ＋ storage probe
    # ------------------------------------------------------------------

    def get_health_epoch(self) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'health_epoch'"
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def set_health_epoch(self, epoch: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('health_epoch', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(epoch),),
            )

    def probe(self) -> bool:
        """G2④：對同一 buffer 寫入→commit→讀回，成功才代表可解除 latch、回報 status="ok"。
        任何例外（SQLite 仍壞）一律回 False，不 raise——呼叫端（`runner.py` 的 recovery
        流程）據此判斷要不要繼續等下一輪，不需要 try/except 包這個呼叫。"""
        token = uuid.uuid4().hex
        try:
            with self._conn() as conn:
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES ('probe', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (token,),
                )
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT value FROM meta WHERE key = 'probe'"
                ).fetchone()
            return row is not None and row[0] == token
        except sqlite3.Error:
            return False

    def append(self, kind: str, payload: dict, *, account: str | None = None,
               mode: str | None = None, cmd_id: str | None = None) -> int:
        """`account`/`mode`：D5/I7 來源端蓋章——callback 落地當下就該傳入，讓事件歸屬
        在落 outbox 那一刻凍結（不可變）；`cmd_id`：僅 command_ledger 去重的 ack 事件
        （由 record_execution/ensure_cmd_ack_pending 呼叫）會帶，report 事件不帶。"""
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO outbox (kind, payload, account, mode, cmd_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (kind, json.dumps(payload, ensure_ascii=False), account, mode, cmd_id),
            )
            return int(cur.lastrowid)

    def pending(self, limit: int = 50) -> list[BufferRow]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, kind, payload, account, mode, cmd_id FROM outbox "
                "WHERE sent_at IS NULL ORDER BY id LIMIT ?", (limit,)).fetchall()
        return [BufferRow(id=r[0], kind=r[1], payload=json.loads(r[2]),
                          account=r[3], mode=r[4], cmd_id=r[5]) for r in rows]

    def mark_sent(self, event_id: int) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE outbox SET sent_at = datetime('now') WHERE id = ?",
                         (event_id,))

    def lookup_command(self, cmd_id: str) -> str | None:
        """D4 agent 端①：command_ledger 命中回傳存檔的 result（JSON 字串，供呼叫端原樣
        存回／比對，不在這裡反序列化）；未命中回 None（呼叫端需走②③④正常流程）。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT result FROM command_ledger WHERE cmd_id = ?", (cmd_id,)
            ).fetchone()
        return row[0] if row is not None else None

    def _append_ack_if_absent(self, conn: sqlite3.Connection, *, cmd_id: str,
                               result_json: str, account: str | None,
                               mode: str | None) -> int:
        """R2-9 check-and-insert：cmd_id 若已有未送（`sent_at IS NULL`）ack 列，直接回傳
        該列 id，不重複 append——partial unique index（`ux_outbox_cmd_unsent`）是預期不會
        撞到的最後一道防線，不是這裡的主要防呆手段（先查再插，行為明確不依賴例外分支）。"""
        row = conn.execute(
            "SELECT id FROM outbox WHERE cmd_id = ? AND sent_at IS NULL", (cmd_id,)
        ).fetchone()
        if row is not None:
            return int(row[0])
        cur = conn.execute(
            "INSERT INTO outbox (kind, payload, account, mode, cmd_id) "
            "VALUES ('cmd_ack', ?, ?, ?, ?)",
            (result_json, account, mode, cmd_id),
        )
        return int(cur.lastrowid)

    def record_execution(self, cmd_id: str, kind: str, result: dict, *,
                          account: str | None, mode: str | None) -> int:
        """D4 agent 端④：執行 native 後（不論成功/失敗/timeout——只要嘗試過，一律記錄，
        確保「每筆 mutating 指令恰好收斂一次」，I6）同一 SQLite 交易寫 command_ledger
        （`kind`＝place/cancel/update）＋ append 一筆 outbox cmd_ack 事件（check-and-insert，
        R2-9）。回傳 ack 在 outbox 的 event_id，供呼叫端記錄／測試斷言；實際送達交給
        `_pump` 走 outbox at-least-once（與 UpReport 共用補送機制，D4）。"""
        result_json = json.dumps(result, ensure_ascii=False)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO command_ledger (cmd_id, kind, result) VALUES (?, ?, ?)",
                (cmd_id, kind, result_json),
            )
            return self._append_ack_if_absent(
                conn, cmd_id=cmd_id, result_json=result_json, account=account, mode=mode
            )

    def ensure_cmd_ack_pending(self, cmd_id: str, result_json: str, *,
                                account: str | None, mode: str | None) -> int:
        """D4 agent 端①命中分支：確保 outbox 有該 cmd 未送的 ack（無則以存檔 result 補
        append，同一 SQLite 交易 check-and-insert）。呼叫端應先 `lookup_command` 命中才
        呼叫這個——`result_json` 直接沿用 lookup_command 回傳的存檔字串，不重新序列化。"""
        with self._conn() as conn:
            return self._append_ack_if_absent(
                conn, cmd_id=cmd_id, result_json=result_json, account=account, mode=mode
            )

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
