"""存储层：只追加事件日志 + 哈希链 + 独立审计 + 签署互斥表。

关键边界：
- ``events`` 只允许 INSERT，不提供 UPDATE/DELETE 接口；事件按 seq 串联哈希链。
- ``audit_log`` 使用独立连接独立提交，业务事务回滚不影响审计留痕。
- ``active_major_locks`` 是签署互斥的最终裁决点，仅在签署事务内插入，
  靠主键冲突在数据库层拒绝并发的冲突签署。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

GENESIS_HASH = "0" * 64


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(payload: Any) -> str:
    """稳定序列化：键排序、无空白，保证哈希可复现。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_hash(prev_hash: str, event_id: str, aggregate_id: str,
                 aggregate_type: str, event_type: str, payload: Any,
                 actor: str | None, created_at: str) -> str:
    body = canonical_json({
        "prev": prev_hash,
        "event_id": event_id,
        "aggregate_id": aggregate_id,
        "aggregate_type": aggregate_type,
        "type": event_type,
        "payload": payload,
        "actor": actor,
        "ts": created_at,
    })
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class EventStore:
    """封装 SQLite 连接，约束事件只追加。"""

    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        if self.path == ":memory:":
            # 共享缓存内存库：多个连接（含审计连接、并发工作连接）看到同一份数据。
            self._dsn = (f"file:memdb-{uuid.uuid4().hex}?mode=memory"
                         "&cache=shared")
            self._memory = True
        else:
            self._dsn = self.path
            self._memory = False
        # check_same_thread=False 以便演示并发签署；写操作另用锁串行化 DDL。
        self._conn = self._open(timeout=10)
        self._conn.execute("PRAGMA journal_mode = WAL") if not self._memory else None
        # 审计使用完全独立的连接，拥有独立事务/提交生命周期。
        self._audit_conn = self._open(timeout=10)
        self._append_lock = threading.Lock()
        self._audit_lock = threading.Lock()
        self._init_schema()

    def _open(self, *, timeout: float = 10) -> sqlite3.Connection:
        conn = sqlite3.connect(self._dsn, timeout=timeout,
                               uri=self._memory, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    # ---------- schema ----------

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id       TEXT NOT NULL UNIQUE,
                    aggregate_id   TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    event_type     TEXT NOT NULL,
                    payload        TEXT NOT NULL,
                    actor          TEXT,
                    created_at     TEXT NOT NULL,
                    prev_hash     TEXT NOT NULL,
                    hash           TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempt_id  TEXT NOT NULL,
                    action      TEXT NOT NULL,
                    actor       TEXT,
                    result      TEXT NOT NULL,
                    detail       TEXT,
                    created_at  TEXT NOT NULL
                );
                -- 业务事务内的签署互斥裁决表
                CREATE TABLE IF NOT EXISTS active_major_locks (
                    major_code     TEXT NOT NULL,
                    academic_year  TEXT NOT NULL,
                    proposal_id    TEXT NOT NULL,
                    version_no     INTEGER NOT NULL,
                    locked_at      TEXT NOT NULL,
                    PRIMARY KEY (major_code, academic_year)
                );
                -- 参考数据（当前态）；历史只存在于事件中
                CREATE TABLE IF NOT EXISTS majors (
                    major_code TEXT PRIMARY KEY,
                    name       TEXT NOT NULL,
                    status     TEXT NOT NULL DEFAULT '在招'
                );
                CREATE TABLE IF NOT EXISTS programs (
                    program_id TEXT PRIMARY KEY,
                    major_code TEXT NOT NULL,
                    name       TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS students (
                    student_id  TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    enroll_year TEXT NOT NULL,
                    major_code  TEXT NOT NULL,
                    program_id  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS faculty (
                    faculty_id TEXT PRIMARY KEY,
                    name       TEXT NOT NULL,
                    major_code TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS demand_versions (
                    version_id TEXT PRIMARY KEY,
                    label      TEXT NOT NULL,
                    checksum   TEXT NOT NULL,
                    payload    TEXT NOT NULL
                );
                """
            )

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def worker_connection(self) -> sqlite3.Connection:
        """供并发签署使用的独立连接。

        每个调用返回新连接（WAL 或共享内存库下均可见同一数据）；
        ``BEGIN IMMEDIATE`` 配合 busy_timeout 将并发签署串行裁决。
        """
        conn = sqlite3.connect(self._dsn, timeout=10, isolation_level=None,
                               uri=self._memory, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 10000")
        return conn

    def close(self) -> None:
        self._conn.close()
        self._audit_conn.close()

    # ---------- audit: 独立提交，不受业务事务影响 ----------

    def audit(self, action: str, result: str, *, actor: str | None = None,
              detail: Any = None, attempt_id: str | None = None) -> str:
        attempt_id = attempt_id or uuid.uuid4().hex
        with self._audit_lock:
            self._audit_conn.execute(
                "INSERT INTO audit_log(attempt_id, action, actor, result, detail, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (attempt_id, action, actor, result,
                 canonical_json(detail) if detail is not None else None, _now()),
            )
            # 关键：审计独立立即提交，即使外层业务事务随后回滚。
            self._audit_conn.commit()
        return attempt_id

    def audit_trail(self) -> list[dict]:
        rows = self._audit_conn.execute(
            "SELECT * FROM audit_log ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]

    # ---------- events: 只追加 ----------

    def append(self, aggregate_id: str, event_type: str, payload: Any, *,
               aggregate_type: str = "proposal", actor: str | None = None) -> dict:
        """追加一个事件并立即提交，返回落库事件。

        哈希链跨所有事件（而非仅同一聚合），任何插入顺序上的篡改都会断链。
        """
        event_id = uuid.uuid4().hex
        created_at = _now()
        with self._append_lock:
            prev = self._conn.execute(
                "SELECT hash FROM events ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            prev_hash = prev["hash"] if prev else GENESIS_HASH
            digest = compute_hash(prev_hash, event_id, aggregate_id,
                                  aggregate_type, event_type, payload,
                                  actor, created_at)
            with self._conn:
                cur = self._conn.execute(
                    "INSERT INTO events(event_id, aggregate_id, aggregate_type,"
                    " event_type, payload, actor, created_at, prev_hash, hash)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    (event_id, aggregate_id, aggregate_type, event_type,
                     canonical_json(payload), actor, created_at, prev_hash, digest),
                )
                seq = cur.lastrowid
        return {
            "seq": seq, "event_id": event_id, "aggregate_id": aggregate_id,
            "aggregate_type": aggregate_type, "event_type": event_type,
            "payload": payload, "actor": actor, "created_at": created_at,
            "prev_hash": prev_hash, "hash": digest,
        }

    def append_in_tx(self, cur: sqlite3.Cursor, aggregate_id: str,
                     event_type: str, payload: Any, *,
                     aggregate_type: str = "proposal",
                     actor: str | None = None) -> str:
        """在调用方事务内追加事件（不提交）；prev_hash 取链尾。

        用于签署等多写入操作的原子事务：若事务回滚，事件一并回滚，
        但该次尝试已通过 :meth:`audit` 独立留痕。
        """
        event_id = uuid.uuid4().hex
        created_at = _now()
        prev = cur.execute(
            "SELECT hash FROM events ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        prev_hash = prev["hash"] if prev else GENESIS_HASH
        digest = compute_hash(prev_hash, event_id, aggregate_id,
                              aggregate_type, event_type, payload,
                              actor, created_at)
        cur.execute(
            "INSERT INTO events(event_id, aggregate_id, aggregate_type,"
            " event_type, payload, actor, created_at, prev_hash, hash)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (event_id, aggregate_id, aggregate_type, event_type,
             canonical_json(payload), actor, created_at, prev_hash, digest),
        )
        return digest

    def events(self, aggregate_id: str | None = None) -> list[dict]:
        if aggregate_id is None:
            rows = self._conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE aggregate_id=? ORDER BY seq",
                (aggregate_id,),
            ).fetchall()
        result = []
        for r in rows:
            item = dict(r)
            item["payload"] = json.loads(item["payload"])
            result.append(item)
        return result

    def verify_chain(self) -> list[int]:
        """校验全量哈希链，返回断链事件的 seq 列表（空列表表示完整）。"""
        broken: list[int] = []
        prev_hash = GENESIS_HASH
        for r in self._conn.execute("SELECT * FROM events ORDER BY seq"):
            digest = compute_hash(prev_hash, r["event_id"], r["aggregate_id"],
                                  r["aggregate_type"], r["event_type"],
                                  json.loads(r["payload"]), r["actor"],
                                  r["created_at"])
            if digest != r["hash"] or r["prev_hash"] != prev_hash:
                broken.append(r["seq"])
            prev_hash = r["hash"]
        return broken


def tampered_events(store: EventStore, seqs: Iterable[int]) -> int:
    """测试辅助：直接绕过存储接口篡改已提交事件，返回受影响行数。

    仅用于验证哈希链的检错能力，生产代码不得调用。
    """
    n = 0
    with store.conn:
        for seq in seqs:
            cur = store.conn.execute(
                "UPDATE events SET payload=? WHERE seq=?",
                (canonical_json({"tampered": True}), seq),
            )
            n += cur.rowcount
    return n
