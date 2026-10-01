"""治理引擎：工作流、版本链、影响快照冻结、签署互斥。

事务边界（对应契约 history_rule / conflict_rule）：

    审计记录 ──独立连接立即提交──► 即使业务事务回滚也留痕
    签署事务 ──BEGIN IMMEDIATE ──► 状态复核 + 互斥锁插入 + 签署事件，原子提交

互斥锁表 ``(major_code, academic_year)`` 主键冲突是并发签署的最终裁决点。
"""
from __future__ import annotations

import hashlib
import sqlite3
from typing import Any

from .storage import EventStore, canonical_json, _now

ACTION_TYPES = {"停招", "合并", "新设"}


class GovernanceError(Exception):
    """业务规则违反基类。"""


class WorkflowError(GovernanceError):
    """非法状态迁移或版本操作。"""


class ConflictError(GovernanceError):
    """与已生效方案占有同一专业（含并发签署落选）。"""


class ValidationError(GovernanceError):
    """提案数据不完整（安置缺项等）。"""


# 允许的状态迁移
TRANSITIONS = {
    "论证": {"公示"},
    "公示": {"审批", "撤回"},
    "审批": {"生效", "撤回"},
    "生效": {"安置"},
    "安置": set(),
    "撤回": set(),  # 撤回后只能重提为新版本
}


class GovernanceEngine:
    def __init__(self, store: EventStore):
        self.store = store

    # ================= 参考数据维护 =================

    def register_major(self, major_code: str, name: str,
                       status: str = "在招", *, actor: str | None = None) -> None:
        self.store.append(major_code, "major-registered",
                          {"major_code": major_code, "name": name, "status": status},
                          aggregate_type="catalog", actor=actor)
        with self.store.conn:
            self.store.conn.execute(
                "INSERT INTO majors(major_code, name, status) VALUES (?,?,?)"
                " ON CONFLICT(major_code) DO UPDATE SET name=excluded.name,"
                " status=excluded.status",
                (major_code, name, status),
            )

    def register_program(self, program_id: str, major_code: str, name: str,
                         *, actor: str | None = None) -> None:
        self.store.append(program_id, "program-registered",
                          {"program_id": program_id, "major_code": major_code,
                           "name": name},
                          aggregate_type="catalog", actor=actor)
        with self.store.conn:
            self.store.conn.execute(
                "INSERT OR REPLACE INTO programs(program_id, major_code, name)"
                " VALUES (?,?,?)",
                (program_id, major_code, name),
            )

    def register_student(self, student_id: str, name: str, enroll_year: str,
                         major_code: str, program_id: str,
                         *, actor: str | None = None) -> None:
        self.store.append(student_id, "student-enrolled",
                          {"student_id": student_id, "name": name,
                           "enroll_year": enroll_year, "major_code": major_code,
                           "program_id": program_id},
                          aggregate_type="catalog", actor=actor)
        with self.store.conn:
            self.store.conn.execute(
                "INSERT OR REPLACE INTO students(student_id, name, enroll_year,"
                " major_code, program_id) VALUES (?,?,?,?,?)",
                (student_id, name, enroll_year, major_code, program_id),
            )

    def register_faculty(self, faculty_id: str, name: str, major_code: str,
                         *, actor: str | None = None) -> None:
        self.store.append(faculty_id, "faculty-registered",
                          {"faculty_id": faculty_id, "name": name,
                           "major_code": major_code},
                          aggregate_type="catalog", actor=actor)
        with self.store.conn:
            self.store.conn.execute(
                "INSERT OR REPLACE INTO faculty(faculty_id, name, major_code)"
                " VALUES (?,?,?)",
                (faculty_id, name, major_code),
            )

    def publish_demand_version(self, version_id: str, label: str,
                               payload: Any, *, actor: str | None = None) -> None:
        checksum = hashlib.sha256(
            canonical_json(payload).encode("utf-8")).hexdigest()
        self.store.append(version_id, "demand-version-published",
                          {"version_id": version_id, "label": label,
                           "checksum": checksum},
                          aggregate_type="demand", actor=actor)
        with self.store.conn:
            self.store.conn.execute(
                "INSERT OR REPLACE INTO demand_versions(version_id, label,"
                " checksum, payload) VALUES (?,?,?,?)",
                (version_id, label, checksum, canonical_json(payload)),
            )

    # ================= 提案与版本链 =================

    def create_proposal(self, proposal_id: str, title: str, action: str,
                        academic_year: str, affected_majors: list[str], *,
                        placements: dict | None = None,
                        faculty_assignments: dict | None = None,
                        program_transfers: dict | None = None,
                        demand_version_id: str | None = None,
                        actor: str | None = None) -> dict:
        if action not in ACTION_TYPES:
            raise ValidationError(f"调整类型必须是 {ACTION_TYPES} 之一")
        existing = self.store.events(proposal_id)
        if existing:
            raise WorkflowError(f"提案 {proposal_id} 已存在，应走重提新版本")
        body = self._build_version_body(
            title, action, academic_year, affected_majors,
            placements or {}, faculty_assignments or {},
            program_transfers or {}, demand_version_id)
        return self.store.append(
            proposal_id, "proposal-created",
            {"version_no": 1, "parent_version_no": None, **body},
            actor=actor)

    def revise(self, proposal_id: str, *, actor: str | None = None,
               **changes: Any) -> dict:
        """撤回后重提：不覆盖旧版本，自增版本号并指回父版本。"""
        current = self._latest_version(proposal_id)
        if current is None:
            raise WorkflowError(f"提案 {proposal_id} 不存在")
        if current["state"] != "撤回":
            raise WorkflowError("只有已撤回的版本才能重提为新版本")
        body = {**current["body"], **changes}
        body = self._build_version_body(
            body["title"], body["action"], body["academic_year"],
            body["affected_majors"], body.get("placements", {}),
            body.get("faculty_assignments", {}),
            body.get("program_transfers", {}),
            body.get("demand_version_id"))
        return self.store.append(
            proposal_id, "proposal-revised",
            {"version_no": current["version_no"] + 1,
             "parent_version_no": current["version_no"], **body},
            actor=actor)

    def withdraw(self, proposal_id: str, reason: str,
                 *, actor: str | None = None) -> dict:
        current = self._latest_version(proposal_id)
        self._require_state(current, {"公示", "审批"})
        return self.store.append(
            proposal_id, "proposal-withdrawn",
            {"version_no": current["version_no"], "reason": reason},
            actor=actor)

    def request_publicity(self, proposal_id: str,
                          *, actor: str | None = None) -> dict:
        current = self._latest_version(proposal_id)
        self._require_state(current, {"论证"})
        return self.store.append(
            proposal_id, "publicity-requested",
            {"version_no": current["version_no"]}, actor=actor)

    # ================= 影响快照冻结 =================

    def request_approval(self, proposal_id: str,
                         *, actor: str | None = None) -> dict:
        """公示 -> 审批：进入审批前冻结影响快照（契约 freeze_point）。"""
        current = self._latest_version(proposal_id)
        self._require_state(current, {"公示"})
        snapshot = self._freeze_snapshot(proposal_id, current, actor=actor)
        self.store.append(
            proposal_id, "approval-requested",
            {"version_no": current["version_no"],
             "snapshot_id": snapshot["snapshot_id"],
             "snapshot_checksum": snapshot["checksum"]},
            actor=actor)
        return snapshot

    def _freeze_snapshot(self, proposal_id: str, version: dict,
                         *, actor: str | None) -> dict:
        body = version["body"]
        majors = body["affected_majors"]
        conn = self.store.conn

        for code in majors:
            if not conn.execute("SELECT 1 FROM majors WHERE major_code=?",
                                (code,)).fetchone():
                raise ValidationError(f"受影响专业 {code} 不存在于专业目录")

        students = [dict(r) for r in conn.execute(
            "SELECT student_id, name, enroll_year, major_code, program_id"
            " FROM students WHERE major_code IN (%s)"
            % ",".join("?" * len(majors)), majors)]
        faculty = [dict(r) for r in conn.execute(
            "SELECT faculty_id, name, major_code FROM faculty"
            " WHERE major_code IN (%s)" % ",".join("?" * len(majors)), majors)]

        placements = body.get("placements", {})
        faculty_assignments = body.get("faculty_assignments", {})

        if body["action"] in {"停招", "合并"}:
            missing_s = [s["student_id"] for s in students
                         if s["student_id"] not in placements]
            if missing_s:
                raise ValidationError(
                    "冻结被拒：在读学生安置不完整，缺少 "
                    + "、".join(sorted(missing_s)))
            missing_f = [f["faculty_id"] for f in faculty
                         if f["faculty_id"] not in faculty_assignments]
            if missing_f:
                raise ValidationError(
                    "冻结被拒：师资去向不完整，缺少 "
                    + "、".join(sorted(missing_f)))

        demand_id = body.get("demand_version_id")
        if demand_id is None:
            raise ValidationError("冻结被拒：未绑定行业需求版本")
        demand = conn.execute(
            "SELECT version_id, label, checksum FROM demand_versions"
            " WHERE version_id=?", (demand_id,)).fetchone()
        if demand is None:
            raise ValidationError(f"行业需求版本 {demand_id} 不存在")

        content = {
            "proposal_id": proposal_id,
            "version_no": version["version_no"],
            "academic_year": body["academic_year"],
            "action": body["action"],
            "majors": majors,
            "students": students,
            "placements": placements,
            "faculty": faculty,
            "faculty_assignments": faculty_assignments,
            "program_transfers": body.get("program_transfers", {}),
            "demand_version": dict(demand),
        }
        checksum = hashlib.sha256(
            canonical_json(content).encode("utf-8")).hexdigest()
        snapshot = {"snapshot_id": f"{proposal_id}-v{version['version_no']}-snap",
                    **content, "checksum": checksum}
        self.store.append(proposal_id, "impact-frozen", snapshot, actor=actor)
        return snapshot

    # ================= 签署（互斥最终裁决点） =================

    def sign(self, proposal_id: str, *, actor: str | None = None) -> dict:
        current = self._latest_version(proposal_id)
        self._require_state(current, {"审批"})
        body = current["body"]
        freeze = self._snapshot_for(proposal_id, current["version_no"])

        detail = {"proposal_id": proposal_id,
                  "version_no": current["version_no"],
                  "majors": body["affected_majors"],
                  "academic_year": body["academic_year"]}
        # 1) 审计在业务事务之外独立提交：无论后续提交还是回滚都留痕。
        attempt_id = self.store.audit("签署", "开始", actor=actor, detail=detail)
        # 2) 独立工作连接（isolation_level=None 手工事务）。文件库下并发线程
        #    各持连接；BEGIN IMMEDIATE + busy_timeout 把签署串成全序裁决。
        conn = self.store.worker_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.cursor()
            recheck = self._latest_version(proposal_id, conn)
            if recheck is None or recheck["state"] != "审批" \
                    or recheck["version_no"] != current["version_no"]:
                raise WorkflowError("签署期间版本状态已变化")
            signed_payload = {
                "version_no": current["version_no"],
                "academic_year": body["academic_year"],
                "action": body["action"],
                "majors": body["affected_majors"],
                "placements": body.get("placements", {}),
                "faculty_assignments": body.get("faculty_assignments", {}),
                "program_transfers": body.get("program_transfers", {}),
                "snapshot_id": freeze["snapshot_id"],
                "snapshot_checksum": freeze["checksum"],
                "demand_checksum": freeze["demand_version"]["checksum"],
            }
            try:
                # 3) 主键冲突 = 同一专业同学年已有方案率先生效（含并发落选）。
                for major in body["affected_majors"]:
                    cur.execute(
                        "INSERT INTO active_major_locks(major_code,"
                        " academic_year, proposal_id, version_no, locked_at)"
                        " VALUES (?,?,?,?,?)",
                        (major, body["academic_year"], proposal_id,
                         current["version_no"], _now()))
            except sqlite3.IntegrityError:
                conn.rollback()
                owner = self._lock_owner(body["affected_majors"],
                                         body["academic_year"], conn)
                self.store.audit("签署", "冲突回滚", actor=actor,
                                 detail={**detail, "冲突对手": owner},
                                 attempt_id=attempt_id)
                raise ConflictError(
                    f"专业已被生效方案 {owner} 占有，本方案签署整体回滚"
                ) from None
            # 4) 互斥锁全部就位后，签署事件才与锁在同一事务内原子落库。
            self.store.append_in_tx(
                cur, proposal_id, "proposal-signed", signed_payload, actor=actor)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        self.store.audit("签署", "生效", actor=actor, detail=detail,
                         attempt_id=attempt_id)
        return signed_payload

    def complete_placement(self, proposal_id: str,
                           *, actor: str | None = None) -> dict:
        current = self._latest_version(proposal_id)
        self._require_state(current, {"生效"})
        return self.store.append(
            proposal_id, "placement-completed",
            {"version_no": current["version_no"]}, actor=actor)

    # ================= 内部工具 =================

    def _build_version_body(self, title, action, academic_year,
                            affected_majors, placements, faculty_assignments,
                            program_transfers, demand_version_id) -> dict:
        if not affected_majors:
            raise ValidationError("受影响专业列表不能为空")
        return {
            "title": title,
            "action": action,
            "academic_year": academic_year,
            "affected_majors": list(affected_majors),
            "placements": dict(placements),
            "faculty_assignments": dict(faculty_assignments),
            "program_transfers": dict(program_transfers),
            "demand_version_id": demand_version_id,
        }

    def _latest_version(self, proposal_id: str,
                        conn: sqlite3.Connection | None = None) -> dict | None:
        """从事件流还原提案当前版本及其状态。"""
        conn = conn or self.store.conn
        rows = conn.execute(
            "SELECT * FROM events WHERE aggregate_id=? ORDER BY seq",
            (proposal_id,)).fetchall()
        versions: dict[int, dict] = {}
        latest: int | None = None
        import json as _json
        for ev in rows:
            p = _json.loads(ev["payload"])
            t = ev["event_type"]
            if t == "proposal-created":
                latest = p["version_no"]
                versions[latest] = {"version_no": latest, "state": "论证",
                                    "body": self._body_of(p)}
            elif t == "proposal-revised":
                latest = p["version_no"]
                versions[latest] = {"version_no": latest, "state": "论证",
                                    "body": self._body_of(p)}
            elif latest is not None:
                v = versions[latest]
                if t == "publicity-requested":
                    v["state"] = "公示"
                elif t == "impact-frozen":
                    v["snapshot"] = p
                elif t == "approval-requested":
                    v["state"] = "审批"
                elif t == "proposal-withdrawn":
                    v["state"] = "撤回"
                elif t == "proposal-signed":
                    v["state"] = "生效"
                elif t == "placement-completed":
                    v["state"] = "安置"
        return versions[latest] if latest is not None else None

    @staticmethod
    def _body_of(p: dict) -> dict:
        return {k: p[k] for k in (
            "title", "action", "academic_year", "affected_majors",
            "placements", "faculty_assignments", "program_transfers",
            "demand_version_id") if k in p}

    def _snapshot_for(self, proposal_id: str, version_no: int,
                      conn: sqlite3.Connection | None = None) -> dict:
        import json as _json
        conn = conn or self.store.conn
        for ev in conn.execute(
                "SELECT event_type, payload FROM events"
                " WHERE aggregate_id=? ORDER BY seq", (proposal_id,)):
            p = _json.loads(ev["payload"])
            if ev["event_type"] == "impact-frozen" \
                    and p["version_no"] == version_no:
                return p
        raise WorkflowError(f"版本 {version_no} 缺少冻结快照，不得签署")

    def _lock_owner(self, majors: list[str], year: str,
                    conn: sqlite3.Connection | None = None) -> str | None:
        conn = conn or self.store.conn
        placeholders = ",".join("?" * len(majors))
        row = conn.execute(
            f"SELECT proposal_id, version_no FROM active_major_locks"
            f" WHERE academic_year=? AND major_code IN ({placeholders})",
            (year, *majors)).fetchone()
        if row:
            return f"{row['proposal_id']}#v{row['version_no']}"
        return None

    @staticmethod
    def _require_state(current: dict | None, allowed: set[str]) -> None:
        if current is None:
            raise WorkflowError("提案不存在")
        if current["state"] not in allowed:
            raise WorkflowError(
                f"当前状态「{current['state']}」不允许该操作，"
                f"允许的起始状态：{sorted(allowed)}")
