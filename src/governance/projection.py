"""读模型：从只追加事件流重放，提供跨学年归属查询。

所有结论都可追溯到具体事件 seq、已生效方案版本与冻结快照；
事件顺序（seq）即裁决全序，先签署者先改变归属。
"""
from __future__ import annotations

from typing import Any

from .storage import EventStore


class GovernanceState:
    def __init__(self, store: EventStore):
        self.store = store
        self._replay()

    def _replay(self) -> None:
        self.students: dict[str, dict] = {}
        self.programs: dict[str, dict] = {}
        self.faculty: dict[str, dict] = {}
        self.majors: dict[str, dict] = {}
        self.demand_versions: dict[str, dict] = {}
        # proposal_id -> {版本号 -> 版本摘要}
        self.proposal_versions: dict[str, dict[int, dict]] = {}
        # 已生效签署，按全局 seq 排序，即归属变更的时间线
        self.signed: list[dict] = []
        self.snapshots_by_version: dict[tuple[str, int], dict] = {}

        for ev in self.store.events():
            t, p, seq = ev["event_type"], ev["payload"], ev["seq"]
            if t == "major-registered":
                self.majors[p["major_code"]] = p
            elif t == "program-registered":
                self.programs[p["program_id"]] = p
            elif t == "student-enrolled":
                self.students[p["student_id"]] = p
            elif t == "faculty-registered":
                self.faculty[p["faculty_id"]] = p
            elif t == "demand-version-published":
                self.demand_versions[p["version_id"]] = p
            elif t in ("proposal-created", "proposal-revised"):
                versions = self.proposal_versions.setdefault(ev["aggregate_id"], {})
                versions[p["version_no"]] = {
                    "proposal_id": ev["aggregate_id"],
                    "version_no": p["version_no"],
                    "parent_version_no": p.get("parent_version_no"),
                    "state": "论证",
                    "title": p["title"],
                    "action": p["action"],
                    "academic_year": p["academic_year"],
                    "affected_majors": p["affected_majors"],
                    "body": p,
                }
            else:
                versions = self.proposal_versions.get(ev["aggregate_id"], {})
                if not versions:
                    continue
                latest = max(versions)
                v = versions[latest]
                if t == "publicity-requested":
                    v["state"] = "公示"
                elif t == "approval-requested":
                    v["state"] = "审批"
                    v["snapshot_id"] = p.get("snapshot_id")
                elif t == "impact-frozen":
                    v["snapshot_id"] = p["snapshot_id"]
                    self.snapshots_by_version[
                        (ev["aggregate_id"], p["version_no"])] = p
                elif t == "proposal-withdrawn":
                    v["state"] = "撤回"
                    v["withdraw_reason"] = p.get("reason")
                elif t == "proposal-signed":
                    v["state"] = "生效"
                    self.signed.append({
                        "seq": seq,
                        "proposal_id": ev["aggregate_id"],
                        "version_no": p["version_no"],
                        "academic_year": p["academic_year"],
                        "action": p["action"],
                        "majors": p["majors"],
                        "placements": p["placements"],
                        "faculty_assignments": p["faculty_assignments"],
                        "program_transfers": p.get("program_transfers", {}),
                        "snapshot_id": p["snapshot_id"],
                        "snapshot_checksum": p["snapshot_checksum"],
                        "signed_at": ev["created_at"],
                    })
                elif t == "placement-completed":
                    v["state"] = "安置"

    # ---------- 提案视图 ----------

    def proposal_history(self, proposal_id: str) -> list[dict]:
        """版本链：全部版本保留，含父版本指针与最终状态。"""
        versions = self.proposal_versions.get(proposal_id, {})
        return [versions[n] for n in sorted(versions)]

    def active_proposals(self) -> list[dict]:
        result = []
        for versions in self.proposal_versions.values():
            v = versions[max(versions)]
            if v["state"] in {"生效", "安置"}:
                result.append(v)
        return result

    def locks(self) -> list[dict]:
        rows = self.store.conn.execute(
            "SELECT * FROM active_major_locks ORDER BY academic_year, major_code"
        ).fetchall()
        return [dict(r) for r in rows]

    # ---------- 逐名学生：跨学年归属前后对照 ----------

    def student_attribution(self, student_id: str) -> dict:
        student = self.students.get(student_id)
        if student is None:
            raise KeyError(f"学生 {student_id} 不存在")
        current = {"major_code": student["major_code"],
                   "program_id": student["program_id"]}
        initial = dict(current)
        changes = []
        for sign in self.signed:
            placement = sign["placements"].get(student_id)
            if not placement:
                continue
            before = dict(current)
            after = {"major_code": placement.get("target_major",
                                                 before["major_code"]),
                     "program_id": placement.get("target_program",
                                                 before["program_id"])}
            changes.append({
                "seq": sign["seq"],
                "academic_year": sign["academic_year"],
                "action": sign["action"],
                "proposal_id": sign["proposal_id"],
                "version_no": sign["version_no"],
                "snapshot_id": sign["snapshot_id"],
                "snapshot_checksum": sign["snapshot_checksum"],
                "before": before,
                "after": after,
                "note": placement.get("note"),
            })
            current = after
        return {
            "student_id": student_id,
            "name": student["name"],
            "enroll_year": student["enroll_year"],
            "initial": self._decorate_attribution(initial),
            "current": self._decorate_attribution(current),
            "changes": [
                {**c,
                 "before": self._decorate_attribution(c["before"]),
                 "after": self._decorate_attribution(c["after"])}
                for c in changes
            ],
        }

    # ---------- 逐套培养方案：跨学年归属前后对照 ----------

    def program_attribution(self, program_id: str) -> dict:
        program = self.programs.get(program_id)
        if program is None:
            raise KeyError(f"培养方案 {program_id} 不存在")
        current = {"major_code": program["major_code"],
                   "program_id": program_id}
        initial = dict(current)
        status = "在招"
        changes = []
        for sign in self.signed:
            transfer = sign["program_transfers"].get(program_id)
            majors = sign["majors"]
            before = dict(current)
            if transfer:
                # 显式迁移：培养方案（或其在读学生培养口径）并入新专业/新方案
                after = {"major_code": transfer.get("target_major",
                                                    before["major_code"]),
                         "program_id": transfer.get("target_program",
                                                    before["program_id"])}
                if transfer.get("status"):
                    status = transfer["status"]
                role = "迁移对象"
            elif sign["action"] == "停招" and current["major_code"] in majors:
                # 所属专业停招：归属专业不再招生，方案进入停招收尾
                after = dict(before)
                status = "停招"
                role = "停招收尾"
            else:
                # 仅作为安置接收方出现时，归属专业不变，不生成变更
                continue
            changes.append({
                "seq": sign["seq"],
                "academic_year": sign["academic_year"],
                "action": sign["action"],
                "proposal_id": sign["proposal_id"],
                "version_no": sign["version_no"],
                "snapshot_id": sign["snapshot_id"],
                "snapshot_checksum": sign["snapshot_checksum"],
                "before": self._decorate_attribution(before),
                "after": self._decorate_attribution(after),
                "role": role,
            })
            current = after
        return {
            "program_id": program_id,
            "name": program["name"],
            "initial": self._decorate_attribution(initial),
            "current": {**self._decorate_attribution(current), "status": status},
            "changes": changes,
        }

    def faculty_attribution(self, faculty_id: str) -> dict:
        member = self.faculty.get(faculty_id)
        if member is None:
            raise KeyError(f"师资 {faculty_id} 不存在")
        current = {"major_code": member["major_code"]}
        changes = []
        for sign in self.signed:
            assignment = sign["faculty_assignments"].get(faculty_id)
            if not assignment:
                continue
            before = dict(current)
            after = {"major_code": assignment.get("target_major",
                                                  before["major_code"])}
            changes.append({
                "seq": sign["seq"],
                "academic_year": sign["academic_year"],
                "proposal_id": sign["proposal_id"],
                "version_no": sign["version_no"],
                "snapshot_id": sign["snapshot_id"],
                "before": before,
                "after": after,
                "note": assignment.get("note"),
            })
            current = after
        return {"faculty_id": faculty_id, "name": member["name"],
                "initial": member["major_code"],
                "current": current["major_code"], "changes": changes}

    # ---------- 汇总 ----------

    def directory_report(self) -> dict:
        """逐名学生 + 逐套培养方案的完整跨学年归属说明。"""
        return {
            "students": [self.student_attribution(sid)
                         for sid in sorted(self.students)],
            "programs": [self.program_attribution(pid)
                         for pid in sorted(self.programs)],
        }

    def _decorate_attribution(self, ref: dict) -> dict:
        major = self.majors.get(ref["major_code"])
        program = self.programs.get(ref["program_id"])
        return {
            "major_code": ref["major_code"],
            "major_name": major["name"] if major else None,
            "program_id": ref["program_id"],
            "program_name": program["name"] if program else None,
        }

    def snapshot(self, proposal_id: str, version_no: int) -> dict:
        return self.snapshots_by_version[(proposal_id, version_no)]
