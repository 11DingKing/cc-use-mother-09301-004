"""端到端治理规则测试：版本链、快照冻结、冲突互斥、回滚留痕、跨学年归属。"""
from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from governance import GovernanceEngine, ConflictError, WorkflowError, EventStore
from governance.projection import GovernanceState
from governance.storage import tampered_events
from governance.engine import ValidationError


def build_catalog(engine: GovernanceEngine) -> None:
    """构造冲突场景的基础数据。

    专业 CS（计算机科学与技术）与 SE（软件工程）各有在读学生、师资、培养方案；
    2027 学年发布一版行业需求数据。
    """
    e = engine
    e.register_major("CS", "计算机科学与技术")
    e.register_major("SE", "软件工程")
    e.register_program("CS-P1", "CS", "计算机科学与技术培养方案2025")
    e.register_program("SE-P1", "SE", "软件工程培养方案2025")
    e.register_student("S001", "张三", "2025", "CS", "CS-P1")
    e.register_student("S002", "李四", "2025", "CS", "CS-P1")
    e.register_student("S003", "王五", "2026", "SE", "SE-P1")
    e.register_faculty("F001", "刘老师", "CS")
    e.register_faculty("F002", "陈老师", "CS")
    e.publish_demand_version("D2027-1", "2027学年行业需求初版",
                             {"CS": {"demand": "平稳"}, "SE": {"demand": "下行"}})


def full_placements(target_major="SE", target_program="SE-P1"):
    return {
        "S001": {"target_major": target_major, "target_program": target_program},
        "S002": {"target_major": target_major, "target_program": target_program},
    }


def full_faculty():
    return {
        "F001": {"target_major": "SE", "note": "转岗软件工程系"},
        "F002": {"target_major": "SE", "note": "承担融合课程"},
    }


class WorkflowAndVersionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = GovernanceEngine(EventStore(":memory:"))
        build_catalog(self.engine)

    def test_state_machine_illegal_transition_rejected(self) -> None:
        e = self.engine
        e.create_proposal("P-A", "停招CS", "停招", "2027", ["CS"],
                          placements=full_placements(),
                          faculty_assignments=full_faculty(),
                          demand_version_id="D2027-1")
        with self.assertRaises(WorkflowError):
            # 论证态不能直接进审批
            e.request_approval("P-A")

    def test_withdraw_and_revise_creates_new_version_chain(self) -> None:
        e = self.engine
        e.create_proposal("P-A", "停招CS", "停招", "2027", ["CS"],
                          placements=full_placements(),
                          faculty_assignments=full_faculty(),
                          demand_version_id="D2027-1")
        e.request_publicity("P-A")
        e.withdraw("P-A", "公示期收到异议", actor="学院负责人")
        with self.assertRaises(WorkflowError):
            # 撤回态不能签署
            e.sign("P-A")
        e.revise("P-A", title="停招CS（修订：完善师资去向）", actor="教务处")
        state = GovernanceState(e.store)
        history = state.proposal_history("P-A")
        self.assertEqual([v["version_no"] for v in history], [1, 2])
        self.assertEqual(history[1]["parent_version_no"], 1)
        self.assertEqual(history[0]["state"], "撤回")  # 旧版本不被覆盖
        self.assertEqual(history[1]["state"], "论证")
        self.assertEqual(history[1]["title"], "停招CS（修订：完善师资去向）")
        # 重提不允许绕过撤回：论证中的新版本不能再 revise
        with self.assertRaises(WorkflowError):
            e.revise("P-A")


class SnapshotFreezeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = GovernanceEngine(EventStore(":memory:"))
        build_catalog(self.engine)

    def _proposal(self, **kw):
        defaults = dict(proposal_id="P-A", title="停招CS", action="停招",
                        academic_year="2027", affected_majors=["CS"],
                        placements=full_placements(),
                        faculty_assignments=full_faculty(),
                        demand_version_id="D2027-1")
        defaults.update(kw)
        return self.engine.create_proposal(**defaults)

    def test_freeze_blocked_when_placement_incomplete(self) -> None:
        e = self.engine
        self._proposal(placements={"S001": full_placements()["S001"]})
        e.request_publicity("P-A")
        with self.assertRaises(ValidationError) as cm:
            e.request_approval("P-A")
        self.assertIn("S002", str(cm.exception))
        # 冻结失败，提案仍停留在公示态
        state = GovernanceState(e.store)
        self.assertEqual(state.proposal_history("P-A")[-1]["state"], "公示")

    def test_freeze_blocked_without_demand_version(self) -> None:
        e = self.engine
        self._proposal(demand_version_id=None)
        e.request_publicity("P-A")
        with self.assertRaises(ValidationError):
            e.request_approval("P-A")

    def test_frozen_snapshot_is_immutable_basis_for_signing(self) -> None:
        e = self.engine
        self._proposal()
        e.request_publicity("P-A")
        snap = e.request_approval("P-A")
        self.assertEqual(sorted(s["student_id"] for s in snap["students"]),
                         ["S001", "S002"])
        self.assertEqual(snap["demand_version"]["version_id"], "D2027-1")
        payload = e.sign("P-A", actor="教务处")
        self.assertEqual(payload["snapshot_checksum"], snap["checksum"])
        # 快照事件永久保留在事件流中
        types = [ev["event_type"] for ev in e.store.events("P-A")]
        self.assertIn("impact-frozen", types)

    def test_signing_requires_frozen_snapshot(self) -> None:
        e = self.engine
        self._proposal()
        e.request_publicity("P-A")
        with self.assertRaises(WorkflowError):
            e.sign("P-A")


class ConflictTest(unittest.TestCase):
    def setUp(self) -> None:
        # 文件库以支持并发线程各持独立连接（WAL + BEGIN IMMEDIATE）
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "governance.db")
        self.engine = GovernanceEngine(EventStore(self.db_path))
        build_catalog(self.engine)

    def tearDown(self) -> None:
        self.engine.store.close()
        self._tmp.cleanup()

    def _drive(self, pid, majors, *, placements=None, faculty=None,
               transfers=None, actor="教务处"):
        e = self.engine
        e.create_proposal(pid, f"方案{pid}", "停招", "2027", majors,
                          placements=placements or {},
                          faculty_assignments=faculty or {},
                          program_transfers=transfers or {},
                          demand_version_id="D2027-1", actor=actor)
        e.request_publicity(pid, actor=actor)
        e.request_approval(pid, actor=actor)
        return e.sign(pid, actor=actor)

    def test_conflicting_proposal_cannot_both_take_effect(self) -> None:
        e = self.engine
        self._drive("P-A", ["CS"], placements=full_placements(),
                    faculty=full_faculty())
        # 第二个方案同样要占有 CS（例如并入不同目标）
        with self.assertRaises(ConflictError) as cm:
            self._drive("P-B", ["CS", "SE"],
                        placements={**full_placements(),
                                    "S003": {"target_major": "CS",
                                             "target_program": "CS-P1"}},
                        faculty={**full_faculty()})
        self.assertIn("P-A#v1", str(cm.exception))
        state = GovernanceState(e.store)
        # 只有 P-A 生效；锁表也只有 CS 归 P-A
        self.assertEqual(
            sorted(v["state"] for v in state.active_proposals()), ["生效"])
        locks = {(r["major_code"], r["proposal_id"]) for r in state.locks()}
        self.assertEqual(locks, {("CS", "P-A")})

    def test_concurrent_signing_exactly_one_wins(self) -> None:
        e = self.engine
        # 两个都到达审批态、都要占有 CS
        for pid in ("P-C", "P-D"):
            e.create_proposal(pid, f"方案{pid}", "停招", "2027", ["CS"],
                              placements=full_placements(),
                              faculty_assignments=full_faculty(),
                              demand_version_id="D2027-1")
            e.request_publicity(pid)
            e.request_approval(pid)

        results: dict[str, object] = {}

        def race(pid: str) -> None:
            try:
                e.sign(pid)
                results[pid] = "生效"
            except ConflictError as exc:
                results[pid] = exc

        t1 = threading.Thread(target=race, args=("P-C",))
        t2 = threading.Thread(target=race, args=("P-D",))
        t1.start(); t2.start(); t1.join(); t2.join()

        winners = [pid for pid, r in results.items() if r == "生效"]
        losers = [pid for pid, r in results.items() if r != "生效"]
        self.assertEqual(len(winners), 1, results)
        self.assertEqual(len(losers), 1)
        self.assertIsInstance(results[losers[0]], ConflictError)
        # 冲突整体回滚：锁表恰好 1 行
        state = GovernanceState(e.store)
        self.assertEqual(len(state.locks()), 1)

    def test_rollback_leaves_no_partial_state_but_audit_remains(self) -> None:
        e = self.engine
        self._drive("P-A", ["CS"], placements=full_placements(),
                    faculty=full_faculty())
        # P-B 正常走完公示、冻结快照、送审（这些事件保留）
        e.create_proposal("P-B", "方案P-B", "停招", "2027", ["CS"],
                          placements=full_placements(),
                          faculty_assignments=full_faculty(),
                          demand_version_id="D2027-1")
        e.request_publicity("P-B")
        e.request_approval("P-B")
        seq_before_sign = len(e.store.events())
        with self.assertRaises(ConflictError):
            e.sign("P-B")
        # 签署事务整体回滚：没有任何半截签署事件/锁残留
        self.assertEqual(len(e.store.events()), seq_before_sign)
        pb_types = [ev["event_type"] for ev in e.store.events("P-B")]
        self.assertEqual(pb_types[-1], "approval-requested")
        self.assertNotIn("proposal-signed", pb_types)
        self.assertEqual(
            [r["major_code"] for r in GovernanceState(e.store).locks()],
            ["CS"])
        # 但两次签署尝试（开始/冲突回滚/生效）都在独立审计里
        trail = e.store.audit_trail()
        self.assertTrue(any(r["result"] == "冲突回滚" for r in trail))
        self.assertTrue(any(r["result"] == "生效" for r in trail))
        self.assertTrue(any(r["result"] == "开始" for r in trail))


class AttributionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = GovernanceEngine(EventStore(":memory:"))
        build_catalog(self.engine)
        e = self.engine
        # 2027 学年：CS 停招，S001/S002 并入 SE/SE-P1，师资转岗，培养方案迁移
        e.create_proposal(
            "P-A", "停招CS并入SE", "停招", "2027", ["CS"],
            placements=full_placements(),
            faculty_assignments=full_faculty(),
            program_transfers={"CS-P1": {"target_major": "SE",
                                         "target_program": "SE-P1",
                                         "status": "并轨收尾"}},
            demand_version_id="D2027-1")
        e.request_publicity("P-A")
        e.request_approval("P-A")
        e.sign("P-A")
        # 2028 学年：新设 AI 专业，S001 再次调整到 AI；用新需求版本
        e.register_major("AI", "人工智能")
        e.register_program("AI-P1", "AI", "人工智能培养方案2028")
        e.publish_demand_version("D2028-1", "2028学年行业需求",
                                 {"AI": {"demand": "旺盛"}})
        e.create_proposal(
            "P-B", "新设AI并调入学生", "新设", "2028", ["AI"],
            placements={"S001": {"target_major": "AI",
                                 "target_program": "AI-P1",
                                 "note": "自愿转入"}},
            demand_version_id="D2028-1")
        e.request_publicity("P-B")
        e.request_approval("P-B")
        e.sign("P-B")
        self.state = GovernanceState(e.store)

    def test_student_attribution_explains_each_change_before_after(self) -> None:
        s1 = self.state.student_attribution("S001")
        self.assertEqual(s1["initial"]["major_code"], "CS")
        self.assertEqual(s1["current"]["major_code"], "AI")
        self.assertEqual([c["academic_year"] for c in s1["changes"]],
                         ["2027", "2028"])
        self.assertEqual(s1["changes"][0]["before"]["major_name"],
                         "计算机科学与技术")
        self.assertEqual(s1["changes"][0]["after"]["program_id"], "SE-P1")
        self.assertEqual(s1["changes"][1]["after"]["program_id"], "AI-P1")
        # 每一步都能追溯到生效版本与冻结快照
        for c in s1["changes"]:
            self.assertTrue(c["snapshot_id"].endswith("-snap"))
            self.assertEqual(len(c["snapshot_checksum"]), 64)

        # S002 只在 2027 变更一次；S003 从未变更
        s2 = self.state.student_attribution("S002")
        self.assertEqual(len(s2["changes"]), 1)
        s3 = self.state.student_attribution("S003")
        self.assertEqual(s3["changes"], [])
        self.assertEqual(s3["current"]["major_code"], "SE")

    def test_program_attribution_tracks_transfer_across_years(self) -> None:
        p = self.state.program_attribution("CS-P1")
        self.assertEqual(p["initial"]["major_code"], "CS")
        self.assertEqual(len(p["changes"]), 1)
        c = p["changes"][0]
        self.assertEqual(c["academic_year"], "2027")
        self.assertEqual(c["before"]["major_code"], "CS")
        self.assertEqual(c["after"]["major_code"], "SE")
        self.assertEqual(c["after"]["program_id"], "SE-P1")
        self.assertEqual(p["current"]["status"], "并轨收尾")

    def test_faculty_attribution(self) -> None:
        f = self.state.faculty_attribution("F001")
        self.assertEqual(f["initial"], "CS")
        self.assertEqual(f["current"], "SE")
        self.assertEqual(f["changes"][0]["proposal_id"], "P-A")

    def test_directory_report_covers_every_student_and_program(self) -> None:
        report = self.state.directory_report()
        self.assertEqual({s["student_id"] for s in report["students"]},
                         {"S001", "S002", "S003"})
        self.assertEqual({p["program_id"] for p in report["programs"]},
                         {"CS-P1", "SE-P1", "AI-P1"})


class EventLogIntegrityTest(unittest.TestCase):
    def test_hash_chain_detects_tampering(self) -> None:
        engine = GovernanceEngine(EventStore(":memory:"))
        build_catalog(engine)
        engine.create_proposal("P-A", "停招CS", "停招", "2027", ["CS"],
                               placements=full_placements(),
                               faculty_assignments=full_faculty(),
                               demand_version_id="D2027-1")
        self.assertEqual(engine.store.verify_chain(), [])
        tampered_events(engine.store, [3])
        # seq=3 自身及其后所有事件断链
        self.assertIn(3, engine.store.verify_chain())


if __name__ == "__main__":
    unittest.main()
