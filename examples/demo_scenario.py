"""端到端场景演示：冲突互斥、冻结快照、版本链、跨学年归属、回滚留痕。

运行：python3 examples/demo_scenario.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from governance import GovernanceEngine, ConflictError, EventStore
from governance.projection import GovernanceState


def main() -> None:
    tmp = tempfile.TemporaryDirectory()
    engine = GovernanceEngine(EventStore(str(Path(tmp.name) / "demo.db")))
    e = engine

    # —— 教务、就业与行业数据本来分散，这里统一登记并全部事件留痕 ——
    e.register_major("CS", "计算机科学与技术", actor="教务处")
    e.register_major("SE", "软件工程", actor="教务处")
    e.register_program("CS-P1", "CS", "计算机科学与技术培养方案2025")
    e.register_program("SE-P1", "SE", "软件工程培养方案2025")
    e.register_student("S001", "张三", "2025", "CS", "CS-P1")
    e.register_student("S002", "李四", "2025", "CS", "CS-P1")
    e.register_faculty("F001", "刘老师", "CS")
    e.publish_demand_version("D2027-1", "2027学年行业需求",
                             {"CS": {"demand": "平稳"}, "SE": {"demand": "下行"}})

    placements = {"S001": {"target_major": "SE", "target_program": "SE-P1"},
                  "S002": {"target_major": "SE", "target_program": "SE-P1"}}
    faculty = {"F001": {"target_major": "SE", "note": "转岗软件工程系"}}

    # —— 方案 A：停招 CS 并入 SE ——
    e.create_proposal("P-A", "停招CS并入SE", "停招", "2027", ["CS"],
                      placements=placements, faculty_assignments=faculty,
                      program_transfers={"CS-P1": {"target_major": "SE",
                                                   "target_program": "SE-P1",
                                                   "status": "并轨收尾"}},
                      demand_version_id="D2027-1", actor="教务处")
    e.request_publicity("P-A", actor="学院负责人")
    snap = e.request_approval("P-A", actor="教务处")  # 审批前冻结
    print(f"[冻结] 快照 {snap['snapshot_id']} 校验和 {snap['checksum'][:12]}…")
    e.sign("P-A", actor="教务处")
    print("[签署] P-A#v1 生效")

    # —— 方案 B：同一专业出现在互相冲突的方案里 ——
    e.create_proposal("P-B", "CS另一种合并方案", "停招", "2027", ["CS"],
                      placements=placements, faculty_assignments=faculty,
                      demand_version_id="D2027-1", actor="教务处")
    e.request_publicity("P-B")
    e.request_approval("P-B")
    try:
        e.sign("P-B")
    except ConflictError as exc:
        print(f"[互斥] P-B 签署被整体回滚：{exc}")

    # —— 撤回 + 重提形成新版本，旧版本保留 ——
    e.create_proposal("P-C", "论证中的SE微调", "合并", "2027", ["SE"],
                      demand_version_id="D2027-1")
    e.request_publicity("P-C")
    e.withdraw("P-C", "公示期异议", actor="学院负责人")
    e.revise("P-C", title="SE微调（修订版）", actor="教务处")
    print("[版本] P-C 版本链:",
          [(v["version_no"], v["state"])
           for v in GovernanceState(e.store).proposal_history("P-C")])

    # —— 最终查询：逐名学生、逐套培养方案 ——
    state = GovernanceState(e.store)
    report = state.directory_report()
    print("\n=== 逐名学生跨学年归属 ===")
    print(json.dumps(report["students"], ensure_ascii=False, indent=2))
    print("=== 逐套培养方案跨学年归属 ===")
    print(json.dumps(report["programs"], ensure_ascii=False, indent=2))
    print("=== 签署审计（含回滚尝试，不丢失历史）===")
    for r in e.store.audit_trail():
        print(f"  {r['result']:6} {r['action']} {r['detail']}")
    print("\n哈希链校验:", "完整" if e.store.verify_chain() == [] else "断裂")
    engine.store.close()
    tmp.cleanup()


if __name__ == "__main__":
    main()
