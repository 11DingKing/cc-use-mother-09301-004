"""HTTP 接口层（标准库实现，无第三方依赖）。

启动：python3 -m governance.api --db governance.db --port 8080

所有写操作走治理引擎（事件溯源 + 工作流约束）；查询走读模型。
响应中凡涉及归属变更，均带方案版本与冻结快照编号。
"""
from __future__ import annotations

import argparse
import json
from typing import Any, Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .storage import EventStore
from .engine import (GovernanceEngine, GovernanceError, WorkflowError,
                     ConflictError, ValidationError)
from .projection import GovernanceState


def _make_handler(store: EventStore) -> type[BaseHTTPRequestHandler]:
    engine = GovernanceEngine(store)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            pass  # 静默；生产可替换为结构化日志

        def _send(self, status: int, body: Any) -> None:
            data = json.dumps(body, ensure_ascii=False,
                              indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _actor(self) -> str | None:
            return self.headers.get("X-Actor")

        def _guard(self, fn: Callable[[], Any], status: int = 400) -> None:
            try:
                result = fn()
            except ConflictError as exc:
                self._send(409, {"error": "冲突", "detail": str(exc)})
            except (WorkflowError, ValidationError) as exc:
                self._send(status, {"error": "业务规则", "detail": str(exc)})
            except GovernanceError as exc:
                self._send(status, {"error": "治理错误", "detail": str(exc)})
            except KeyError as exc:
                self._send(404, {"error": "未找到", "detail": str(exc)})
            else:
                self._send(200, result if result is not None else {"ok": True})

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.strip("/").split("/")
            state = GovernanceState(store)
            if path == ["health"]:
                self._send(200, {"status": "ok",
                                 "chain_ok": store.verify_chain() == []})
            elif path == ["proposals"]:
                self._guard(lambda: {"proposals": [
                    {"proposal_id": pid,
                     "versions": state.proposal_history(pid)}
                    for pid in sorted(state.proposal_versions)]})
            elif len(path) == 2 and path[0] == "proposals":
                self._guard(lambda: {
                    "proposal_id": path[1],
                    "versions": state.proposal_history(path[1])}, 404)
            elif path == ["students"]:
                self._guard(lambda: {"students": [
                    state.student_attribution(sid)
                    for sid in sorted(state.students)]})
            elif len(path) == 2 and path[0] == "students":
                self._guard(lambda: state.student_attribution(path[1]), 404)
            elif len(path) == 2 and path[0] == "programs":
                self._guard(lambda: state.program_attribution(path[1]), 404)
            elif path == ["programs"]:
                self._guard(lambda: {"programs": [
                    state.program_attribution(pid)
                    for pid in sorted(state.programs)]})
            elif path == ["locks"]:
                self._guard(lambda: {"locks": state.locks()})
            elif path == ["audit"]:
                self._guard(lambda: {"audit": store.audit_trail()})
            elif path == ["report"]:
                self._guard(lambda: state.directory_report())
            else:
                self._send(404, {"error": "未知路由"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.strip("/").split("/")
            body = self._read_body()
            actor = self._actor()

            def proposal_create() -> dict:
                engine.create_proposal(
                    body["proposal_id"], body["title"], body["action"],
                    body["academic_year"], body["affected_majors"],
                    placements=body.get("placements", {}),
                    faculty_assignments=body.get("faculty_assignments", {}),
                    program_transfers=body.get("program_transfers", {}),
                    demand_version_id=body.get("demand_version_id"),
                    actor=actor)
                return {"proposal_id": body["proposal_id"], "state": "论证"}

            routes: dict[tuple[str, ...], Callable[[], Any]] = {
                ("majors",): lambda: (
                    engine.register_major(body["major_code"], body["name"],
                                          body.get("status", "在招"),
                                          actor=actor), None)[1],
                ("programs", "catalog"): lambda: (
                    engine.register_program(body["program_id"],
                                            body["major_code"], body["name"],
                                            actor=actor), None)[1],
                ("students",): lambda: (
                    engine.register_student(body["student_id"], body["name"],
                                           body["enroll_year"],
                                           body["major_code"],
                                           body["program_id"], actor=actor),
                    None)[1],
                ("faculty",): lambda: (
                    engine.register_faculty(body["faculty_id"], body["name"],
                                            body["major_code"], actor=actor),
                    None)[1],
                ("demand",): lambda: (
                    engine.publish_demand_version(body["version_id"],
                                                  body["label"],
                                                  body["payload"],
                                                  actor=actor), None)[1],
                ("proposals",): proposal_create,
            }
            if tuple(path) in routes:
                self._guard(routes[tuple(path)])
                return
            if len(path) == 3 and path[0] == "proposals":
                pid, action = path[1], path[2]
                actions = {
                    "publicity": lambda: engine.request_publicity(pid, actor=actor),
                    "approval": lambda: engine.request_approval(pid, actor=actor),
                    "sign": lambda: engine.sign(pid, actor=actor),
                    "withdraw": lambda: engine.withdraw(
                        pid, body.get("reason", ""), actor=actor),
                    "revise": lambda: engine.revise(pid, actor=actor, **body),
                    "complete": lambda: engine.complete_placement(
                        pid, actor=actor),
                }
                if action in actions:
                    self._guard(actions[action])
                    return
            self._send(404, {"error": "未知路由"})

    return Handler


def serve(db_path: str, port: int) -> None:
    store = EventStore(db_path)
    server = ThreadingHTTPServer(("0.0.0.0", port), _make_handler(store))
    print(f"治理后端已启动: http://0.0.0.0:{port}  (db={db_path})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="专业结构调整治理后端")
    parser.add_argument("--db", default="governance.db")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    serve(args.db, args.port)


if __name__ == "__main__":
    main()
