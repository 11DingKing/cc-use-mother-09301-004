# 专业结构调整治理

地方高校专业停招、合并、新设的后端治理系统。它把提案与受影响专业、在读学生安置、
师资去向及行业需求版本连接起来，在审批前冻结影响快照，阻止冲突方案同时生效，
并提供逐名学生、逐套培养方案的跨学年归属说明。

## 核心不变量

1. **方案冲突**：`active_major_locks(major_code, academic_year)` 以主键约束保证
   同一专业同一学年只能被一个已生效方案占有。签署在 `BEGIN IMMEDIATE` 事务内最终
   裁决，并发签署恰好一个胜出，落选方整体回滚。
2. **影响快照**：进入审批（公示→审批）前必须冻结快照，固化受影响专业、在读学生、
   安置、师资去向与行业需求版本及其校验和；签署以该快照为准，冻结前校验安置完整性。
3. **版本链**：撤回或重提不覆盖旧版本，`version_no` 自增并以 `parent_version_no`
   指回父版本；全部历史只追加保存。
4. **只追加事件 + 回滚留痕**：事实来源是哈希链事件日志（每条事件含前序哈希）；
   签署尝试先经**独立连接独立提交**的审计表记录，业务事务随后回滚也不丢失历史。
5. **跨学年归属**：读模型重放已生效签署，逐名学生、逐套培养方案给出每次变更的
   前后归属，并指向生效方案版本与冻结快照。

## 目录

- `domain/contract.json`：领域角色、状态机、约束规则与样例。
- `src/governance/storage.py`：只追加事件存储、哈希链、独立审计、签署互斥表。
- `src/governance/engine.py`：工作流状态机、版本链、快照冻结、签署事务。
- `src/governance/projection.py`：读模型，跨学年归属查询。
- `src/governance/api.py`：标准库 HTTP 接口（无第三方依赖）。
- `src/domain_contract/`：契约读取与确定性校验。
- `examples/demo_scenario.py`：端到端场景演示。
- `tools/check_contract.py`：命令行契约摘要。
- `tests/`：契约测试 + 治理规则测试（含真实多线程并发签署）。

## 状态机

```
论证 ──► 公示 ──► 审批 ──► 生效 ──► 安置
          │         │
          └──► 撤回 ◄┘        撤回后重提 => 自增新版本（version_no+1，指回父版本）
```

## 运行

```bash
# 测试
python3 -m unittest discover -s tests -v

# 编译检查
python3 -m compileall -q src tools tests examples

# 端到端演示
PYTHONPATH=src python3 examples/demo_scenario.py

# HTTP 服务
PYTHONPATH=src python3 -m governance.api --db governance.db --port 8080
```

## HTTP 接口摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/majors` `/programs/catalog` `/students` `/faculty` `/demand` | 登记分散的教务/就业/行业数据（均事件留痕） |
| POST | `/proposals` | 创建提案（版本 1） |
| POST | `/proposals/{id}/publicity` `/approval` `/sign` `/withdraw` `/revise` `/complete` | 工作流推进 |
| GET | `/proposals[/{id}]` | 提案及完整版本链 |
| GET | `/students/{id}` `/programs/{id}` | 逐名学生 / 逐套培养方案跨学年前后归属 |
| GET | `/report` | 全部学生与培养方案归属汇总 |
| GET | `/locks` `/audit` `/health` | 生效占有表 / 审计留痕（含回滚尝试） / 哈希链健康 |

写请求可带 `X-Actor` 头（教务处 / 学院负责人 / 行业顾问）。冲突签署返回 `409`，
安置不完整等冻结前校验失败返回 `400`。

## 持久化与并发

- SQLite（WAL 或共享缓存内存库）。事件表只提供 INSERT，不提供更新/删除接口。
- 审计使用独立连接与独立提交生命周期，保证「即使签署并发或事务回滚也不丢失历史」。
- `store.verify_chain()` 可随时校验全量哈希链；`tests/` 中含篡改检错用例。
