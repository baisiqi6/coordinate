# Coordinate

面向长期项目和可替换 agent runtime 的确定性 coordination kernel。

Coordinate 提供持久化运行状态、幂等状态转换、消息投递、runner 调度、恢复、审计和 GitHub gate 等机制。它不是一个固定的 AI coordinator，也不拥有产品判断：当前与用户交互、读取状态并决定下一步的人或 agent 才是 **Operator**，并在需要时承担 coordinator 角色。

Coordinate 与 MultiNexus、项目 Harness 和当前 Operator 共同形成跨宿主机、跨 session、跨 agent 生态的顶层 harness 系统。这里的“顶层”是编排作用域，不是新增中央实体；Coordinate 只承担其中的确定性协调内核和持久化控制面。跨仓库的产品定位、角色和 source-of-truth 规则由集成项目的 active product definition 维护；本仓库只维护 Coordinate 自身的实现与运维边界。

## Coordinate 的职责边界

Coordinate 负责确定性机制：

- SQLite 中的 events、jobs、leases、receipts、deliveries 和运行时投影。
- 幂等 lifecycle transition、retry、recovery、reconciliation 和 fail-closed gate。
- 跨宿主机的 runtime request、agent/job claim、进度与结构化结果。
- Discord、KOOK、webhook、stdout 等可见总线的 durable outbox。
- Git branch、PR、CI、review 和 merge-gate evidence。

Coordinate 不负责：

- 替 Operator 做开放式规划、取舍或验收判断。
- 重建 Claude Code、Codex、OpenCode 等 executor 内部的 agent team、subagent 或 workflow。
- 保存 agent-local 私有思维链或把聊天 transcript 当作项目状态权威。
- 成为项目稳定规范、源代码、GitHub 或 provider session 的第二套 source of truth。

## 与 Harness、MultiNexus 的关系

```text
人类或 agent Operator
        │ 决策、监督、验收
        ▼
Coordinate ── 持久化协调、authority、恢复、可见性
        │
        ├── MultiNexus agentd / vendor-native agent runtime
        ├── 项目 workspace 中的稳定规范和 active plan
        └── Discord / KOOK / GitHub / 其他 adapter
```

- 产品仓库保存稳定的 scope、architecture、domain model、runbook，以及真实消费者需要的最小兼容文件。
- Coordinate-managed 项目的 active canonical plan 留在对应 workspace。
- `$MYHARNESS_ROOT/projects/<project_id>/` 保存 task-scoped 过程材料与历史正文，不参与 Coordinate runtime。
- Coordinate DB 保存实时运行事实；provider-native JSONL 只作为 worker 观察证据。

当前跨仓产品状态与计划在 MultiNexus 的活跃 harness 中维护；Coordinate 仓库不复制第二份。

## 部署拓扑

Coordinate 区分四个操作环境：

- **Local development**：repo worktree + 显式本地 DB；只代表本地测试状态。
- **Coding host**：保存 canonical repo/harness 文件、运行 provider/agentd、执行 `*-files` 文件半边和 Git/GitHub 副作用。
- **Control plane**：运行 Coordinate 服务并持有生产 DB 的主机；通过严格 SSH wrapper（如 `coord-ssh`）调用远端 CLI，不得直接编辑 DB。
- **Deployed runtime copy**：`/opt/coordinate` 与 `/opt/multinexus` 是受审部署副本，不是普通开发 worktree；普通 operator 命令必须保留 `/opt` fail-closed guard。

拓扑和恢复命令以 [`docs/runbook.md`](docs/runbook.md) 与
[`skills/coordinate-operator/SKILL.md`](skills/coordinate-operator/SKILL.md) 为准。具体主机名、DB 路径和 SSH wrapper 由部署配置决定，不在源码中硬编码。

## 当前能力

- Workspace、host profile、agent、executor 与 capacity 注册和查询。
- File-backed harness onboarding、doctor、audit、reconcile 与 host-aware split operation。
- Assignment、handoff、blocker、review-result 和 completion receipt 生命周期。
- Generic subprocess adapter 与 managed runtime request/job/agentd 执行链。
- Attempt token、lease、timeout、retry 和 crash/restart 恢复保护。
- Event policy、delivery outbox 以及 Discord/KOOK/stdout 可见消息。
- GitHub branch、PR、CI、review 与 merge gate 证据。
- Operator pending、projection drift 和恢复诊断。

精确 CLI 以当前程序帮助为准，不在 README 复制完整命令手册：

```bash
PYTHONPATH=src python3 -m coordinate --help
PYTHONPATH=src python3 -m coordinate runtime --help
PYTHONPATH=src python3 -m coordinate assignment --help
```

## 本地安装（fresh install）

标准 Coordinate 安装只需克隆仓库、创建虚拟环境并从仓库根目录安装：

```bash
git clone https://github.com/baisiqi6/coordinate.git
cd coordinate
python3 -m venv .venv

# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate

pip install .
```

安装完成后，console script `coordinate` 会被写入虚拟环境：

- macOS / Linux：`.venv/bin/coordinate`
- Windows：`.venv\Scripts\coordinate.exe`

**把该绝对可执行路径原样填到 MultiNexus 的 `coordinator_cli_path`**，例如：

```toml
agentd_mode = true
coordinator_cli_path = "/absolute/path/to/coordinate/.venv/bin/coordinate"
coordinator_db_path = "/absolute/path/to/coordinate/data/coordinator.sqlite3"
```

- `coordinator_db_path` 必须是**绝对路径**，并且只能由当前这台宿主机上的进程访问；
- 不要把本地 SQLite 文件挂载给多台宿主机共享，也不要使用相对路径依赖运行时当前目录；
- 多宿主机部署通常通过受控 wrapper 调用 Coordinate；本地 console script 路径仅用于同一宿主机的直接运行。

## 本地开发

```bash
cd "${COORDINATE_REPO:-$HOME/projects/coordinate}"
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py'
PYTHONPATH=src python3 -m coordinate --db /tmp/coordinate-dev.sqlite3 workspace list
```

需要验证 runtime 闭环时，先按 [`docs/runbook.md`](docs/runbook.md) 区分本地、coding host、
control plane 与 deployed copy；不要把本地临时 DB 的结果当作生产状态。

## 文档

- [文档索引](docs/README.md)
- [范围与非目标](docs/scope.md)
- [当前架构](docs/architecture.md)
- [领域模型](docs/domain-model.md)
- [运维与恢复](docs/runbook.md)
- [Operator skill](skills/coordinate-operator/SKILL.md)

已结束 phase、旧 handoff/bootstrap、被取代设计和过程证据不保留在当前 README 导航中；需要追溯时
由私有 canonical 的 task artifact repository 提供。当前行为始终以代码、CLI help、当前架构和
运行时状态为准。
