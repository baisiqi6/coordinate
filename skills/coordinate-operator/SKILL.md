---
name: coordinate-operator
description: "Use when operating Coordinate across local development, coding hosts, production control plane, Remote MCP, Runtime HTTP, GitHub, or Discord/KOOK. This parent skill is a progressive-disclosure router: load only the task-specific subskill for workspace/task lifecycle, runtime execution, messaging, GitHub collaboration, worker supervision, production recovery, or Windows multi-CLI work. It is for AI operator onboarding, not for implementing new Coordinate features."
---

# Coordinate Operator

本 skill 只保存跨场景都成立的心智模型、authority 与路由。具体领域规则位于 `subskills/`；不要因为
Coordinate 被触发就一次性读取所有模块。

## 顶层定位

Coordinate 是顶层多 Agent harness 中的确定性运行时控制面，不是固定 Coordinator，也不取代当前
Operator 的判断。它可以把拥有自身 subagent、agent team 或 workflow 的 Agent 当作复合 Executor，
不展开或重新实现其内部编排。

按需求使用最小组合：

- 只需要 SDD/TDD、review 与跨 session 项目记忆：EXharness。
- 需要 durable job、event、lease、receipt 与恢复：增加 Coordinate。
- 需要自动调用 vendor CLI、恢复 provider session 或跨主机执行：增加 MultiNexus agentd/adapters。
- 需要 Discord/KOOK、多 Bot 与可见协作：再启用 MultiNexus bridge。

## 权威与拓扑

- Local development：repo worktree + 显式本地 DB，只代表本地测试状态。
- Production control plane：持有生产 Coordinate DB 与服务；普通操作不得直接编辑 DB。
- Coding host：持有 canonical repo/harness 文件、provider/agentd 与 Git/GitHub 副作用。
- Deployed runtime copy：`/opt/coordinate`、`/opt/multinexus`，只由受审 deployment/recovery 更新。
- Private task artifacts：`${MYHARNESS_ROOT:-$HOME/projects/myharness}/projects/<project_id>/`，保存过程
  evidence，不是 runtime DB 或 active plan authority。

不同事实不能互相冒充：

- Coordinate DB：runtime events/jobs/leases/receipts/deliveries/executor projection。
- Repo/harness：稳定项目协议、canonical checklist/plan。
- GitHub：Issue/branch/commit/PR/CI/review。
- Discord/KOOK：可见消息总线，不是持久状态存储。

Remote MCP 是 Operator 北向控制面；Runtime HTTP 是 agentd 南向数据面；CLI/SSH 是尚未 MCP 化的
mutation、deployment、rotation、recovery 与 break-glass。三者不共享 credential，也不自动 fallback。

## 按需加载路由

先识别当前任务，再只读取对应子 skill：

| 当前任务 | 加载 |
|---|---|
| workspace 注册、harness path、task/checklist、assignment/completion/reconcile | `subskills/workspace-task-lifecycle/SKILL.md` |
| Remote MCP principal、tool visibility、typed lifecycle call、northbound scope | `subskills/remote-mcp-control/SKILL.md` |
| request/job、agentd、executor/capacity、lease、runner | `subskills/runtime-execution/SKILL.md` |
| event/delivery、Discord/KOOK、channel binding/provisioning | `subskills/messaging-delivery/SKILL.md` |
| GitHub Issue 认领、branch/PR、CI/review/merge gate | `subskills/github-collaboration/SKILL.md` |
| 从 task/job 入口只读重建一次执行的关联 trace（evidence states、next gate） | `coordinate trace task|job`（Issue #11 R1 只读投影；细节看 `--help`） |
| 观察 worker、JSONL、direct/managed delegation、局部 Operator | `subskills/worker-supervision/SKILL.md` |
| deploy、`/opt`、production SSH、policy mutation、事故恢复 | `subskills/production-recovery/SKILL.md` |
| Windows Codex/OMP/ZCode、Remote MCP 配置、NSSM、credential rotation | `subskills/windows-multi-cli/SKILL.md` |
| 仅需理解整体对象与状态关系 | `references/mental-model.md` |

跨领域任务按实际顺序逐个加载。例如“Windows agentd 接入并执行 canary”先读 Windows 配置模块，再读
runtime execution；只有发生 production mutation 时才加载 production/recovery。不要为可能用到而预加载。

历史入口 `references/command-reference.md`、`references/workflows.md`、
`references/troubleshooting.md` 现在只是迁移索引，避免旧链接断裂；它们不是第二份完整手册。

## 跨模块稳定规则

- 所有重要或跨 session 的 managed task 使用 Coordinate lifecycle；ordinary 小任务不强制 checklist node。
- 不直接编辑 harness JSON。文件 mutation 通过 Coordinate/harnessctl 的受控入口；DB mutation 通过
  Coordinate service/CLI/MCP。
- principal 的 actor/workspace/platform/tool grant 来自 server policy；调用参数不能扩大 authority。
- 默认需要明确授权；已有目标、范围、时限和操作边界清楚的持久授权时不机械重复提问。无论是否已授权，
  preflight、review、receipt、recovery 与 fail-closed gate 不省略。
- `ready=true`、HTTP `200`、进程在线或 Discord Bot 在线只证明各自那一层，不自动证明业务完成。
- 行为不明确时读取当前代码和 `--help`，不要让 skill 复制一份会漂移的完整 CLI/provider 手册。
- 除非用户明确提供平台、目标与 token context，不发送真实 Discord/KOOK 消息。

## 最小启动

```bash
cd "${COORDINATE_REPO:-$HOME/projects/coordinate}"
git status --short
PYTHONPATH=src python3 -m coordinate --help
export DB="${COORDINATE_DB:-$HOME/projects/coordinate/data/coordinator.sqlite3}"
skills/coordinate-operator/scripts/inspect.sh --db "$DB"
```

本地 wrapper：

```bash
skills/coordinate-operator/scripts/mac.sh workspace list
skills/coordinate-operator/scripts/mac.sh operator pending WORKSPACE
skills/coordinate-operator/scripts/mac.sh runtime executor list
```

解析顺序：`COORDINATOR_PYTHON_BIN` → `$REPO/.venv/bin/python` → PATH 中的 `python3`。

## 发现缺口

将观察到的 workflow、expected/actual、影响和建议切片写入当前项目 Issue 或 private task artifact。
保持事实可复核，不把临时发现塞回父 skill，也不在操作时静默改变架构。
