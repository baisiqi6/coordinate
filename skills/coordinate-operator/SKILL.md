---
name: coordinate-operator
description: Use when operating the Coordinate project across local development, coding-host files, and production control-plane via coord-ssh. Covers workspace setup, runner jobs, assignment transitions, visible-message delivery, GitHub branch/PR/CI/review state, merge gate checks, and operational troubleshooting. This skill is for AI operator onboarding, not for implementing new Coordinate features.
---

# Coordinate Operator

当你需要操作 Coordinate 而不是从头重新发现其 CLI 和状态模型时，使用本 skill。

本 skill 是 operator 指南。它不是项目测试计划。通用验证与恢复边界见
`docs/runbook.md`；具体部署的冒烟步骤应保存在对应 private operations 资料中。

## 拓扑

必须区分四个环境：

- **Local development**：repo worktree + 显式本地 DB；只代表本地测试状态。
- **Production control plane**：运行 Coordinate 服务并持有生产 DB 的主机；典型部署使用 `/var/lib/coordinate/coord.sqlite3`，通过严格 SSH wrapper `coord-ssh` 调用远端 CLI（如 `/usr/local/bin/coord-local`）。具体路径由部署配置决定，不得在 server 上直接编辑 DB。
- **Coding host**：保存 canonical repo/harness 文件、运行 provider/agentd、执行 `*-files` 文件半边和 Git/GitHub 副作用。
- **Deployed runtime copy**：`/opt/coordinate` 与 `/opt/multinexus`，只由受审部署/恢复流程更新；普通 operator 命令必须保留 `/opt` fail-closed guard。
- **Private task artifact repo**：可用 `$MYHARNESS_ROOT/projects/<project_id>/` 保存 plan、bootstrap、
  review、handoff、verdict、receipt 与 archive index；它不是 runtime DB，也不是产品 repo。

Coordinate DB 保存 runtime events、jobs、leases、receipts、deliveries、executor/capacity projection；repo harness 文件保存稳定项目规范。Discord/KOOK 只是可见总线。

## 组合选择

Operator 应按需求使用最小部署组合：

- 只需要当前 agent 遵循 SDD/TDD、review 与测试纪律：使用 EXharness，不启动 Coordinate runtime。
- 需要 durable job、event、lease、receipt 与恢复：增加 Coordinate；当前 agent、Operator 或已有
  runner 可以主动执行和报告 job。
- 需要自动调用 vendor agent CLI、恢复 provider session 或跨主机执行：增加 MultiNexus
  `agentd/adapters`。
- 需要 Discord/KOOK、多 Bot 和可见协作：再启用 MultiNexus bridge。

不要把 MultiNexus 等同于 Discord，也不要把 Coordinate 等同于 vendor agent executor。没有消息平台
需求时可以使用 executor-only；已有可靠执行者时也可以只使用 Coordinate，不为完整性增加运行层。

Coordinate 对 path 的约束**分层**，不要笼统说"harness_root 必须 workspace-local"。当前代码有四条
不同路径（`workspace_cli.py` + `onboarding.py`）：

- **`workspace add --harness-root` / host-profile `--harness-root`（注册/存储层）**：可保存任意 absolute
  path（控制面 authority data；`workspace add` 只存储，host profile 存 host-native 路径）。注册本身不做
  containment 检查。
- **`workspace init-harness --mode minimal --root`（minimal file harness 层）**：默认 mode 就是 `minimal`。
  `--root` / `--plan-doc` 明确允许 absolute path（help: "relative to workspace path unless absolute"）。
  `init_file_harness`（`onboarding.py:686`）对 absolute root 直接 `mkdir(parents=True)` 创建目录/文件，
  **不做 containment 拒绝**；`_relative_to_workspace`（`onboarding.py:1178`）对外部路径静默返回 absolute
  path（`except ValueError: return str(path.resolve())`），不 fail-closed。**即 minimal file harness 技术
  上支持 external absolute root**——一旦这样用，它就是 runtime-aware active harness，不是"runtime 不感知
  的归档"。这是 Coordinate 的 capability，不是本项目采用的 policy。
- **`workspace init-harness --mode full`（full harness 层，Boundary B）**：**忽略 CLI `--root`**，使用
  已注册的 `workspace.harness_root`，并用 `hr_path.relative_to(ws_root)` 强制其在 `workspace.path` 内
  （`onboarding.py:966-982`），否则拒绝写 checklist（`harness-checklist.json` 或 legacy
  `mvp-checklist.json`）、`harness-state` 与 events。
- **split-operation `plan_doc`（mutation fingerprint 层，Boundary A）**：强制 POSIX workspace-relative
  （拒绝绝对/`..`/空段/反斜杠）。这是 lexical guard，**不**提供 symlink/TOCTOU 抵抗。

**区分 policy 与 capability**：
- **本项目当前 operator policy**：MultiNexus/Coordinate 的 active plan 保持 workspace-local，`myharness`
  只作过程材料归档（由 skill/operator 经 `$MYHARNESS_ROOT` 消费）。即 operator 不利用 minimal file
  harness 的 external-root capability 把 active plan 外置。
- **Coordinate runtime capability**：minimal file harness 技术上支持 external absolute root（见上）。
  本项目选择不使用该 capability 来外置 active plan；若未来要用，须先完成 `artifact_root` 完整 contract
  design（schema/ExecutionContext/digest）并明确边界。

因此：不要用 symlink、路径穿越或放宽 guard 绕过；在出现真实 runtime consumer 前，repo-local harness
只保留实际 consumer 需要的最小机器状态/locator，task-scoped 过程正文由 skill/operator 经
`$MYHARNESS_ROOT` 归档（Coordinate runtime 不感知），不由受审迁移计划外置 active plan。

## 基本规则

- 本地开发路径：`${COORDINATE_REPO:-$HOME/projects/coordinate}`。
- 私有 task artifact repo 通过 `${MYHARNESS_ROOT:-$HOME/projects/myharness}` 发现；不要在 skill、
  bootstrap 或脚本中硬编码个人绝对路径。
- `myharness` project commit 只允许包含 `projects/<project_id>/` 子树。多个项目同时写入时使用
  独立 branch/worktree；单 writer 不额外引入锁。根目录共享文件使用独立维护 commit。
- 生产调用必须经过 `coord-ssh`；不要在 server 上直接改 `/var/lib/coordinate/coord.sqlite3`。
- 本地使用 CLI 时：

```bash
cd "${COORDINATE_REPO:-$HOME/projects/coordinate}"
PYTHONPATH=src python3 -m coordinate --db "$DB" ...
```

- 优先使用辅助 wrapper：

```bash
skills/coordinate-operator/scripts/mac.sh ...
```

- 如果行为不明确，先阅读 `src/coordinate/` 下的实现再猜测。
- 如果当前工作流暴露出缺失能力、令人困惑的 UX 或 bug，记录到当前项目的 issue tracker
  或 private task artifact repository；不要把临时发现写进稳定规范。
- 不要把工作流状态放在 Discord/KOOK bot 记忆中。运行时状态属于 SQLite；
  稳定项目状态属于 harness/repo 文件。
- Channel → workspace 绑定由 Coordinate 唯一持有（`(platform, channel_id) -> workspace_id`）。
  managed Discord/KOOK 入站消息必须先 resolve；未绑定、lookup 失败或 workspace 不一致一律
  fail closed，没有 silent fallback。绑定/解绑用 `workspace channel bind`/`release`
  （event-first、幂等，`--actor`/`--reason`/`--idempotency-key` 必填；改绑必须先显式 release）。
  详见 `references/command-reference.md` 的 “Channel Binding” 与 `references/workflows.md` 的
  “绑定 Channel 到 Workspace”。
- 所有任务生命周期操作默认使用 Coordinate 命令。不要要求普通 worker agent
  直接使用 harness CLI。
- Harness CLI 和 harness skill 是底层修复和维护接口。仅在 Coordinate 缺少
  所需操作、调试 Coordinate 与 harness 的同步或维护 harness 本身时使用。
- 不要直接编辑 harness JSON。Harness mutation 必须通过 Coordinate 服务
  经由 `harnessctl` 进行。
- 除非用户明确提供平台、目标和 token 上下文，否则不要发送真实 Discord/KOOK
  消息。

## 授权边界

- 本 skill 的操作面（DB、生产 SSH、`/opt` 部署、split-operation mutation、GitHub 副作用）默认按
  high-risk 边界执行：preflight、review、receipt、recovery 和 fail-closed gate 不因授权省略。
  ordinary 低风险任务（单 session 可完成、无生产/删除/部署/跨主机副作用）由
  `long-running-project-harness` skill 的 ordinary 模式处理，不在此 skill 重复定义。
- 默认需要用户明确授权。
- 已存在目标、范围、时限和操作边界明确的持久授权时，不机械重复询问。
- preflight、review、receipt、recovery 和 fail-closed gate 不因授权而省略。
- `merge gate ready=true` 只证明技术前置条件满足，不自动授予 merge，除非现有
  授权明确覆盖。
- 只有当前明确授权或有界持久授权覆盖 merge/deploy 时才执行；不得把技术 gate
  或历史授权推断成新的权限。

## Checklist 与任务权威矩阵

按部署 profile 与任务重要性选择入口；ordinary 小任务不强制 checklist node：

| 场景 | 正确入口 |
|---|---|
| ordinary 小任务 | task spec → worker → independent review → tests |
| managed same-host 重要/跨 session 任务 | `coordinate task create`（checklist file + DB 的 combined contract） |
| managed split-host 重要任务 | `task create-files` → code/deploy boundary → `task create-record` |
| Standalone 重要任务 | `harnessctl add-item` |
| managed metadata/lifecycle | canonical plan + Coordinate wrapper/receipt 路径；不直接裸 `harnessctl add-item/update-item/mark-done` |

稳定规则：

- **old/new resolver**：只有 `harness-checklist.json` 时用新名，只有 `mvp-checklist.json` 时用旧名；两者皆无或并存 fail closed。既有实例继续使用其现有 filename，不自动迁移。
- **combined create 部分失败恢复**：`coordinate task create` 是 file-first、record-second 的 combined contract；`--operation-id` 固定 split operation。file 半边已提交而 DB 半边失败时，用同一 `--operation-id` 重跑（或按 recovery 参数重跑 `task create-record`）幂等补齐，不重复 mutation。
- **freshness**：`harness-state.json` 与 `docs/current/*` 是可重建 cache/pointer，不是 authority；与 checklist bytes 不一致时以 checklist 为准，重新运行 `harnessctl state`。
- **filename migration** 必须独立 authority；`--ack-managed-profile` 等 acknowledgement flag 不等于 authority。
- 存在有界持久授权时不机械重复提问，但 acknowledgement flag 不授予 mutation；preflight/review/receipt/fail-closed gate 不省略。

完整 flags 与恢复命令留在 `references/command-reference.md`；本 skill 只保留稳定规则。

## Dogfood 委派规则

在 dogfood 多 agent 开发时，除非明确要求，operator 不应实现 worker 任务。
预期的协作循环是：

```text
operator -> coordinate task/plan/assignment
operator -> task handoff --target-agent TARGET_AGENT
daemon/worker -> 可见 Discord/KOOK 状态 + [handoff] @TARGET_AGENT
target agent -> 通过可见渠道报告 accept/progress/closeout
operator -> review-result -> mark-done
```

重要边界：

- `[PLAN]`、`[ASSIGN]`、`[HANDOFF]`、`[PROGRESS]` 和 `[DONE]` 等状态广播
  是可见性，不是另一个 agent 被委派的证明。
- 真实的 Discord agent 交接需要 `task handoff ... --target-agent <agent-id>`，
  这样 policy 才能创建 agent 特定的 `[handoff] <@discord-user-id>` delivery。
- 使用 workspace agent 注册表进行目标解析；如果 `--target-agent` 失败，
  注册 agent 映射，而不是回退到通用 worker 交接。
- 执行传输已迁移：当前 managed path 是 bridge → Coordinate runtime request/job
  → per-agent `agentd`。`generic_subprocess` 是本地/legacy runner 路径，不再是
  唯一执行路径。

## 心智模型

Coordinate 围绕事件驱动的 runtime 控制面：

```text
命令 -> 服务 -> event/job/delivery -> worker -> 外部副作用
runtime request -> job -> claim -> agentd -> report -> event -> policy -> delivery
assignment request/accept/handoff/blocker/unblock/closeout/mark-done -> event
branch -> PR -> CI -> review -> merge gate
```

仅在需要时加载详情：

- 核心模型：`references/mental-model.md`
- 命令：`references/command-reference.md`
- 常见工作流：`references/workflows.md`
- Runner 操作：`references/runner-operations.md`
- Worker 观察和活跃性：`references/worker-observation.md`
- Delivery 和 bus 操作：`references/delivery-and-bus.md`
- GitHub 集成：`references/github-integration.md`
- 故障排除：`references/troubleshooting.md`

## 标准操作顺序

1. 检查上下文：

```bash
cd "${COORDINATE_REPO:-$HOME/projects/coordinate}"
git status --short
PYTHONPATH=src python3 -m coordinate --help
```

2. 设置 DB 路径：

```bash
export DB="${COORDINATE_DB:-$HOME/projects/coordinate/data/coordinator.sqlite3}"
```

3. 检查当前 Coordinate 状态：

```bash
skills/coordinate-operator/scripts/inspect.sh --db "$DB"
```

4. 选择你需要的窄工作流。默认不运行宽泛的副作用。

在监督委派的 worker 时，在宣布其空闲、挂起或死亡之前，先阅读
`references/worker-observation.md`。安静的 worktree 不是充分证据：先检查
provider 原生事件流或会话日志，然后将其与进程状态、仓库变更和验证结果
关联。同一物理 agent 的不同 session/job/worktree 以持久 ID 和 lease 区分。

## 快速命令 Wrapper

wrapper 使用项目路径并设置 `PYTHONPATH=src`：

```bash
skills/coordinate-operator/scripts/mac.sh workspace list
skills/coordinate-operator/scripts/mac.sh event list --workspace-id WORKSPACE
skills/coordinate-operator/scripts/mac.sh job list --workspace-id WORKSPACE
skills/coordinate-operator/scripts/mac.sh delivery list --platform stdout
skills/coordinate-operator/scripts/mac.sh operator pending WORKSPACE
skills/coordinate-operator/scripts/mac.sh runtime executor list
skills/coordinate-operator/scripts/mac.sh runtime capacity list
```

覆盖默认值：

```bash
MAC_REPO=/path/to/coordinate MAC_DB=/tmp/coordinator.sqlite3 \
  COORDINATOR_PYTHON_BIN=/path/to/python3 \
  skills/coordinate-operator/scripts/mac.sh workspace list
```

Python 解析顺序：`COORDINATOR_PYTHON_BIN` -> `$REPO/.venv/bin/python` -> `python3` on PATH。

## 当你发现缺口时

向当前项目的 issue tracker 或 private task artifact repository 追加带日期的说明，包含：

- 观察到的工作流
- 预期行为
- 实际行为
- 影响
- 如果明显，建议的下一个切片

保持说明基于事实。操作 Coordinate 时不要静默改变架构或运行时行为。
