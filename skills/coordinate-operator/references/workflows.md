# Operator 工作流

将这些用作操作形态，而不是测试。通用验证边界见 `docs/runbook.md`；部署专用冒烟步骤
保存在对应 private operations 资料中。

当前命令中的 path 约束**分层**（四条代码路径）：
- split-operation `--plan-doc` 强制 workspace-relative（Boundary A，lexical，不防 symlink/TOCTOU）；
- `workspace init-harness --mode full`（Boundary B）忽略 CLI `--root`，用注册的 `workspace.harness_root`
  并强制其在 workspace 内；
- `workspace init-harness --mode minimal --root`（默认 minimal）**允许 absolute root**，`init_file_harness`
  直接 mkdir、不做 containment 拒绝——技术上支持 external root；
- `workspace add --harness-root` 注册可存任意 absolute path。

**本项目 policy**：保持 active plan workspace-local，不利用 minimal 的 external-root 能力。独立私有
`myharness` 用于 task-scoped 过程材料和历史归档（由 skill/operator 消费，Coordinate runtime 不感知），
不能直接替换 `--plan-doc` 或被当作 active harness root；只有出现真实 runtime consumer 并完成
`artifact_root` 完整 contract design 后，才考虑 contract cutover。

## 注册 Workspace

1. 选择稳定的 workspace id。
2. 注册仓库路径和 harness root。
3. 如果已知，设置默认可见 bus/destination。

```bash
$MAC workspace add WORKSPACE \
  --path /path/to/repo \
  --harness-root /path/to/repo/docs \
  --base-branch main \
  --branch-namespace agents \
  --default-bus stdout \
  --default-destination local
```

## 绑定 Channel 到 Workspace

managed Discord/KOOK 入站消息要被 strict MultiNexus 接受前，channel 必须先绑定到 workspace。
Coordinate 是唯一权威。

1. 为每个 canonical channel 选择目标 workspace。
2. 用稳定 idempotency key 绑定；重复同一调用是安全的 `replayed`。
3. 用 `list`/`resolve` 证明所有 active channel 已绑定且无冲突。
4. 改绑时先以当前 workspace 显式 `release`，再 `bind` 新 workspace。

```bash
$MAC workspace channel bind discord CHANNEL_ID WORKSPACE \
  --actor operator --reason '...' --idempotency-key bind-discord-CHANNEL_ID
$MAC workspace channel resolve discord CHANNEL_ID
$MAC workspace channel list --workspace-id WORKSPACE

# 改绑：先 release（expected workspace 必须匹配），再 bind
$MAC workspace channel release discord CHANNEL_ID \
  --expected-workspace-id WORKSPACE \
  --actor operator --reason '...' --idempotency-key release-discord-CHANNEL_ID
$MAC workspace channel bind discord CHANNEL_ID OTHER_WORKSPACE \
  --actor operator --reason '...' --idempotency-key rebind-discord-CHANNEL_ID
```

绑定后 release/rebind 会使旧 workspace 的 context/provider session 不再被新消息使用；
不要复用旧 scope。

## 初始化有 Plan 支持的 Phase

当仓库有 phase 计划但还没有完整 harness 运行时使用。

1. 注册或更新 workspace。
2. 在专用子目录中初始化最小 file-backed harness。
3. 创建有 plan 支持的 task，或使用 `init-harness` 创建的 task。
4. 无需 `harnessctl` 进行对账。
5. 当需要可见 worker 交接时，pump `plan.ready` 事件。

```bash
$MAC workspace init-harness WORKSPACE \
  --root docs/project-harness \
  --task-id TASK \
  --plan-doc docs/path/to/phase-plan.md \
  --title 'Phase title'

$MAC workspace audit WORKSPACE --no-refresh
$MAC reconcile WORKSPACE --no-refresh

$MAC policy pump-events --workspace-id WORKSPACE --platform stdout --destination local
$MAC worker delivery --platform stdout --once
```

`workspace audit --no-refresh` 将 file-backed 状态能力与 `harnessctl` 能力分开报告。如果 `assignment_lifecycle_available=false`，还不要使用 assignment mutation 命令；在 harness 运行时存在之前，使用 `task create`、events、jobs 和 deliveries。

## 登记重要任务（managed）

重要/跨 session 任务必须经 Coordinate 入口登记，不裸跑 harnessctl mutation：

- managed same-host：`$MAC task create WORKSPACE --task-id TASK --plan-doc docs/path/to/phase-plan.md
  [--operation-id UUID]` — combined create：checklist file 半边 + DB record 半边，幂等。
- managed split-host：`task create-files` → commit/deploy → `task create-record`。
- Standalone：`harnessctl add-item`。
- ordinary 小任务：task spec → worker → independent review → tests，不强制 checklist node。

入口矩阵与 old/new resolver、部分失败恢复、freshness、migration acknowledgement 规则见
`SKILL.md` 的“Checklist 与任务权威矩阵”；完整 flags 见 `command-reference.md`。

## 将 Event 转为可见消息

1. 确认事件存在。
2. 用 policy 渲染它。
3. 创建或 pump delivery。
4. 用 worker delivery 发送。

```bash
$MAC event list --workspace-id WORKSPACE
$MAC policy render-event EVENT_ID --platform stdout --destination local
$MAC policy create-delivery EVENT_ID --platform stdout --destination local
$MAC worker delivery --platform stdout --once
```

## 运行 Generic Subprocess Job

1. 注册受信任的 runner profile。
2. 为 task 创建 job。
3. 运行单个 job 或 pump pending jobs。
4. 如果失败，检查日志/结果。
5. Pump policy events 使结果可见。

```bash
$MAC runner add PROFILE --runner-type generic_subprocess --command '...'
$MAC job create WORKSPACE --task-id TASK --runner-profile-id PROFILE --branch BRANCH
$MAC job pump --workspace-id WORKSPACE --limit 1
$MAC job list --workspace-id WORKSPACE
$MAC policy pump-events --workspace-id WORKSPACE --platform stdout --destination local
$MAC worker delivery --platform stdout --once
```

`generic_subprocess` 是本地/legacy runner 路径。当前 managed path 是 bridge →
Coordinate runtime request/job → per-agent `agentd`。

## 操作 Runtime Job

```bash
$MAC runtime request submit WORKSPACE \
  --target-agent AGENT_ID \
  --prompt 'task prompt' \
  --origin-json '{"platform":"discord","destination":"CHANNEL","message_id":"MESSAGE","session_scope_id":"discord:CHANNEL"}' \
  --reply-json '{"platform":"discord","destination":"CHANNEL"}'
$MAC runtime job claim --agent-id AGENT_ID
$MAC runtime job report JOB_ID --agent-id AGENT_ID --status done --result-json '{}'
$MAC runtime job lease reap
```

恢复 recoverable job 前必须关联 provider-native JSONL/进程证据，并使用显式
recovery reason、`--prior-process-stopped` 与 lease/attempt token 约束。

## 操作 Assignment 生命周期

典型任务状态流：

```text
assignment request -> accept -> 按需 blocker/handoff/unblock -> closeout -> review-result -> mark-done
```

host-aware mark-done 拆分为 `mark-done-prepare` / `mark-done-files` /
`mark-done-record`，保证 coding-host 文件边与 control-plane 事件边的 authority 分离。

每个命令写入持久化 Coordinate 事件。支持的事件可以通过 policy 转换为可见 deliveries。

## 操作 GitHub 状态

典型 GitHub 状态流：

```text
branch allocate -> pr link -> ci check -> review check -> merge gate
```

`merge gate` 读取最新本地 CI/review 事件并查询当前 PR head SHA。当需要最新 CI/review
状态时，先运行 `ci check` 和 `review check`。

## 恢复形态

如果有什么看起来卡住了：

1. 检查 jobs、events 和 deliveries。
2. 运行 workspace audit。
3. 仅在没有活跃发送方时恢复 `sending` deliveries。
4. 显式重试失败的 jobs 或失败的 deliveries。
5. 对 recoverable runtime job，先确认 provider 进程/会话已停止，再使用显式
   recovery reason 和 lease/attempt token 约束重试。

```bash
$MAC workspace audit WORKSPACE
$MAC delivery list --status sending
$MAC delivery recover-sending --platform stdout
$MAC job retry JOB_ID --reason 'operator retry'
```
