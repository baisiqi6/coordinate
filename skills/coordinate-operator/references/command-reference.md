# 命令参考

所有示例假设：

```bash
cd "${COORDINATE_REPO:-$HOME/projects/coordinate}"
export DB="${MULTI_AGENT_COORDINATOR_DB:-$HOME/projects/coordinate/data/coordinator.sqlite3}"
export MAC="skills/coordinate-operator/scripts/mac.sh"
```

本地开发可以直接运行 wrapper；生产控制面通过 `coord-ssh` 调用远端 `coord-local`，
不要直接编辑 `/var/lib/coordinate/coord.sqlite3`。

下面的 `--harness-root`、`--root` 和 `--plan-doc` 示例的约束**分层**（四条代码路径，不要笼统理解为"都必须 workspace-local"）：

- `workspace add --harness-root` / host-profile `--harness-root`（注册/存储层）：可保存任意 absolute path
  （控制面 authority data，注册只存储，不做 containment 检查）。
- `workspace init-harness --mode minimal --root`（默认 minimal）：`--root`/`--plan-doc` 允许 absolute
  path；`init_file_harness` 对 absolute root 直接 mkdir，**不做 containment 拒绝**（`_relative_to_workspace`
  对外部路径静默返回 absolute）。即 minimal file harness **技术上支持 external absolute root**。
- `workspace init-harness --mode full`（Boundary B）：**忽略 CLI `--root`**，用已注册的
  `workspace.harness_root` 并强制其在 `workspace.path` 内（`relative_to` 校验）。
- split-operation `--plan-doc`（Boundary A）：强制 POSIX workspace-relative（拒绝绝对/`..`/空段/反斜杠；
  lexical guard，**不**防 symlink/TOCTOU）。

**policy vs capability**：本项目 operator policy 保持 active plan workspace-local，不利用 minimal 的
external-root capability。task-scoped 过程材料（非 active plan）的归档位置是
`${MYHARNESS_ROOT:-$HOME/projects/myharness}/projects/<project_id>/`（由 skill/operator 消费，Coordinate
runtime 不感知）；在出现真实 runtime consumer 并完成 `artifact_root` 完整 contract design 之前，不要
把 `--plan-doc` 改成外部路径，也不要用 minimal 的 external-root 能力把 active harness 外置到 myharness。

## Workspace

```bash
$MAC workspace add WORKSPACE \
  --path /path/to/repo \
  --harness-root /path/to/repo/docs \
  --base-branch main \
  --branch-namespace agents \
  --default-bus stdout \
  --default-destination local

$MAC workspace list
$MAC workspace audit WORKSPACE
$MAC workspace audit WORKSPACE --no-refresh
$MAC workspace init-harness WORKSPACE \
  --root docs/project-harness \
  --task-id TASK \
  --plan-doc docs/path/to/phase-plan.md \
  --title 'Phase title'

$MAC state WORKSPACE --no-refresh
$MAC reconcile WORKSPACE --no-refresh
```

## Channel Binding

Coordinate 是 `(platform, channel_id) -> workspace_id` 的唯一持久权威；MultiNexus 在把任何
managed Discord/KOOK 入站消息放入 context/prompt/handoff/job 前必须先 resolve。mutation 是
event-first 且幂等：`--actor`/`--reason`/`--idempotency-key` 必填。

```bash
# 绑定 channel 到 workspace
$MAC workspace channel bind discord CHANNEL_ID WORKSPACE \
  --actor operator --reason 'bind #nexus to multinexus' --idempotency-key bind-discord-CHANNEL_ID

# 解析：未绑定是正常结果 {"binding": null, "status": "unbound"}，exit 0
$MAC workspace channel resolve discord CHANNEL_ID

# 列出（可选过滤）
$MAC workspace channel list
$MAC workspace channel list --platform kook
$MAC workspace channel list --workspace-id WORKSPACE

# 改绑前必须显式 release；--expected-workspace-id 必须与当前绑定一致（fail-closed）
$MAC workspace channel release discord CHANNEL_ID \
  --expected-workspace-id WORKSPACE \
  --actor operator --reason 'rebind to another project' --idempotency-key release-discord-CHANNEL_ID
```

- `platform` 只接受 `discord`/`kook`；`channel_id` 是 opaque id，非法值（空、首尾空白、控制
  字符、>128 code points）在所有子命令 fail loud（exit 1），不当作 unbound。
- 同一 channel 已绑到其他 workspace 时 bind 冲突（exit 1），必须先 release。
- 同一 idempotency key 的完全相同调用返回 `replayed`，不重复 mutation；复用 key 但参数不同或
  跨操作 fail closed（exit 1）。
- 冲突、非法 key、未知 workspace、CLI/DB failure 一律 exit 1。

## 有 Plan 支持的 Task

```bash
$MAC task create WORKSPACE \
  --task-id TASK \
  --plan-doc docs/path/to/phase-plan.md \
  --title 'Phase title' \
  --owner worker \
  --branch agents/worker/TASK \
  --payload-json '{"test_baseline":"python -m unittest discover tests/"}'
```

`task create` 写入/更新 coordinator task mirror 并追加幂等 `plan.ready` 事件。

## Operator 待办

```bash
$MAC operator pending WORKSPACE
```

列出等待 operator 决策或推进的任务。

## Events

```bash
$MAC event append EVENT_TYPE --workspace-id WORKSPACE --task-id TASK --actor ACTOR --payload-json '{}'
$MAC event list --workspace-id WORKSPACE
```

## Policy 和 Deliveries

```bash
$MAC policy render-event EVENT_ID --platform stdout --destination local
$MAC policy create-delivery EVENT_ID --platform stdout --destination local
$MAC policy pump-events --workspace-id WORKSPACE --platform stdout --destination local

$MAC delivery list
$MAC delivery list --status failed
$MAC delivery send DELIVERY_ID
$MAC delivery pump --platform stdout --limit 20
$MAC delivery recover-sending --platform stdout

$MAC worker delivery --platform stdout --once --limit 20
$MAC worker delivery --platform stdout --interval 5 --limit 20
```

## Runtime: Executor / Capacity

```bash
$MAC runtime executor sync --source /path/to/agent-registry.toml
$MAC runtime executor list
$MAC runtime executor show INSTANCE_ID

$MAC runtime capacity sync --source /path/to/agent-registry.toml
$MAC runtime capacity list
$MAC runtime capacity show AGENT_ID
```

`ExecutionContext`、executor binding、capacity policy 和 worktree resource 的边界
由 catalog 定义；operator 只查询和同步，不内联决策。

## Runtime: Request / Job

```bash
$MAC runtime request submit WORKSPACE \
  --target-agent AGENT_ID \
  --prompt 'task prompt' \
  --origin-json '{"platform":"discord","destination":"CHANNEL","message_id":"MESSAGE","session_scope_id":"discord:CHANNEL"}' \
  --reply-json '{"platform":"discord","destination":"CHANNEL"}'

$MAC runtime job claim --agent-id AGENT_ID
$MAC runtime job claim --agent-id AGENT_ID --recoverable --recovery-reason '...' --prior-process-stopped
$MAC runtime job report JOB_ID --agent-id AGENT_ID --status done --result-json '{}'
$MAC runtime job progress JOB_ID --agent-id AGENT_ID --stage coding --summary 'working'
$MAC runtime job lease renew JOB_ID --agent-id AGENT_ID --attempt-token ATTEMPT --lease-id LEASE_ID
$MAC runtime job lease reap
```

- `claim` 返回当前 `attempt_token` 和 managed `lease_id`；后续 `progress`、
  `report`、`lease renew` 必须按 claim 结果携带适用的 token/lease。
- 恢复 recoverable job 前必须关联 provider-native JSONL/进程证据，并使用显式
  recovery reason、`--prior-process-stopped` 与 lease/attempt token 约束。

## Runner Profiles 和 Jobs

```bash
$MAC runner add PROFILE \
  --runner-type generic_subprocess \
  --command 'bash scripts/runners/my-runner.sh {prompt_path} {result_path}'

$MAC runner list
$MAC runner examples
$MAC runner example codex-wrapper

$MAC job create WORKSPACE \
  --task-id TASK \
  --runner-profile-id PROFILE \
  --branch BRANCH \
  --timeout-seconds 600

$MAC job list --workspace-id WORKSPACE
$MAC job run JOB_ID
$MAC job pump --workspace-id WORKSPACE --limit 5
$MAC job cancel JOB_ID --reason 'operator requested'
$MAC job retry JOB_ID --reason 'retry after fix'
```

`generic_subprocess` 是本地/legacy runner 路径。当前 managed path 是 bridge →
Coordinate runtime request/job → per-agent `agentd`。

## Issue 拆分（host-aware）

```bash
$MAC issue scan WORKSPACE
$MAC issue triage WORKSPACE --issue-url URL
$MAC issue materialize WORKSPACE --issue-url URL

# coding-host 半边：只更新本地 harness 文件
$MAC issue materialize-files \
  --workspace-path /path/to/repo \
  --harness-root docs/project-harness \
  --workspace-id WORKSPACE \
  --operation-id OPERATION \
  --event-id EVENT_ID \
  --task-id TASK \
  --plan-doc docs/path/to/plan.md

# server 半边：只写控制面 DB 事件
$MAC issue materialize-record WORKSPACE \
  --event-id EVENT_ID \
  --plan-doc docs/path/to/plan.md \
  --operation-id OPERATION \
  --input-fingerprint INPUT_SHA256 \
  --before-fingerprint BEFORE_SHA256 \
  --after-fingerprint AFTER_SHA256 \
  --task-id TASK
```

## Assignment 转换

```bash
$MAC assignment request WORKSPACE --task-id TASK --owner OWNER --session SESSION
$MAC assignment accept WORKSPACE --task-id TASK --owner OWNER --session SESSION
$MAC assignment handoff WORKSPACE --task-id TASK --target TARGET --reason 'handoff reason'
$MAC assignment blocker WORKSPACE --task-id TASK --actor ACTOR --reason 'blocked by ...'
$MAC assignment unblock WORKSPACE --task-id TASK --actor ACTOR --decision approved --reason 'unblocked'
$MAC assignment closeout WORKSPACE --task-id TASK --reviewer REVIEWER
$MAC assignment review-result WORKSPACE --task-id TASK --reviewer REVIEWER --decision approved --summary 'looks good'
$MAC assignment mark-done WORKSPACE --task-id TASK
```

### Host-aware mark-done（files + record）

```bash
# 1) control plane 签发一次性 receipt
$MAC assignment mark-done-prepare WORKSPACE --task-id TASK

# 2) coding host 验证 receipt 并更新本地 mvp-checklist.json
$MAC assignment mark-done-files WORKSPACE \
  --workspace-path /path/to/repo \
  --harness-root docs/project-harness \
  --task-id TASK \
  --receipt RECEIPT \
  --event-cli-path "$HOME/.local/bin/coord-ssh"

# 3) commit/push checklist 并部署后，control plane 重新验证 deployed harness，
#    再写入 task.done 并消费 receipt
$MAC assignment mark-done-record WORKSPACE --task-id TASK --receipt RECEIPT
```

Harness 写操作必须通过这些命令或相应服务进行。不要手动编辑 harness JSON。

## GitHub 集成

```bash
$MAC branch allocate WORKSPACE --task-id TASK --owner OWNER
$MAC pr link WORKSPACE --task-id TASK --pr-url PR_URL
$MAC pr link WORKSPACE --task-id TASK --branch BRANCH
$MAC ci check WORKSPACE --task-id TASK
$MAC review check WORKSPACE --task-id TASK
$MAC merge gate WORKSPACE --task-id TASK
```

`merge gate` 读取最新本地 CI/review 事件并查询当前 PR head SHA。当需要最新
CI/review 状态时，先运行 `ci check` 和 `review check`。`ready=true` 仍需由当前
明确授权或有界持久授权覆盖 merge。

## Phase 8.4: Worker Push → PR Publish

`pr publish` 验证 worker 主机报告的 branch/commit 确实已推送到 GitHub，然后创建 PR
（或链接现有开放 PR）。它**不**推送 branch，也**不**合并。

```bash
# Host 侧：拥有 `gh` 和 GitHub token 的 coding host（Mac/Windows）。
$MAC pr publish WORKSPACE \
  --task-id TASK \
  --repo OWNER/REPO \
  --branch BRANCH \
  --head-owner OWNER \
  --base main \
  --title "TASK title" \
  --body "TASK body" \
  --commit <40-hex SHA> \
  --pushed true|false \
  --actor OPERATOR \
  [--remote origin] \
  [--validation "314 tests OK (2 skipped)"]

# 拆分模式：coding host 仍运行 `gh`；它只将 preflight 和仅记录操作
# 转发到远端 coord CLI。coordinate 的 server 副本从不运行 `gh`，
# 也从不持有 GitHub token。
$MAC pr publish WORKSPACE --task-id TASK ... --event-cli-path "$HOME/.local/bin/coord-ssh"

# 远端仅记录 sink（server DB，从不调用 `gh`）：
$MAC pr publish-record WORKSPACE --result-json '<host PublishResult JSON>'

# 远端只读 preflight（server DB，从不调用 `gh`）：
$MAC pr publish-preflight WORKSPACE \
  --repo OWNER/REPO --branch BRANCH --commit <40-hex SHA> --task-id TASK
```

结果（全部写入事件日志并以各自颜色渲染到 Discord）：

| 动作 | 事件 | 时机 |
| --- | --- | --- |
| 创建 | `pr.created` | `gh pr list` 没有 head 的开放 PR；`gh pr create` 成功。 |
| 链接 | `pr.linked` | `gh pr list` 返回了开放 PR；我们将其绑定到任务。 |
| 需要推送 | `push.required` | `pushed=false`，或远端 ref 缺失。worker 主机必须推送并重新运行。 |
| 阻塞 | `publish.blocked` | 验证、mirror 冲突、SHA 不匹配、发现不匹配、head_owner 不匹配或任何 `gh` 失败。 |

Preflight：使用 `--event-cli-path` 时，主机在任何 `gh` 调用之前自动运行远端 `pr publish-preflight`。如果远端返回 `ok=false`，主机返回 `publish.blocked` 而不调用 GitHub。使用 `--preflight-event-cli-path` 覆盖 preflight CLI 路径。

`ci.pending` 是内部事件，不渲染为可见 delivery。CI/review 事件包含 PR head SHA；`merge gate` 验证最新本地 CI/review 事件与当前 PR head 匹配。
