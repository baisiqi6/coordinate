# Coordinate 运维手册

## 快速参考

```bash
# Harness 命令
scripts/harness/harnessctl state
scripts/harness/harnessctl validate
scripts/harness/harnessctl doctor
scripts/harness/harnessctl session-init
```

## 新 Workspace 初始化顺序

1. 在 coordinator 中注册 workspace：`workspace add <id> --path ... --harness-root ...`
2. 运行 `workspace init-harness <id> --mode full --source <reference-workspace-scripts/harness>` 创建完整 harness 运行时
3. 运行 `workspace doctor <id>` 验证 full_harness_runtime 状态
4. 在 `docs/project-harness/tasks/<task-id>/plan.md` 下创建计划
5. 使用 coordinator `task create` 注册第一个任务
6. 运行 `workspace audit <id>` 确认无 drift

## Channel binding（platform channel → workspace）

Coordinate 持有 `(platform, channel_id) -> workspace_id` 的唯一持久权威。上线 strict
MultiNexus 前，必须把生产 allowlist 中的每个 canonical channel 绑定到其 workspace，并用
`list`/`resolve` 证明无遗漏、无冲突。

```bash
# 绑定（event-first，幂等；--actor/--reason/--idempotency-key 必填）
coordinate workspace channel bind <platform> <channel_id> <workspace_id> \
  --actor <actor> --reason <reason> --idempotency-key <key>

# 解析：未绑定是正常结果 {"binding": null, "status": "unbound"}，exit 0
coordinate workspace channel resolve <platform> <channel_id>

# 列出（可选过滤）
coordinate workspace channel list [--platform <platform>] [--workspace-id <workspace_id>]

# 改绑前必须显式 release，--expected-workspace-id 必须与当前绑定一致（fail-closed）
coordinate workspace channel release <platform> <channel_id> \
  --expected-workspace-id <workspace_id> \
  --actor <actor> --reason <reason> --idempotency-key <key>
```

- `platform` 只接受 `discord`/`kook`（归一化）；`channel_id` 是 opaque id，非法值
  （空、首尾空白、控制字符、>128 code points）在所有子命令 fail loud（exit 1），不当作 unbound。
- 同一 channel 已绑到其他 workspace 时 bind 冲突（exit 1），必须先 release。
- 重复同一 idempotency key 的完全相同调用返回 `replayed`，不重复 mutation；复用 key 但参数
  不同或跨操作则 fail closed（exit 1）。
- 冲突、非法 key、未知 workspace、数据库/CLI failure 一律 exit 1。

## Harness 权威来源边界（内部 vs Sidecar vs /opt）

`workspace.path`（代码 checkout）和 `workspace.harness_root`（harness 状态）
是**刻意分离的概念**。根据仓库归属，它们可以是同一棵树或不同的树：

- **内部/托管仓库** — harness 位于仓库*内部*并随其提交。
  `workspace.path == workspace.harness_root` 的父目录（例如 multinexus：
  `path=…/multinexus`，`harness_root=…/multinexus/docs/project-harness`）。
  使用 `workspace init-harness --mode full`，它会将 `scripts/harness/`
  脚手架到 checkout 中，并**要求 `harness_root` 在 `workspace.path` 内部**
  （`onboarding.full_init` 拒绝树外的 `harness_root` 以防止路径穿越）。
- **外部/上游仓库** — harness 位于**目标 checkout 之外的 sidecar workspace**，
  因为提交给上游的 PR 不能包含我们的 harness 文件。示例：
  - 代码 checkout：`…/projects/opencode`
  - harness root：`…/projects/harness-workspaces/opencode`

  这里**不要**运行 `init-harness --mode full`（它会写入上游 checkout，
  且当 `harness_root` 在路径外时会被阻止）。改为将 `harness_root` 指向
  sidecar 并使用 host-aware 流程：`issue materialize-files`（coding-host 半程）
  只同步 `harness_root/mvp-checklist.json` 并支持 `workspace.path` 之外的
  sidecar `harness_root`；代码 checkout 保持无 harness 文件
  （由 `tests/test_issues.py::IssueMaterializeHostAwareTests::test_files_supports_sidecar_harness_root` 覆盖）。
- **服务器 `/opt/*` 副本是部署产物，不是权威来源。** 它们由当前环境中经过审查的
  deployment flow 生成，不含 git 历史，会被下次部署覆盖。
  `issue materialize` / `materialize-files` 拒绝任何包含 `/opt/` 的
  `workspace.path` 或 `harness_root`，除非设置了 `--allow-runtime-copy`。
  要变更 `/opt` workspace 的 harness 状态，在 coding host 上运行
  `materialize-files`，commit/push，部署，然后通过 coord-ssh 运行
  `materialize-record`（仅 DB，从不触碰 `/opt` 文件系统）。

Worker bootstrap（`task handoff`）向 worker 暴露两个值：它渲染
`execution_workspace_path`（`cd` / 运行 git 的位置）和 `execution_harness`
（读取 `harness-state.json` / `progress.md` 的 harness root），按目标 agent 的
host profile 重新映射 — 因此 coding host 上的 worker 永远不会被告知把服务器
`/opt/*` 部署副本当作其工作树。

## Phase 8.4: Worker Push → PR Publish

`pr publish` 命令验证 worker 主机报告的 branch/commit 确实已推送到 GitHub，
然后创建或链接 PR。托管模型使用**两个不同的 CLI 子命令**：

```bash
# Coding host（Mac/Windows）— 运行 `gh` 并持有 GitHub token。
# 默认模式：`publish_pr` 对本地 DB 运行。
coordinate pr publish WORKSPACE \
  --task-id TASK --repo OWNER/REPO --branch BRANCH \
  --head-owner OWNER --base main --title "title" --body "body" \
  --commit <40-hex SHA> --pushed true|false \
  [--remote origin] [--validation "..."]

# 同一主机，带 `--event-cli-path`：本地运行 publish_pr 后，
# 将 PublishResult JSON 转发到远端 coord CLI，后者对远端 DB 运行
# `pr publish-record`（仅记录）。
# `--event-cli-path` 还会在任何 `gh` 调用之前使用相同路径触发远端
# `pr publish-preflight`（如需要可用 `--preflight-event-cli-path` 覆盖）。
coordinate pr publish WORKSPACE ... \
  --event-cli-path "$HOME/.local/bin/coord-ssh"

# 远端 sink（仅记录，从不调用 `gh`）：
coordinate pr publish-record WORKSPACE --result-json '<host PublishResult JSON>' \
  [--actor operator]

# 远端 preflight（只读，从不调用 `gh`）：
coordinate pr publish-preflight WORKSPACE \
  --repo OWNER/REPO --branch BRANCH --commit <40-hex SHA> --task-id TASK
```

仅记录的 sink 针对远端 task mirror 重新验证主机的声明，重新计算规范
event type/payload/幂等键，并在 `action in {created, linked}` 时严格验证
repo、branch、commit、head/base、远端 SHA 和 PR URL，然后才 upsert 远端
`tasks.pr` 列。这就是远端 `merge gate` 读取远端 DB 时能看到 PR 的原因。
Event 追加和 mirror upsert 在一个 SAVEPOINT 内使用无提交 DB 原语，
包括在 Python 3.10/3.11 上；它不信任主机 event 字段，也不会在失败后留下半状态。

Preflight 耦合：当设置了 `--event-cli-path` 时，主机还会在任何 `gh` 调用之前
运行远端 `pr publish-preflight`。如果远端返回 `ok=false`，主机会短路并返回
`publish.blocked` 而不触碰 GitHub。这保证了在远端状态重新验证之前不会发生
GitHub 写入。如果远端 sink 和远端 mirror 验证器通过不同的 CLI 到达，
使用 `--preflight-event-cli-path` 覆盖 preflight 路径。

当任务已有 PR 时，preflight 返回 `link_existing`。其 commit 只能在
task/repo/branch/PR 绑定保持不变的情况下前进。主机只读地发现同一 PR 并验证
其新的 head SHA 和 base；只有经验证的 `linked` 结果才能推进远端 publish commit。
Repo、branch 或 PR 重新绑定仍被阻止。

结果（全部写入事件日志并以各自颜色渲染到 Discord）：

| 事件 | 时机 | 可见性 |
| --- | --- | --- |
| `pr.created` | `gh pr list` 没有 head 的开放 PR；`gh pr create` 成功 | `[PR]`（绿色） |
| `pr.linked` | 开放 PR 已存在（headRefOid + baseRefName 均匹配） | `[PR]`（黄色） |
| `push.required` | `pushed=false`，或远端 ref 404 | `[PUSH_REQUIRED]`（黄色） |
| `publish.blocked` | 验证、mirror 冲突、SHA 不匹配、发现不匹配、head_owner 不匹配或 `gh` 失败 | `[BLOCKER]`（红色） |

严格输入（任何偏差 → `publish.blocked`）：

- `--repo` `^[a-z0-9._-]+/[a-z0-9._-]+$`
- `--commit` 40 字符小写十六进制
- `--pushed` 字面 `true` / `false`
- `--head_owner` 必须等于 repo owner（fork 工作流不在范围内）
- `--base` 必需且显式（从不从 `workspace.base_branch` 派生，
  以避免一个控制 workspace 携带多个目标仓库时的跨仓库陷阱）

CLI 退出码：

- `0` 仅当 `result.action in {created, linked}`。
- `1` 在 `push.required` / `publish.blocked` 时（CI / harnessctl 可以快速失败）。
- `2` argparse / 验证失败。

幂等性：`pr.created` / `pr.linked` 键包含已解析的 PR URL，因此在瞬时
event 写入失败后重新运行永远不会重复事件，也永远不会调用两次 `gh pr create`
（发现步骤先找到现有 PR，且 headRefOid + baseRefName 必须均匹配）。
Worker report 字段（`repo/branch/commit/remote/pushed/validation`）在发起
publish 调用之前是可选的；旧 report 继续工作。

`.py` event_cli 路径自动前置 `sys.executable`，因此 Windows coding host
（`coord-ssh-win.py`）无需在 worker 脚本中硬编码 `python` 即可正确启动。

CI 说明：没有配置 GitHub checks 的开放 PR 是 pending 状态。此时
`gh pr checks` 返回退出码 1、空 stdout 和 stderr 上的 `no checks reported`；
`ci check` 将该确切响应规范化为空 check 列表并写入 `ci.pending`。
其他非 JSON 失败仍然 fail closed。

## Runtime Bridge / Agentd 冒烟验证

Phase 7 运行时命令为以下链路提供第一个 CLI 形态的服务边界：

```text
bridge -> coordinate -> agentd
```

> 前置条件：真实项目应先完成上文[新 Workspace 初始化顺序](#新-workspace-初始化顺序)。以下最小
> runtime smoke 只注册 `mac-smoke` workspace，并为必填的 harness root 使用临时目录；它不替代
> `workspace init-harness`，也不把临时目录当作长期项目状态。

```bash
mkdir -p data
SMOKE_HARNESS_ROOT="$(mktemp -d)"
PYTHONPATH=src python3 -m coordinate \
  --db data/coordinator.sqlite3 \
  workspace add mac-smoke \
  --path "$PWD" \
  --harness-root "$SMOKE_HARNESS_ROOT" \
  --base-branch main
```

为该 workspace 注册 agent 所在 host 的执行路径映射；request preflight 会据此构建 fail-closed
execution context：

```bash
PYTHONPATH=src python3 -m coordinate \
  --db data/coordinator.sqlite3 \
  workspace host-profile set mac-smoke \
  --host-id mac \
  --workspace-path "$PWD" \
  --harness-root "$SMOKE_HARNESS_ROOT"
```

注册 agentd。这还会创建同 id 的 `agentd` runner profile：

```bash
PYTHONPATH=src python3 -m coordinate \
  --db data/coordinator.sqlite3 \
  runtime agent register \
  --agent-id mac-codex \
  --host-id mac \
  --capabilities-json '{"models":["codex"]}'
```

提交规范化的 bridge 请求。non-task request 必须携带 bounded、稳定、可跨消息复用的
session scope；同一 Discord channel 的连续消息应复用同一个 scope。本示例与
`destination=channel-1` 对齐：

```bash
PYTHONPATH=src python3 -m coordinate \
  --db data/coordinator.sqlite3 \
  runtime request submit mac-smoke \
  --target-agent mac-codex \
  --prompt "hello from bridge" \
  --origin-json '{"platform":"discord","destination":"channel-1","message_id":"msg-1","session_scope_id":"discord:channel-1"}' \
  --reply-json '{"platform":"discord","destination":"channel-1"}'
```

以 agentd 身份认领 pending job：

```bash
PYTHONPATH=src python3 -m coordinate \
  --db data/coordinator.sqlite3 \
  runtime job claim \
  --agent-id mac-codex
```

报告结果。如果 `response_text` 存在且原始请求有回复目标，coordinate 会创建
返回原始平台的 pending delivery：

```bash
PYTHONPATH=src python3 -m coordinate \
  --db data/coordinator.sqlite3 \
  runtime job report <job-id> \
  --agent-id mac-codex \
  --status done \
  --result-json '{"response_text":"done"}'
```

不要让远端 bridge/agentd 客户端直接指向 SQLite 文件。客户端应调用 coordinate
命令或未来围绕相同运行时服务函数的 HTTP wrapper。


## Host-Aware Mark-Done：完成回执协议

Host-aware mark-done 将 coding host 的规范 `mvp-checklist.json` mutation 与
服务器端 `task.done` 事件绑定在**一个服务器签发的、一次性完成回执**之下。
两个半程不能再独立推进：回执是唯一授权，它存在于控制面事件账本中：

    completion.authorized → completion.claimed → completion.applied → task.done + completion.consumed

权威和信任路径：

- 回执仅在控制面上签发和查询。Coding host 提供 `receipt_id`；服务器从账本
  重新派生 `workspace_id`、`task_id`、`authorized_actor`、过期时间和指纹。
  客户端提供的 workspace/task/过期声明从不被信任。
- Coding host 通过远端 coord CLI（`--event-cli-path`，通常是 `coord-ssh`
  wrapper）**在线**验证、预留和确认回执。没有该路径时，正常的
  `mark-done-files` 命令会 fail closed。
- 该协议刻意采用两阶段：回执在规范写入*之前*移动到 `claimed`，在写入落地
  *之后*移动到 `applied`。如果主机在中间死亡，账本显示 `claimed`
  （可诊断的部分状态），永远不会是虚假的 `applied`。记录侧要求 `applied`；
  它不会消费仅 `claimed` 的回执。

### 标准完成流程

```bash
# 1. 控制面：验证 closeout/review/forge gate 并签发回执。
coord-ssh assignment mark-done-prepare discord-nexus \
  --task-id <task_id> --actor operator
# => result.receipt_id（记录它）

# 2. Coding host：验证 + 预留回执，变更规范 checklist，然后确认 —
#    全部通过远端 coord CLI。mark-done-files 自动运行
#    preflight -> mark-done-claim（预留）-> 本地写入 + 结构化
#    completion_receipt 元数据 -> mark-done-apply（确认）。
coordinate assignment mark-done-files \
  --workspace-path /path/to/<workspace> \
  --harness-root docs/project-harness \
  --workspace-id discord-nexus \
  --task-id <task_id> \
  --receipt <receipt_id> \
  --event-cli-path "$HOME/.local/bin/coord-ssh" \
  --verification "completion authorized by receipt <receipt_id>"

# 3. Commit + push 更新后的 checklist，然后按当前部署配置执行受审部署。
git add docs/project-harness/mvp-checklist.json
git commit -m "harness: mark-done for <task_id>"
git push
# 部署命令由当前环境配置提供；public 文档不硬编码 private topology。

# 4. 控制面：重新验证已部署的 harness，并在追加 task.done 的同时
#    原子地消费回执。
coord-ssh assignment mark-done-record discord-nexus \
  --receipt <receipt_id> --actor operator

# 5. 验证状态。
coord-ssh state discord-nexus
coord-ssh event list discord-nexus
```

回执记录 `before_fingerprint` / `after_fingerprint`（对
`{id, status, workflow:{status, branch}}` 的 SHA-256）；自由文本 `verification`
仅为描述性，被排除在指纹之外。记录侧重新读取**已部署**的 harness，要求任务
为 `done`/`closed`，并要求已部署指纹与 applied after-fingerprint 匹配，
然后才写入 `task.done`。

### 恢复

After-fingerprint 在规范写入*之前*确定性计算，因此预留可以在写入之前记录它。
如果 coding host 在预留之后但在写入期间/之前或 apply 确认之前崩溃，账本显示
`completion.claimed`（可诊断的部分状态 — 永远不会是虚假的 `applied`）。
重新运行 `mark-done-files --receipt <id>` 会幂等地收敛：预留（在匹配的
expected-after 上幂等）、本地写入（done/closed 后幂等无操作，带匹配的
`completion_receipt` 元数据）、apply 确认（新的或幂等的）。如果
`mark-done-record` 从未运行，回执保持 `applied` 并在 `event list` 中可见；
重新运行 record 来消费它。

### 仅修复路径（drift 对账）

当必须对账一个 `task.done` 在历史上在回执协议之外写入的任务（例如由不同
actor 写入）时，拆分命令仍可作为**显式仅修复**路径使用：

```bash
# 文件侧：--repair-reason 是必需的。
coordinate assignment mark-done-files \
  --workspace-path <path> --harness-root <root> \
  --task-id <task_id> --repair-reason "drift: historical task.done by omp"

# 记录侧：--repair-reason 是必需的。
coord-ssh assignment mark-done-record discord-nexus \
  --task-id <task_id> --repair-reason "drift: historical task.done by omp"
```

产生的事件会标记 `repair_only=true` 和原因。没有 `--repair-reason`
（且没有 `--receipt`）时，两个命令都会 fail closed。普通的拆分旁路不再是
静默默认。

### 传统单主机 `assignment mark-done`

`assignment mark-done`（在一个进程中运行 `harnessctl mark-done` 并写入
`task.done`）保留给真正的单主机设置。它不是 host-aware 回执协议的一部分，
其结果携带 `host_aware_warning` 引导 operator 使用上述回执流程。不要在
`mark-done-files` 和 `mark-done-record` 之间运行它。

### /opt 防护

`mark-done-files` 拒绝变更 `/opt/` 下的任何路径，除非传递
`--allow-runtime-copy`。`/opt` 树是部署产物，不是开发权威来源。始终在
coding host 的 git checkout 上运行 `mark-done-files`，然后部署已提交的结果。
