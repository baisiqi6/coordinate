# Coordinate 运维手册

## 快速参考

```bash
# Harness 命令
scripts/harness/harnessctl state
scripts/harness/harnessctl validate
scripts/harness/harnessctl doctor
scripts/harness/harnessctl session-init
```

## Task/Job 只读 Trace（Issue #11 R1）

- `coordinate trace task WORKSPACE_ID TASK_ID [--history-limit N]`（默认 20、最大 100）
- `coordinate trace job JOB_ID [--workspace-id WORKSPACE_ID]`
- stdout 只有 `{"trace": <TraceProjectionV1>}`；字段带六种 evidence state
  （present/missing/unknown/unavailable/stale/failed）。纯只读投影：不刷新 forge
  或 task mirror，不联网。契约以 `coordinate trace --help` 与
  `src/coordinate/trace_projection.py` 为准。

## 本地 fresh install

标准安装步骤：

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

安装后 console script 位于：

- macOS / Linux：`.venv/bin/coordinate`
- Windows：`.venv\Scripts\coordinate.exe`

该绝对路径可直接作为 MultiNexus 的 `coordinator_cli_path` 使用；对应的
`coordinator_db_path` 必须是同一宿主机可访问的绝对 SQLite 路径，不可被多台宿主机共享。

---

## 新 Workspace 初始化顺序

1. 在 Coordinate 中注册 workspace：`workspace add <id> --path ... --harness-root ...`
2. 运行 `workspace init-harness <id> --mode full --source <reference-workspace-scripts/harness>` 创建完整 harness 运行时
3. 运行 `workspace doctor <id>` 验证 full_harness_runtime 状态
4. 在 `<harness-root>/tasks/<task-id>/plan.md` 下创建计划
5. 使用 Coordinate `task create` 注册第一个任务
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

### 通过 Coordinator Bot 创建并绑定 Discord Channel

仅当 Coordinator Bot 在 control channel 所属 Category 具有 `Manage Channels`，且发起者 ID 已加入
`COORDINATOR_ALLOWED_USER_IDS` 时使用：

```text
@Coordinator channel create <workspace_id> <channel_name>
```

- 命令只在固定 control channel 生效；不能指定其它 guild、Category、permission overwrite 或 channel type。
- human actor 只需在 allowlist；Agent Bot actor 还必须已注册到目标 workspace。
- workspace 已有唯一 Discord binding 时不会再建频道，只会幂等返回并修复默认投递路由；多个 binding
  或多个 recovery marker 时 fail closed。
- 创建中断后重复相同命令会复用 deterministic topic marker 对应的频道。不要手工复制 marker。
- 命令不自动 rename/delete/rebind。改绑继续使用上面的显式 `release` → `bind` 流程。
- 把 Operator Bot ID 加入 `COORDINATOR_ALLOWED_USER_IDS` 是生产配置 mutation；撤权时移除该 ID 并重启
  daemon，不要修改数据库来模拟撤权。

已有 Remote MCP Operator principal 时，优先调用 typed tool `coordinate.channel_create`：

```json
{"input":{"workspace_id":"WORKSPACE","channel_name":"CHANNEL_NAME","idempotency_key":"STABLE_KEY"}}
```

首次通常返回 `pending`；保持参数和 key 完全一致重放，直到得到 `provisioned`（含 `channel_id`）或
`failed`（仅静态 `reason_code`）。同 key 异参、同 workspace 并发新 key、缺少 exact tool/workspace/
`discord` platform grant 均 fail closed。该工具不接收 guild/category/token/permission payload，Discord
mutation 仍仅由 daemon 完成；失败后先检查原因，再使用新 key 显式重试。

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
  只同步 resolver-selected checklist（`harness_root/harness-checklist.json` 或 legacy
  `harness_root/mvp-checklist.json`，恰好一个存在）并支持 `workspace.path` 之外的
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

## Registry 查询是严格只读（strict read-only）

`workspace list` 与 `workspace host-profile list` 是纯查询命令，以严格只读方式打开
**existing-only** 的数据库连接（SQLite URI `mode=ro` + `PRAGMA query_only=ON`，并在任何
查询前做精确 schema 兼容性门）：

- 绝不创建或 migrate 数据库：缺失 DB、旧 schema（`user_version < 16`）、未知 schema
  （`> 16`）一律 fail closed——stderr 输出 `error: ...` 且 exit 1，数据库文件与
  `-journal`/`-wal`/`-shm` sidecar 零变化。
- 行为变更：此前的「查询时静默创建空 DB 并返回空列表」不再成立；需要建库时使用
  显式 writable 命令（如 `workspace add`）。
- 只读路径复用同一套 registry domain 函数（`list_workspaces` /
  `list_workspace_host_profiles`），不建立第二套 registry/domain store；writable
  composition 与 mutation 命令行为不变。
- 权威边界：`reconcile`（含 `--task-id`）是 completion 后 scoped mirror recovery，不是
  legacy first-adoption 入口；无 split-operation envelope 的 legacy item 在显式
  adoption entry 出现前保持 untouched。

## Managed Dependency 更新

checklist item 的 `dependencies` 字段是依赖的唯一权威。更新已登记 task 的依赖时
使用 Coordinate 受控入口，不裸改 JSON、也不裸跑 `harnessctl update-item`：

```bash
# same-host combined：preflight harnessctl → checklist 原子 mutation →
# refresh state → 只同步目标 task mirror（禁止 full reconcile）
coordinate task update-dependencies WORKSPACE \
  --task-id TASK --add DEP_A [--add DEP_B] [--remove DEP_C]

# coding-host file-only：只写 canonical checklist，不开 DB、不做 harnessctl
# preflight；调用者显式承担 commit/deploy/refresh/reconcile 边界。
# --workspace-path 与 --harness-root 必须是 absolute path（相对路径在任何
# mutation 前 fail closed，绝不相对进程 cwd 解析）
coordinate task update-dependencies-files \
  --workspace-path /path/to/repo --harness-root /path/to/repo/docs \
  --workspace-id WORKSPACE --task-id TASK [--add DEP] [--remove DEP]
```

规则：

- 至少一个 `--add`/`--remove`；同一 ID 同时 add/remove、target 不存在、**新增的**
  dependency 不存在、self-dependency 一律 fail closed（零 mutation）。`remove-absent`
  按 desired-state 语义是 `already_satisfied` no-op，不 fail。
- desired-state 幂等：add-existing / remove-absent 是 no-op（结果标
  `already_satisfied`）；结果同时给出每个请求的 `applied`/`already_satisfied` 与
  本次持锁 mutation snapshot 的 `dependencies`（不承诺解锁后无人再改；targeted
  reconcile 读取当时最新 canonical checklist，合法并发更新由 mirror 跟随最新权威）。
- combined 在 mutation 前要求 workspace 的 `harnessctl` 可用；缺失时零 mutation
  fail closed（minimal workspace 用 `update-dependencies-files`，不要在 combined
  下把正常缺失伪装成 recovery）。
- file 半边已提交而 refresh/reconcile 失败时，输出结构化 recovery：checklist 仍
  是权威，用同一命令重跑（幂等收敛）或
  `coordinate reconcile WORKSPACE --task-id TASK` 补齐 DB mirror。
- `/opt` runtime-copy guard 保持 fail closed；`--allow-runtime-copy` 仅用于显式
  repair。split-host 后半程复用既有 `reconcile WORKSPACE --task-id TASK`，不新增
  `update-dependencies-record`。

## Legacy Item 显式首 Adoption（task adopt）

canonical checklist 中已存在、但没有 `split_operation` envelope 的 legacy item，
用 `task adopt` 首次纳入 managed lifecycle。它与 `task create`（创建新 item，
before-state 是 absent）和 `reconcile`（仅 completion repair / mirror 刷新）互不
替代：create 会拒绝 legacy unbound item（`legacy_unbound_item`），reconcile 永远
不会补 envelope 或 ledger。

```bash
# same-host combined：只读 prepare + stale gate → checklist envelope →
# 已部署 readback 复核 → 单一事务内 ledger + task mirror + plan.ready
coordinate task adopt WORKSPACE --task-id TASK --plan-doc PLAN

# 只读 prepare：输出 expected item fingerprint / plan sha256 / operation id，
# 不改任何文件与 DB（split-host 第一步，或人工核对用）
coordinate task adopt WORKSPACE --task-id TASK --plan-doc PLAN --prepare-only

# split-host：coding host 只写 checklist envelope（要求传入 prepare 输出的
# expected fingerprints，stale gate 在写入前校验）
coordinate task adopt-files --workspace-path P --harness-root H \
  --workspace-id WORKSPACE --operation-id OP --task-id TASK --plan-doc PLAN \
  --expected-item-fingerprint FP --expected-plan-sha256 SHA

# split-host 后半程：commit/push/deploy 之后在 control plane 运行
coordinate task adopt-record WORKSPACE --operation-id OP \
  --input-fingerprint IN --before-fingerprint BE --after-fingerprint AF \
  --task-id TASK --plan-doc PLAN
```

exact argv 以各子命令 `--help` 为准。规则：

- file half 只给既有 unbound item 追加 `task.adopt` envelope：`id`、title、
  status/workflow、priority、dependencies、plan locator 等业务字段与
  checklist authority 一律不变，不创建第二个 item；DB 侧只是 mirror。
- lifecycle mirror 与 reconcile 一样使用 `workflow.status`，缺失时回退到 `status`；
  合法 legacy item 不要求顶层 `phase`。
- 旧 reconcile 已创建的 DB mirror 只有在 file-owned payload 与当前 legacy item
  完全一致时才可升级；保留既有 owner/branch/PR/publish evidence。任何不一致或
  operation identity 冲突继续 fail closed，不做宽松覆盖。
- record 失败时按返回的 **同一 operation id / fingerprint** 运行 `task
  adopt-record` recovery（幂等收敛）；**不得**用 `reconcile --task-id` 代替
  首 adoption 或 record recovery。
- fail closed 语义：item 不存在（`item_not_found`）、已绑定其他 operation、
  malformed envelope、双 checklist authority、缺 checklist/plan、非法相对路径、
  prepare 之后 item/dependency/plan bytes 漂移（`fingerprint_drift`）均零写入
  拒绝；exact same operation replay 不改 checklist bytes/mtime、不重复
  ledger/mirror/event。
- 无 DB schema migration；projection doctor 已识别 `task.adopt`。

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
在项目支持的 Python 版本上均如此；它不信任主机 event 字段，也不会在失败后留下半状态。

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

## MCP agent 接口（R1 stdio / R5 Remote lifecycle）

MCP SDK 是 optional extra，安装 `coordinate[mcp]`（`mcp>=2,<3`）后启动：

```bash
# --db 是全局参数，必须位于子命令之前；actor 固定为启动配置，调用者不能伪造
coordinate --db <absolute-path> mcp serve --transport stdio --actor mcp
```

当前共 12 个工具。原 5 个工具为：`coordinate.operator_pending`、`coordinate.workspace_audit`
（固定 `refresh=False`，只读 harness 文件）、`coordinate.runtime_request_submit`
（exact `target_agent` 或 typed `routing_request` 二选一，`idempotency_key` 必填，
replay 幂等、冲突 fail closed）、`coordinate.runtime_job_get`（只读，不存在返回
`not_found`）、`coordinate.runtime_agent_list`（无过滤只读）。所有调用返回统一
envelope（`ok`/`data`/`error`），成功与失败同时写入 `structured_content` 与等价
JSON `TextContent`。

R5 仅为真实 Windows/多主机 Operator 闭环增加 6 个 workspace-scoped typed tools：

- `coordinate.task_create_record`：split-host `task create-files` 部署后的 DB record 半边；server 从
  deployed checklist/plan bytes 重算并核验 envelope/fingerprint，不信任 caller 文件内容。
- `coordinate.completion_prepare`、`coordinate.completion_preflight`、
  `coordinate.completion_claim`、`coordinate.completion_apply`、
  `coordinate.completion_consume`：复用既有 completion receipt 状态机；actor 固定为 request principal，
  consume 再次把 caller workspace 与 receipt workspace 绑定。

六工具要求 exact `{"input": object}` wire shape，inner model `extra="forbid"`；Remote middleware 先做
principal/tool/workspace scope，再做 shape/domain 校验。旧 principal policy 默认仍只见原 5 工具；新增
grant 必须逐 principal 显式配置。没有新增 assignment transition、Git、deploy、shell、recovery 或通用
payload/path tool。

第 12 个 `coordinate.channel_create` 是独立的 typed provisioning request：要求
`workspace_id`、`channel_name`、显式 `idempotency_key`，Remote path 还要求固定 `discord` platform grant。
它返回 durable 三态投影，不把 Bot token/client 或 Discord API 搬入 MCP service。

`coordinate.runtime_agent_list` 同时返回声明状态与派生活跃状态：`online_state`
表示 operator 是否允许该 agent 运行；`liveness_state` 根据 `last_seen_at` 推导为
`online`、`stale`、`unknown` 或 `offline`，并附带 `last_seen_age_seconds` 与
`liveness_stale_after_seconds`。声明为 online 但超过 90 秒未见活动的 agent 不再
参与 executor routing。agentd 的正常 claim 轮询最多每 30 秒刷新一次活动时间，
长任务由 lease renewal 刷新；这些自动刷新不写 `agent.heartbeat` event，避免把
运行账本膨胀为轮询日志。显式 `runtime agent heartbeat` 仍保留给兼容和诊断路径。

- MCP 是 adapter：状态、校验、幂等与 transaction 边界全部来自现有 domain/CLI 核心与同一 DB。
- 未安装 extra 时其它 CLI 不受影响；启动 MCP 只返回安装提示，不抛 import traceback。
- `stdout` 只写 protocol；日志/诊断只写 `stderr`。
- R1 不暴露 claim/report/lease/shell/SQL/文件工具，不做 remote transport/auth。

生产 Remote MCP 使用独立的
`deploy/systemd/coordinate-mcp-http.service`，而不是把 stdio server 暴露到公网：

- unit 只监听 `127.0.0.1:8766`，并要求同时存在 server-local digest-only client
  policy `/etc/coordinate/mcp-http-clients.json` 与 exact allowed Host 配置
  `/etc/coordinate/mcp-http.env`；缺任一文件时 `ConditionPathExists` 使其 fail safe；
- bearer token 明文不得进入 unit、仓库或 argv；client policy 只保存 digest；
- 外部访问由环境自己的受控 tunnel/reverse proxy 提供，本仓库的 unit 不开放公网 listener；
- 安装或启用该 unit 属于 deployment/recovery authority。普通源码安装只提供模板，不自动修改
  systemd 或创建 `/etc/coordinate/*`。

现代协议（2026-07-28）：MCP host 先 `server/discover` 协商，此后每次请求在
`params._meta` 自带 protocolVersion/clientInfo/capabilities，无需 legacy
`initialize`；响应带 `resultType=complete`。协议层无 session：没有
`Mcp-Session-Id`、SSE resumability 或 `Last-Event-ID`。host 断线重连后重试必须
复用同一 `idempotency_key`，结果以 DB 中持久 job/event 为准；在 2026-07-28
envelope 上声明其它版本会得到 JSON-RPC `-32022`（`data.supported` 列出可协商
版本，`2025-11-25` 等旧版本只能走 legacy `initialize` 握手）。

## Runtime HTTP data plane（R2A：loopback）

aiohttp 是 optional extra，安装 `coordinate[runtime-http]`（`aiohttp>=3.9`）后启动：

```bash
# 只接受 127.0.0.1 / ::1 / localhost；auth policy 是 server-local digest-only JSON
coordinate --db <absolute-path> runtime-http serve \
  --host 127.0.0.1 --port 8765 \
  --auth-file /etc/coordinate/runtime-http-clients.json
```

- policy 文件 strict schema（`version: 1` + `clients[]`：bridge 必须带
  `platforms`/`workspace_ids`，agentd 必须带唯一 `agent_id`）；unknown/
  duplicate/坏 digest/group-world writable 一律启动失败。missing/unknown/bad
  token 返回同一个静态 `401`；cross-role/scope 越界返回静态 `403`。
- bridge 的 `workspace_ids` 是静态/bootstrap scope，不是动态 Discord channel 的第二份项目清单。
  对已获准 `platform` 上的 channel，Coordinate 的 active binding 可以派生动态 workspace scope：
  resolve 返回 canonical binding；所有带 channel 的 submit（包括静态 scope 内 workspace）都必须让
  `origin`（以及非 `none` 的 `reply`）精确绑定到同一 workspace；静态 scope 外的 job get 必须从
  stored origin 重验当前 binding。unbound、mismatch、
  cross-platform、binding lookup failure 一律静态 `403`，不能仅凭 caller 提供的 workspace/job id 越权。
- 端点：`GET /healthz`、`GET /readyz`（public loopback）与 7 个受认证业务端点
  （channel resolve、request submit、workspace-bound job get、normal claim、
  progress、report、lease renew）。explicit reap/recoverable/operator endpoint
  不实现；recovery 只走 CLI/SSH。
- credential rotation 的 zero-inflight proof 使用现有可执行证据：
  `job list --status running` 为空、8765 端口无 established connection、只读 DB
  active lease count 为零；顺序为“确认零 in-flight → policy 原子替换 → client
  secret 切换 → service restart → readiness/auth smoke”。不新增 global
  lease-list CLI。
- **本节点 server 不暴露 recoverable/reap endpoint，也不声称 lease 会被动自愈**。
  恢复必须描述为 operator 先运行现有 lease reap，再在确认 prior process
  stopped 后显式 `--recoverable` + audited reason；禁止 blind re-claim。
- 未安装 extra 时其它 CLI 不受影响；启动 runtime-http 只返回安装提示。
- 生产以独立 systemd unit（`deploy/systemd/coordinate-runtime-http.service`）
  运行，`ConditionPathExists` 缺 policy 时 fail safe；坏 policy 有界失败。

## Runtime Bridge / Agentd 冒烟验证

本地 fresh install 后使用 `.venv/bin/coordinate`（Windows：`.venv\Scripts\coordinate.exe`）
作为 CLI。以下示例按源码模式 `PYTHONPATH=src python3 -m coordinate` 给出，开发 worktree
中两者等价。

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
  --agent-id mac-codex \
  --claim-request-id <uuid-per-logical-claim>
```

`--claim-request-id` 是兼容旧 client 的可选参数，但新 agentd 必须发送。相同 key 且
参数摘要相同会回放同一个成功 claim；摘要冲突、lease 过期、job 已终态或 marker 损坏返回
`409 conflict`，不得新建 attempt。只有成功 claim 写入 marker；`queue_empty` 不写 marker，
下一次轮询使用新 key。Runtime HTTP 的 `POST /v1/jobs/claim` 使用同名 JSON field。只读
reconcile 可用于诊断，不能单独授权新 claim。

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

Host-aware mark-done 将 coding host 对 resolver-selected canonical checklist
（`harness-checklist.json` 或 legacy `mvp-checklist.json`，恰好一个存在）的 mutation 与
服务器端 `task.done` 事件绑定在**一个服务器签发的、一次性完成回执**之下。
两个半程不能再独立推进：回执是唯一授权，它存在于控制面事件账本中：

    completion.authorized → completion.claimed → completion.applied → task.done + completion.consumed

权威和信任路径：

- 回执仅在控制面上签发和查询。Coding host 提供 `receipt_id`；服务器从账本
  重新派生 `workspace_id`、`task_id`、`authorized_actor`、过期时间和指纹。
  客户端提供的 workspace/task/过期声明从不被信任。
- Coding host 通过二选一的远端 transport **在线**验证、预留和确认回执：legacy
  `--event-cli-path`（通常是 `coord-ssh` wrapper），或窄 Remote MCP pair
  `--event-mcp-url` + `--event-mcp-token-env`。二者互斥；MCP pair 必须同时存在并要求
  `--workspace-id`，非 loopback 明文 HTTP 在读取 token 前拒绝。没有任一完整路径时，正常的
  `mark-done-files` 命令 fail closed。
- 该协议刻意采用两阶段：回执在规范写入*之前*移动到 `claimed`，在写入落地
  *之后*移动到 `applied`。如果主机在中间死亡，账本显示 `claimed`
  （可诊断的部分状态），永远不会是虚假的 `applied`。记录侧要求 `applied`；
  它不会消费仅 `claimed` 的回执。

### 标准完成流程

以下命令块是 legacy CLI/SSH compatibility 流程，仍可用于没有 Remote MCP principal 的环境：

```bash
# 1. 控制面：验证 closeout/review/forge gate 并签发回执。
coord-ssh assignment mark-done-prepare coordinate \
  --task-id <task_id> --actor operator
# => result.receipt_id（记录它）

# 2. Coding host：验证 + 预留回执，变更规范 checklist，然后确认 —
#    全部通过远端 coord CLI。mark-done-files 自动运行
#    preflight -> mark-done-claim（预留）-> 本地写入 + 结构化
#    completion_receipt 元数据 -> mark-done-apply（确认）。
coordinate assignment mark-done-files \
  --workspace-path /path/to/<workspace> \
  --harness-root docs \
  --workspace-id coordinate \
  --task-id <task_id> \
  --receipt <receipt_id> \
  --event-cli-path "$HOME/.local/bin/coord-ssh" \
  --verification "completion authorized by receipt <receipt_id>"

# Remote MCP 形态（Agent 日常路径；token 值仅存在环境变量，不进入 argv）：
coordinate assignment mark-done-files \
  --workspace-path /path/to/<workspace> \
  --harness-root docs \
  --workspace-id coordinate \
  --task-id <task_id> \
  --receipt <receipt_id> \
  --event-mcp-url "https://coordinate.example/mcp" \
  --event-mcp-token-env COORDINATE_REMOTE_MCP_TOKEN \
  --verification "completion authorized by receipt <receipt_id>"

# 3. Commit + push 更新后的 checklist，然后按当前部署配置执行受审部署。
#    （git add 作用于 resolver-selected checklist：新名或 legacy 名，恰好一个存在）
git add "$(python3 scripts/harness/harness_common.py --resolved-checklist)"
git commit -m "harness: mark-done for <task_id>"
git push
# 部署命令由当前环境配置提供；public 文档不硬编码 private topology。

# 4. 控制面：重新验证已部署的 harness，并在追加 task.done 的同时
#    原子地消费回执。
coord-ssh assignment mark-done-record coordinate \
  --receipt <receipt_id> --actor operator

# 5. 验证状态。
coord-ssh state coordinate
coord-ssh event list coordinate
```

已配置 R5 Remote MCP principal 的 Agent 日常路径不需要 `coord-ssh`：

0. local review 批准后先 commit/push 并受审部署，使 server deployed bytes 能读到 `review_approved`。
1. 调 `coordinate.completion_prepare`（`workspace_id` + `task_id`；actor 由 principal 固定）取得 receipt。
2. coding host 运行上面的 `mark-done-files` MCP 形态，执行 preflight → claim → local atomic write → apply。
3. 再次 commit/push checklist 并受审部署，使 server deployed bytes 能读到 `done/closed`。
4. 调 `coordinate.completion_consume`（`workspace_id` + `receipt_id`）重新核验 deployed fingerprint，原子写
   `task.done + completion.consumed`。
5. 用 `coordinate.workspace_audit` / `coordinate.runtime_job_get` 监督；该轻量路径不伪装成完整
   `operator_pending` assignment-action parity。

同理，split-host task 创建为：coding host `task create-files` → commit/push/deploy → Remote MCP
`coordinate.task_create_record`。file half 或 deployed readback 缺失时 DB 保持零 mutation；不得把 checklist
bytes/full envelope 作为 MCP 参数传入。

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
coord-ssh assignment mark-done-record coordinate \
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
