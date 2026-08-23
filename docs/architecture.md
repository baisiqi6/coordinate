# Coordinate 架构

> **状态：当前实现架构。** 历史阶段规划和 task 过程材料不进入稳定文档导航，
> 也不重新定义产品或仓库边界。

## 在系统中的位置

```text
人类或 agent Operator
        │ 决策并调用工具
        ▼
Coordinate 协调内核
        ├── HarnessAdapter ──> 规范项目 harness
        ├── Runtime/jobs ────> MultiNexus agentd 或其他 runner
        ├── Policy/outbox ───> Discord / KOOK / webhook / stdout
        └── Forge adapters ──> Git / GitHub 证据
```

Coordinate 刻意将确定性状态机制与可替换判断分离。`operator.py` 辅助函数可以从
记录的状态推断待办行动，但不会把服务变成自主 Operator。

`tasks.phase` 是 harness 工作流投影。运行时完成和计划决策保留为事件；
`operator pending` 从这些事件派生关注点，并返回显式的快照/过期元数据，
而不是存储 `awaiting_operator` phase 覆盖层。

## 组件映射

| 领域 | 主要模块 | 职责 |
|---|---|---|
| 入口 | `cli.py`, `daemon.py`, `__main__.py` | CLI/API/bot 命令接入和服务生命周期 |
| Agent 接口 | `agent_interface.py`, `mcp_server.py`, `mcp_cli.py` | bounded agent-facing facade 与 MCP stdio adapter（R1） |
| Runtime data plane | `runtime_interface.py`, `runtime_http.py`, `runtime_http_cli.py` | 共享 runtime facade 与 loopback HTTP adapter（R2A） |
| 持久化存储 | `schema.py`, `db.py`, `events.py` | SQLite schema、幂等 events、jobs、deliveries、agents、mirrors；`workspace list` / `host-profile list` 走严格只读连接（`mode=ro` + `query_only` + 精确 schema gate，绝不创建/migrate DB） |
| 项目生命周期 | `assignments.py`, `transitions.py`, `handoff.py`, `plan_gate.py` | 经验证的生命周期转换和任务级交接产物 |
| Harness 边界 | `harness.py`, `reconcile.py`, `audit.py`, `doctor.py` | 调用 harness mutations、刷新投影、报告 drift |
| 依赖更新入口 | `task_dependencies.py`, `planning_cli.py` | checklist `dependencies` 字段的受控 mutation：combined（preflight + file-first + targeted reconcile）与 coding-host file-only 两入口 |
| 执行 | `runtime.py`, `jobs.py`, `worker.py`, `agent_registry.py` | 注册/认领/运行/重试/恢复 agent 工作并接收结构化结果 |
| 可见性 | `policy.py`, `bus.py`, `discord_rendering.py` | 将持久化事件转换为可重试的可见 delivery |
| Forge 证据 | `branches.py`, `prs.py`, `ci.py`, `reviews.py`, `github.py` | 跟踪 branch、PR、CI、review、publish 和 merge-gate 证据 |
| Operator 支持 | `operator.py`, `onboarding.py`, `issues.py` | 待办视图、workspace 初始化、issue 物化 |

## 主要流程

### 托管执行

```text
Operator 提交意图
  → Coordinate 验证 workspace、task、target 和幂等键
  → 创建持久化 event + job
  → runner 或 MultiNexus agentd 认领 job
  → progress/heartbeat 延续可观察的活跃状态
  → 结构化报告关闭本次尝试
  → policy 创建可见 delivery，Operator 评估下一个 gate
```

Job 的成功结束是证据，不是项目完成。Review、forge 状态、验收和 closeout
仍是独立的 gate。

### Harness 生命周期 mutation

```text
Operator 命令
  → Coordinate 服务验证转换
  → HarnessAdapter 调用 harnessctl
  → 规范 harness 文件变更
  → Coordinate 追加持久化 event
  → reconcile 刷新 task mirror
```

Coordinate 不直接手动编辑 harness JSON。如果 harness mutation 成功但后续记录步骤
失败，audit/reconcile 会报告 drift，而不是静默创造第二个真相。

### Managed dependency 更新

checklist item 的 `dependencies` 字段是依赖的唯一权威；`tasks.payload_json` 只是
可查询投影。更新入口（`src/coordinate/task_dependencies.py`）只修改既有 item 的
`dependencies`，不新增依赖表、事件类型或通用 `update-item`：

```text
coordinate task update-dependencies WORKSPACE --task-id TASK --add DEP --remove DEP
  → harnessctl_available() preflight（缺失时零 mutation fail closed）
  → checklist 原子 mutation（复用 mutate_checklist）
  → refresh_state() + targeted reconcile(task_id=TASK)

coordinate task update-dependencies-files --workspace-path P --harness-root H ...
  → 只写 canonical checklist；不开 DB、无 harnessctl preflight
```

- desired-state 幂等：add-existing / remove-absent 是 no-op（`already_satisfied`），
  file-half 已成功后的同命令重跑可收敛；
- 每个请求在结果中标为 `applied` 或 `already_satisfied`，同时给出本次持锁
  mutation snapshot 的 `dependencies`（targeted reconcile 读取当时最新 canonical
  checklist；合法并发更新由 mirror 跟随最新权威，不错误覆盖）；
- file 成功而 refresh/reconcile 失败时返回结构化 recovery，明确 checklist 已提交，
  并给出同命令重跑或 `coordinate reconcile WORKSPACE --task-id TASK` 两个恢复命令；
- combined 禁止 full reconcile；/opt runtime-copy guard 保持 fail closed
  （`--allow-runtime-copy` 只用于显式 repair）。

#### Legacy item 显式首 adoption（task.adopt）

已存在于 canonical checklist、但没有 `split_operation` envelope 的 legacy item，
通过**显式**入口 `task adopt` 首次纳入 managed lifecycle，与 `task create`
（before-state 是 absent，创建新 item）和 `reconcile`（只做 completion repair /
mirror 刷新）严格区分：

```text
coordinate task adopt WORKSPACE --task-id TASK --plan-doc PLAN
  → 只读 prepare：推导 expected item fingerprint + plan bytes digest（stale gate 输入）
  → file half：锁内校验 expected fingerprint/digest 后只给既有 unbound item
    写入 task.adopt envelope（业务字段/identity/checklist authority 不变；DB 是 mirror）
  → record half：从已部署 workspace/harness/plan readback 复核 fingerprint 后，
    单一 SAVEPOINT 内原子建立 split_operations ledger + task mirror + plan.ready
```

- lifecycle projection 与 reconcile 共用 `workflow.status → status` 规则；legacy item
  不要求顶层 `phase`，prepare/file/record 使用同一投影，避免 file half 后才发现
  确定性不兼容；
- stale-input gate：prepare 之后 item projection（title/status/dependencies
  等）或 plan bytes 发生漂移，在文件写入前 fail closed（`fingerprint_drift`）；
- split-host 路径：`task adopt --prepare-only` 取得 expected fingerprints →
  coding host `task adopt-files` → commit/push/deploy → control plane
  `task adopt-record`（exact argv 以各子命令 `--help` 为准）；
- record 失败时返回**同一 operation id/fingerprint** 的 `task adopt-record`
  结构化 recovery；不得用 `reconcile --task-id` 代替首 adoption 或 record recovery；
- 幂等：exact same operation replay 不改 checklist bytes/mtime、不重复
  ledger/mirror/event；已绑定其他 operation、malformed envelope、双 checklist
  authority、缺 checklist/plan、非法相对路径均 fail closed；
- 旧 reconcile 已建立的 mirror 仅在 file-owned payload 与当前 legacy item 完全一致
  时可被显式 adoption 原子升级；Coordinate-owned owner/branch/PR/publish evidence
  被保留，任何 lifecycle 或 operation identity 冲突仍在 DB mutation 前拒绝；
- 无 schema migration；projection doctor 已识别 `task.adopt`（合法 file-pending
  是 recognized warning，record-applied 零 unsupported，drift 仍精确报 finding）。

### 可见 delivery

```text
持久化 event → policy 渲染器 → delivery 行 → bus adapter → 平台消息 id
```

Events 和 deliveries 分离，这样平台故障不会抹除行动记录。
平台消息记录是持久化状态的人类可见投影。

当 MultiNexus bridge 等调用方已经发送可见回复并指定 `reply.platform=none` 时，Coordinate
只保留 job result 与 `job.completed` event，不再创建没有发送消费者的 delivery。
旧版本创建的 `platform=none,status=pending` 行可能仍存在于 ledger；它们只是历史审计记录，
批量 pump 默认跳过，显式发送则 fail-closed。不要把这些旧行视为当前 transport backlog。

## 权威和投影规则

- Harness 意图和工作流字段通过 `HarnessAdapter` 读取。
- `tasks` 是可查询的镜像；其 phase 从 harness 对账，而 forge 和 event 指针
  保留为 Coordinate 拥有的投影。
- `events` 是 Coordinate 记录行动的持久化运行时/审计账本。
- `jobs` 和 `deliveries` 是 Coordinate 拥有的运行时状态。
- GitHub 结果是最后已知证据，直到从 GitHub 刷新。
- 面向 Operator 的摘要和 Discord/KOOK 消息是派生视图。

Reconciliation 可以刷新镜像并发出 drift 事件。它不得静默重写已接受的项目意图
或 forge 真相。

## Agent-facing MCP 接口（stdio / private Remote MCP）

`coordinate mcp serve` 通过标准 MCP 暴露 12 个 bounded tools：原有 read/runtime tools、6 个
workspace lifecycle tools，以及 `coordinate.channel_create`。stdio 与 Remote HTTP 共用同一份
schema/handler；Remote 再施加 request-scoped principal/tool/workspace/platform policy。MCP 是 adapter
而不是第二个 authority：

- 每次 tool 调用在 SDK worker thread 内创建并关闭自己的 SQLite connection，
  直接调用现有 domain/query 函数；不新增 DB 表、第二套 job/session state、缓存或 daemon。
  mutation tool 只在既有 DB authority 内写入有界 event/job 等事实。
- 所有成功/失败都返回统一 envelope，同时写入 `structured_content` 与等价
  JSON `TextContent`；失败设置 `is_error=true`。错误映射集中在
  `agent_interface.py`（invalid_request / not_found / conflict / unavailable /
  internal），未知异常只在 `stderr` 留诊断。
- submit 的 actor 由启动配置固定，调用者不能伪造；`idempotency_key` 必填，
  exact replay 不重复 event/job，冲突 payload fail closed；routed 输入只收
  raw builder fields，由 `build_routing_request()` 在 server 侧规范化。
- audit 固定 `refresh=False`，只读当前 harness 文件；drift/stale/unavailable
  是审计数据，不是 transport failure。
- MCP SDK 是 optional extra（`coordinate[mcp]`，`mcp>=2,<3`）；未安装时
  `import coordinate` 与其它 CLI 不受影响，只有启动 MCP 返回安装提示。
- `stdout` 只写 protocol；日志与诊断只写 `stderr`。R1 不暴露
  claim/report/lease/shell/SQL/文件工具，也不做 remote transport/auth。
- 协议 era：R1 同时接受 legacy（`initialize` 握手，≤2025-11-25）与现代
  2026-07-28 stateless era。现代路径每次请求在 `params._meta` 携带
  `io.modelcontextprotocol/protocolVersion` / `clientInfo` /
  `clientCapabilities`，无需 `initialize`；服务器支持 `server/discover`，
  在 modern envelope 上声明其它版本返回 JSON-RPC error `-32022`
  （`data.supported` 列出可协商版本）；响应含 `resultType=complete` 与
  `serverInfo` metadata。协议层无 session：没有 `Mcp-Session-Id`、SSE
  resumability 或 `Last-Event-ID`，连接状态不是恢复依据。
- 幂等与恢复是应用层职责：重复提交、断线重试只依赖 `idempotency_key` 与
  持久 job/event/delivery authority；MCP 不引入第二套 session/task 状态。
- `coordinate.channel_create` 只追加或重放 `channel.provision.requested`，不持有 Discord token/client。
  独立 Coordinator daemon 从 append-only request/terminal events 派生 pending 集合，复用既有
  provisioning core、workspace lock、topic marker 与 channel binding 完成外部 mutation；daemon 离线或
  重启不依赖 MCP session 或 delivery pump 的内存 cursor。调用者以同一 key 重放取得
  `pending` / `provisioned` / `failed` 投影。
- Remote MCP 生产 adapter 使用独立 loopback systemd unit
  `deploy/systemd/coordinate-mcp-http.service`；digest-only policy、exact allowed Host 与
  `ConditionPathExists` 只约束 transport，不改变同一 DB/domain authority。

## Runtime HTTP data plane（R2A：loopback）

`coordinate runtime-http serve` 是 daemon/bridge/agentd 的 southbound runtime
data plane，与 MCP 共用同一个 bounded facade：

- 模块：`runtime_interface.py`（共享 7 use case facade）、`runtime_http.py`
  （auth policy + aiohttp server）、`runtime_http_cli.py`（启动面）。
  `AgentInterface.runtime_request_submit` / `runtime_job_get` 委托同一
  `RuntimeInterface`；R1 job-id-only 与 HTTP workspace-bound 两种 job-get 形状
  共享同一个 core，`MESSAGE_JOB_NOT_FOUND` 文本不变。
- 端点集恰好是真实 consumer 的 7 个 use case：channel resolve、request
  submit、精确 job get、normal claim、progress、report、managed lease renew。
  没有 consumer 的 explicit reap/recoverable/operator endpoint 不实现；recovery
  继续走 CLI/SSH，且本节点 server 不暴露 recoverable/reap 端点。
- 认证：server-local digest-only JSON policy（`--auth-file`），principal
  （client id、role、platform/workspace scope、agent identity）只来自 policy；
  body 不能伪造 actor/agent/scope。missing/unknown/bad token 返回同一个静态
  401；cross-role/scope 越界返回静态 403。bridge 的静态 `workspace_ids` 保留 bootstrap/non-channel
  scope；对允许 platform 上的 managed channel，active channel binding 是动态 workspace authority。
  所有 channel-bearing request 均须以精确 origin/reply binding 校验，即使 workspace 也在静态 scope；
  静态 scope 外的 job get 还须从 stored origin 重验当前 binding。不能把静态 scope 变成 channel 校验
  bypass，也不能把动态能力实现成 workspace 通配符或第二份同步清单。
- 每次 domain call 在 worker thread 内创建/关闭短连接（`asyncio.to_thread` +
  bounded semaphore，默认 16）；不共享跨 request connection/transaction，不
  伪装可取消 thread——进入 transaction 后执行到既有边界。
- 只接受 loopback 绑定（`127.0.0.1`/`::1`/`localhost`），参数校验阶段拒绝其它
  host；无 debug mode、OpenAPI/docs、CORS、websocket、SSE。body 上限 1 MiB，
  `Content-Type` 必须是 JSON。
- aiohttp 是 optional extra（`coordinate[runtime-http]`）；未安装时其它 CLI 与
  `--help` 不受影响，只有启动返回安装提示。日志只记录 request id、client id、
  role、method、route template、status、latency；token/prompt/result/DB path/
  raw identifier 不落日志。
- 独立 systemd unit（`deploy/systemd/coordinate-runtime-http.service`），带
  `ConditionPathExists`，缺 auth policy 时 fail safe；坏 policy 有界失败，不
  无限 crash loop。

## Channel binding 权威

Coordinate 是 `(platform, channel_id) -> workspace_id` 的唯一持久权威。MultiNexus
在把任何 managed Discord/KOOK 入站消息放入 context、prompt、handoff 或 job submit
之前必须先 resolve；channel 未绑定、lookup 失败或显式 workspace 与 binding 不一致时
fail closed，没有 silent fallback。

- `channel_bindings` 只保存 active row；`PRIMARY KEY (platform, channel_id)` 本身就是
  “一个 channel 最多一个 active workspace”的数据库约束。actor/reason/history 写入 events，
  不新增 status、task_id、history 表或 cache。
- canonical key：`platform` 归一化为 `discord`/`kook`；`channel_id` 是 opaque 平台 id，
  只强制外边界（非空、无首尾空白、无控制字符、≤128 code points），不臆造平台 regex。
  非法 key 在 bind/resolve/release/list 全部 fail loud，不伪装成 unbound。
- bind/release 是 event-first mutation：`channel.binding.bound` /
  `channel.binding.released` event 与 active row 变更在同一 SAVEPOINT 提交或回滚。
  event 的 `workspace_id` 是目标 workspace，`target` 是 canonical `<platform>:<channel_id>`。
- 幂等：bind/release 要求非空 `actor`/`reason`/`idempotency_key`；exact replay 只回
  receipt、不重复 mutation；cross-operation/cross-payload 复用 fail closed；同 workspace
  再 bind 是 `already_bound` no-op，已 unbound 再 release 是 `already_unbound` no-op；
  replay 历史 release 绝不删除后来 rebind 的 active row。改绑必须先以 expected workspace
  显式 release。
- 读取走 `workspace channel resolve`/`list`；变更走 `workspace channel bind`/`release`，
  只经 `workspace_cli` 注册。
- Discord 自助 onboarding 不建立第二份 registry。Coordinator daemon 只在固定 control channel
  接受 `channel create <workspace> <channel-name>`；actor 必须在
  `COORDINATOR_ALLOWED_USER_IDS` 中，若 actor 是 Agent Bot，还必须属于目标 workspace。daemon
  只在 control channel 的现有 Category 内创建 text channel，以 workspace 派生的 deterministic
  topic marker 恢复“Discord 已创建、DB 尚未 bind”的中断窗口，再调用同一
  `bind_channel_workspace()` 并校准 workspace 默认投递目的地。已有多个 binding 或多个 marker
  时 fail closed；同一 daemon 内按 workspace 的非持久 `asyncio.Lock` 只负责串行化 Discord
  副作用，不成为新权威。release/rebind/delete 仍是显式 operator mutation。
- 已获 exact Remote MCP grant 的主 Operator 可改用 `coordinate.channel_create` 发起同一个 provisioning
  core。Remote middleware 固定要求目标 workspace 与 `platform=discord` scope；MCP 进程不读取 Bot token，
  Discord command 与 MCP request 也不形成两份 channel registry。
- daemon 接收 Agent report 时先解析消息实际 parent channel 的 binding，再同时核对 report
  workspace 与 Agent membership；delivery transport 则严格使用 delivery row 的 destination，
  不再把所有消息固定发送到 control channel。

## 文档与过程材料生命周期

Coordinate 的文档体系也遵循单一权威原则。早期设计只区分代码与 harness，没有明确规定
phase、bootstrap、review round 和已取代设计如何退出当前导航，导致过程证据长期堆积在产品仓库。
当前补充以下生命周期边界：

```text
产品仓库
  ├── 当前稳定规范：scope / architecture / domain-model / runbook
  └── 真实产品代码和仍有消费者的兼容接口

私有 task artifact repository（可选、非 runtime）
  ├── task-scoped plan / bootstrap / review / receipt
  └── 已结束 phase、被取代设计和历史正文

Coordinate DB
  └── events / jobs / leases / receipts / deliveries 等运行时事实
```

- 活跃的 Coordinate-managed plan 保持在对应 workspace；artifact repository 不接管 runtime authority。
- task/phase 收口后，过程正文迁入 `$MYHARNESS_ROOT/projects/<project_id>/`；归档不进入当前状态导航。
- 迁移前先把 README、架构、测试和 skill 更新到当前权威入口。只有代码或运行协议确实依赖路径时
  才保留最小兼容文件；不能仅为旧文档互相引用而永久保留 locator 链。
- 历史 `open`、`pending` 或 verdict 不会因归档而继续成为当前事实；需要重新复现或显式激活。
- 产品 Git history 是恢复兜底，不是日常历史浏览界面；不为归档重写历史。

这个分层是 harness 架构的一部分，而不是临时仓库卫生规则：它防止过程记忆反过来成为第二套
source of truth，同时保留跨 session 恢复和审计所需的证据。

## 部署模型

- `<coordinate-checkout>` 是本地源码 checkout。
- `<deployed-coordinate-root>` 是当前环境配置的已部署服务副本。
- 生产运行时真相通过当前环境配置的 Coordinate 数据库和 CLI 读取。
- 本地历史 harness 文件不能替代生产运行时状态。

部署拓扑是运维配置，不是产品不变量。Coordinate 必须能通过其他主机布局或
调用面保持可用。

## 当前参考

- 仓库范围：`docs/scope.md`
- Coordinate 实体：`docs/domain-model.md`
- 运维：`docs/runbook.md`
- AI operator 入门：`skills/coordinate-operator/SKILL.md`
