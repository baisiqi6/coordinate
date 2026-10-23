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

## 什么时候需要 Coordinate / MultiNexus

| 需求 | 最小组合 |
|---|---|
| 当前 agent 直接开发，只需要持久计划、SDD/TDD、审查和测试纪律 | EXharness |
| 需要 durable job、任务状态、事件、审查记录和可恢复流程 | EXharness + Coordinate |
| 需要系统自动调用 agent CLI、恢复 provider session 或跨宿主机执行 | EXharness + Coordinate + MultiNexus `agentd/adapters` |
| 需要 Discord/KOOK、多 Bot 和多 agent 可见协作 | 三者完整部署 |

Coordinate 可以独立作为控制面使用：当前 agent、Operator 或已有 runner 主动领取和报告 job 即可。
但 Coordinate 不会因为安装完成就自动获得 Claude Code、Qoder、Grok 等 vendor runtime 的原生
session 控制；需要这种托管执行时，使用 MultiNexus `agentd/adapters` 或实现等价的自定义 executor。
Discord/KOOK 只是可选 transport，不是使用 Coordinate 或 MultiNexus executor 层的前提。

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
- 面向 agent 的 MCP stdio / private Remote MCP 接口（共用 15 个 tool schema；包括窄的
  `coordinate.channel_create` provisioning request）。
- 面向 daemon/bridge/agentd 的 loopback runtime HTTP data plane（R2A：
  `coordinate runtime-http serve`，受认证的 7 个 use case，仅监听 loopback）。

精确 CLI 以当前程序帮助为准，不在 README 复制完整命令手册：

```bash
.venv/bin/coordinate --help
.venv/bin/coordinate runtime --help
.venv/bin/coordinate assignment --help
```

或使用源码模式（开发时）：

```bash
PYTHONPATH=src python3 -m coordinate --help
PYTHONPATH=src python3 -m coordinate runtime --help
PYTHONPATH=src python3 -m coordinate assignment --help
```

## 本地安装（fresh install）

运行要求：Python 3.11+。CLI 使用标准库 `tomllib`；旧版元数据的 Python 3.10 声明未覆盖这一实际依赖。

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

需要 MCP agent 接口（stdio 或 private Remote MCP）时安装 optional extra：

```bash
pip install 'coordinate[mcp]'   # mcp>=2,<3
```

未安装该 extra 时，`import coordinate`、`coordinate --help` 与其余 CLI 不受影响；
只有启动 MCP 会返回安装提示。安装后启动：

```bash
coordinate --db <path> mcp serve   # MCP stdio，DB 与 actor 由启动配置决定
```

需要 loopback runtime HTTP data plane（R2A，`coordinate runtime-http serve`）时
安装 optional extra：

```bash
pip install 'coordinate[runtime-http]'   # aiohttp>=3.9
```

R2A 只接受 `127.0.0.1` / `::1` / `localhost` 绑定；启动前必须提供 server-local
auth policy（`--auth-file`，digest-only JSON，不得 group/world writable）：

```bash
coordinate --db <path> runtime-http serve \
  --host 127.0.0.1 --port 8765 \
  --auth-file /etc/coordinate/runtime-http-clients.json
```

未安装该 extra 时其它 CLI 不受影响，只有启动 runtime-http 返回安装提示。
HTTP 只是 adapter：状态、校验、幂等与 transaction 边界全部来自现有
domain 核心与同一 DB；不创建第二份 job/lease/client 状态。

安装完成后，console script `coordinate` 会被写入虚拟环境：

- macOS / Linux：`.venv/bin/coordinate`
- Windows：`.venv\Scripts\coordinate.exe`

**把该绝对可执行路径原样填到 MultiNexus 的 `coordinator_cli_path`**，例如：

```toml
agentd_mode = true
coordinator_cli_path = "/Users/<you>/projects/coordinate/.venv/bin/coordinate"
coordinator_db_path = "/Users/<you>/projects/coordinate/data/coordinator.sqlite3"
```

- `coordinator_db_path` 必须是**绝对路径**，并且只能由当前这台宿主机上的进程访问；
- 不要把本地 SQLite 文件挂载给多台宿主机共享，也不要使用相对路径依赖运行时当前目录；
- 生产部署通常使用 `coord-ssh` / `coord-local` 这类 wrapper 做远程或受控调用，本地 console script 路径仅用于单台 coding host 的直接运行。

## 项目频道工具

现有 Remote MCP 增加 `coordinate.channel_read`、`coordinate.channel_send` 和
`coordinate.channel_wait`。三个工具都使用 `{"input": {...}}`，要求 `workspace_id`、
`channel_id`，且仅支持已绑定到该 workspace 的 Discord 普通服务器文字频道。
服务端通过认证 policy 的 `channel_agent_id` 固定发送 Agent；客户端不能指定发送身份、凭据、Agent 或桌面会话。
新操作不会回退到 `DISCORD_BOT_TOKEN`。

认证 policy 必须显式授予对应 tool、workspace 与 `discord` platform。
stdio 虽公布相同 schema，但缺少 Remote MCP principal 时拒绝这三个操作。
read/wait 直接读取平台，不改已有任务、session、上下文缓存或共享已读位置。

- `read`：默认最近 20 条；`after_cursor` 可读后续页，`limit` 为 1–100。
  游标绑定 workspace/channel，调用者各自保存；不代表完整历史存档。
- `send`：必填 `text` 和 `send_key`，可选同频道 `reply_to_message_id`。
  正文不超过 1800 UTF-16 code units。相同 key 必须使用完全相同参数。
  `delivery_uncertain` 表示可能已经发送；沿用原 key 查询，不能自动换 key 重发。
- `wait`：必填 `after_cursor`，默认等待 20 秒，最多 30 秒。
  正常到期返回 `status=timeout`；网络或权限错误不会冒充空消息。

工具消息使用固定 content 标题与 embed 正文，禁用 mentions。
`channel_io.v1` 投递由一次发送路径管理，通用 pump/recover 不会重发。
升级时应先让所有发送/恢复进程采用此保护，再授予新工具权限。
部署前用一条已知的非本 bot 消息验证正文可读；不能用空列表证明历史权限正常。
这些工具不唤醒桌面会话；Codex/DSH 接入另作测试会话验收。

### 服务端频道身份配置

在现有 auth policy client 条目增加可选字段 `"channel_agent_id": "example-agent"`。
它必须与 canonical Agent registry ID 相同；桌面 executor ID 不等同于此 ID。
省略或设为 null 不影响已有非频道工具，但不允许新的频道 read/send/wait。

服务进程设置 `COORDINATE_CHANNEL_IDENTITIES_FILE=/absolute/service/channel-identities.json`：

```json
{
  "version": 1,
  "registry_path": "/absolute/deployment/agent-registry.toml",
  "credentials": {
    "example-agent": {"token_file": "/absolute/service/secrets/example-agent"}
  }
}
```

这里只保存凭据文件引用。Bot ID 从已有 `parse_agents_toml` 读取的 canonical registry
获得，要求有效的 `[registry]` id/version/hash、无解析错误、唯一 Discord ID 且 Agent enabled。
旧 registry 条目省略 `enabled` 时默认 true。配置拒绝未知/重复 JSON key、相对路径及内联 token。
配置与 token 文件须为服务用户或 root 拥有的普通私有文件（例如 mode 0600）。
服务启动时验证并冻结配置和 registry；变更须受控重启。缺少配置仅禁用新的频道操作，
配置损坏则启动失败。Token 在每次新操作开始读取一次，read/send/wait 同一操作始终使用该 token。

每个新操作先以 `GET /users/@me` 校验实际 Bot ID 和 bot 标记，再查频道权限。
发送还必须核对 POST 响应 author 后才记录 sent。`sender_unbound` 表示 policy 未绑定；
`sender_unavailable` 表示配置、凭据或身份查询不可用；`sender_identity_mismatch` 表示实际身份不符。
这些错误均不发送。POST 后无法确认作者或保存结果时返回 `delivery_uncertain`，只能用原 key 查询。

新 receipt 增加 `sender_identity`、`actual_author_bot_id`、`identity_status`、`binding_current`。
身份 snapshot 包含 version、client_id、agent_id、discord_user_id，以及 registry_source_id/version/hash；
不含凭据引用。仅确认 sent 才标记 verified；未知/拒绝结果为 unconfirmed。
重放先检查已有授权和 ledger，不读 token 或访问 Discord。当前 client/Agent/Bot tuple 变化或解绑时，
返回 `idempotency_conflict` 和原 receipt，`binding_current=false`；token 轮换或无关 roster 更新不改变 key。
历史无 snapshot 行保持原样，并显示 `legacy_unattributed`，不追认其发送身份。
通用 outbox 仍不能重试频道行。

固定入口和 mode 0600 不提供同一 Unix UID 下的恶意进程隔离。实际部署、服务重启、凭据采用
及真实频道作者 ID 验收仍是独立操作，不由本地测试代替。

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
