# Coordinator 心智模型

Coordinate 是顶层 harness 系统中的确定性运行时控制面，不是顶层本身、固定的
Coordinator 或唯一权威。当前 Operator 负责判断；Coordinate 通过事件记录 runtime
事实，repo/harness 文件保存稳定项目协议。顶层可以把拥有自身 subagent、agent team
或 workflow 的 agent 当作复合 Executor，不展开或重新实现其内部编排。

## 拓扑

- **Local development**：repo worktree + 显式本地 DB；只代表本地测试状态。
- **Production control plane**：运行 Coordinate 服务并持有生产 DB 的主机；典型部署使用 `/var/lib/coordinate/coord.sqlite3`，通过严格 SSH wrapper `coord-ssh` 调用远端 CLI（如 `/usr/local/bin/coord-local`）。具体路径由部署配置决定，不得在 server 上直接编辑 DB。
- **Coding host**：保存 canonical repo/harness 文件、运行 provider/agentd、执行
  `*-files` 文件半边和 Git/GitHub 副作用。
- **Deployed runtime copy**：`/opt/coordinate` 与 `/opt/multinexus`，只由受审部署/
  恢复流程更新；普通命令必须保留 `/opt` fail-closed guard。

## 持久化状态

- SQLite 存储运行时状态：workspaces、events、jobs、leases、deliveries、runner profiles、
  task mirrors、executor/capacity projection。
- Harness/repo 文件存储稳定的项目协议状态。
- GitHub 存储代码审查、分支、commits、PRs 和 CI checks。
- Discord/KOOK 是可见消息总线，不是持久化状态存储。

## 主要对象

Workspace：
- 已注册的 harness 项目。
- 具有 `path`、`harness_root`、可选 `harnessctl_path`、默认 bus/destination、`base_branch` 和 `branch_namespace`。

Task mirror：
- Coordinator 对 harness 任务的本地视图。
- 跟踪任务 id、phase/status 类元数据、owner、branch、PR、payload、最新事件。

Event：
- 规范运行时事实。
- Events 为可见消息策略和审计/恢复提供输入。

Delivery：
- 可见消息的 outbox 行。
- Delivery worker 将 pending/failed 行发送到 stdout、Discord 或 KOOK。

Runner profile：
- 受信任的本地命令配置。
- `generic_subprocess` 是本地/legacy runner 路径。
- 当前 managed path 是 bridge → Coordinate runtime request/job → per-agent `agentd`。

Job：
- Pending/running/done/failed/cancelled/recoverable 的 runner 调用。
- 产生日志、可选结构化结果 JSON 和 job 生命周期事件。

ExecutionContext / executor / capacity：
- `ExecutionContext` 把 job 绑定到具体执行身份、容量策略、worktree resource。
- Executor catalog 通过 `runtime executor sync` 从 agent-registry 权威同步。
- Capacity catalog 通过 `runtime capacity sync` 同步，决定路由与并发边界。

## 工作流形态

```text
命令 -> 服务 -> event/job/delivery -> worker -> 外部副作用
runtime request -> pending job -> claim -> agentd -> report -> event -> policy -> delivery
```

在 coordinator 已有服务的地方避免直接副作用。使用 CLI 或服务层，使状态保持可恢复。

## 授权边界

- 默认需要用户明确授权。
- 已存在持久授权时，不机械重复询问，但仍执行 preflight、review、receipt、recovery 和 fail-closed gate。
- `merge gate` 是本地 DB gate 加当前 PR head 检查，始终返回 `human_gate_required=true`。
- `ready=true` 只意味着技术前置条件满足；不是自动合并的许可。

## /opt fail-closed guard

普通 operator 命令不得修改 `/opt/coordinate` 或 `/opt/multinexus`。
`--allow-runtime-copy` 只允许出现在明确受审的 deploy/repair/恢复流程中；操作原因
和证据由该流程记录，不能把这个 flag 当成普通绕过开关。
