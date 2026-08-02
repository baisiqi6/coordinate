# 故障排除

从以下开始：

```bash
skills/coordinate-operator/scripts/inspect.sh --db "$DB" --workspace WORKSPACE
```

## Job 处于 Pending 状态

- 还没有 job worker daemon。
- 运行 `job pump --workspace-id WORKSPACE --limit N`。
- 检查 runner profile 存在且类型为 `generic_subprocess`（managed path 使用
  `runtime job claim`）。

## Job 失败

检查：

```bash
$MAC job list --workspace-id WORKSPACE --status failed
$MAC runtime job claim --agent-id AGENT_ID --recoverable --recovery-reason '...' --prior-process-stopped
```

然后从 job 结果中读取 job 的 `logs_path` 和 `result_path`。

常见原因：

- runner 命令以非零退出
- 结果 JSON 无效
- 结果路径指向目录
- 超时
- runner profile 命令假设存在缺失的本地工具
- recoverable job 被重复 claim 但缺少 `--prior-process-stopped` 或 lease 冲突

## Event 存在但没有可见消息

运行：

```bash
$MAC policy pump-events --workspace-id WORKSPACE --platform stdout --destination local
$MAC delivery list --status pending --platform stdout
```

如果事件类型不受支持，policy pump 会跳过它。
查询真实 backlog 时始终指定目标 transport platform。若看到
`platform=none,status=pending`，它是旧版本留下的 durable audit record，不是待发送消息；
当前 runtime 在可见回复已由其他组件发送时不再创建该 delivery。

## Delivery 失败

运行：

```bash
$MAC delivery list --status failed --platform stdout
$MAC worker delivery --platform stdout --once
```

对于 Discord/KOOK，检查 token 和 destination。不要把 token 粘贴到仓库文件中。

## Merge Gate 为 False

检查：

```bash
$MAC pr link WORKSPACE --task-id TASK --pr-url PR_URL
$MAC ci check WORKSPACE --task-id TASK
$MAC review check WORKSPACE --task-id TASK
$MAC merge gate WORKSPACE --task-id TASK
```

gate 基于最新本地事件，并验证事件 PR/head 与 task mirror PR/当前 PR head 匹配。GitHub 变更后重新接入 CI/review。

`ready=true` 只证明技术前置条件满足；仍需明确授权才能 merge。

## Harness Mutation 失败

查找 `harness.mutation_failed` 事件和可见的 `[BLOCKER]` deliveries。不要手动编辑 harness JSON。修复 harnessctl 路径/状态并重新运行幂等命令。

## Host-aware 文件/记录不同步

- `*-files` 命令只改本地 harness 文件；`*-record` 命令只写控制面 DB。
- 正常路径需要 receipt 与远端 coord CLI（如 `coord-ssh`）验证/claim。
- 如果文件已改但 DB 未记录，先确认 receipt 状态，再决定是 repair-record 还是
  回滚文件变更。

## /opt 相关错误

普通 operator 命令不能修改 `/opt/coordinate` 或 `/opt/multinexus`。
`--allow-runtime-copy` 只允许出现在明确受审的 deploy/repair/恢复流程中，并由
该流程记录操作原因和证据。
