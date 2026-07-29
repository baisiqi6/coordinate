# Runner 操作

当前 runner 实现支持 `generic_subprocess` 与 managed path。

## Runner 契约

Runner profile 是受信任的本地命令配置。不要从原始 Discord/KOOK 文本构建 runner 命令。

`generic_subprocess` 命令可以使用占位符：

- `{job_id}`
- `{workspace_id}`
- `{workspace_path}`
- `{task_id}`
- `{prompt_path}`
- `{branch}`
- `{worktree_path}`
- `{logs_path}`
- `{result_path}`

Runner 进程还接收带 `COORDINATOR_` 前缀的环境变量，例如：

- `COORDINATOR_JOB_ID`
- `COORDINATOR_WORKSPACE_ID`
- `COORDINATOR_WORKSPACE_PATH`
- `COORDINATOR_TASK_ID`
- `COORDINATOR_BRANCH`
- `COORDINATOR_LOGS_PATH`
- `COORDINATOR_RESULT_PATH`

## Managed path

当前 managed path 是 bridge → Coordinate runtime request/job → per-agent `agentd`。

- bridge 通过 `runtime request submit` 提交请求，创建 pending job。
- agent 通过 `runtime job claim` 领取 job，可选 `--recoverable`、
  `--recovery-reason` 与 `--prior-process-stopped`。
- agentd 执行任务，通过 `runtime job report` 报告结果，并携带
  `attempt_token` / `lease_id` 约束。
- operator 使用 `runtime executor sync/list/show` 与 `runtime capacity sync/list/show`
  维护执行身份与容量 catalog。

## 结构化结果 JSON

如果 runner 向 `COORDINATOR_RESULT_PATH` 写入 JSON，`jobs.py` 会将其合并到 job 结果中。

有用字段：

```json
{
  "agent_status": "done",
  "summary": "short result",
  "artifact_paths": [],
  "branch": "agents/codex/task",
  "commit": "optional-sha",
  "pr": "optional-pr-url",
  "logs_path": "optional-log-path"
}
```

`agent_status` 值 `blocked`、`failed` 或 `declined` 会强制 job 变为 failed。

## Recoverable job 恢复

恢复 recoverable job 前必须：

1. 关联 provider-native JSONL/进程证据，确认前序进程/会话已停止。
2. 使用显式 `--recovery-reason`。
3. 提供 `--prior-process-stopped` 确认。
4. 遵守当前 lease/attempt token 约束，避免与仍在运行的 agent 冲突。

## 当前限制

有 `job pump` 但没有长期运行的 `worker jobs` 循环。如果需要无人值守的 job 消费，
请记录到当前项目的 issue tracker 或 private task artifact repository。
