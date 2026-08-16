---
name: coordinate-operator-runtime-execution
description: Use when operating Coordinate runtime requests and jobs, agentd claim/report, leases and recovery, executor or capacity catalogs, liveness, or legacy runner profiles. Do not load for checklist-only, GitHub-only, messaging-only, or deployment-only tasks.
---

# Runtime Execution 与 Fleet

先选择实际执行路径：

- managed：bridge/Operator → runtime request/job → per-agent `agentd` → report；优先使用。
- legacy/local：`generic_subprocess` runner；只在明确需要本地受信任命令时使用。

legacy runner 的占位符、结果 JSON 与 recoverable contract 见 `references/runner-operations.md`。

## Managed runtime

```bash
$MAC runtime executor sync --source /path/to/agent-registry.toml
$MAC runtime executor list
$MAC runtime capacity sync --source /path/to/agent-registry.toml
$MAC runtime capacity list
$MAC runtime request submit WORKSPACE --target-agent AGENT_ID --prompt 'task prompt'
$MAC runtime job claim --agent-id AGENT_ID
$MAC runtime job report JOB_ID --agent-id AGENT_ID --status done --result-json '{}'
$MAC runtime job lease renew JOB_ID --agent-id AGENT_ID \
  --attempt-token ATTEMPT --lease-id LEASE
```

`claim` 返回的 `attempt_token`/`lease_id` 约束后续 progress/report/renew。恢复 recoverable job 前，先用
worker-supervision 子 skill 证明前序 provider process/session 已停止，再提供明确 recovery reason 与
`--prior-process-stopped`。不要把 stale `online_state` 当成可达；同时核验 liveness、last_seen 与真实 canary。

## 完成判据

managed job 至少关联 request/job/attempt、target agent、execution context/binding、provider result 和独立
验证。进程在线、claim polling 或 Discord 回复不能单独证明任务正确完成。
