---
name: coordinate-operator-worker-supervision
description: Use when supervising a delegated coding worker, interpreting provider-native JSONL or session logs, deciding thinking versus idle versus dead, choosing direct CLI versus managed Coordinate execution, or delegating a bounded subtask to a local Operator. Do not load merely to inspect Coordinate jobs.
---

# Worker Supervision 与 Delegation

观察证据、两次观察规则、JSONL 脱敏与完成边界见 `references/worker-observation.md`。

## 选择 direct 或 managed

- 一次性、同宿主机、边界清晰、不要求 durable runtime evidence：可通过 `invoke-coding-agents` 使用 provider
  CLI。
- 需要跨 session/host 恢复、durable job、lease、receipt、provider session 或消息 delivery：使用
  Coordinate + MultiNexus registered executor。

direct CLI 结果不能冒充 managed job/receipt；选择 managed lifecycle 后不能用 direct CLI 绕过 assignment、
review 或 closeout gate。

## 局部 Operator

dogfood 小修、ordinary 小任务或主线之外独立分支可以委派局部 Operator。它可以直接完成很小任务，或
管理自己的 worker/reviewer 并先做任务线验收；主 Operator 保留最终核验。委派不产生新 authority，仍用
独立 Issue/item、session、branch/worktree 和原 Coordinate lifecycle。

可见 `[PLAN]`/`[HANDOFF]`/`[DONE]` 消息只证明广播，不证明目标 Agent 已接受。真实 managed handoff 使用
registered agent identity 与 durable task/job evidence。
