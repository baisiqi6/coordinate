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

## Reviewer context 与 runtime identity

Coordinate 记录 Reviewer 的 agent/session/job、receipt 与 verdict，但不另行定义审查方法；采用
EXharness 时，以其 `reviewer-strategy.md` 为语义权威。

- Reviewer 必须独立于 Worker 及其 mutation authority；连续验证上一轮局部修复时，可以复用
  Reviewer 自己的 session。
- 有明显锚定风险、争议判断、架构转向或高风险最终 closeout 时，使用 `fresh`；一般最终 closeout
  优先 `limited-fresh`，只传底层目标、canonical plan、当前代码/diff、non-goals 与必要证据，
  不传旧 verdict。
- 无论复用还是刷新上下文，都必须记录并核验真实 agent/session/job locator。context mode 的选择
  不会授予 merge、deploy、delete 或其他新增 authority。
