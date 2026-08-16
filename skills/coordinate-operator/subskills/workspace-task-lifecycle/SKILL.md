---
name: coordinate-operator-workspace-task-lifecycle
description: Use when a Coordinate Operator must register or audit a workspace, reason about harness paths, create or revise an important task, update checklist dependencies, reconcile file and DB state, run assignment transitions, or complete a task through receipts. Do not load for runtime jobs, GitHub-only work, messaging-only work, or deployment.
---

# Workspace 与 Task Lifecycle

本子 skill 处理 repo/harness 稳定状态与 Coordinate task mirror 的边界。先选择模块：

| 当前任务 | 读取 |
|---|---|
| workspace 注册、`harness_root`、minimal/full、myharness 边界 | `references/workspace-and-paths.md` |
| task create/revise/dependency、assignment、completion、reconcile | `references/task-lifecycle.md` |

只查参数时运行 `skills/coordinate-operator/scripts/mac.sh <group> --help`，不要加载旧的全量命令手册。

## 共享 authority

- canonical checklist/plan 在 coding-host repo；Coordinate DB task 是 runtime/query mirror，不是第二份可编辑
  checklist。
- managed same-host 使用 combined contract；managed split-host 使用 file half → commit/deploy → record half。
- `harness-state.json` 与 `docs/current/*` 是可重建 cache/pointer；与 checklist 不一致时以 checklist 为准。
- 不直接编辑 JSON，不用 caller bytes/self-reported digest 绕过 deployed readback。
- ordinary 小任务不强制 checklist；重要或跨 session 任务必须登记。

## 完成判据

文件状态、DB mirror、review/receipt、deployed bytes 和 final event 必须按所选 lifecycle 一致。一个半边成功
不是完整成功；按结构化 recovery 使用同一 operation/receipt 收敛，不创建第二个 task 或 operation。
