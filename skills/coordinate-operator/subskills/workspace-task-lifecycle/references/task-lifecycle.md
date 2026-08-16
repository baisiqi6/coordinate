# Managed Task Lifecycle

## 选择入口

| 场景 | 入口 |
|---|---|
| ordinary 小任务 | task spec → worker → independent review → tests |
| managed same-host 重要任务 | `task create` combined contract |
| managed split-host 重要任务 | `task create-files` → commit/deploy → `task create-record` |
| dependency same-host | `task update-dependencies` |
| dependency coding-host file-only | `task update-dependencies-files` |
| Standalone 重要任务 | `harnessctl add-item` |

旧/新 checklist resolver：只有 `harness-checklist.json` 或只有 `mvp-checklist.json` 时使用唯一存在者；两者
皆无或并存时 fail closed。filename migration 需要独立 authority，acknowledgement flag 不授予 mutation。

## Create、revision 与 dependency

```bash
$MAC task create WORKSPACE --task-id TASK \
  --plan-doc docs/project-harness/tasks/TASK/plan.md --title 'Task title'
$MAC plan revise WORKSPACE --task-id TASK \
  --plan-doc docs/project-harness/tasks/TASK/plan.md
$MAC task update-dependencies WORKSPACE --task-id TASK --add DEP_A --remove DEP_B
```

`task create` file-first、record-second；DB half 失败时用同一 `operation_id` 重跑。`plan revise` 不新建
DB-only task、不更换 plan identity、不自动批准。dependency 是 checklist 唯一权威；add-existing 与
remove-absent 是 desired-state no-op。

## Assignment 与 completion

```text
request -> accept -> [handoff|blocker -> unblock] -> closeout
-> review-result -> completion/mark-done
```

Remote MCP 日常 completion 使用 `coordinate.completion_prepare` / `preflight` / `claim` / `apply` /
`consume`。CLI/SSH split-host compatibility 使用 `mark-done-prepare` → coding-host `mark-done-files` →
commit/deploy → `mark-done-record`。未知项目没有 exact-commit deployment/readback 时不得完成 record/consume。

## Reconcile 与 recovery

full reconcile 检查 workspace；`reconcile --task-id TASK` 只用于 completion receipt 已 consumed、目标 task
只剩 mirror drift、且 checklist/state 已显式刷新时。它不能掩盖目标自身 branch/PR/publish conflict，也不
修改 out-of-scope 历史 drift。部分失败按输出中的同一 operation/receipt 重放，不重新生成 authority。
