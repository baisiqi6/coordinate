# Managed Task Lifecycle

## 选择入口

| 场景 | 入口 |
|---|---|
| ordinary 小任务 | task spec → worker → independent review → tests |
| managed same-host 重要任务 | `task create` combined contract |
| managed split-host 重要任务 | `task create-files` → commit/deploy → `task create-record` |
| legacy checklist item 首次纳入（same-host） | `task adopt` combined contract |
| legacy checklist item 首次纳入（split-host） | `task adopt --prepare-only` → `task adopt-files` → commit/deploy → `task adopt-record` |
| dependency same-host | `task update-dependencies` |
| dependency coding-host file-only | `task update-dependencies-files` |
| Standalone 重要任务 | `harnessctl add-item` |

旧/新 checklist resolver：只有 `harness-checklist.json` 或只有 `mvp-checklist.json` 时使用唯一存在者；两者
皆无或并存时 fail closed。filename migration 需要独立 authority，acknowledgement flag 不授予 mutation。

## workflow.mode boundary（EXharness #9 兼容）

Coordinate validator 与 EXharness `validate_checklist.py` byte-identical，接受 `workflow.mode`
（`ordinary`/`high-risk`）schema：mode-only todo、mode+status 合法；非法 mode/null、`workflow={}`、
doing+mode-only、mode+其他 lifecycle 字段而无 status 均 fail closed。managed `task create`
（create/create-files/create-record）不暴露 `--mode`：新建 item 保持 `workflow.status` 无 `mode`，
按 EXharness 语义为 effective high-risk。ordinary 小任务继续走 task spec → worker → independent
review → tests；未来若出现真实 managed ordinary consumer，另开 create+upgrade authority slice，
本 boundary 不构成半套 mode mutation 协议。

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

## Legacy item 显式首 adoption（task adopt）

已存在于 canonical checklist、但没有 `split_operation` envelope 的 legacy item 用
`task adopt` 首次纳入 managed lifecycle（operation kind `task.adopt`，不伪装成
`task.create`）。与 create/reconcile 的区分：`task create` 拒绝 legacy unbound item；
`reconcile --task-id` 保持 completion-repair-only，不能代替首 adoption 或 record
recovery。

```bash
$MAC task adopt WORKSPACE --task-id TASK --plan-doc PLAN            # same-host combined
$MAC task adopt WORKSPACE --task-id TASK --plan-doc PLAN --prepare-only   # 只读 prepare
$MAC task adopt-files ...   # coding host：只写 envelope（需 prepare 的 expected fingerprints）
$MAC task adopt-record ...  # control plane：commit/deploy 后写 ledger + mirror + plan.ready
```

流程语义（exact argv 以 `--help` 为准）：

- same-host combined：只读 prepare（expected item fingerprint + plan bytes digest，
  构成显式 stale gate）→ file half 在锁内校验后只给既有 unbound item 追加
  envelope（业务字段/identity/checklist authority 不变，不建第二个 item）→
  record half 从已部署 readback 复核后，单一 SAVEPOINT 原子建立 ledger、task
  mirror 与 `plan.ready`。DB 侧始终只是 mirror。
- lifecycle projection 与 reconcile 共用 `workflow.status → status` 规则；legacy
  item 不要求顶层 `phase`。旧 reconcile 创建的 matching mirror 可由显式 adoption
  原子升级并保留 Coordinate-owned owner/branch/PR/publish evidence；任何 file-owned
  payload 或 operation identity 冲突继续 fail closed。
- record 失败：用返回的**同一 operation id/fingerprint** 运行 `task adopt-record`
  recovery（幂等收敛）；不得改用 reconcile。
- fail closed：item 不存在、已绑定其他 operation、malformed envelope、双
  checklist authority、缺 checklist/plan、非法相对路径、prepare 后 item/status/
  dependency/plan bytes 漂移均零写入拒绝；exact replay 不改 bytes/mtime、不重复
  ledger/mirror/event。
- 无 DB schema migration；projection doctor 已识别 `task.adopt`（file-pending 是
  recognized warning、record-applied 零 unsupported、drift 精确报 finding）。

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
