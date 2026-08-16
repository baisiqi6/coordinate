# Workspace 与 Harness Path

## 四条不同路径

- `workspace add --harness-root` / host-profile `--harness-root`：注册层可保存任意 absolute path；注册本身
  不做 containment。
- `workspace init-harness --mode minimal --root`：默认 minimal；absolute root 技术上可用，
  `init_file_harness` 直接创建，不做 containment 拒绝。
- `workspace init-harness --mode full`：忽略 CLI `--root`，使用注册的 `workspace.harness_root`，并要求其
  位于 workspace 内。
- split-operation `plan_doc`：强制 POSIX workspace-relative；这是 lexical guard，不提供
  symlink/TOCTOU 抵抗。

不要把“本项目 policy”误写成“Coordinate capability”。当前 Coordinate/MultiNexus policy 是 active plan
保持 workspace-local；`${MYHARNESS_ROOT}` 只保存 task-scoped 过程材料和历史归档。若未来要把 active
artifact 外置，先完成 `artifact_root` 的 schema、ExecutionContext、digest 与 consumer contract，不用
symlink 或放宽 guard 绕过。

## 常用入口

```bash
$MAC workspace add WORKSPACE --path /absolute/repo \
  --harness-root /absolute/repo/docs/project-harness \
  --base-branch main --branch-namespace agents
$MAC workspace list
$MAC workspace audit WORKSPACE
$MAC workspace init-harness WORKSPACE --task-id TASK \
  --plan-doc docs/project-harness/tasks/TASK/plan.md --title 'Task title'
$MAC state WORKSPACE --no-refresh
$MAC reconcile WORKSPACE --no-refresh
```

具体 flags 使用当前子命令 `--help`。普通 operator mutation 保留 `/opt` fail-closed guard；runtime copy repair
属于 production/recovery 子 skill。
