# GitHub 集成

GitHub 仍然是 branch、PR、CI、review 和 merge 的权威来源。Coordinator 记录并暴露本地接入的状态。

## Branch 分配

```bash
$MAC branch allocate WORKSPACE --task-id TASK --owner OWNER
```

这在 coordinator 状态中记录稳定的 branch 名称。它不创建 git branch。

## PR 链接

```bash
$MAC pr link WORKSPACE --task-id TASK --pr-url PR_URL
$MAC pr link WORKSPACE --task-id TASK --branch BRANCH
```

显式 PR URL 不需要 `gh`。按 branch 发现使用 `gh pr list`。

## CI 检查

```bash
$MAC ci check WORKSPACE --task-id TASK
```

使用 `gh pr checks` 并将当前 PR head SHA 与 CI 事件一起记录。空 check 列表被视为 `ci.pending`，而不是通过。真实的 GitHub CLI 将没有配置 checks 的 PR 报告为退出码 1、空 stdout 和 stderr 上的 `no checks reported`；该确切响应也被规范化为空 check 列表。其他非 JSON 失败仍是错误。

事件：

- `ci.passed`：可见为 `[CI]`。
- `ci.failed`：可见为 `[BLOCKER]`。
- `ci.pending`：仅内部，不在 `SUPPORTED_EVENT_TYPES` 中。

## Review 检查

```bash
$MAC review check WORKSPACE --task-id TASK
```

使用 `gh pr view --json reviewDecision` 并将当前 PR head SHA 与 review 事件一起记录。

事件：

- `pr_review.approved`：可见为 `[APPROVED]`。
- `pr_review.changes_requested`：可见为 `[BLOCKER]`。
- `pr_review.required`：可见为 `[REVIEW]`。

## Merge Gate

```bash
$MAC merge gate WORKSPACE --task-id TASK
```

读取本地 CI/review 事件并查询当前 PR head SHA。它要求：

- task mirror 有 PR
- 当前 PR 和当前 head 的最新本地 review 事件是 approved
- 当前 PR 和当前 head 的最新本地 CI 事件是 passed

它始终返回 `human_gate_required=true`，从不合并。

## Phase 8.4: Worker Push → PR Publish

```bash
$MAC pr publish WORKSPACE \
  --task-id TASK \
  --repo OWNER/REPO \
  --branch BRANCH \
  --head-owner OWNER \
  --base main \
  --title "title" \
  --body "body" \
  --commit <40-hex SHA> \
  --pushed true|false
```

在创建或链接 PR 之前，验证 worker 报告的 branch 确实存在于 GitHub 上。仅在支持 GitHub 的主机（Mac/Windows coding host）上运行 — `gh` 是唯一允许调用 `gh api` / `gh pr list` / `gh pr create` 的东西。默认模式将事件 + mirror 写入 coding host 上的**本地 DB**。

当 merge gate 在远端 DB（如控制面主机）上运行时，远端仅记录 sink（`coordinate pr publish-record`）是使 mirror 可见的原因。使用 `--event-cli-path` 时，主机将其 `PublishResult.to_dict()` JSON 转发到运行 `pr publish-record` 的远端 coord CLI；远端 sink 重新验证主机的声明、追加事件并 upsert 远端 `tasks.pr` 列。远端 CLI 从不调用 `gh`。

### 输入

- `--repo` `OWNER/REPO`（验证为 `^[a-z0-9._-]+/[a-z0-9._-]+$`）。
- `--branch` worker branch 名称（例如 `agents/mac-claude/<task>`）。
- `--head-owner` head branch 的 GitHub owner。**必须等于 repo owner** — fork 工作流不在范围内。不匹配时以 `publish.blocked (head_owner_mismatch)` fail closed。
- `--base` **必须显式**；不要从 `workspace.base_branch` 派生，因为一个控制 workspace 可能携带多个目标 repo。
- `--commit` 完整 40 字符小写十六进制 SHA。
- `--pushed` 严格 `true` 或 `false`。任何其他值都被拒绝为 `publish.blocked (invalid_pushed)`。

### 结果

| 事件 | 时机 | 可见性 |
| --- | --- | --- |
| `pr.created` | 未找到开放 PR，`gh pr create` 成功 | `[PR]`（绿色） |
| `pr.linked` | head 的开放 PR 已存在（headRefOid + baseRefName 均匹配） | `[PR]`（黄色） |
| `push.required` | `pushed=false`，或远端 ref 404 | `[PUSH_REQUIRED]`（黄色） |
| `publish.blocked` | 验证 / mirror 冲突 / SHA 不匹配 / 发现不匹配 / head_owner 不匹配 / `gh` 失败 | `[BLOCKER]`（红色） |

幂等性：`pr.created` 和 `pr.linked` 键包含已解析的 PR URL，因此瞬时事件写入失败后重新运行永远不会重复事件，也永远不会调用两次 `gh pr create`（发现步骤先找到现有 PR，且 headRefOid + baseRefName 必须均匹配）。

### Host/server 拆分

```bash
# Mac/Windows coding host — 运行 `gh` + 持有 GitHub token。
coordinate pr publish WORKSPACE --task-id TASK --repo OWNER/REPO \
  --branch BRANCH --head-owner OWNER --base main \
  --title "..." --body "..." --commit <40-hex SHA> --pushed true \
  [--remote origin] [--validation "..."] \
  [--event-cli-path "$HOME/.local/bin/coord-ssh"]

# 控制面 /opt/coordinate — 仅记录远端 sink：
coordinate pr publish-record WORKSPACE --result-json '<host PublishResult JSON>'
```

`--event-cli-path` 将主机的完整 `PublishResult.to_dict()` JSON 转发到调用 `pr publish-record` 的远端 coord CLI。仅记录 sink 从不调用 `gh`；它针对远端 task mirror 重新验证主机的声明，重新计算事件和幂等键，并对 `action in {created, linked}` 要求规范 repo/branch/SHA、`head_ref`、base、repo 范围的 PR URL 和 `remote_sha == reported_commit`。事件追加和 mirror upsert 在远端 `tasks.pr` 列变更之前共享一个事务。这就是远端 `merge gate` 读取远端 DB 时能看到经验证 PR 的原因。

`.py` event_cli 路径自动前置 `sys.executable`，因此 Windows coding host（`coord-ssh-win.py`）无需在 worker 脚本中硬编码 `python` 即可正确启动。

### Preflight 保证

当设置了 `--event-cli-path` 时，主机还会在任何 `gh` 调用之前运行远端 `pr publish-preflight`。如果远端返回 `ok=false`，主机返回 `publish.blocked` 而不触碰 GitHub。使用 `--preflight-event-cli-path` 覆盖 preflight 路径。这是保证"在远端状态重新验证之前不发生 GitHub 写入"的机制。

对于已链接的 PR，相同的 task/repo/branch/PR 绑定可以发布新 commit。主机在记录 sink 推进 `publish_metadata.reported_commit` 之前验证现有 PR URL、新 `headRefOid` 和 base；任何绑定变更仍被阻止。

### CLI 退出码

- `0` 仅当 `result.action in {created, linked}`。
- `1` 在 `push.required` / `publish.blocked` 时（CI / harnessctl 可以基于非零退出快速失败）。
- `2` argparse / 验证失败。

### Worker report payload

当 worker 主机为 GitHub issue 任务报告 `action=done` 时，必须在同一 `[agent-report]` 块中包含 publish 元数据：

```text
[agent-report] action=done workspace_id=discord-nexus task_id=<task> \
  repo=<owner/name> branch=<branch> commit=<40hex> remote=origin \
  pushed=true validation="314 tests OK (2 skipped); git diff --check clean" \
  summary="implementation complete; closeout requested"
```

仅携带 `summary` / `reason` 的旧 report 继续工作 — 在发起 `pr publish` 调用之前，新字段是可选的。
