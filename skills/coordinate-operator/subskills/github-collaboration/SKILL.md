---
name: coordinate-operator-github-collaboration
description: Use when a Coordinate Operator must claim a GitHub Issue, derive a task identity, allocate a branch, publish or link a PR, inspect CI or review state, evaluate a merge gate, or coordinate host/server GitHub evidence. Do not load for tasks without GitHub state.
---

# GitHub Collaboration

完整稳定流程与 publish contract 见 `references/github-integration.md`。GitHub 是 Issue/branch/commit/PR/CI/
review 的 authority；Coordinate 只记录接入后的 event/mirror。

## 最小规则

- `issue scan` 是候选快照，不是 claim。接受前实时检查 open、unassigned、无 active implementation PR，
  通过 assignee/约定 label cooperative claim 后再次读取远端。
- 单 repo 可用 `issue-N`；多 repo workspace 必须显式 repo-qualified task ID。
- branch checklist 是 merge candidate，main checklist 是 accepted snapshot；合并前运行 resolver-selected
  checklist validator。
- `merge gate ready=true` 只证明 Coordinate 记录的当前 PR head CI/review 前置条件，不自动授权 merge。
- coding host 持有 `gh`/GitHub credential；control plane record sink 不运行 `gh`、不持有 GitHub token。

只查 flags 时运行对应 `branch/pr/ci/review/merge/issue --help`，不读取跨领域总手册。
