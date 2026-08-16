---
name: coordinate-operator-remote-mcp-control
description: Use when an Agent Operator must connect to Coordinate through Remote MCP, inspect principal-scoped tools, reason about exact tool/workspace/platform grants, call typed workspace lifecycle tools, or decide whether an operation belongs in MCP versus CLI/SSH. This is cross-platform; load the Windows multi-CLI subskill only for Windows client configuration.
---

# Remote MCP Northbound Control

Remote MCP 是 Agent 面向 Coordinate 的 northbound adapter，不是 SSH/CLI 的完整镜像，也不是新的
authority。服务端 registry/domain service 与 Coordinate DB 仍是唯一 runtime 实现。

## Scope 与认证

- principal 的 actor、tool、workspace 与 platform grant 只来自 server-local digest policy；调用参数不能
  覆盖 actor 或扩大 scope。
- `tools/list` 是 principal 视角的 deletion-filtered tool list，只证明 tool grant；workspace/platform 需
  server restricted readback 或已授权的 exact read-only probe。
- 每个物理 Operator client 使用独立 principal/token。worker 不继承 Operator credential。
- 非 loopback 使用 HTTPS；token 只通过受限 env/secret source 注入，不写 argv、prompt、日志或 Git。

## Tool 边界

日常 workspace lifecycle 使用 registry 中真实存在、strict typed `input` 的工具，例如 task record、
completion prepare/preflight/claim/apply/consume，以及有界 `coordinate.channel_create`。具体可见集合始终以
当前 principal 的 authenticated `tools/list` 为准，不把文档中的示例当部署事实。

coding host 仍持有 canonical checklist/plan 并执行 file half、review、Git commit/push 与 deployment。
MCP record/consume 只接受受约束的 deployed-byte/readback contract，不接受 caller checklist bytes、任意
path/payload 或 self-reported digest escape hatch。

assignment request/accept/handoff/blocker/unblock/closeout/review-result、Git/deploy、lease reap/recovery 与
policy rotation 若未作为受审 typed tool 暴露，继续走 Coordinate CLI/SSH；不要用通用
`call(tool,payload)` 旁路。

## 与其他子 skill 的组合

- task/completion file+record lifecycle：`../workspace-task-lifecycle/SKILL.md`
- channel provisioning 与 binding：`../messaging-delivery/SKILL.md`
- Windows client、preflight、NSSM、rotation：`../windows-multi-cli/SKILL.md`
- production policy/deploy/recovery：`../production-recovery/SKILL.md`

只加载当前操作需要的邻接模块。例如只做 `tools/list` 不需要读取 task lifecycle；只有真正执行 completion
时才加载 workspace/task 模块。
