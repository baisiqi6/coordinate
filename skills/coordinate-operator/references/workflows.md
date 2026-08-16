# Operator 工作流入口索引

> 兼容旧链接的迁移索引。工作流正文已按 authority 和真实使用场景拆入子 skill。

| 工作流 | 加载 |
|---|---|
| 注册 workspace、创建/更新/关闭重要 task | `../subskills/workspace-task-lifecycle/SKILL.md` |
| 通过 Agent client 调用 scoped Remote MCP | `../subskills/remote-mcp-control/SKILL.md` |
| 派发 managed job、恢复 lease、维护 executor | `../subskills/runtime-execution/SKILL.md` |
| 绑定/创建频道、发送或恢复 delivery | `../subskills/messaging-delivery/SKILL.md` |
| 认领 Issue、发布 PR、检查 CI/review/merge gate | `../subskills/github-collaboration/SKILL.md` |
| 监督 worker 或委派局部 Operator | `../subskills/worker-supervision/SKILL.md` |
| deployment、production mutation、break-glass recovery | `../subskills/production-recovery/SKILL.md` |
| Windows 多 CLI/agentd 接入 | `../subskills/windows-multi-cli/SKILL.md` |

只读取当前工作流所需模块；跨模块任务按执行顺序逐个加载。
