# 故障排查入口索引

> 兼容旧链接的迁移索引。先按故障所在 authority/data plane 选择一个模块。

| 症状 | 加载 |
|---|---|
| checklist、task mirror、receipt、reconcile、harness path | `../subskills/workspace-task-lifecycle/SKILL.md` |
| Remote MCP tool 不可见、scope/typed input/protocol | `../subskills/remote-mcp-control/SKILL.md` |
| pending/failed job、lease、agentd、runner、executor | `../subskills/runtime-execution/SKILL.md` |
| event 不可见、delivery 失败、channel workspace 查询失败 | `../subskills/messaging-delivery/SKILL.md` |
| Issue/PR/CI/review/merge gate | `../subskills/github-collaboration/SKILL.md` |
| worker 安静、provider JSONL、session/process 状态 | `../subskills/worker-supervision/SKILL.md` |
| `/opt`、systemd、production policy、SSH/recovery | `../subskills/production-recovery/SKILL.md` |
| Windows CLI timeout/`401`/`403`、NSSM、token rotation | `../subskills/windows-multi-cli/SKILL.md` |

不要先把所有模块读入上下文。若症状跨层，从最靠近失败证据的一层开始，只有证据指向下一层时再加载。
