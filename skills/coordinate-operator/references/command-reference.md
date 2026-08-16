# 命令入口索引

> 兼容旧链接的迁移索引，不是完整 CLI 手册。命令参数以当前 `coordinate --help` 与子命令 `--help` 为准。

按任务加载对应子 skill：

- workspace/task/assignment/completion：`../subskills/workspace-task-lifecycle/SKILL.md`
- Remote MCP principal/tools/lifecycle：`../subskills/remote-mcp-control/SKILL.md`
- runtime request/job/executor/capacity/runner：`../subskills/runtime-execution/SKILL.md`
- event/delivery/channel：`../subskills/messaging-delivery/SKILL.md`
- Issue/branch/PR/CI/review/merge：`../subskills/github-collaboration/SKILL.md`
- production/deploy/recovery：`../subskills/production-recovery/SKILL.md`
- Windows/multi-CLI bootstrap/preflight：`../subskills/windows-multi-cli/SKILL.md`

通用本地入口：

```bash
export MAC="skills/coordinate-operator/scripts/mac.sh"
$MAC --help
$MAC <group> --help
```

不要一次加载所有子 skill 来重建旧的总手册。
