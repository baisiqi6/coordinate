---
name: coordinate-operator-production-recovery
description: Use for Coordinate production deployment, systemd or /opt runtime copies, server policy mutation, credential installation or rotation, coord-ssh break-glass, rollback, or incident recovery. Treat this as the high-risk module; do not load for ordinary local or read-only operator work.
---

# Production、Deployment 与 Recovery

这是高风险模块。通用部署/恢复事实以 `docs/runbook.md`、当前 deployment scripts、systemd units 与 live
readback 为准；本 skill 不复制会漂移的完整生产手册。

## Authority

- 只有明确授权或范围/目标/时限清楚的持久授权覆盖时执行 production mutation、merge 或 deploy。
- `coord-ssh` 是尚未 MCP 化 mutation、policy rotation、deployment 与 break-glass 的受限入口；不得直接
  编辑 `/var/lib/coordinate/coord.sqlite3`。
- `/opt/coordinate`、`/opt/multinexus` 只由受审 deploy/repair 更新；`--allow-runtime-copy` 不是普通绕过开关。
- secret 明文不进入 Git、argv、prompt、日志或 receipt；server policy 保存 digest-only，backup 保持权限受限。

## 顺序

```text
read-only baseline
-> in-flight job/lease/connection gate
-> reviewed canonical commit
-> bounded backup + rollback locator
-> deploy one component/boundary at a time
-> service/readback/outcome canary
-> restart/recovery proof
-> receipt
```

HTTP `200` 或 `systemctl active` 只证明相应层。验收需要目标业务 outcome，例如 managed job 的 claim/report/
exact result、principal-scoped tools、delivery platform message id 或 deployed-byte readback。

## 故障原则

先判断故障属于 control plane、Runtime HTTP、Remote MCP、coding host、provider 或 message platform，再加载
对应子 skill。恢复前确认旧 writer/process 已停止，避免双 consumer；失败时优先单 component/agent 回滚，
不以重启全套系统代替根因定位。
