# Coordinate 文档索引

## 权威来源映射

| 问题 | 当前权威来源 |
|---|---|
| Coordinate 在顶层多 agent harness 中处于什么位置？ | [`../README.md`](../README.md) 和 [`scope.md`](scope.md) |
| Coordinate 负责什么？ | [`scope.md`](scope.md) |
| Coordinate 如何实现？ | [`architecture.md`](architecture.md) 和 [`domain-model.md`](domain-model.md) |
| Coordinate 如何运维？ | [`runbook.md`](runbook.md) 和 [`../skills/coordinate-operator/SKILL.md`](../skills/coordinate-operator/SKILL.md) |
| 历史 phase、旧设计和 task 证据在哪里？ | 私有 task artifact repo 的归档索引；本仓库只保留当前稳定规范入口 |
| 当前集成项目的计划/状态？ | 对应 workspace 的活跃 harness / plan |
| 生产运行时状态？ | 已部署 Coordinate DB（通过部署配置的远端 CLI/wrapper 查询） |

本仓库文档可以概述共享产品定义，但不得创建另一个可编辑的 Operator、Coordinate、
MultiNexus、Harness 或 Executor 定义。

## 当前 Coordinate 文档

- `scope.md` — 仓库范围与非目标。
- `architecture.md` — 当前实现组件、流程和权威边界。
- `domain-model.md` — Coordinate 拥有的运行时实体和投影。
- `runbook.md` — 日常运维与恢复。

## 历史材料

已结束 phase、旧 bootstrap/handoff 设计、被取代的架构调研和长篇过程记录不再占据当前导航。
私有 canonical 可以把它们保存在独立 task artifact repository；公开稳定版不携带其 locator、
运行状态或过程正文。只有真实代码或运行协议依赖路径时才保留最小兼容文件。
任何历史文本与当前规范冲突时，以 `scope.md`、`architecture.md`、`domain-model.md`、
`runbook.md` 和当前代码为准。
