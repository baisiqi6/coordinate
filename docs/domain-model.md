# Coordinate 领域模型

> **状态：当前 Coordinate 拥有的实体。** 产品级角色如 Operator、Harness 和
> Executor 在共享产品定义中定义。

## 运行时实体

### Workspace

项目代码 checkout 及其 harness 边界的注册。`path` 和 `harness_root` 刻意分离，
这样外部仓库可以使用 sidecar harness。Host profiles 将同一逻辑 workspace
映射到主机特定路径。

### Event

通过 Coordinate 观察或执行的物化行动的不可变、幂等记录。Events 携带 actor、
target、task、因果关系、payload 和创建时间。它们是运行时/审计事实，
不是项目文档或 forge 状态的替代品。

### Job

一次托管执行尝试。Job 标识其 workspace、task、runner、目标 agent、
prompt/result 产物、状态、attempt token、活跃性和恢复元数据。
已完成的 job 本身不意味着项目任务已被接受或完成。

### Delivery

从 event 派生的、面向平台目标的可重试 outbox 记录。Delivery 状态和平台消息 ID
是运维事实；渲染的消息是视图。

### Agent

托管 agent 或 bridge 的运行时注册，包括 host、客户端类型、能力、在线状态、
负载和最后心跳。这不定义永久项目角色或授予 Operator 权限。

### RunnerProfile

目标运行时的执行策略。Runner 可以是本地子进程、wrapper、MultiNexus agentd
或其他 adapter。Runner profiles 描述调用机制，不描述 executor 内部的 agent 图。

### TaskMirror

选定 harness 和 forge 字段（如 phase、owner、branch、PR 和最新 event）的
可查询 Coordinate 投影。它必须经过对账，绝不能成为独立编辑的项目真相。

`phase` 字段特别镜像 harness 工作流。计划决策和运行时 Operator 关注点存在于
event 账本中，在查询时派生；它们不是额外的 phase 值。

### TaskGroup

Coordinate 拥有的相关任务分组，用于 operator 视图和工作流路由。
它不替代任何任务的规范计划。

### DecisionRequest

显式 review、blocker、closeout、escalation 或其他决策的持久化请求。
记录存储请求和结果决策证据；它不自主做决策。

### WorkspaceHostProfile

逻辑 workspace 的主机特定路径和命令位置。它允许交接和执行产物针对目标主机
渲染，而不改变项目身份。

## 关系

```text
Workspace
  ├── TaskMirror ──> 引用规范 harness 任务
  ├── Event ───────> 可能触发 Job 和 Delivery
  ├── Job ─────────> 使用 RunnerProfile，可能以 Agent 为目标
  ├── Delivery ────> 将 Event 投影到可见平台
  ├── TaskGroup ───> 分组任务标识符
  ├── DecisionRequest
  └── WorkspaceHostProfile
```

## 所有权摘要

| 实体或事实 | 权威来源 |
|---|---|
| 运行时 event、job、delivery、agent 注册、runner 状态 | Coordinate DB |
| 已接受的范围、计划、验收、粗粒度任务工作流 | Harness 文件 |
| TaskMirror 字段 | 派生；从 harness/events/forge 证据对账 |
| 代码和 commit | Git |
| PR、CI、review、merge | 配置的 forge |
| Agent 本地会话和调用上下文 | MultiNexus 或所属运行时 |
