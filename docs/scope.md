# Coordinate 范围

> **状态：当前 Coordinate 仓库边界。**
>
> 跨仓库产品角色和权威来源归属由集成项目的 active product definition 定义。

## 项目

Coordinate 是一个确定性协调内核和持久化控制面工具包，面向由可替换的人类和
agent Operator 执行的长期项目。

Coordinate 不是 Operator。它提供 Operator 用来检查、委派、恢复、审查和关闭工作
的机制，而不依赖于单个 agent 会话。

## 范围内

- Workspace 和 host-profile 注册。
- 持久化 events、jobs、deliveries、agents、runner profiles 和 task mirrors。
- 通过配置的 harness mutation API 进行幂等任务生命周期转换。
- 托管 worker 和 reviewer 交接准备。
- 运行时 agent 注册、心跳、请求认领、进度和报告接收。
- Job 重试、取消、超时、恢复和结构化结果捕获。
- Outbox 策略和通过 stdout、Discord、KOOK 或 webhook adapter 的可见 delivery。
- Git branch、PR、CI、review 和 merge-gate 证据。
- Reconciliation、drift 检测、workspace doctor 和面向 operator 的待办事项。

## 范围外

- 充当永久 AI coordinator 或做开放式产品决策。
- 重新实现 executor 原生的 subagent、team、skill 或 workflow 引擎。
- 拥有 agent 调用会话、对话上下文或 adapter 特定的恢复逻辑。
- 拥有项目源代码、Git 历史、PR review 真相或 CI 真相。
- 定义可复用的 file-backed harness 协议。
- 把 Discord、KOOK 或任何其他可见消息记录当作持久化项目状态。

## 边界

- 当前人类或 agent **Operator** 选择行动；Coordinate 验证和记录机制。
- Harness 拥有已接受的范围、计划、验收标准和粗粒度项目工作流。
- Coordinate 拥有持久化运行时执行、event、job、delivery 和 runner 记录。
- Coordinate task 行是项目工作流事实的镜像，不是独立可编辑的权威。
- MultiNexus 或其他 runner 拥有 agent 本地执行和会话状态。
- Git 和配置的 forge 拥有代码、commit、PR、CI、review 和 merge 事实。
- 内部 executor 委派保持不透明，除非子任务需要独立管理的生命周期。
