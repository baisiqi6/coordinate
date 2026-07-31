# Coordinate 架构

> **状态：当前实现架构。** 历史阶段规划和 task 过程材料不进入稳定文档导航，
> 也不重新定义产品或仓库边界。

## 在系统中的位置

```text
人类或 agent Operator
        │ 决策并调用工具
        ▼
Coordinate 协调内核
        ├── HarnessAdapter ──> 规范项目 harness
        ├── Runtime/jobs ────> MultiNexus agentd 或其他 runner
        ├── Policy/outbox ───> Discord / KOOK / webhook / stdout
        └── Forge adapters ──> Git / GitHub 证据
```

Coordinate 刻意将确定性状态机制与可替换判断分离。`operator.py` 辅助函数可以从
记录的状态推断待办行动，但不会把服务变成自主 Operator。

`tasks.phase` 是 harness 工作流投影。运行时完成和计划决策保留为事件；
`operator pending` 从这些事件派生关注点，并返回显式的快照/过期元数据，
而不是存储 `awaiting_operator` phase 覆盖层。

## 组件映射

| 领域 | 主要模块 | 职责 |
|---|---|---|
| 入口 | `cli.py`, `daemon.py`, `__main__.py` | CLI/API/bot 命令接入和服务生命周期 |
| 持久化存储 | `schema.py`, `db.py`, `events.py` | SQLite schema、幂等 events、jobs、deliveries、agents、mirrors |
| 项目生命周期 | `assignments.py`, `transitions.py`, `handoff.py`, `plan_gate.py` | 经验证的生命周期转换和任务级交接产物 |
| Harness 边界 | `harness.py`, `reconcile.py`, `audit.py`, `doctor.py` | 调用 harness mutations、刷新投影、报告 drift |
| 执行 | `runtime.py`, `jobs.py`, `worker.py`, `agent_registry.py` | 注册/认领/运行/重试/恢复 agent 工作并接收结构化结果 |
| 可见性 | `policy.py`, `bus.py`, `discord_rendering.py` | 将持久化事件转换为可重试的可见 delivery |
| Forge 证据 | `branches.py`, `prs.py`, `ci.py`, `reviews.py`, `github.py` | 跟踪 branch、PR、CI、review、publish 和 merge-gate 证据 |
| Operator 支持 | `operator.py`, `onboarding.py`, `issues.py` | 待办视图、workspace 初始化、issue 物化 |

## 主要流程

### 托管执行

```text
Operator 提交意图
  → Coordinate 验证 workspace、task、target 和幂等键
  → 创建持久化 event + job
  → runner 或 MultiNexus agentd 认领 job
  → progress/heartbeat 延续可观察的活跃状态
  → 结构化报告关闭本次尝试
  → policy 创建可见 delivery，Operator 评估下一个 gate
```

Job 的成功结束是证据，不是项目完成。Review、forge 状态、验收和 closeout
仍是独立的 gate。

### Harness 生命周期 mutation

```text
Operator 命令
  → Coordinate 服务验证转换
  → HarnessAdapter 调用 harnessctl
  → 规范 harness 文件变更
  → Coordinate 追加持久化 event
  → reconcile 刷新 task mirror
```

Coordinate 不直接手动编辑 harness JSON。如果 harness mutation 成功但后续记录步骤
失败，audit/reconcile 会报告 drift，而不是静默创造第二个真相。

### 可见 delivery

```text
持久化 event → policy 渲染器 → delivery 行 → bus adapter → 平台消息 id
```

Events 和 deliveries 分离，这样平台故障不会抹除行动记录。
平台消息记录是持久化状态的人类可见投影。

`platform=none` 是保留的 audit-only sink：当 MultiNexus bridge 等调用方已经发送可见回复时，
Coordinate 仍保存 delivery ledger 行用于幂等与审计，但不会把它当作 transport backlog。
无 platform 过滤的 ledger 查询仍显示这些记录；批量 pump 默认跳过它们，显式发送则 fail-closed。

## 权威和投影规则

- Harness 意图和工作流字段通过 `HarnessAdapter` 读取。
- `tasks` 是可查询的镜像；其 phase 从 harness 对账，而 forge 和 event 指针
  保留为 Coordinate 拥有的投影。
- `events` 是 Coordinate 记录行动的持久化运行时/审计账本。
- `jobs` 和 `deliveries` 是 Coordinate 拥有的运行时状态。
- GitHub 结果是最后已知证据，直到从 GitHub 刷新。
- 面向 Operator 的摘要和 Discord/KOOK 消息是派生视图。

Reconciliation 可以刷新镜像并发出 drift 事件。它不得静默重写已接受的项目意图
或 forge 真相。

## Channel binding 权威

Coordinate 是 `(platform, channel_id) -> workspace_id` 的唯一持久权威。MultiNexus
在把任何 managed Discord/KOOK 入站消息放入 context、prompt、handoff 或 job submit
之前必须先 resolve；channel 未绑定、lookup 失败或显式 workspace 与 binding 不一致时
fail closed，没有 silent fallback。

- `channel_bindings` 只保存 active row；`PRIMARY KEY (platform, channel_id)` 本身就是
  “一个 channel 最多一个 active workspace”的数据库约束。actor/reason/history 写入 events，
  不新增 status、task_id、history 表或 cache。
- canonical key：`platform` 归一化为 `discord`/`kook`；`channel_id` 是 opaque 平台 id，
  只强制外边界（非空、无首尾空白、无控制字符、≤128 code points），不臆造平台 regex。
  非法 key 在 bind/resolve/release/list 全部 fail loud，不伪装成 unbound。
- bind/release 是 event-first mutation：`channel.binding.bound` /
  `channel.binding.released` event 与 active row 变更在同一 SAVEPOINT 提交或回滚。
  event 的 `workspace_id` 是目标 workspace，`target` 是 canonical `<platform>:<channel_id>`。
- 幂等：bind/release 要求非空 `actor`/`reason`/`idempotency_key`；exact replay 只回
  receipt、不重复 mutation；cross-operation/cross-payload 复用 fail closed；同 workspace
  再 bind 是 `already_bound` no-op，已 unbound 再 release 是 `already_unbound` no-op；
  replay 历史 release 绝不删除后来 rebind 的 active row。改绑必须先以 expected workspace
  显式 release。
- 读取走 `workspace channel resolve`/`list`；变更走 `workspace channel bind`/`release`，
  只经 `workspace_cli` 注册。

## 文档与过程材料生命周期

Coordinate 的文档体系也遵循单一权威原则。早期设计只区分代码与 harness，没有明确规定
phase、bootstrap、review round 和已取代设计如何退出当前导航，导致过程证据长期堆积在产品仓库。
当前补充以下生命周期边界：

```text
产品仓库
  ├── 当前稳定规范：scope / architecture / domain-model / runbook
  └── 真实产品代码和仍有消费者的兼容接口

私有 task artifact repository（可选、非 runtime）
  ├── task-scoped plan / bootstrap / review / receipt
  └── 已结束 phase、被取代设计和历史正文

Coordinate DB
  └── events / jobs / leases / receipts / deliveries 等运行时事实
```

- 活跃的 Coordinate-managed plan 保持在对应 workspace；artifact repository 不接管 runtime authority。
- task/phase 收口后，过程正文迁入 `$MYHARNESS_ROOT/projects/<project_id>/`；归档不进入当前状态导航。
- 迁移前先把 README、架构、测试和 skill 更新到当前权威入口。只有代码或运行协议确实依赖路径时
  才保留最小兼容文件；不能仅为旧文档互相引用而永久保留 locator 链。
- 历史 `open`、`pending` 或 verdict 不会因归档而继续成为当前事实；需要重新复现或显式激活。
- 产品 Git history 是恢复兜底，不是日常历史浏览界面；不为归档重写历史。

这个分层是 harness 架构的一部分，而不是临时仓库卫生规则：它防止过程记忆反过来成为第二套
source of truth，同时保留跨 session 恢复和审计所需的证据。

## 部署模型

- `<coordinate-checkout>` 是本地源码 checkout。
- `<deployed-coordinate-root>` 是当前环境配置的已部署服务副本。
- 生产运行时真相通过当前环境配置的 Coordinate 数据库和 CLI 读取。
- 本地历史 harness 文件不能替代生产运行时状态。

部署拓扑是运维配置，不是产品不变量。Coordinate 必须能通过其他主机布局或
调用面保持可用。

## 当前参考

- 仓库范围：`docs/scope.md`
- Coordinate 实体：`docs/domain-model.md`
- 运维：`docs/runbook.md`
- AI operator 入门：`skills/coordinate-operator/SKILL.md`
