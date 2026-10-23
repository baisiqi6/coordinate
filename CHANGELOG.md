# 变更日志

## [0.6.0] — 2026-10-22

- 新增 Agent 实例绑定的身份切片：认证 principal 固定 canonical Agent/Bot；频道 read/send/有界 wait 使用各自的凭据上下文，核对实际 Bot 身份和发送结果 author。
- 频道工具升级后要求显式配置 `channel_agent_id` 及服务端身份凭据映射；缺失或不匹配时拒绝操作，不回退全局 Bot。非频道工具继续兼容。
- 发送回执保留身份快照；旧记录不改写归属，未知结果和身份漂移不自动重发。无 schema 迁移，不新增队列或数据库。
- 原生桌面往返、真实 Discord author 与 Windows 验收仍需独立执行；源码测试和发布不构成这些验收。

## [0.5.0] — 2026-10-21

- 新增现有 Remote MCP 的项目频道 read/send/有界 wait，严格校验 principal 与 workspace/channel 授权，使用独立游标。
- 发送复用 events/deliveries；同 key 改参报冲突，未知结果不自动重发。固定 content 与 embed 正文、禁用 mentions，隔离既有 handoff/report/lifecycle 解析。
- 无 schema 迁移。升级所有发送/恢复进程以保护 channel_io.v1 后，再授予新工具权限。桌面自动唤醒不包含在本工具中。

## [0.4.4] — 2026-10-16

- 修复 assignment blocker 未透传 --unblock-owner 的问题，默认仍为 human，保留解除阻塞权限和幂等规则。
- 修复 coding-host completion 相对 harness 路径错误地依赖当前工作目录的问题，统一从显式 workspace 解析。
- 新增只读 operator task 恢复摘要，标明当前/最近执行人、解除阻塞人、阻塞原因和最新反馈；保留事件来源、未知责任及镜像可能过期的提示，不刷新任务或改变租约生命周期。
- 配合 EXharness v0.2.3 的恢复摘要和 BLOCKER 接收人修复，补充真实脚本集成与 CLI 历史合同回归。

## [0.4.3] — 2026-10-01

- 修复 trace 在 provider session 仅保存在 terminal result metadata 时显示 unknown 的问题；只读取两个固定 session locator，保留精确来源及已知来源冲突时的 unknown 行为，不投影其他 result 内容。
- 明确 completion receipt 消费后的当前 task mirror 核验、定向 reconcile 与 audit 顺序；现有 lifecycle authority 和 schema 保持不变。
