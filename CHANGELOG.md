# 变更日志

## [0.4.4] — 2026-10-16

- 修复 assignment blocker 未透传 --unblock-owner 的问题，默认仍为 human，保留解除阻塞权限和幂等规则。
- 修复 coding-host completion 相对 harness 路径错误地依赖当前工作目录的问题，统一从显式 workspace 解析。
- 新增只读 operator task 恢复摘要，标明当前/最近执行人、解除阻塞人、阻塞原因和最新反馈；保留事件来源、未知责任及镜像可能过期的提示，不刷新任务或改变租约生命周期。
- 配合 EXharness v0.2.3 的恢复摘要和 BLOCKER 接收人修复，补充真实脚本集成与 CLI 历史合同回归。

## [0.4.3] — 2026-10-01

- 修复 trace 在 provider session 仅保存在 terminal result metadata 时显示 unknown 的问题；只读取两个固定 session locator，保留精确来源及已知来源冲突时的 unknown 行为，不投影其他 result 内容。
- 明确 completion receipt 消费后的当前 task mirror 核验、定向 reconcile 与 audit 顺序；现有 lifecycle authority 和 schema 保持不变。
