# 变更日志

## [0.4.3] — 2026-10-01

- 修复 trace 在 provider session 仅保存在 terminal result metadata 时显示 unknown 的问题；只读取两个固定 session locator，保留精确来源及已知来源冲突时的 unknown 行为，不投影其他 result 内容。
- 明确 completion receipt 消费后的当前 task mirror 核验、定向 reconcile 与 audit 顺序；现有 lifecycle authority 和 schema 保持不变。
