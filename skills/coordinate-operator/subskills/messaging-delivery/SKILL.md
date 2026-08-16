---
name: coordinate-operator-messaging-delivery
description: Use when operating Coordinate events, delivery outbox, Discord or KOOK visible messages, channel-to-workspace binding, managed channel provisioning, or delivery recovery. Do not load for runtime jobs that have no messaging concern.
---

# Messaging、Delivery 与 Channel

按需读取：

- event → delivery → bus、状态与 recovery：`references/delivery-and-bus.md`
- channel binding、release/rebind、managed provisioning：`references/channel-binding.md`

消息平台只是可见总线。事件/任务真相在 Coordinate DB，项目规范在 repo/harness；不要从 Bot memory 或
消息历史恢复 authority。

除非用户已给出明确平台、目标、token context 与 mutation authority，否则只做 read-only inspect/dry-run。
