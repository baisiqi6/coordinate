# Delivery 和 Bus 操作

可见消息流经 policy 和 delivery outbox。

```text
event -> render_event_payload -> delivery 行 -> worker delivery -> bus
```

## 平台

- `stdout`：本地 dry-run bus。
- `discord`：通过 `DISCORD_BOT_TOKEN` 的真实 Discord 发送。
- `kook`：通过 `KOOK_BOT_TOKEN` 的真实 KOOK 发送。
- `discord_webhook`：通过 `DISCORD_WEBHOOK_URL` 的真实 Discord webhook 发送。
- `none`：不是 transport。当前 runtime 在 bridge 已负责可见回复时不创建 delivery；数据库中若有
  `platform=none`，它是旧版本遗留的审计行，没有 bus adapter。

除非用户明确要求并提供 destination/token 上下文，否则不要使用真实平台发送。

## Delivery 状态

- `pending`：对于 transport platform 表示准备发送。旧的 `none,pending` 行不属于待发送工作。
- `sending`：中断时发送正在进行中。
- `sent`：已完成并有平台消息 id。
- `failed`：可重试的失败。
- `dead`：达到最大尝试次数。

`pump_deliveries` 消费 pending 和可重试的 failed deliveries。为兼容旧 ledger，未指定
platform 的批量 pump 仍跳过 `none`；显式选择 `none` 或直接发送该记录会 fail-closed。
排查真实 backlog 时应始终指定 transport platform。

## 恢复

仅在确认没有活跃发送方后使用 `recover-sending`：

```bash
$MAC delivery recover-sending --platform stdout
$MAC worker delivery --platform stdout --recover-sending --once
```

## 真实平台要求

Discord：

```bash
export DISCORD_BOT_TOKEN=...
$MAC worker delivery --platform discord --once
```

KOOK：

```bash
export KOOK_BOT_TOKEN=...
$MAC worker delivery --platform kook --once
```

Destinations 是平台频道 ID 或等效的房间 ID。token 不要放在仓库文件和 runner profiles 中。
