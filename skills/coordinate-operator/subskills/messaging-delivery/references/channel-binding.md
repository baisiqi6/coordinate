# Channel Binding 与 Provisioning

Coordinate 唯一持有 `(platform, channel_id) -> workspace_id`。MultiNexus 在构建 managed context/prompt/job
前必须 resolve；unbound、lookup failure 或 workspace mismatch 都 fail closed，无 silent fallback。

```bash
$MAC workspace channel bind discord CHANNEL_ID WORKSPACE \
  --actor operator --reason '...' --idempotency-key bind-discord-CHANNEL_ID
$MAC workspace channel resolve discord CHANNEL_ID
$MAC workspace channel list --workspace-id WORKSPACE
$MAC workspace channel release discord CHANNEL_ID \
  --expected-workspace-id WORKSPACE --actor operator \
  --reason 'rebind' --idempotency-key release-discord-CHANNEL_ID
```

改绑先 release；相同 idempotency key 只允许同一 intent。Coordinator Bot managed create 只需要 control
Category 的有界 `Manage Channels`，不授予 `Administrator`。Remote MCP `coordinate.channel_create` 需要
exact tool/workspace/`discord` grant；以同一 input/key 重放 `pending` 直到 `provisioned`/`failed`。它不
授权 rename/delete/rebind、guild/category/permission payload 或 Bot token。

创建频道不会迁移主 Operator transcript；新频道中的 provider session 是新的 session scope。
