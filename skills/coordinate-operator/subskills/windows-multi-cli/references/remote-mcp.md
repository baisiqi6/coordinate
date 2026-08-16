# Windows 与多 CLI Remote MCP Bootstrap

只在 Windows 上的 Codex、OMP、ZCode 等 Operator client 需要连接唯一云端 Coordinate，或出现
“导入了 MCP server 但一直 timeout/401”的问题时读取本 reference。Windows agentd 的南向 Runtime HTTP
属于另一个模块，见 `runtime-http-agentd.md`；credential rotation 见 `credential-rotation.md`。

## 先固定三条边界

1. **每个 Operator client 一个 principal 和 token**：Codex、OMP、ZCode 不共享 credential；principal
   只获得真实需要的 `tools`、`workspace_ids`、`platforms`。
2. **worker profile 不带 Operator authority**：managed agentd 启动 vendor CLI 时使用独立 profile；其
   service environment、profile 和 project config 都不能包含 Remote MCP token/server。
3. **服务器只存 digest**：token 明文只在 Windows 的受限 secret store/file 或进程环境中短暂存在；
   不写入 Git、prompt、日志、argv 或 Coordinate DB。

`tools/list` 只能证明该 principal 的 tool grant。它不能证明 workspace/platform grant；后两者要用
server policy 的受限 readback，或该 principal 已获授权的只读 workspace tool 做 exact probe。不要为
“证明 scope”调用 `coordinate.channel_create` 等 mutation tool。

## Bootstrap 顺序

### 1. 管理侧准备 principal

- 为目标 CLI 生成新的随机 token；服务器 policy 只写 `token_sha256`。
- 使用明确 `client_id`，例如 `windows-codex-operator`、`windows-omp-operator`、
  `windows-zcode-operator`。
- 从空 grant 开始，只加入实际 tool/workspace/platform。不要复制另一个 client 的整条 policy。
- 原子写 policy、保留权限受限 backup，重启/刷新 Remote MCP service 后先验证既有 principal 不回归。

### 2. Windows 安装 token

优先使用当前 client 原生支持的 `token env`/secret mechanism。已验证版本的 Codex 可在
`%USERPROFILE%\.codex\config.toml` 中只保存 env var 名：

```toml
[mcp_servers.coordinate]
url = "https://coordinate.example/mcp"
bearer_token_env_var = "COORDINATE_REMOTE_MCP_TOKEN"
```

升级或换发行版后先核对当前 Codex 的 config schema；不要把该键名当成所有历史/未来版本的永久契约。

不要把一个全局永久环境变量同时供多个 CLI 使用。用每个 client 自己的 launcher，从 DPAPI 绑定的
secret 或仅当前用户可读文件解密后，只对子进程设置对应 env。检查 launcher/secret ACL：

```powershell
icacls "$env:USERPROFILE\.coordinate-secrets"
```

OMP 必须使用独立 Operator profile，并先读取当前版本的真实配置根：

```powershell
omp --profile windows-operator config path
```

只在这个 profile 内配置 Coordinate MCP。不要依赖 “import Codex config” 迁移 credential：它最多复制
server 定义，不能证明 bearer token 的 env/file 语义也被复制。ZCode 同理，应使用自己的 client profile
和 token；不要指向 Codex 的 DPAPI file 或 env var。

若当前 OMP/ZCode 版本只接受 literal Authorization header，不要把 token 写进共享默认 profile。使用
该 client 的 per-process bootstrap/受限 profile file，并把这项 vendor limitation 记录在实例 receipt；
升级后优先切回 env/file 引用。配置 schema 随 vendor 版本变化，本 skill 不复制一份会漂移的完整 JSON。

### 3. 在启动 Agent session 前运行只读 preflight

token 已进入当前进程环境时：

```powershell
python skills/coordinate-operator/subskills/windows-multi-cli/scripts/mcp-preflight.py `
  --url https://coordinate.example/mcp `
  --token-env COORDINATE_REMOTE_MCP_TOKEN `
  --expected-tool coordinate.operator_pending
```

POSIX 可使用 mode 受限 token file：

```bash
python skills/coordinate-operator/subskills/windows-multi-cli/scripts/mcp-preflight.py \
  --url https://coordinate.example/mcp \
  --token-file "$HOME/.coordinate-secrets/zcode.token" \
  --expected-tool coordinate.channel_create
```

Windows preflight 不接受 `--token-file`：Python stdlib 无法在这个小诊断脚本中可靠证明 NTFS DACL，若再
复制一套 ACL parser 反而会形成第二套安全实现。Windows 应由 ACL 受限的 launcher/secret store 只向
当前子进程设置 token env，再使用 `--token-env`；无法建立该边界就 fail closed。

脚本使用当前 Coordinate 的 `2026-07-28` stateless MCP discovery；旧实例应先升级，不在这个面向当前
云端拓扑的诊断脚本中维护第二条 legacy lifecycle。它不回显 token、请求 body 或 server error body，
只输出安全分类与已授权 tool 名称。

| classification | 含义 | 下一步 |
|---|---|---|
| `ready` | 网络、认证、协议、expected tool 均通过 | 再核验 exact workspace/platform scope |
| `authentication` / `401` | token 不匹配、已轮换或 client 用错 principal | 比对 digest 与 client_id，不先查代理 |
| `forbidden` / `403` | Host/Origin/policy 拒绝 | 查允许的 host/origin 与 principal policy，不扩大 wildcard |
| `tool_scope` | 认证成功，但 expected tool 被 deletion filter 移除 | 只补该 tool 的明确 grant |
| `network` | DNS/TLS/proxy/route/timeout | 再查 FlClash/Tailscale/系统代理和证书 |
| `protocol` | endpoint、MCP version 或 response framing 不兼容 | 核对部署版本和 client contract |

如果 preflight 在毫秒级返回 `401`，而 OMP/Codex UI 显示 “timed out after 30000ms”，应按认证失败处理；
这是 client 错误呈现，不能据此修改网络或 Coordinate server。

Remote MCP 当前没有匿名 health endpoint：未带 token 请求 `/mcp`、`/healthz` 或 `/readyz` 得到统一
`401`，只能证明 listener 已响应，不能证明某个 principal 可用。服务健康应组合核验 systemd/listener 与
authenticated preflight，不要把匿名 `401` 误报为服务宕机。

完成 authenticated preflight 后，只在任务确实涉及 managed worker 时再读取
`runtime-http-agentd.md`；只在新增、失效或轮换 credential 时再读取 `credential-rotation.md`。
