# Windows Runtime HTTP Agentd

只在 Windows managed worker、NSSM tunnel、Runtime HTTP credential 或 restart persistence 属于当前任务时
读取本 reference。普通 Remote MCP Operator 配置不需要本模块。

## 启动前做 worker 否定性检查

- worker 的 vendor profile path 与 Operator profile 不同；
- worker profile 没有 Coordinate MCP server 文件；
- NSSM `AppEnvironmentExtra` 没有任何 `COORDINATE_REMOTE_MCP_*`；
- project-local MCP config 不会重新注入 server；
- managed worker 的 MultiNexus `agents.toml` 显式选择 worker profile。

profile 名不同不是充分证据；同时检查实际 config root、process environment 和可见 MCP server，并用真实
provider canary 证明该 profile 可工作。

## 数据面与 recovery 边界

- Remote MCP：Operator 北向 interface。
- Runtime HTTP：agentd 南向 claim/report；每个 agent identity 独立 token，经 loopback 或持久 SSH local
  forward 访问。
- CLI/SSH：尚未 MCP 化 mutation、deploy、rotation 与 recovery/break-glass。

Runtime HTTP tunnel 故障时 agentd 应 fail closed，不自动切回 CLI/SSH 形成双 consumer。Operator 要恢复
CLI/SSH，先停止 HTTP agentd、核验 job/lease，再显式切换。

## Windows steady state

一台 host 使用一个 host-level NSSM tunnel，而不是每个 agent 一个 tunnel：

- tunnel 使用独立 SSH key；server `authorized_keys` 只允许 `permitopen="127.0.0.1:8765"`，并禁用
  shell/PTY、agent/X11 forwarding；不要复用个人 Operator key；
- NSSM 以 `LocalSystem` 运行时，私钥和 `known_hosts` 放在服务专用目录，由 `SYSTEM` 与
  `BUILTIN\\Administrators` 控制；不要为绕过 OpenSSH 检查放宽用户 key ACL；
- 每个 agentd 使用独立 Runtime HTTP principal/token file，并通过
  `MULTINEXUS_COORDINATE_HTTP_CLIENT_ID` / `MULTINEXUS_COORDINATE_HTTP_TOKEN_FILE` 绑定；Windows DACL
  只允许 owner、当前 service identity、`SYSTEM` 与 `BUILTIN\\Administrators`；
- agentd NSSM service 显式依赖 tunnel service；启动前 `http://127.0.0.1:<local-port>/healthz` 必须为
  `200`。tunnel 失败时不自动改回 CLI。

## 验收

逐 agent 执行 restart 与 managed job canary。保留 request/job/attempt、client_id、provider session（若
adapter 支持）和 exact result receipt，并在 server access log 证明 claim/report 来自 Runtime HTTP；进程
存在或 Discord Bot 显示在线都不是充分证据。
