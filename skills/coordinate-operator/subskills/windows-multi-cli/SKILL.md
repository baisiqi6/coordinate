---
name: coordinate-operator-windows-multi-cli
description: Use when a Coordinate Operator must configure or diagnose Windows Codex, OMP, ZCode, or another CLI against Remote MCP; isolate Operator and worker profiles; provision the Windows Runtime HTTP agentd tunnel; or rotate per-client credentials. Load this nested skill only for Windows or multi-CLI control-plane work, not for ordinary Coordinate operation.
---

# Coordinate Operator：Windows 与 Multi-CLI

这是 `coordinate-operator` 的按需子 skill。父 skill 保存通用 topology、authority 与 lifecycle；本 skill
只处理 Windows/multi-CLI 的实例配置和诊断。不要把这里的 vendor/platform 细节回填到父
`SKILL.md`，也不要在普通 Coordinate 操作时一次性读取全部 references。

若任务涉及 Remote MCP tool 语义、typed lifecycle 或 principal scope，而不只是 Windows client 配置，
先读取同级 `../remote-mcp-control/SKILL.md`；本 skill 不复制跨平台 northbound protocol。

## 先选择最小模块

| 当前问题 | 只读取 |
|---|---|
| Codex/OMP/ZCode 的 Remote MCP 配置、`401`、`403`、timeout、`tools/list` | `references/remote-mcp.md` |
| managed worker profile 隔离、NSSM tunnel、Runtime HTTP token/DACL、service restart | `references/runtime-http-agentd.md` |
| 单个 principal 的 credential 创建、轮换或回滚 | `references/credential-rotation.md` |
| 首次端到端接入 | 按上表顺序逐个读取；每个 gate 通过后再进入下一模块 |

`scripts/mcp-preflight.py` 是 Remote MCP 的只读诊断器，可直接执行；只有需要解释或修改其契约时才读取
脚本源码。

## 共享边界

- Remote MCP 是 Operator 北向控制面；Runtime HTTP 是 agentd 南向数据面；CLI/SSH 只承担尚未 MCP 化的
  mutation、deployment、rotation、recovery 与 break-glass。三者不共享 token，也不自动 fallback。
- 每个 Operator client 与每个 agent identity 使用独立 principal。grant 从空集开始，只授予真实需要的
  exact tool/workspace/platform；认证成功不能冒充 scope 已满足。
- managed worker 使用独立 vendor profile，不继承 Operator MCP server、credential 或 process env。
- token 明文不进入 Git、prompt、argv、日志、task artifact 或 Coordinate DB；服务器 policy 只保存
  digest。Windows token source 必须由 client launcher/secret store 和 DACL 共同约束。
- vendor config schema 会变化。先读取当前版本的原生 config/help，再应用本 skill 的稳定边界；不要维护
  第二份完整 vendor CLI 手册。

## 完成判据

只声明已经实际证明的层级：

1. authenticated preflight 证明 network/auth/protocol/tool grant；
2. server-side restricted readback 或授权的 exact probe 证明 workspace/platform scope；
3. worker 否定性检查证明没有 Operator credential；
4. Runtime HTTP health、claim/report canary 与 restart 验证证明 agentd steady state；
5. receipt 记录 principal/client/profile/service locator、digest fingerprint 与 rollback locator，但不记录 secret。

只完成前一层时，不把它提升为完整接入成功。
