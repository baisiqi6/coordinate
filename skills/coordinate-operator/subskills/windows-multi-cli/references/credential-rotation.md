# Windows Multi-CLI Credential Rotation

只在新增、失效或轮换某一个 Remote MCP / Runtime HTTP principal 时读取本 reference。不要为了普通连接
诊断提前执行 rotation。

## 原则

- 每个 client/agent identity 单独轮换，不扇出共享 token。
- 服务器只保存 `token_sha256`；明文只进入目标 client 的受限 secret source。
- 新 credential 未通过验证前保留旧 digest；验证完成后才 retire old。
- rotation 不扩大 tool/workspace/platform scope，也不以重启全部 Agent 代替逐 principal 验证。

## 顺序

```text
prepare new digest
-> install new client secret
-> direct authenticated preflight
-> verify exact tool/workspace/platform scope
-> activate target launcher or service
-> run target-specific canary
-> retire old digest
-> preserve bounded rollback receipt
```

Remote MCP 使用本子 skill 的 `scripts/mcp-preflight.py` 验证 authentication 与 tools。Runtime HTTP 使用
loopback health、该 `client_id` 的 claim/report canary 和 server access log 验证；不能用一个数据面的
结果替另一个数据面背书。

## 失败与回滚

任一步失败时撤销新 digest、恢复旧 launcher/service secret 和旧 policy backup，并只重启目标 client 或
agentd。receipt 记录 client/principal、时间、digest fingerprint、backup locator、验证结果和 retire 状态，
不记录明文 token。
