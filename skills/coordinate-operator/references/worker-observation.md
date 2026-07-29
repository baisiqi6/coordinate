# Worker 观察

当 operator、coordinator、architect 或 reviewer 需要在不打断有用工作的情况下
监督委派的 worker 时，使用本协议。它适用于 Claude Code、Codex、OpenCode/OMP、
Hermes、OhMyPi 和未来的 adapter。

## 证据层

按以下顺序将它们视为不同的证据层：

1. Provider 原生事件流或会话日志，通常是 JSONL：时间戳、状态变化、工具调用、
   工具结果和用户可见的推理摘要。
2. 进程和传输状态：PID、退出状态、终端/会话状态、provider 错误、排队、
   限流和重连。
3. 仓库和产物状态：`git status`、diff、生成的文件、日志和结果产物。
4. 验证状态：测试、构建、linter、运行时探测和 coordinator job/result 记录。

没有文件变化只意味着还看不到文件变化。Worker 可能正在阅读、推理、搜索、
等待 provider 或运行长工具。不要仅从 `git diff` 推断不活跃。

## 观察循环

1. 在交接时记录 worker 身份、provider、session/job ID、worktree、预期结果
   路径和已知日志路径。
2. 使用 session/job ID 发现 provider 原生流。优先使用 adapter 提供的
   `logs_path`；否则使用 provider 的本地会话索引或终端输出。
3. 仅采样足够识别前进进展的近期事件。跟踪最新事件时间戳和事件类型，
   而不是反复转储整个日志。
4. 将观察到的状态分类为：`thinking`、`reading`、`editing`、`testing`、
   `blocked`、`idle` 或 `dead`。
5. 将流与进程状态和仓库/产物状态关联。
6. 在接受完成之前独立验证声明的结果。

如果不存在结构化流，按顺序回退到 provider 的原生会话日志、附加的终端输出、
进程状态、文件系统产物和验证命令。记录该观察置信度较低。

## 活跃性判定

- `thinking`、`reading`、`editing` 或 `testing` 需要近期匹配的事件证据；
  不需要 diff。
- `blocked` 需要明确的 blocker、provider 故障、缺失权限或带支持证据的
  重复失败操作。
- `idle` 意味着进程/会话仍可用，但在至少两个观察间隔内没有出现前进进展
  证据。
- `dead` 需要更强的证据，如进程退出、会话终止、不可恢复的传输故障，或
  持续沉默且没有存活进程。不要从一个安静的间隔就标记 worker 死亡。

使用上下文敏感的间隔。大模型推理、包安装、构建和远端 provider 队列可能
合法地安静数分钟。过期警告是检查下一个证据层的提示，而不是重启或复制
任务的自动许可。

## JSONL 处理

JSONL 是运维事件记录，不是完整私有思维链的承诺。用它回答有界问题：

- 会话是否收到了任务？
- 事件时间戳是否在推进？
- 哪个工具或阶段是活跃的？
- 命令是完成、失败还是超时？
- Worker 是在等待 provider 还是本地执行？

提取紧凑字段，如时间戳、事件类型、工具名称、状态和脱敏摘要。不要发布
原始 prompt、私有推理、凭证、token、环境机密或敏感工具参数。优先使用
session ID 和稳定日志句柄，而不是将大型事件体复制到报告中。

## 完成边界

Worker 活跃不等于 worker 正确。仅当所有必需层一致时才接受完成：

```text
event/session 证据 -> 进程完成 -> 预期产物存在
-> 独立测试/运行时检查通过 -> reviewer 接受
```

将 provider 执行失败与实现失败分开。例如，排队、限流、过载或 CLI 崩溃
可能留下没有代码变更的有效交接；在得出任务设计错误的结论之前，重试或
更改 provider 策略。
