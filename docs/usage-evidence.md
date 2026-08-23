# 托管 Job Usage Evidence、幂等累计与 Warning（Issue #12）

> 本文档是 Coordinate authority 层的稳定规范入口。MultiNexus producer 侧的
> normalized contract 由 MultiNexus 仓库维护；Coordinate 只做严格验证、
> attempt 级 ledger 与 task-scoped warning，不做计费、不做价格表、不做
> hard stop。

## 1. 目标与边界

Operator 需要能审计三件事：

1. 某个受管 attempt 实际报告了哪些 usage 事实；
2. replay、失败重报或 reclaim 是否造成重复累计；
3. 一个明确 task scope 的观测值达到阈值时，是否产生一次可审计 warning。

**不做**：企业计费、价格表、estimated cost producer、hard stop/自动取消、
后台聚合 daemon、第二套 task/provider registry、逐 token 实时流。

## 2. 数据模型（schema v16）

### 2.1 `job_attempt_usage`

每个 attempt 最多一条 canonical usage evidence：

| 列 | 说明 |
|---|---|
| `job_id`, `attempt_token` | 主键；usage 的唯一 authority（reclaim 后新 attempt 是另一行） |
| `workspace_id`, `task_id` | scope 快照；`task_id IS NULL` 的 evidence 可持久化但不进入 task 累计 |
| `evidence_json`, `evidence_digest` | canonical V1 evidence 与 sha256 digest |
| `observed_tokens` | 所有非空 input/output/cache bucket 之和；全空则为 NULL（unknown 不冒充 0） |
| `provider_cost_microusd` | 仅当全部 record 都提供 cost 时才求和，否则 NULL |
| `completeness` | attempt 级汇总：全部 complete → complete；全部 unknown → unknown；其余 partial |
| `terminal_event_id`, `event_created` | nullable/best-effort terminal event locator；不是主 identity |
| `recorded_at` | 写入时间 |

外键：`job_id → jobs ON DELETE RESTRICT`、`workspace_id → workspaces ON DELETE
RESTRICT`、`terminal_event_id → events ON DELETE SET NULL`；`task_id` 不是外键
（task mirror 可删除，usage/evidence 必须保留）。

### 2.2 `task_usage_warning_policies`

每个 `(workspace_id, task_id)` 一条当前 policy revision：

- `revision` 单调递增（正整数）；相同 revision + 相同 body 幂等 replay，冲突
  body fail closed；
- `observed_tokens_threshold` 正整数；warning boundary 为
  `observed_tokens >= threshold`（单 attempt 观测值，不是 task 累计）；
- `enabled`：disabled 后不再产生新 warning，历史 evidence/event 保留；
- `workspace_id → workspaces ON DELETE RESTRICT`。

## 3. Typed contract V1

Managed result 可选携带 `usage_evidence`：

```json
{
  "contract_version": 1,
  "records": [
    {
      "provider": "qoder",
      "model": "lite",
      "input_tokens": 0,
      "output_tokens": 0,
      "cache_read_tokens": 0,
      "cache_write_tokens": 0,
      "provider_cost_microusd": 0,
      "source": "provider_reported",
      "completeness": "complete"
    }
  ]
}
```

规则（Coordinate 是 validation authority，`src/coordinate/usage_evidence.py`）：

- `records` 允许 1..8 条，按 `(provider, model or "")` 唯一并 canonical sort；
  `model` 可为 `null`；同一 provider 不能出现两条 `model=null`；
- 五个数值字段为非负 integer 或 `null`（`null` 就是 unknown，不用 0 代替）；
  token 上限 `2^53-1`，`provider_cost_microusd` 上限 signed 64-bit；越界直接
  拒绝，绝不截断；多 record 的 aggregate cost 也必须留在 signed 64-bit
  范围内，否则整块 evidence 以 typed validation error 拒绝；
- `source ∈ {provider_reported, estimated, unknown}`；`source=unknown` 时所有
  数值字段必须为 `null`；
- `completeness ∈ {complete, partial, unknown}` 不是 producer 的自由标签：
  Coordinate 按 null pattern 重推导并要求完全一致（五值全非空 → complete；
  source=unknown 且五值全空 → unknown；其余 → partial）；
- 未知字段、字段缺失、类型错误、负数、越界、重复 `(provider, model)` 均为
  invalid evidence；
- **missing evidence**（字段缺失、显式 null、空 object、空 `records`）保持旧
  行为：不写 usage row、不失败。

汇总规则：`observed_tokens` 只求和非空 token buckets；attempt cost 任一 record
cost 为 null → aggregate cost 为 null；completeness 按上表汇总。

## 4. Terminal report 行为

`report_job_result` 在任何 terminal 分支之前调用集中的
`split_usage_evidence`：验证并从 submitted result 剥离完整 evidence，替换为
bounded summary（contract_version、digest、completeness、observed_tokens、
provider_cost_microusd、record_count、每 record 的 provider/model/source/
completeness）。running、terminal replay、late-result 与所有 `**result` spread
只能看到 sanitized result 或 bounded locator/summary；canonical evidence 只存
在 `job_attempt_usage`。

- **invalid evidence → 整个 terminal report 原子失败**：不释放 lease、不落
  terminal event、不半写 usage；job 保持 running，agentd 可用相同 body 有界
  重试；
- 只有通过 current attempt token + lease authority 的 accepted current
  attempt 在同一 transaction 内写 usage row、terminal event 与可选
  `usage.warning`；
- 同一 attempt 的 exact replay 不新增 row、不重复累计、不重复 warning；
  conflicting replay 遵守 terminal result immutable，不改变既有 usage；
- `timed_out` attempt 的 evidence 保留；reclaim 后新 attempt 写另一条 row；
  连续两个 `timed_out` attempt 即使复用旧 terminal event locator，usage rows
  也互相独立（`(job_id, attempt_token)` 才是 identity）；
- late result（recoverable timed_out → done/failed）不复制 usage row：该
  attempt 已有 row 则保留第一条被接受的 evidence；没有则用 late evidence 补
  一条。

## 5. Warning policy

CLI（V1 只支持 task scope）：

```
coordinate runtime usage policy-set <workspace> --task-id <task> \
    --revision N --warn-observed-tokens N [--disable] [--actor ...]
coordinate runtime usage status <workspace> --task-id <task>
```

- revision 必须严格递增；相同 revision + 相同 body 幂等 replay，冲突 body
  fail closed；
- `policy-set` 本身**不即时评估**；只在 scope 内下一个成功接受的 terminal
  report 时评估；
- 每个 policy revision 最多一个 `usage.warning` event（idempotency key =
  `runtime:usage:warning:<sha256(JSON([workspace_id,task_id,revision]))>`，其中
  JSON 使用无空格的紧凑序列化）；
- event 包含 revision、scope、threshold、observed、completeness 汇总与触发
  job/attempt；`causation_id` 只在当前 terminal event 确实由本 attempt 新建时
  写入，repeat-status attempt 复用旧 terminal event 时 `causation_id=null`，
  不伪造 causation；
- warning 只提示：不取消 job、不阻止 claim/report、不自动改路由；
- **V1 的 warning 可见 authority 是 `runtime usage status` 与 append-only
  event audit**；`usage.warning` 不进入 Discord/KOOK renderer，Operator 必须
  主动查询。

`status` 输出：policy、attempt ledger（每行 digest/observed/cost/
completeness/records/terminal locator）、aggregate（observed_tokens 求和；
cost 仅当所有 attempt 都有 cost 时才求和）、warnings 事件列表。

## 6. 迁移与部署顺序

- schema v16 migration 是 `BEGIN IMMEDIATE` 原子块：两表与 version bump 同
  事务，失败回滚后 v15 数据保持字节语义可用，可重跑；
- 先部署 Coordinate v16（旧 MultiNexus 不带 `usage_evidence`，行为不变），
  再部署 MultiNexus producer；新 MultiNexus 连旧 Coordinate 时可选字段只作
  普通 result 保存，不得导致 job 失败；
- rollback 窗口：v15 code 与 v16 extra tables 可共存；回滚后新写入的 usage
  rows 对 v15 不可读但不会被删除，重新升级后仍可读取。

## 7. 测试

- `tests/test_usage_evidence.py` — V1 strict validation（complete/partial/
  unknown、null、负数、越界、重复 model、records 上限、未知字段、missing
  evidence、canonical sort/digest、bounded summary）；
- `tests/test_usage_runtime.py` — accepted terminal 写一条 usage、exact
  replay 不重复、conflicting replay 不改变、invalid evidence 原子失败、
  timed_out + reclaim + done 两条 row、连续 timed_out 独立 row、late result
  不复制、task_id null 排除 policy scope、cost 汇总 null；
- `tests/test_usage_policy.py` — revision 单调/幂等/冲突、`< threshold` 无
  warning、`== threshold` 一次、`> threshold` 不重复、causation 指向真实
  terminal event、disable 后不再 warning、status 汇总；
- `tests/test_db.py::SchemaV16Tests` — v15→v16 migration 成功与失败回滚、
  幂等、RESTRICT 保留语义；
- `tests/test_cli_contract.py` + `tests/fixtures/cli_contract.json` — 新 CLI
  leaves 的 contract snapshot 与 delta 证明。
