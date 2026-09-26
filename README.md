# Event-Time Stream Aggregator

本地实时事件窗口聚合与结果推送服务：按**事件时间**做固定窗口聚合，用**水位线**
驱动窗口触发、修正与最终结论；入口侧做**幂等去重**（乱序/重试/并发下每个事件只影响
结果一次）；出口侧提供**增量订阅 + 断点续传**，并用有界结果日志对慢消费者施加
背压，保证内存不随积压无界增长。

## 1. 快速开始

```bash
uv sync
uv run python main.py        # 启动 HTTP 服务 (127.0.0.1:8000)
uv run pytest                # 运行全部测试（日志会打印事件/水位线/判定依据）
```

## 2. 窗口与水位线语义

- **固定窗口（tumbling）**：窗口为左闭右开区间 `[start, start + window_size_ms)`，
  `start = floor(ts_ms / window_size_ms) * window_size_ms`，按 `(key, window)` 聚合
  `count` 与 `total`。
- **水位线 `W = max(已接受事件时间) − grace_ms`**，单调递增，永不回退；
  也可用 `POST /watermark` 显式推进（低于当前水位线的推进为空操作）。
- **判定与触发规则（确定性、可解释）**：

| 情形 | 判定 / 触发 | 规则 |
|---|---|---|
| 事件时间 `ts >= W` | `accepted` + `on_event` | 正常事件，更新窗口并推送部分结果 |
| `ts < W` 但窗口未关闭（`window_end > W`） | `accepted` + `correction` | 乱序但仍在宽限期内，结果以**修正**形式重新推送（含最新累计值） |
| 窗口已关闭（`window_end <= W`）后到达 | `late`（拒绝） | 窗口已 `final`，事件**不丢弃而是明确拒绝**，不会被计入也不会改变结论 |
| 水位线越过窗口结束时间 | `final` | 推送最终结果并清除窗口状态；之后该窗口只可能出现这一个 FINAL |

因此窗口生命周期为：零或多次 `on_event`/`correction` → 恰好一次 `final`；
FINAL 之后到达的事件返回 `late`，结论确定且可复现。

> 宽限期语义：`grace_ms` 是“晚到多久内仍可修正”的容忍度。事件晚于水位线
> 超过该容忍度（窗口已 final）才被拒绝。

## 3. 幂等与拒绝原因

提交事件按固定顺序判定，四种结论**可区分**：

1. `invalid` —— `event_id`/`key` 为空、`ts_ms < 0`、`value` 非有限值（NaN/Inf）；
   非法事件**不会占用幂等名额**，修正载荷后同 id 仍可正常提交。
2. `duplicate` —— `event_id` 已被接受过；重试与并发重复提交均被忽略，结果只影响一次。
3. `late` —— 所属窗口已随水位线 finalize；返回原因含窗口区间与当前水位线。
4. `accepted` —— 计入聚合，原因 `aggregated`。

去重表为**有界 FIFO**（容量 `dedup_capacity`），容量应大于重试时间窗口；
极端情况下被淘汰的 id 会再次被视为新事件（容量换内存的明确取舍）。
引擎内部用单把锁串行化提交，32 线程并发提交同一事件时恰好 1 次 accepted。

## 4. 增量订阅、断点续传与背压

- 所有推送结果进入**全序追加日志**，每条结果有严格递增、连续的 `seq`。
- `GET /results?after_seq=N&limit=M` 返回 `seq > N` 的至多 M 条；
  客户端把最后见到的 seq 作为检查点，重连后续传，**不缺失也不重复**。
- 结果日志**有界**（容量 `log_capacity`），超出即淘汰最旧条目——这是背压机制：
  内存不会因消费慢而无界增长。若客户端的续传点已被淘汰，返回明确错误
  （引擎抛 `GapError`，HTTP `409 {"error":"resume_gap","oldest_seq":...}`），
  客户端从 `oldest_seq - 1` 重新订阅，而不是静默丢数。
- 聚合状态同样有界：窗口随水位线 finalize 后立即清除。

## 5. HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `POST /events` | 提交单条事件 `{event_id,key,ts_ms,value}`，返回 decision/reason/本次触发的结果 |
| `GET /watermark` | 查看当前水位线 |
| `POST /watermark` | 显式推进水位线 `{watermark_ms}`，返回被 finalize 的窗口结果 |
| `GET /results?after_seq=&limit=` | 断点续传式拉取结果 |
| `GET /health` | 健康检查 |

## 6. 关键配置项（`EngineConfig`）

| 配置 | 默认值 | 含义 |
|---|---|---|
| `window_size_ms` | 60000 | 固定窗口长度（ms） |
| `grace_ms` | 5000 | 水位线宽限期：`W = max_ts - grace_ms` |
| `dedup_capacity` | 100000 | 幂等 id 表容量（FIFO 淘汰） |
| `log_capacity` | 10000 | 结果日志容量（背压阈值，超出淘汰最旧） |

## 7. 验证方法

测试分四组，运行时通过 `log_cli` 打印每条输入事件、水位线变化前后值、
判定结论、原因与触发的结果，结论可复现：

```bash
uv run pytest tests/test_windows_watermark.py      # 乱序/迟到/修正/final/水位线单调
uv run pytest tests/test_idempotency_concurrency.py # 重复投递、非法事件、8×50 并发写入
uv run pytest tests/test_subscription_backpressure.py # 断点续传、409 缺口、5000 条洪流下内存有界
uv run pytest tests/test_api.py                    # 端到端 HTTP 行为
```

手工验证示例：

```bash
curl -s localhost:8000/events -H 'content-type: application/json' \
  -d '{"event_id":"e1","key":"a","ts_ms":10000,"value":2.5}'
curl -s -X POST localhost:8000/watermark -H 'content-type: application/json' \
  -d '{"watermark_ms":60000}'          # 触发窗口 [0,60s) final
curl -s 'localhost:8000/results?after_seq=0&limit=100'
```

## 8. 代码结构

```
aggregator/
  types.py        # Event / Submission / WindowResult / Decision / Trigger
  watermark.py    # 单调水位线追踪
  dedup.py        # 有界幂等 id 表
  windows.py      # 窗口状态与 on_event/correction/final 规则
  result_log.py   # 有界全序结果日志、续传与 GapError
  engine.py       # 线程安全编排（校验→去重→迟到判定→聚合→推送）
  api.py          # FastAPI 接口
tests/            # 四组测试
```
