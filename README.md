# event-time-stream-aggregator

本地实时事件窗口聚合与结果推送服务。面向**乱序到达、重复投递、下游消费变慢**
三类问题，保证各入口（内存 API / HTTP API）得到一致且可解释的结论。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run pytest -q        # 运行测试（日志含输入事件、水位线与判定依据）
uv run python main.py   # 启动 HTTP 服务（127.0.0.1，随机端口）
```

## 窗口与水位线语义

- **窗口**：按事件时间的滚动窗口（tumbling window），半开区间 `[start, end)`，
  大小由 `window_size` 决定。聚合维度为 `(key, window)`，统计 `count` 与 `total`。
- **水位线（watermark）**：`watermark = max(已见事件时间) - out_of_orderness`，
  单调递增、永不回退，因此触发顺序与到达顺序无关、完全确定。
- **窗口生命周期**（每次发射都带 `state` 与可读的 `reason`）：
  1. `OPEN`：窗口累积中，不发射结果；
  2. `TRIGGERED`：`watermark >= window_end` 时发射首个结论；
  3. `CORRECTED`：触发后、终结前到达的迟到事件会修正窗口并重新发射
     （条件：`watermark < window_end + allowed_lateness`）；
  4. `FINAL`：`watermark >= window_end + allowed_lateness` 时发射最终结论，
     窗口永久关闭、不再变化。
- **迟到事件**：窗口已 `FINAL` 后到达的事件不会被静默丢弃，也不会重复计入，
  而是返回 `ignored / too_late` 并附带判定依据（当前水位线与窗口终结边界）。

## 幂等与并发规则

- 每个事件以 `event_id` 去重，`check_and_mark` 在聚合锁内原子完成：
  同一事件在重试或并发提交下**只影响结果一次**。
- 每个事件的判定结果可机器区分：

  | status     | reason         | 含义                                       |
  |------------|----------------|--------------------------------------------|
  | `accepted` | `none`         | 已计入窗口                                 |
  | `ignored`  | `duplicate`    | 同一 `event_id` 已应用过                   |
  | `ignored`  | `too_late`     | 事件窗口已 `FINAL`（超过允许迟到时间）     |
  | `rejected` | `invalid`      | 非法负载（空 id/key、非有限时间或数值）    |

- 去重存储有容量上限（`dedup_capacity`），超出后按 FIFO 逐出最旧 id，
  避免内存无界增长。
- 所有入口共享同一把聚合锁，并发写入下每个事件恰好计入一次，
  发射序号 `seq` 全局单调。

## 订阅、断点续传与背压

- 每条发射结果带全局单调 `seq`，写入只追加的结果日志（`ResultLog`）。
- **增量订阅**：`subscribe(after_seq=N)` 从序号 N 之后重放；
  客户端重连时带上最近一次 `ack` 的序号即可**不缺失、不重复**。
- **背压**：`poll` 取到的结果必须 `ack`。未确认结果数达到
  `log_capacity` 时，`poll` 抛出 `SubscriptionClosed`（明确拒绝），
  而不是让缓冲无界增长；客户端可从 `acked_seq` 重新订阅续传。
- HTTP 入口：`POST /events`（返回逐事件判定）、`GET /watermark`、
  `GET /results?after_seq=&limit=`（增量拉取）。

## 关键配置项（`AggregatorConfig`）

| 配置               | 默认值   | 含义                                       |
|--------------------|----------|--------------------------------------------|
| `window_size`      | 60.0     | 滚动窗口大小（秒）                         |
| `out_of_orderness` | 10.0     | 水位线滞后于最大事件时间的秒数             |
| `allowed_lateness` | 30.0     | 窗口触发后仍接受修正的额外秒数             |
| `dedup_capacity`   | 100000   | 去重存储记住的最大事件 id 数               |
| `log_capacity`     | 10000    | 每个订阅允许的最大未确认结果数（背压阈值） |

## 验证方法

```bash
uv run pytest -q                # 全部测试
uv run pytest -q -s             # 打印输入事件、水位线与判定依据日志
```

测试覆盖：

- `tests/test_windowing.py`：乱序累积、迟到修正（CORRECTED）、
  终结后拒绝（too_late）、迟到契约内不同到达顺序结论一致；
- `tests/test_idempotency.py`：重复投递只计一次、非法事件可区分拒绝、
  64 线程并发提交同一 id 仅一次生效、多线程并发写入总数精确；
- `tests/test_subscription.py`：断点续传不缺失不重复、
  消费落后触发背压拒绝、边消费边确认永不阻塞、非法 ack 被拒绝。

## 代码结构

```
eventagg/
  models.py        # 事件、判定结果、窗口结果等类型
  watermark.py     # 水位线跟踪（单调不回退）
  window.py        # 滚动窗口分配与窗口聚合状态
  dedup.py         # 幂等去重存储（原子 check-and-mark，容量有界）
  aggregator.py    # 核心引擎：校验→去重→水位线→窗口→发射
  subscription.py  # 结果日志与订阅（断点续传、背压拒绝）
main.py            # FastAPI HTTP 入口
```
