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
  `GET /stats`（运行状态快照）、会话式订阅端点（见下）；
  `GET /results?after_seq=&limit=` 保留为无状态一次性拉取（兼容旧用法）。

## 有界保留与续传点过旧

- 结果日志只保留最近 `retention_capacity` 条，超出后最老的按 FIFO 淘汰，
  服务长期运行内存有界。
- 客户端断开很久后拿旧续传点回来，若该位置之前的结果已被淘汰，
  服务端**显式拒绝**（内存 API 抛 `ResumePointExpired`，HTTP 返回
  `410 + reason=resume_expired`），提示客户端做全量同步，
  绝不悄悄跳过一段、给出缺数据却看似正常的结果。
- 两类拒绝可机器区分：

  | 场景             | 内存 API              | HTTP                       | 恢复方式                     |
  |------------------|-----------------------|----------------------------|------------------------------|
  | 消费太慢（背压） | `SubscriptionClosed`  | `409 reason=backpressure`  | ack 已取结果或按 acked 重连  |
  | 续传点太旧       | `ResumePointExpired`  | `410 reason=resume_expired`| 全量同步（resume_from=0）    |

- 窗口一旦 `FINAL`，其聚合状态立即回收；长期运行时存活窗口数只与
  迟到视界有关，不与运行时长有关（回收不改变任何后续判定，
  超期事件仍按水位线得到 `too_late`）。

## HTTP 会话式订阅

```bash
# 1. 建会话（新客户端 resume_from=0；重连时带上最近 ack 的序号）
curl -X POST localhost:PORT/sessions -H 'content-type: application/json' \
     -d '{"resume_from": 0}'
# -> {"session_id": "...", "resume_from": 0}

# 2. 拉取本会话的下一批结果（每个会话独立推进，互不影响）
curl "localhost:PORT/sessions/<sid>/results?limit=100"

# 3. 确认消费位置；只有 ack 过的位置才算真正消费掉
curl -X POST localhost:PORT/sessions/<sid>/ack \
     -H 'content-type: application/json' -d '{"upto_seq": 42}'

# 4. 断线重连：用最近 ack 的序号建新会话，不缺失、不重复
curl -X POST localhost:PORT/sessions -d '{"resume_from": 42}' ...
```

- 未确认积压超过 `log_capacity` 时，拉取返回 `409 reason=backpressure`；
  ack 之后或按 acked 位置重连即可继续。
- 会话被 LRU 淘汰或不存在时返回 `404 reason=unknown_session`，
  按最近 ack 位置重建会话即可。
- ack 超过已投递位置返回 `400 reason=invalid_ack`。
- `GET /stats` 返回运行快照：`open_windows`（仍在累积的窗口数）、
  `retained_results` / `evicted_results`（保留/已淘汰结果数）、
  以及 `accepted`、`ignored_duplicate`、`ignored_too_late`、
  `rejected_invalid`、`backpressure`、`resume_expired` 各类计数。

## 关键配置项（`AggregatorConfig`）

| 配置                  | 默认值   | 含义                                       |
|-----------------------|----------|--------------------------------------------|
| `window_size`         | 60.0     | 滚动窗口大小（秒）                         |
| `out_of_orderness`    | 10.0     | 水位线滞后于最大事件时间的秒数             |
| `allowed_lateness`    | 30.0     | 窗口触发后仍接受修正的额外秒数             |
| `dedup_capacity`      | 100000   | 去重存储记住的最大事件 id 数               |
| `log_capacity`        | 10000    | 每个订阅允许的最大未确认结果数（背压阈值） |
| `retention_capacity`  | 10000    | 结果日志保留的最大条数（超出淘汰最老）     |

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
  消费落后触发背压拒绝、边消费边确认永不阻塞、非法 ack 被拒绝；
- `tests/test_retention.py`：保留上限与 FIFO 淘汰、续传点过旧被显式拒绝
  （且与背压可区分）、空闲订阅被淘汰追上后在 poll 时拒绝；
- `tests/test_reclamation.py`：窗口 FINAL 后状态回收、长跑下存活窗口数有界、
  `/stats` 各类计数与仪表；
- `tests/test_http_sessions.py`：HTTP 会话式订阅的进度延续、
  只有 ack 才算消费、慢消费者 409 背压与恢复、多客户端互不影响、
  续传点过旧 410、未知会话 404、非法 ack 400。

本地手动验证（服务启动后）：

```bash
uv run python main.py   # 记住输出中的端口号 PORT
# 产生一批结果后，连续观察内存受控与计数变化：
curl localhost:PORT/stats
```

## 兼容范围

- 窗口触发 / 修正 / 终结的判定、重复投递与超期事件的结论完全不变；
- 内存 API `subscribe(after_seq)` / `poll` / `ack` 签名不变，
  仅在续传点过旧时新增 `ResumePointExpired` 显式拒绝（此前会静默缺数据）；
- `GET /results` 无状态拉取保留，新客户端建议改用会话式端点。

## 代码结构

```
eventagg/
  models.py        # 事件、判定结果、窗口结果等类型
  watermark.py     # 水位线跟踪（单调不回退）
  window.py        # 滚动窗口分配与窗口聚合状态
  dedup.py         # 幂等去重存储（原子 check-and-mark，容量有界）
  stats.py         # 运行计数器（各类拒绝/忽略，线程安全）
  aggregator.py    # 核心引擎：校验→去重→水位线→窗口→发射；FINAL 后回收状态
  subscription.py  # 有界结果日志与订阅（断点续传、背压、续传点过旧拒绝）
  sessions.py      # 会话注册表：每客户端独立游标，LRU 上限
main.py            # FastAPI HTTP 入口（create_app 工厂 + 会话式订阅端点）
```
