# 多实例 Redis / PostgreSQL 数据流说明

本文说明多个 marketdata 应用实例同时连接同一个 Redis 和 PostgreSQL 时，数据如何查询、缓存、入库，以及并发写入如何处理。

## 1. 结论先行

生产多实例建议配置：

```bash
MARKETDATA_STORAGE_DIALECT=postgres
MARKETDATA_STORAGE_DSN='postgres://marketdata_app:password@postgres-host:5432/marketdata?sslmode=disable'
MARKETDATA_CACHE_REDIS_URL='redis://:password@redis-host:6379/0'
MARKETDATA_CACHE_KEY_PREFIX='marketdata:prod'
```

运行后：

- Redis 是短期接口缓存，用于减少重复访问 gotdx 上游。
- PostgreSQL 是历史数据持久化存储，用于 K 线、历史 ticks、覆盖状态和回填任务；SQLStore 也具备 securities upsert/query 能力，但当前 `GetSecurityInfo` 请求路径主要使用 Redis + gotdx。
- 多个应用实例共享同一份 PostgreSQL 数据。
- 多个应用实例共享同一份 Redis 缓存，但必须用 `MARKETDATA_CACHE_KEY_PREFIX` 做环境隔离。
- 默认不配置 Redis/PostgreSQL 时，仍使用进程内 memory cache + memory store，适合本地开发和 smoke test。

## 2. 组件职责

### 2.1 应用实例

每个应用实例都会启动：

- HTTP API，可选 Gin router。
- 可选 gRPC server。
- gotdx 上游连接池。
- MarketDataService 查询编排层。
- Cache backend：Redis 或进程内 memory。
- HistoryStore backend：PostgreSQL / SQLite / MySQL / 进程内 memory。

多个实例之间不直接通信。它们通过 Redis 和 PostgreSQL 共享状态。

### 2.2 Redis

Redis 存放接口级缓存，不作为权威历史库。

当前缓存的数据类型包括：

- 实时 quotes。
- order book。
- 当日 ticks 分页。
- 历史 ticks 查询结果页。
- K 线查询结果页。
- XDXR 除权除息数据。
- securities 证券基础信息查询结果。
- finance 信息。
- trading-day 交易日信息。

Redis key 会带统一 prefix：

```text
{MARKETDATA_CACHE_KEY_PREFIX}:...
```

如果没有显式配置 prefix，Redis 后端使用默认 marketdata prefix。生产、测试、开发不要共用同一个 prefix。

### 2.3 PostgreSQL

PostgreSQL 是多实例下的权威持久化层。

主要表：

- `securities`：证券基础信息。
- `history_ticks`：历史逐笔成交。
- `kline_bars`：K 线数据。
- `history_coverage`：某个数据范围是否已经完整覆盖。
- `backfill_tasks`：异步/失败后回填任务。
- `trading_days`：预留交易日表结构。

历史数据和覆盖状态写入都已经做了幂等 upsert。多实例重复写同一条数据时，会更新同一行，不会产生重复主数据。`securities` 表也支持 upsert，但当前 HTTP/API 查询路径尚未把证券基础信息自动落 PostgreSQL。

## 3. 请求查询顺序

核心原则：

```text
Redis cache -> PostgreSQL store -> gotdx upstream -> 写 PostgreSQL -> 写 Redis -> 返回
```

不是所有接口都查 PostgreSQL。实时行情类通常只走 Redis + gotdx；历史/回测类会优先查 PostgreSQL。

## 4. K 线接口数据流

接口示例：

```text
GET /api/v1/kline?market=SH&code=600000&period=day&adjust=qfq&start_date=2020-01-01&end_date=2024-12-31
```

### 4.1 普通查询流程

流程：

```text
1. 根据 symbol + period + adjust + start/count/times/start_date/end_date 生成 Redis cache key
2. 如果 force_refresh=false，先查 Redis
3. Redis 命中且缓存内容能覆盖本次请求，直接返回
4. Redis 未命中或覆盖不完整，查 PostgreSQL kline_bars
5. PostgreSQL 数据能覆盖请求范围，返回本地数据，并回写 Redis
6. PostgreSQL 数据不完整，计算缺口 gaps
7. 只向 gotdx 拉取缺口区间
8. gotdx 返回后 upsert 写入 PostgreSQL
9. 合并本地数据 + 新拉取数据
10. 如果合并结果能覆盖请求，写 Redis
11. 返回结果
```

### 4.2 增量补齐行为

假设已有缓存/存储：

```text
2020-01-01 ~ 2021-12-31
2023-01-01 ~ 2024-12-31
```

再次请求：

```text
2020-01-01 ~ 2024-12-31
```

服务会检查 PostgreSQL 中已有 K 线数据，发现 `2022-01-01 ~ 2022-12-31` 缺口，然后只对缺口请求 gotdx。不会因为请求范围变大就无条件全量重拉。

注意：增量补齐主要针对日 K 且请求带 `start_date` + `end_date` 的场景。没有明确日期范围的分页请求更偏接口结果缓存，不适合表达长期覆盖范围。

### 4.3 入库规则

K 线写入 `kline_bars`。

唯一键语义：

```text
market + code + period + adjust_type + bar_time
```

同一个 bar 被多个实例同时写入时，PostgreSQL upsert 会更新同一行。

### 4.4 coverage 规则

K 线拉取成功后会写 `history_coverage`。

coverage 用途：

- 标记某个 symbol/period/adjust/date range 已经拉取过。
- 辅助后续判断本地数据是否完整。
- 避免已经完整的数据被后来的 missing/error 状态降级。

已是 `covered` 的 coverage，不会被后续 `missing` 覆盖成不完整状态。

## 5. 历史 ticks 接口数据流

接口示例：

```text
GET /api/v1/history/ticks?market=SH&code=600000&date=2026-06-29&full=true
```

### 5.1 查询流程

```text
1. 根据 symbol + trade_date + start/count/full/with_transaction_flag 生成 Redis cache key
2. 如果 force_refresh=false，先查 Redis
3. Redis 命中且能覆盖请求，直接返回
4. Redis 未命中，查 PostgreSQL history_ticks
5. PostgreSQL 数据能覆盖请求，返回本地数据，并回写 Redis
6. PostgreSQL 不完整，访问 gotdx 上游
7. gotdx 返回后 upsert 写 PostgreSQL
8. full=true 时写 coverage=covered
9. 写 Redis
10. 返回结果
```

### 5.2 入库规则

历史 ticks 写入 `history_ticks`。

唯一键语义：

```text
market + code + trade_date + trade_time + sequence
```

多实例同时写同一笔 tick 时使用 upsert，不会重复插入。

### 5.3 full 参数

`full=true` 表示请求完整交易日数据。只有完整拉取成功后，才适合把当天 coverage 标为 `covered`。

如果只是分页请求，例如 `count=10`，它只是结果页缓存，不应代表当天完整覆盖。

## 6. securities 基础信息数据流

接口示例：

```text
GET /api/v1/securities?symbol=SH:600000&refresh=false
```

流程：

```text
1. 先按 fetch query 查 Redis
2. Redis 命中后，在应用内按用户请求做二次过滤
3. Redis 未命中，访问 gotdx 获取证券列表/分页
4. 写 Redis
5. 当前请求路径写 Redis 后返回；不会自动写 PostgreSQL
```

证券基础信息唯一键：

```text
market + code
```

SQLStore 层支持同一证券重复写入时 upsert 更新字段；如果后续把 securities 请求路径或后台同步任务接入 PostgreSQL，可复用该能力。

## 7. trading-day 交易日数据流

接口：

```text
GET /api/v1/trading-day
```

流程：

```text
1. 查 Redis trading_day cache
2. 命中则直接返回
3. 未命中则调用 gotdx MACServerInfo
4. 成功后写 Redis
5. 返回结果
```

重要约束：

- 交易日不能靠本地 K 线可靠推导，因为开盘前可能没有当天 K 线记录。
- MAC client 如果连接 broken，会断开并重连同一 MAC host，再重试一次。
- 如果重连后仍失败，接口返回 upstream_unavailable。

## 8. 实时 quotes / orderbook / ticks 数据流

这类接口偏实时查询，不以 PostgreSQL 为主。

典型流程：

```text
1. 查 Redis
2. 对未命中的 symbol 调 gotdx
3. gotdx 返回后写 Redis
4. 合并 Redis 命中数据和 gotdx 返回数据
5. 返回
```

这类数据 TTL 通常较短，重启/过期后重新从 gotdx 获取即可。

## 9. 多实例并发写入如何处理

### 9.1 Redis 并发

多个实例可以同时写相同 Redis key。

特点：

- Redis 缓存不是权威数据源。
- 后写入覆盖先写入通常可接受。
- 不同环境必须用不同 prefix，避免测试覆盖生产缓存。

目前 Redis 已提供基础锁能力，但主查询路径主要依靠：

- 进程内 singleflight 减少单实例内重复请求。
- PostgreSQL upsert 保证持久化幂等。
- Redis TTL 限制缓存生命周期。

如果后续遇到多实例 cache miss 同时打 gotdx 的压力问题，可把 K 线/history ticks 的回源路径接入 Redis 分布式锁，做跨实例 singleflight。

### 9.2 PostgreSQL 主数据写入

PostgreSQL 使用 upsert。

效果：

- 多实例同时拉到同一根 K 线，只会落一行。
- 多实例同时拉到同一笔 tick，只会落一行。
- 多实例同时更新 securities，同一个 symbol 只会落一行。
- coverage 不会从 covered 被降级到 missing/error。

### 9.3 回填任务抢占

`backfill_tasks` 用于记录失败后或缺数据时需要补拉的数据。

PostgreSQL 下任务领取使用原子 claim：

```sql
FOR UPDATE SKIP LOCKED
```

效果：

- 多个实例同时领取任务时，不会拿到同一个 pending task。
- 某个实例拿到任务后，任务状态会变为 running。
- 其他实例会跳过已锁定/已领取的任务。

注意：这个原子抢占能力主要针对 PostgreSQL。SQLite/MySQL 当前不作为多实例抢占核心推荐。

## 10. force_refresh 行为

接口支持 `force_refresh=true` 的地方，会跳过 Redis 和本地 store 的命中路径，直接请求 gotdx。

行为：

```text
force_refresh=false:
  Redis -> PostgreSQL -> gotdx

force_refresh=true:
  gotdx -> PostgreSQL -> Redis
```

用途：

- 手动刷新疑似过期数据。
- 测试 gotdx 上游返回。
- 修复缓存污染。

生产中不建议高频使用 `force_refresh=true`，否则会绕过缓存，增加 gotdx 上游压力。

## 11. count/start/start_date/end_date 与缓存的关系

### 11.1 Redis key 是请求级别的

K 线 Redis key 包含：

```text
symbol + period + adjust + start + count + times + start_date + end_date
```

历史 ticks Redis key 包含：

```text
symbol + trade_date + start + count + full + with_transaction_flag
```

因此不同查询条件通常对应不同 Redis key。

### 11.2 PostgreSQL 是数据级别的

PostgreSQL 不按请求原样存结果页，而是存标准化后的数据行：

- K 线按 bar_time 存。
- ticks 按 trade_time + sequence 存。

所以 PostgreSQL 可以支撑增量补齐和跨请求复用。

例如：

```text
第一次请求 2020-01-01 ~ 2021-12-31
第二次请求 2020-01-01 ~ 2024-12-31
```

第二次可以复用第一次已经入库的数据，只补缺口。

## 12. 推荐部署配置

### 12.1 单实例本地开发

```bash
go run ./cmd/marketdata --addr ':8083'
```

行为：

- memory cache。
- memory store。
- 重启丢数据。

### 12.2 单实例本地 + SQLite

```bash
go run ./cmd/marketdata --addr ':8083' \
  --storage-dialect sqlite \
  --storage-dsn 'file:marketdata.sqlite?_pragma=foreign_keys(1)&_time_format=sqlite'
```

适合本地调试持久化，不适合多实例共享。

### 12.3 多实例生产

每个实例使用相同 PostgreSQL 和 Redis：

```bash
go run ./cmd/marketdata --addr ':8083' \
  --storage-dialect postgres \
  --storage-dsn 'postgres://marketdata_app:password@postgres-host:5432/marketdata?sslmode=disable' \
  --storage-max-open-conns 20 \
  --storage-max-idle-conns 5 \
  --cache-redis-url 'redis://:password@redis-host:6379/0' \
  --cache-key-prefix 'marketdata:prod'
```

环境变量版本：

```bash
MARKETDATA_STORAGE_DIALECT=postgres
MARKETDATA_STORAGE_DSN='postgres://marketdata_app:password@postgres-host:5432/marketdata?sslmode=disable'
MARKETDATA_STORAGE_MAX_OPEN_CONNS=20
MARKETDATA_STORAGE_MAX_IDLE_CONNS=5
MARKETDATA_CACHE_REDIS_URL='redis://:password@redis-host:6379/0'
MARKETDATA_CACHE_KEY_PREFIX='marketdata:prod'
```

## 13. 运维需要关注的点

### PostgreSQL

需要：

- 应用可访问 PostgreSQL 内网地址和端口。
- 应用账号有建表、建索引、SELECT、INSERT、UPDATE、DELETE 权限。
- 建议单独数据库、单独账号。
- 开启每日备份。
- 磁盘支持扩容。

### Redis

需要：

- 应用可访问 Redis 内网地址和端口。
- Redis key 不和其他业务混用，或者必须配置 prefix 隔离。
- 配置合理 maxmemory 和淘汰策略。
- Redis 不作为历史数据权威存储，丢缓存可以接受。

### 网络

应用还需要访问 gotdx 上游行情服务器，通常是 `7709` 端口。

## 14. 当前边界和后续优化

当前已经完成：

- PostgreSQL 主数据 upsert。
- coverage 防降级。
- PostgreSQL backfill 原子抢占。
- Redis key prefix。
- Redis 基础锁能力。

仍需按压力情况决定是否继续做：

- 将 Redis 分布式锁接入 K 线/history ticks 回源路径，实现跨实例 cache miss singleflight。
- 为 `trading_days` 表增加正式交易日落库和人工修正能力。
- 增加后台 worker 主动消费 `backfill_tasks`，而不是只在请求路径触发补齐。
- 为 PostgreSQL 表增加按日期分区，适合 history_ticks 数据量非常大的场景。
