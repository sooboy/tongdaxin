# Tongdaxin Market Data Service

通达信行情 HTTP 服务。当前实现基于 `github.com/bensema/gotdx` 拉取实时行情、盘口、分笔、历史分笔、K 线、复权 K 线、除权除息、证券基础信息、财务信息和交易日状态。

## 快速启动

离线 smoke test，只验证 HTTP 服务、路由和本地逻辑：

```bash
go run ./cmd/marketdata --offline --addr ":8083"
```

连接真实通达信上游：

```bash
go run ./cmd/marketdata --addr ":8083"
```

推荐的本地联调启动参数：

```bash
go run ./cmd/marketdata --addr ":8083" \
  --max-hosts-per-pool 4 \
  --clients-per-host 2 \
  --timeout-sec 3
```

说明：

- `--max-hosts-per-pool 4`：每类接口池从可用 TDX 服务器里选最快的 4 台。
- `--clients-per-host 2`：每台已选服务器建立 2 个独立 gotdx client 连接。
- gotdx 单个 client 串行处理请求；提高并发时应增加连接数，而不是共享同一个 client 并发读写。
- 不建议一开始把连接数开太大。TDX 是公共上游，连接过多可能触发 broken pipe、短包、限流或被动断开。

建议范围：

```text
max-hosts-per-pool: 4~6
clients-per-host: 2~3
```

## 缓存与持久化

默认启动方式：

```bash
go run ./cmd/marketdata --addr ":8083"
```

默认行为：

- 使用进程内内存缓存。
- 使用进程内内存历史 store。
- 服务重启后缓存和历史数据都会丢失。
- 请求历史/K 线数据时，会优先查本地 store/cache；未命中时再请求 gotdx 上游，并写回本地。

### Redis 缓存

```bash
go run ./cmd/marketdata --addr ":8083" \
  --cache-redis-url "redis://127.0.0.1:6379/1"
```

或：

```bash
MARKETDATA_CACHE_REDIS_URL="redis://127.0.0.1:6379/1" \
go run ./cmd/marketdata --addr ":8083"
```

Redis 用于接口缓存，不等同于历史数据持久化。

### SQLite 持久化

```bash
go run ./cmd/marketdata --addr ":8083" \
  --storage-dialect sqlite \
  --storage-dsn 'file:marketdata.sqlite?_pragma=foreign_keys(1)&_time_format=sqlite'
```

如果 `--storage-dialect sqlite` 但不传 `--storage-dsn`，默认使用：

```text
file:marketdata.sqlite?_pragma=foreign_keys(1)&_time_format=sqlite
```

支持的持久化后端：

```text
sqlite, postgres, mysql
```

## 常用接口

健康检查：

```bash
curl http://127.0.0.1:8083/api/v1/health
```

指标快照：

```bash
curl http://127.0.0.1:8083/api/v1/metrics
```

批量行情：

```bash
curl 'http://127.0.0.1:8083/api/v1/quotes?symbols=SH:600000,SZ:000001'
```

单只行情：

```bash
curl 'http://127.0.0.1:8083/api/v1/quotes?market=SH&code=600000'
```

日 K：

```bash
curl 'http://127.0.0.1:8083/api/v1/kline?market=SH&code=600000&period=day&adjust=none&start_date=2025-01-01&end_date=2026-06-29&count=0&force_refresh=false'
```

前复权日 K：

```bash
curl 'http://127.0.0.1:8083/api/v1/adjusted-kline?market=SH&code=600000&period=day&adjust=qfq&start_date=2025-01-01&end_date=2026-06-29&count=0&force_refresh=false'
```

证券基础信息：

```bash
curl 'http://127.0.0.1:8083/api/v1/securities?markets=SH,SZ&start=0&count=100&refresh=false'
```

单只证券基础信息：

```bash
curl 'http://127.0.0.1:8083/api/v1/securities?symbol=SH:600000&refresh=false'
```

交易日状态：

```bash
curl http://127.0.0.1:8083/api/v1/trading-day
```

交易日接口返回字段说明：

- `today`：上游返回的当前自然日。
- `is_today_trading_day`：今天是否交易日。
- `latest_trading_day`：最近交易日。
- `previous_trading_day`：上一个交易日。gotdx MAC 的 `last2` 字段不稳定；本项目会在必要时用日 K 线推导。
- `trading_sessions`：交易时段。
- `open_minutes` / `close_minutes`：从当天 00:00 开始计算的分钟数，例如 `570 = 09:30`，用于程序判断是否处于交易时段。
- `open` / `close`：可读时间字符串，适合展示。

## 回测 K 线压测记录

环境：

```bash
go run ./cmd/marketdata --addr ":18083" \
  --max-hosts-per-pool 4 \
  --clients-per-host 2 \
  --timeout-sec 3
```

压测模型：

- 内存缓存 + 内存历史 store。
- 20 只常见股票。
- 每只请求普通日 K 和前复权日 K，共 40 个请求。
- 日期范围：`2025-01-01 ~ 2026-06-29`。
- 并发：8。
- 跑两轮：冷请求打 gotdx 上游；热请求命中内存缓存 / 内存 store。

结果：

| 场景 | 请求数 | 成功 | 错误 | 总耗时 | 吞吐 | p50 | p95 | max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 冷请求 | 40 | 40 | 0 | 1.208s | 33.12 rps | 236ms | 329ms | 354ms |
| 热缓存 | 40 | 40 | 0 | 0.052s | 772.49 rps | 7.8ms | 14.6ms | 16.2ms |

数据结果：

- 每个请求返回 357 根日 K。
- 总返回 14280 根 bar。
- 单轮响应体约 3.14 MB。
- 返回区间：`2025-01-02T15:00:00+08:00` 到 `2026-06-26T15:00:00+08:00`。

注意：即使请求 `end_date=2026-06-29`，gotdx 日 K 当前只返回到上一个已完成交易日 `2026-06-26`。回测场景应按“已完成交易日”使用日 K；如果需要当日未收盘数据，应通过快照/分笔聚合，而不是依赖日 K。

压测期间未观察到：

- HTTP 5xx
- `upstream_unavailable`
- panic
- reconnect
- broken pipe

## Postman

Postman 集合在：

```text
docs/postman/tongdaxin-marketdata.postman_collection.json
```

本地环境文件：

```text
docs/postman/tongdaxin-marketdata-local.postman_environment.json
```

## 验证

```bash
go test ./...
go test -race ./...
go vet ./...
```
