# TDX Go 客户端库并发承载分析：`injoyai/tdx` vs `bensema/gotdx`

日期：2026-06-24  
分析对象：

- `/Users/regan/work/go/src/github.com/injoyai/tdx`
- `/Users/regan/work/go/src/github.com/bensema/gotdx`

## 结论

不能直接认为这两个库的“单实例”能稳定承载 100 QPS。

- `gotdx`：单个 `Client` 明确通过 `sync.Mutex` 串行化协议请求；要承载 100 QPS，需要外部多 `Client` / 多连接池 / 多 host 分摊。
- `injoyai/tdx`：单个 `Client` 通过 `MsgID + Wait` 关联请求响应，模型上比 `gotdx` 更接近异步管线；但源码不能证明单连接稳定 100 QPS，仍建议用连接池和限流。
- 两者都没有看到明确的 QPS 限流器或服务端承载保证。100 QPS 是否可行主要取决于 TDX 服务端、请求类型、单请求耗时、连接数、超时策略和失败率。

## 1. `gotdx` 并发能力

### 1.1 源码证据

`gotdx` 的通用协议调用入口 `executeProtocol` 会对整个请求加锁：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_helpers.go:12-17`
  - `executeProtocol[T any](client *Client, protocol proto.Protocol[T])`
  - 内部调用 `client.mu.Lock()` / `defer client.mu.Unlock()`
  - 然后进入 `executeProtocolLocked`

`gotdx` 的 socket 交互是同步写请求、同步读响应头、同步读 payload：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client.go:236-308`
  - `exchange(builder proto.RequestBuilder)`
  - 写入 `sendData`
  - `io.ReadFull(client.conn, headerBytes)`
  - `io.ReadFull(client.conn, msgData)`
  - 期间设置连接 deadline

`Client` 本身包含互斥锁：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client.go:43-52`
  - `type Client struct { ... mu sync.Mutex ... }`

连接生命周期也通过锁保护：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client.go:310-335`：`Connect`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client.go:337-362`：`ConnectEx`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client.go:364-390`：`Disconnect`

### 1.2 推论

`gotdx` 单个 `Client` 是线程安全但串行执行的：

- 多 goroutine 同时调用同一个 `Client` 不应乱包；
- 但同一时刻只有一个请求能在 socket 上执行；
- 单连接 QPS 近似受单次请求往返耗时限制：

```text
单连接 QPS ≈ 1 / 单次请求耗时
```

如果单次请求平均 50ms，单连接理论上约 20 QPS；如果 100ms，则约 10 QPS；如果是 K线、F10、文件、历史分笔等重接口，实际会更低。

因此，`gotdx` 单个 `Client` 不能靠增加 goroutine 达到 100 QPS；请求会在 mutex 上排队。

### 1.3 100 QPS 的实现条件

`gotdx` 要承载 100 QPS，需要外层连接池：

```text
N 个 gotdx.Client × 每连接实际 QPS ≈ 目标 QPS
```

例如：

- 单连接实测 10 QPS：至少需要 10+ 个连接；
- 单连接实测 20 QPS：至少需要 5+ 个连接；
- 还应预留失败重试、服务端抖动、host 切换和限流余量。

`gotdx` 提供 host/address pool 相关 option，但它们是连接地址选择能力，不是请求级连接池：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/options.go:12-23`
  - `TCPAddressPool`
  - `ExTCPAddressPool`
  - `MacTCPAddressPool`
  - `MacExTCPAddressPool`
  - `AutoSelectFastest`
  - `MaxRetryTimes`
  - `TimeoutSec`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/options.go:56-117`
  - `WithTCPAddressPool`
  - `WithExTCPAddressPool`
  - `WithMacTCPAddressPool`
  - `WithMacExTCPAddressPool`
  - `WithTimeoutSec`
  - `WithAutoSelectFastest`

结论：`gotdx` 有 host 池和最快 host 探测，但没有自动把请求并发分发到多个连接的连接池。

## 2. `injoyai/tdx` 并发能力

### 2.1 源码证据

`injoyai/tdx` 的 `Client` 结构包含异步等待器、请求缓存和递增消息 ID：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:117-123`
  - `Wait *wait.Entity`
  - `m *maps.Safe`
  - `msgID uint32`

发送请求时会为每个 frame 设置 `MsgID`，写入 socket 后按 `MsgID` 等待结果：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:252-262`
  - `f.MsgID = atomic.AddUint32(&this.msgID, 1)`
  - `this.Client.Write(f.Bytes())`
  - `this.Wait.Wait(conv.String(f.MsgID))`

接收侧解析响应中的 `MsgID`，从缓存中取上下文，然后唤醒对应 waiter：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:125-245`
  - `protocol.Decode(msg.Payload())`
  - `this.m.GetAndDel(conv.String(f.MsgID))`
  - `this.Wait.Done(conv.String(f.MsgID), resp)`

连接建立后会启动读循环：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:113`
  - `go cli.Client.Run()`

主行情默认等待超时为 2 秒：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:83-85`
  - `Wait: wait.New(time.Second * 2)`

扩展行情默认等待超时为 10 秒：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client_exhq.go:36-38`
  - `Wait: wait.New(time.Second * 10)`

库内提供简单连接池：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/pool.go:19-87`
  - `NewPool(dial func() (*Client, error), number int)`
  - `Get`
  - `Put`
  - `Do`
  - `Go`

`Manage` 默认连接数为 1，但支持配置连接数：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/manage.go:11-12`
  - `DefaultClients = 1`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/manage.go:115-118`
  - `WithClients(clients int)`

### 2.2 推论

`injoyai/tdx` 的单连接模型不像 `gotdx` 那样在每个协议请求外层加全局 mutex。它通过 `MsgID` 关联请求和响应，因此从设计上更接近“单连接多请求 in-flight”。

但源码不能证明它能稳定承载单连接 100 QPS：

1. 底层 `ios/client.Client.Write` 的并发安全性不在本仓库源码内，不能仅凭当前仓库确认。
2. 单 TCP 连接同时压很多请求时，TDX 服务端是否稳定按 `MsgID` 返回、是否限流，源码无法证明。
3. 默认主行情等待超时 2 秒，高并发下如果排队或服务端抖动，容易触发 timeout。
4. 部分解析依赖请求时缓存的上下文，高并发超时后清理路径需要压测验证。
5. K线、历史分笔、F10、文件等重接口不适合和实时快照接口混跑在同一个连接上。

结论：`injoyai/tdx` 比 `gotdx` 更适合做异步并发尝试，但 100 QPS 仍应通过连接池、批量请求、缓存和限流实现，而不是压单连接。

## 3. 100 QPS 的可行性分接口判断

### 3.1 快照 / 五档报价类接口

适合较高频，但应优先批量请求，而不是单股票单请求打满 QPS。

相关接口示例：

- `injoyai/tdx`：`GetQuote`
- `gotdx`：`GetSecurityQuotes`、`StockQuotesDetail`、`StockQuotesList`

更合理的调用模式：

```text
少量请求 × 每请求多个 symbol
```

例如每 200ms 拉一批 50～100 个股票，通常比 100 个单股票请求更适合 TDX 协议和公网服务器。

### 3.2 K线 / 历史成交 / F10 / 文件类接口

不建议按 100 QPS 打。

原因：

- 响应体更大；
- 历史分笔、历史分时可能分页多；
- F10 和文件下载更慢；
- 服务端限流、断连和 timeout 风险更高；
- 失败重试会进一步放大流量。

这类接口更适合：

```text
低并发 + 队列 + 本地缓存 + 增量更新
```

## 4. 两个库的高并发承载对比

| 维度 | `injoyai/tdx` | `gotdx` |
|---|---|---|
| 单 Client 是否串行 | 源码意图不是严格串行，按 `MsgID` 关联响应 | 是，`executeProtocol` 外层 `sync.Mutex` 串行化 |
| 单 Client 并发安全 | [INFERENCE] 取决于底层 Write、Wait、TDX 服务端行为 | 是，但通过串行化保证 |
| 内置请求连接池 | 有：`NewPool` / `Manage.WithClients` | 无 |
| Host 池 / 地址选择 | 有 host、random、range dial | 有 address pool、fastest host 探测 |
| 单实例 100 QPS | 未证明，不建议压单连接 | 不支持，mutex 会排队 |
| 多连接 100 QPS | 可用内置 pool 尝试 | 需自行实现 pool |
| 更适合高频快照 | 相对更适合，但必须压测 | 需要多 client 池 |
| 更适合稳定同步 SDK 调用 | 一般 | 较清晰，串行模型更稳 |
| 最大风险 | 单连接 in-flight 压力、timeout、底层 Write 未确认、服务端限流 | 单连接 QPS 低，需要外部池；重试会放大排队 |

## 5. 推荐架构

不建议：

```text
100 goroutines -> 同一个 gotdx.Client
```

因为请求会在 `sync.Mutex` 上排队。

也不建议：

```text
100 goroutines -> 同一个 injoyai/tdx.Client
```

因为单 TCP 连接、服务端限流、默认超时和底层写安全性都没有经过当前证据证明。

建议架构：

```text
业务 API 层
  -> rate limiter
  -> request coalescing / 按 market 与接口类型聚合
  -> 100ms~1s 短缓存
  -> worker pool
       -> 多个 TDX Client
       -> 多个 TDX host 分散
       -> 按接口类型设置独立并发上限
```

按接口类型分池：

| 池 | 接口类型 | 并发策略 |
|---|---|---|
| quote-pool | 五档快照 / 批量报价 | 最高，但应批量化 |
| kline-pool | K线 | 中低并发 |
| history-pool | 历史分笔 / 历史分时 | 低并发 |
| f10-file-pool | F10 / 文件 / 板块 | 很低并发，强缓存 |
| gbbq-cache-pool | gbbq / 复权数据 | 定时同步，不走实时高频路径 |

## 6. 初始压测参数建议

以下是保守压测起点，不是源码保证：

```text
quote: 5~10 个连接，每连接 5~10 QPS 起步
kline: 2~4 个连接，每连接 1~3 QPS 起步
history/f10/file: 1~2 个连接，每连接 <1~2 QPS 起步
```

逐步提高到目标 100 QPS，并观察：

- 成功率；
- p50 / p95 / p99 latency；
- EOF；
- i/o timeout；
- connection reset；
- payload parse error；
- 返回数量不匹配；
- 服务端是否断连或疑似限流。

建议验收标准至少包括：

```text
成功率 > 99%
p95 latency 满足业务要求
持续 10~30 分钟无持续 EOF / timeout / reset
无明显服务端封禁或 host 大面积失败
```

## 7. 最短答案

- `gotdx` 单个 `Client` 扛不了 100 QPS：请求被 `sync.Mutex` 串行化；要 100 QPS 必须做多 `Client` 连接池和限流。
- `injoyai/tdx` 单个 `Client` 理论并发能力更强：用 `MsgID + Wait` 关联响应；但 100 QPS 没有源码或测试证明，仍建议用连接池，不要压单连接。
- 100 QPS 的关键不只在库，而在 TDX 服务端限流和请求类型。五档快照应批量化 + 短缓存；K线、F10、历史类接口不应按 100 QPS 打。
