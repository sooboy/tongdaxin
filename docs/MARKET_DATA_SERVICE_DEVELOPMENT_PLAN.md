# 行情服务系统设计开发计划

## 1. 文档目的

本文档用于指导行情服务系统的一期设计、开发、测试和交付。系统一期目标是支撑回测场景下的高频历史数据读取，同时对外稳定承载 1000 QPS 请求。系统通过 gotdx 协议接入、多行情服务器连接池、历史数据本地化、缓存削峰、批量聚合、请求合并、限流熔断和可观测能力，保证回测读取吞吐、服务稳定性和后续扩展能力。

本文档是内部开发文档，可以直接写明 gotdx 接入方案、接口映射、连接池实现和落库策略。客户版文档另行保持“自有协议分析 / 协议适配 / 行情源服务器”的对外表述。
开发周期固定为 **4 周**；本文档不再采用 8 周排期，也不把第 5～8 周作为一期交付范围。

## 2. 一期建设范围

### 2.1 一期接口范围

| 类别 | 接口能力 | 说明 |
|---|---|---|
| 实时快照 | 批量快照行情 | 支持多股票批量查询，优先走热点缓存 |
| 五档盘口 | 买一至买五、卖一至卖五 | 通过 gotdx 五档快照接口获取，支持单标的或批量查询 |
| 当日分笔 | 当日分笔成交分页查询 | 通过 gotdx 当日分笔接口获取，支持 start/count 或 cursor 分页 |
| 历史分笔 | 指定交易日历史分笔查询 | 回测高频使用，必须按交易日持久化，支持分页、全量回补和本地缓存 |
| K线 | 日 K、分钟 K | 回测核心数据，支持周期、起止位置、数量控制和本地增量补齐 |
| 复权 K线 | 前复权、后复权、不复权 | 支持 gotdx 服务端复权参数；后续可扩展本地复权因子 |
| 除权除息 | 除权除息事件 | 支持按标的查询，日级缓存，用于复权和回测校验 |
| 基础信息 | 股票代码、名称、市场、类型 | 支持本地持久化和定时刷新 |
| 财务/F10 | 财务摘要、公司资料 | 非回测主链路，强缓存、低优先级 |
| 运维接口 | 健康检查、指标输出 | 支持监控和故障定位 |

### 2.2 一期性能目标

| 指标 | 目标 |
|---|---:|
| 对外总吞吐 | ≥ 1000 QPS |
| API 成功率 | ≥ 99.9% |
| 热点快照缓存命中 p95 | ≤ 50ms |
| 普通快照 p95 | ≤ 200ms |
| 当日分笔 p95 | ≤ 500ms，小分页或缓存命中优先 |
| 历史分笔 p95 | ≤ 300ms，本地库命中优先；miss 后异步回补 |
| K线 p95 | ≤ 200ms，本地库命中优先；miss 后增量补齐 |
| 服务可用性 | ≥ 99.9% |
| 历史数据本地命中率 | ≥ 95%，回测主链路不直接依赖实时上游 |
| 热点缓存命中率 | ≥ 80%，按真实流量校准 |
| 长稳压测 | 1000 QPS 持续 2～4 小时，包含历史分笔/K线高占比混合流量 |

### 2.3 一期不做范围

以下能力可作为二期扩展，不纳入一期主交付：

1. Web 管理后台；
2. 多租户计费系统；
3. 实时推送长连接服务；
4. 多行情源融合校验；
5. 全市场多年历史数据一次性大规模回补；
6. 5000～10000 QPS 扩容方案；
7. 复杂权限审计系统。

## 3. 总体架构设计

### 3.1 架构分层

```text
外部调用方
  │
  ▼
API 服务层
认证 / 参数校验 / 限流 / 统一错误码 / 请求追踪
  │
  ▼
业务服务层
HistoryQueryService / QuoteService / TickService / KlineService / AdjustService / SecurityService / FinanceService
  │
  ▼
历史数据存储与缓存层
历史分笔库 / K线库 / 复权基础数据 / 热点快照缓存 / singleflight 请求合并
  │
  ▼
调度与连接池层
gotdx client pool / 按接口分池 / 多行情服务器节点 / 健康评分 / 熔断 / 重连 / 动态权重
  │
  ▼
gotdx 协议适配层
gotdx API 封装 / 请求参数转换 / 响应标准化 / 错误分类 / 指标埋点
  │
  ▼
行情源服务器集群
主行情节点 / 扩展行情节点 / 备用节点
```

### 3.2 核心设计决策

#### 决策 1：回测历史数据走本地优先，不直接透传上游

系统初期核心场景是支撑回测，历史分笔和 K线会被高频读取。回测请求必须优先命中本地数据库或缓存；只有缺口数据、增量补齐和后台刷新任务才访问上游行情源。

目标链路：

```text
回测请求
  -> HistoryQueryService
  -> 本地历史分笔/K线库
  -> 命中后直接返回
  -> miss 时创建回补任务并按策略同步/异步补齐
```

对外 1000 QPS 不直接等于上游 1000 QPS。实时快照通过缓存、请求合并和批量查询削峰；历史数据通过本地化承载。

#### 决策 2：按接口类型拆分连接池，历史数据池提高优先级

快照、分笔、历史分笔、K线、F10 等接口耗时不同，必须隔离资源。由于一期目标支撑回测，历史分笔和 K线不再按“低频接口”处理，而是作为核心读取链路建设本地存储和独立回补池。

| 连接池 | 用途 | 并发策略 |
|---|---|---|
| quote-pool | 快照、五档盘口、批量报价 | 高频、低延迟、批量优先 |
| tick-pool | 当日分笔 | 中高频、分页拉取、单标的刷新限频 |
| history-pool | 历史分笔、历史成交、历史分时回补 | 回测核心数据回补池，中等并发、强缓存、队列化 |
| kline-pool | K线、复权 K线回补 | 回测核心数据回补池，中等并发、本地库优先 |
| static-pool | F10、财务、文件类低频信息 | 很低并发、强缓存 |
| adjust-pool | 除权除息、复权基础数据 | 定时刷新，用于回测复权校验 |

#### 决策 3：多行情服务器并行接入

系统启动时加载多组行情源服务器，并为每组服务器建立连接池。

调度依据：

1. 节点连接状态；
2. 最近响应延迟；
3. 最近错误率；
4. 熔断状态；
5. 当前连接池负载；
6. 接口类型匹配程度。

节点健康评分示例：

```text
health_score = latency_score * 0.4
             + success_rate_score * 0.4
             + recent_error_score * 0.2
```

#### 决策 4：历史类数据是回测主链路，必须本地化和可批量读取

历史分笔、K线、复权基础数据是回测高频访问数据。系统必须提供本地持久化、批量查询、按交易日分区、缺口检测和后台回补能力；实时访问 gotdx 只用于数据补齐，不作为回测查询主路径。

## 4. 模块设计

### 4.1 API 服务层

职责：

1. 暴露 REST/gRPC 接口；
2. 参数校验和默认值处理；
3. 统一错误码；
4. API 级限流；
5. 客户级限流；
6. 请求追踪 ID；
7. 访问日志；
8. 屏蔽内部行情源协议细节。

建议 REST 接口：

| 接口 | 方法 | 说明 |
|---|---|---|
| `/api/v1/quotes` | GET/POST | 批量快照行情 |
| `/api/v1/orderbook` | GET/POST | 五档盘口 |
| `/api/v1/ticks` | GET | 当日分笔成交 |
| `/api/v1/history-ticks` | GET | 历史分笔成交 |
| `/api/v1/kline` | GET | K线 |
| `/api/v1/adjusted-kline` | GET | 复权 K线 |
| `/api/v1/xdxr` | GET | 除权除息事件 |
| `/api/v1/securities` | GET | 股票基础信息 |
| `/api/v1/finance` | GET | 财务摘要 |
| `/api/v1/trading-day` | GET | 交易日状态 |
| `/api/v1/health` | GET | 健康检查 |
| `/api/v1/metrics` | GET | 监控指标，按部署策略决定是否公开 |

### 4.2 业务服务层

| 服务 | 职责 |
|---|---|
| HistoryQueryService | 回测历史数据查询入口；优先读取本地历史分笔/K线库，负责覆盖状态判断和缺口回补触发 |
| QuoteService | 快照、五档盘口、批量报价、热点刷新 |
| TickService | 当日分笔、历史分笔、分页查询、全量回补 |
| KlineService | 日 K、分钟 K、历史 K线、本地增量补齐 |
| AdjustService | 前复权、后复权、除权除息事件处理 |
| SecurityService | 代码表、市场、证券类型、基础状态 |
| FinanceService | 财务摘要、F10、公司资料 |
| SourceHealthService | gotdx 行情源节点健康、延迟、错误率、熔断状态 |

### 4.3 gotdx 接入与协议适配层

本层直接依赖 `github.com/bensema/gotdx`，是行情源访问的唯一入口。业务层只依赖统一 `MarketDataProvider` 接口，不直接调用 gotdx，便于后续替换协议实现、增加缓存回放源或接入自研协议栈。

源码依据：

1. `gotdx/client.go:27-40` 提供 `New`、`NewEx`、`NewMAC`、`NewMACEx` 四类客户端构造函数；一期主要使用 `New`，扩展行情预留 `NewEx`。
2. `gotdx/options.go:12-23` 支持主行情、扩展行情、MAC 地址池、自动测速、重试次数和超时配置。
3. `gotdx/client.go:63-78` 提供 `ProbeHosts`、`FastestHost`，用于启动探测和节点排序。
4. `gotdx/client_helpers.go:12-17` 的 `executeProtocol` 对单个 `Client` 加 `sync.Mutex`，所以单 client 请求会串行。
5. `gotdx/client.go:236-308` 的 `exchange` 是一次写请求、同步读响应的模型；高并发必须依赖多 client、多连接、多节点池化。

建议接口抽象：

```go
type MarketDataProvider interface {
    GetQuotes(ctx context.Context, symbols []Symbol) ([]Quote, error)
    GetOrderBook(ctx context.Context, symbols []Symbol) ([]OrderBook, error)
    GetTicks(ctx context.Context, symbol Symbol, req TickRequest) ([]Tick, error)
    GetHistoryTicks(ctx context.Context, symbol Symbol, req HistoryTickRequest) ([]Tick, error)
    GetKLine(ctx context.Context, symbol Symbol, req KLineRequest) ([]Bar, error)
    GetAdjustedKLine(ctx context.Context, symbol Symbol, req AdjustedKLineRequest) ([]Bar, error)
    GetXDXR(ctx context.Context, symbol Symbol) ([]XDXREvent, error)
    GetSecurityInfo(ctx context.Context, req SecurityQuery) ([]SecurityInfo, error)
    GetFinance(ctx context.Context, symbol Symbol) (*FinanceInfo, error)
}
```

gotdx 初始化示例：

```go
opts := []gotdx.Option{
    gotdx.WithTCPAddressPool(mainAddrs...),
    gotdx.WithTimeoutSec(3),
    gotdx.WithAutoSelectFastest(true),
}

cli := gotdx.New(opts...)
_, err := cli.Connect()
```

接入步骤：

1. 在项目模块中引入 `github.com/bensema/gotdx`、`github.com/bensema/gotdx/types` 和需要转换的 `proto` 类型。
2. 配置主行情地址池，优先使用内置 `MainHostAddresses` / `BrokerHostAddresses`，同时允许配置文件覆盖。
3. 启动时执行 `ProbeHosts` / `FastestHost`，生成节点延迟、可达性和初始权重。
4. 按接口类型创建 gotdx client pool；池内每个连接都是独立 `*gotdx.Client`，不得把一个 client 当作并发多路复用连接使用。
5. 每个 client 创建后执行 `Connect`；扩展行情能力需要单独通过 `NewEx` / `ConnectEx` 初始化。
6. 对 gotdx 返回的 `proto.SecurityQuote`、`proto.TransactionData`、`proto.HistoryTransactionData`、`proto.SecurityBar`、`proto.GetXDXRInfoReply` 做统一模型转换。
7. 错误分类为连接失败、读写超时、无可达节点、协议解析失败、空数据、上游限流/异常数据；分别进入重试、熔断、降级或回补队列。
8. 服务关闭时逐个调用 `Disconnect`，避免连接泄漏。

gotdx API 映射：

| 业务能力 | gotdx API | 实现说明 |
|---|---|---|
| 批量快照 | `StockQuotesDetail` / `GetQuotesDetail` / `StockQuotes` | 优先使用批量接口，按市场拆组后合并结果 |
| 五档盘口 | `GetSecurityQuotes` / `StockQuotesDetail` | 读取 `BidLevels`、`AskLevels` 转为统一 `OrderBook` |
| 当日分笔 | `StockTransaction` / `StockFullTransaction` | 小分页走 `StockTransaction`，全量回补走 `StockFullTransaction` |
| 历史分笔 | `StockHistoryTransaction` / `StockHistoryFullTransaction` / `StockHistoryTransactionWithTrans` / `StockHistoryFullTransactionWithTrans` | 回测核心数据，拉取后必须落库；优先使用本地查询 |
| K线 | `GetKLine` / `StockKLine` / `StockFullKLine` / `StockKLineOffset` | 用于历史回补和缺口补齐，回测查询走本地库 |
| 复权 K线 | `GetKLine(... adjust)` / `StockKLine(... adjust)` + `types.AdjustNone` / `types.AdjustQFQ` / `types.AdjustHFQ` | 一期使用 gotdx 服务端复权参数；保留本地复权因子扩展点 |
| 除权除息 | `GetXDXRInfo` | 日级缓存，作为复权校验和本地复权扩展基础 |
| 代码表 | `StockCount` / `StockList` / `StockAll` | 启动或定时同步，写入本地证券表 |
| 财务/F10 | `GetFinanceInfo` / `GetCompanyCategories` / `GetCompanyContent` / `StockF10` | 非回测主链路，低优先级池和长 TTL 缓存 |

协议适配层交付代码建议拆分：

```text
internal/provider/gotdx_provider.go       // MarketDataProvider 实现
internal/provider/gotdx_mapper.go         // proto -> domain model 转换
internal/provider/gotdx_errors.go         // 错误分类、重试判断
internal/source/gotdx_pool.go             // gotdx client 池
internal/source/host_probe.go             // 节点测速、健康评分
internal/history/backfill_gotdx.go        // 历史分笔/K线回补任务
```

### 4.4 调度与连接池层

#### 4.4.1 连接池结构

```text
SourceManager
  ├── quote-pool
  │     ├── server A connections
  │     ├── server B connections
  │     └── server C connections
  ├── tick-pool
  ├── history-pool
  ├── kline-pool
  ├── static-pool
  └── adjust-pool
```

#### 4.4.2 初始连接配置

| 连接池 | gotdx client 类型 | 行情源节点数 | 每节点连接数 | 初始总连接数 | 说明 |
|---|---|---:|---:|---:|---|
| quote-pool | `gotdx.New` | 5 | 4 | 20 | 快照、五档盘口、批量报价 |
| tick-pool | `gotdx.New` | 4 | 2 | 8 | 当日分笔分页 |
| history-pool | `gotdx.New` | 4 | 2 | 8 | 历史分笔、历史成交、历史分时回补 |
| kline-pool | `gotdx.New` | 4 | 2 | 8 | K线、复权 K线回补 |
| adjust-pool | `gotdx.New` | 3 | 1 | 3 | XDXR、复权基础数据 |
| static-pool | `gotdx.New` | 2 | 1 | 2 | 财务、F10、文件类信息 |
| ex-pool | `gotdx.NewEx` | 2 | 1 | 2 | 扩展行情预留，一期按需启用 |

该配置是一期压测起点。由于单个 gotdx `Client` 内部串行执行协议请求，池内连接数必须按独立 client 数量计算；不能把一个 `Client` 暴露给多个 goroutine 后期待并行吞吐。

#### 4.4.3 熔断策略

节点进入熔断的条件：

1. 连续连接失败超过阈值；
2. 最近窗口错误率超过阈值；
3. 最近窗口 p95 延迟超过阈值；
4. 返回解析错误持续出现；
5. 读写超时持续出现。

熔断恢复流程：

```text
closed 正常
  │ 错误率升高
  ▼
open 熔断
  │ 冷却时间到
  ▼
half-open 半开探测
  │ 成功达到阈值
  ▼
closed 恢复
```

### 4.5 缓存与聚合层

#### 4.5.1 缓存策略

| 数据 | 缓存策略 |
|---|---|
| 热点快照 | 内存 + Redis，100ms～1000ms TTL，后台刷新 |
| 五档盘口 | 内存 + Redis，100ms～1000ms TTL，批量刷新 |
| 批量报价 | 按 market + code 缓存，批量合并刷新 |
| 当日分笔 | 按 symbol + page/cursor 短 TTL 缓存，必要时日内落库 |
| 历史分笔 | 本地库为主，按 symbol + trade_date 分区；Redis 缓存热门日分页索引 |
| K线 | 本地库为主，按 symbol + period + adjust_type 存储；热点回测区间进入 Redis/内存 |
| 除权除息 | 日级缓存 + 本地表，作为复权校验数据 |
| F10/财务 | 长 TTL 缓存，低频刷新 |

#### 4.5.2 请求合并策略

1. 同一 symbol 的并发刷新请求只允许一个进入上游；
2. 批量报价按 market 分组；
3. 热点 symbol 后台定时刷新；
4. 冷门 symbol 按需刷新；
5. 历史分笔按 symbol + trade_date 建立回补任务，避免重复拉取；
6. K线按 symbol + period + adjust_type 建立缺口补齐任务；
7. 回测批量查询优先合并为本地库范围查询，不逐标的逐日访问上游；
8. 缓存 miss 时先查历史覆盖表，确认缺口后再进入 gotdx 回补队列。

### 4.6 本地数据存储层

建议一期数据存储：

| 数据 | 存储方式 | 说明 |
|---|---|---|
| 股票代码表 | MySQL/PostgreSQL | 定时更新，支持 market/code 唯一索引 |
| 交易日历 | MySQL/PostgreSQL | 定时更新，回测查询和回补任务依赖 |
| K线 | MySQL/PostgreSQL 起步，数据量扩大后迁移列式库 | 按 symbol + period + adjust_type + time 建联合索引，支持区间扫描 |
| 当日分笔 | Redis 短缓存 + 可选日内落库 | 高频分页，收盘后可转历史分笔表 |
| 历史分笔 | MySQL/PostgreSQL 分区表或列式库 | 按 trade_date 分区，按 symbol + trade_time 建索引，回测主链路 |
| 历史覆盖表 | MySQL/PostgreSQL | 记录 symbol/date/period/adjust_type 覆盖状态、水位和校验摘要 |
| 除权除息 | MySQL/PostgreSQL | 日级更新，支持复权校验和本地因子扩展 |
| 财务/F10 | MySQL/PostgreSQL | 低频更新 |
| 热点快照 | 内存 + Redis | 短 TTL |
| 回补任务 | MySQL/PostgreSQL + 队列 | 记录缺口、优先级、重试次数、错误原因 |

### 4.7 回测历史数据链路

一期系统的第一优先级不是把所有 gotdx 接口实时透传出去，而是把回测常用历史数据沉淀到本地。历史分笔、K线、复权基础数据按“先覆盖、再服务、持续补齐”的策略建设。

#### 4.7.1 查询链路

```text
Backtest Client
  -> /api/v1/history-ticks 或 /api/v1/kline
  -> HistoryQueryService
  -> 历史覆盖表检查
  -> 本地历史分笔/K线分区表
  -> Redis/内存热点区间缓存
  -> 返回数据 + coverage 状态
```

#### 4.7.2 miss 处理策略

| miss 类型 | 处理方式 |
|---|---|
| 小缺口 | 同步进入 gotdx 回补，完成后写库并返回 |
| 大缺口 | 创建后台回补任务，接口返回缺口状态或按业务策略等待 |
| 上游超时 | 保留任务，指数退避重试，避免阻塞回测主线程 |
| 多请求同一缺口 | singleflight 合并为一个回补任务 |
| 非交易日或无数据 | 写入覆盖表，避免重复请求上游 |

#### 4.7.3 回补任务优先级

1. 用户正在回测的 symbol/date 区间最高优先级；
2. 当日新增数据和最近 N 个交易日次高优先级；
3. 常用指数、ETF、核心股票池预热；
4. 全市场长期历史回补放入二期或离线任务，不阻塞一期交付。

#### 4.7.4 数据一致性

1. 每次 gotdx 回补写入 `source_address`、`fetch_time`、`raw_count`、`checksum`；
2. 历史分笔按 `symbol + trade_date` 做覆盖水位；
3. K线按 `symbol + period + adjust_type` 做覆盖水位；
4. 除权除息更新后标记受影响 K线区间，触发复权数据重算或重新拉取；
5. API 返回 `cached`、`coverage_status`、`source_time`，便于回测侧判断数据质量。

## 5. 数据模型设计

### 5.1 Symbol

| 字段 | 类型 | 说明 |
|---|---|---|
| market | string | 市场 |
| code | string | 证券代码 |
| name | string | 证券名称 |
| type | string | 股票、ETF、指数、债券等 |
| status | string | 正常、停牌、退市等 |

### 5.2 Quote

| 字段 | 说明 |
|---|---|
| symbol | 标准证券标识 |
| last_price | 最新价 |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| pre_close | 昨收价 |
| volume | 成交量 |
| amount | 成交额 |
| bid_levels | 买一至买五 |
| ask_levels | 卖一至卖五 |
| quote_time | 行情时间 |
| source_time | 源数据时间 |
| cached | 是否来自缓存 |

### 5.3 Tick

| 字段 | 说明 |
|---|---|
| symbol | 标准证券标识 |
| trade_date | 交易日期，历史分笔必填 |
| trade_time | 成交时间 |
| price | 成交价 |
| volume | 成交量 |
| amount | 成交额 |
| direction | 买卖方向，按行情源能力标准化 |
| sequence | 分笔序号或页内顺序 |
| source | 数据来源 |
| cached | 是否来自缓存 |

### 5.4 Bar

| 字段 | 说明 |
|---|---|
| symbol | 标准证券标识 |
| period | 周期 |
| adjust_type | 不复权 / 前复权 / 后复权 |
| time | K线时间 |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| close | 收盘价 |
| volume | 成交量 |
| amount | 成交额 |
| source | 数据来源 |

### 5.5 XDXREvent

| 字段 | 说明 |
|---|---|
| symbol | 标准证券标识 |
| event_date | 事件日期 |
| event_type | 分红、送转、配股、扩缩股等 |
| cash_dividend | 现金分红 |
| bonus_share | 送转股比例 |
| allotment_price | 配股价 |
| allotment_ratio | 配股比例 |
| raw_fields | 原始扩展字段，便于追踪 |

### 5.6 HistoryCoverage

| 字段 | 说明 |
|---|---|
| dataset | `history_tick` / `kline` / `adjusted_kline` |
| symbol | 标准证券标识 |
| trade_date | 交易日期；K线区间可为空 |
| period | K线周期，历史分笔为空 |
| adjust_type | 不复权 / 前复权 / 后复权 |
| status | covered / partial / missing / failed |
| row_count | 本地已覆盖记录数 |
| checksum | 数据校验摘要 |
| source_address | 最近一次 gotdx 行情源地址 |
| last_fetch_time | 最近一次拉取时间 |
| last_error | 最近一次错误原因 |

### 5.7 BackfillTask

| 字段 | 说明 |
|---|---|
| task_id | 回补任务 ID |
| dataset | 回补数据集 |
| symbol | 标准证券标识 |
| start_date | 起始日期 |
| end_date | 结束日期 |
| period | K线周期 |
| adjust_type | 复权类型 |
| priority | 回测即时缺口最高，预热任务次之 |
| status | pending / running / success / failed / retrying |
| retry_count | 重试次数 |
| next_retry_time | 下次重试时间 |
| error_message | 错误信息 |

## 6. 开发计划

### 6.1 周期估算

一期开发周期固定为：**4 周**（不是 8 周）。  
建议人员：项目负责人/架构师 1 人，后端工程师 3 人，测试/QA 1 人，运维/DevOps 1 人。

4 周为压缩交付周期，需采用并行开发：gotdx 接入、服务接口、历史数据本地化、缓存削峰、存储与压测同步推进。管理后台、多租户计费、全市场多年历史数据大规模回补、实时推送等能力不纳入一期，作为二期扩展。

### 6.2 阶段拆解

| 阶段 | 周期 | 目标 | 主要任务 | 交付物 |
|---|---:|---|---|---|
| 阶段 1：需求冻结、gotdx 接入与历史库设计 | 第 1 周 | 明确范围并打通 gotdx 基础链路 | 接口确认、回测流量模型确认、部署环境确认、行情源节点清单确认、gotdx 模块引入、`New`/`Connect`/`ProbeHosts` 验证、Provider 接口设计、历史分笔/K线/覆盖表/回补任务表设计、基础连接池 | 需求规格、详细设计、gotdx 接入模块、历史库表结构、连接池原型 |
| 阶段 2：核心行情与历史回补开发 | 第 2 周 | 完成主要业务 API 和回测数据写入链路 | 快照、五档盘口、批量报价、当日分笔、历史分笔、K线、复权 K线、XDXR、基础资料接口、proto 到统一模型转换、历史分笔/K线回补任务、落库与覆盖水位 | 初版行情服务、核心 API、统一模型、历史回补链路 |
| 阶段 3：本地查询加速、缓存削峰与连接池调优 | 第 3 周 | 支撑历史高频读取和 1000 QPS 混合流量 | 热点缓存、历史区间缓存、分页缓存、请求合并、批量聚合、接口限流、上游削峰、history/kline pool 调优、除权除息、财务/F10、定时刷新任务 | 缓存聚合模块、限流模块、历史查询优化、任务模块 |
| 阶段 4：可观测性、历史重载压测与验收 | 第 4 周 | 达到 1000 QPS 目标并完成交付 | 指标、日志、链路追踪、gotdx 错误分类指标、熔断、降级、重连、节点健康状态、功能测试、历史高占比混合压测、长稳测试、故障演练、参数调优 | 监控指标、故障处理能力、历史压测报告、验收报告、部署文档 |

### 6.3 里程碑

| 里程碑 | 时间 | 验收标准 |
|---|---:|---|
| M1：gotdx 基础链路与历史库确认 | 第 1 周末 | gotdx 主行情连接、多节点探测、基础请求、Provider 原型、历史分笔/K线表结构和覆盖表确认 |
| M2：核心接口与历史回补可用 | 第 2 周末 | 快照、五档、批量报价、分笔、历史分笔、K线、复权接口联调通过；历史数据可写库并记录覆盖状态 |
| M3：回测查询加速完成 | 第 3 周末 | 本地历史查询、热点缓存、请求合并、分页缓存、限流策略、除权除息、财务/F10 可用 |
| M4：1000 QPS 历史高占比验收 | 第 4 周末 | 监控、日志、熔断、降级、节点切换可用；历史高占比压测达标并输出验收报告 |

## 7. 测试计划

### 7.1 功能测试

1. 快照字段完整性；
2. 五档盘口档位完整性；
3. 批量查询数量一致性；
4. 当日分笔分页、条数、顺序、字段完整性；
5. 历史分笔按交易日查询、分页、全量回补正确性；
6. K线周期、起止、数量正确性；
7. 前复权 / 后复权 / 不复权参数正确性；
8. 除权除息事件字段正确性；
9. 基础资料查询正确性；
10. 财务/F10 缓存和刷新正确性；
11. 异常参数返回错误码正确性。

### 7.2 性能测试

| 场景 | 目标 |
|---|---|
| 热点快照 1000 QPS | 验证缓存命中和服务吞吐 |
| 回测混合接口 1000 QPS | 历史分笔 40%、K线 30%、快照 20%、当日分笔 5%、静态数据 5% |
| 批量报价压测 | 验证请求合并和批量聚合 |
| 分笔分页压测 | 验证 tick-pool 隔离和分页缓存 |
| 历史分笔查询压测 | 验证本地分区表、热点分页缓存和 history-pool 回补队列 |
| K线区间查询压测 | 验证本地 K线区间扫描、复权类型过滤和 kline-pool 回补队列 |
| 缓存失效冲击测试 | 验证 singleflight、防击穿和历史覆盖表 miss 判断 |
| 上游节点故障压测 | 验证 gotdx client 重连、熔断和节点切换 |
| 长稳测试 | 1000 QPS 持续 2～4 小时，历史数据请求占比不低于 70% |

### 7.3 稳定性测试

1. 单行情服务器断开；
2. 多行情服务器部分不可用；
3. 网络超时；
4. 响应解析异常；
5. Redis 短暂不可用；
6. 数据库短暂不可用；
7. 缓存集中失效；
8. 高峰流量突增；
9. 历史分笔大分页请求冲击；
10. 上游请求持续超时。

### 7.4 验收指标

| 指标 | 验收值 |
|---|---:|
| 总 QPS | ≥ 1000 QPS |
| API 成功率 | ≥ 99.9% |
| 快照接口 p95 | ≤ 200ms |
| 热点缓存命中 p95 | ≤ 50ms |
| 分笔接口 p95 | ≤ 500ms |
| 历史分笔接口 p95 | ≤ 300ms，本地库命中 |
| K线接口 p95 | ≤ 200ms，本地库命中 |
| 历史数据本地命中率 | ≥ 95% |
| gotdx 上游错误可观测性 | 按节点、接口、错误类型可查询 |
| 错误率 | ≤ 0.1% |
| 节点故障恢复 | 自动切换，不影响整体服务 |
| 监控完整性 | 核心指标均可观测 |
| 长稳测试 | 2～4 小时无持续错误放大、连接泄漏、内存泄漏 |

## 8. 部署计划

### 8.1 推荐拓扑

```text
负载均衡
  │
  ├── 行情服务实例 1
  ├── 行情服务实例 2
  ├── 行情服务实例 3
  │
  ├── Redis / 缓存集群
  ├── MySQL/PostgreSQL 数据库
  ├── Prometheus / Grafana 监控
  └── 日志系统
```

### 8.2 初始资源建议

| 组件 | 建议 |
|---|---|
| 行情服务实例 | 3 台起，便于高可用 |
| 单实例 CPU | 4 核以上 |
| 单实例内存 | 8GB 以上 |
| Redis | 主从或集群模式 |
| 数据库 | MySQL/PostgreSQL 主从，按数据量评估 |
| 监控 | Prometheus + Grafana |
| 日志 | Loki / ELK / 云日志服务 |

实际资源以压测报告为准。

## 9. 风险与应对

| 风险 | 影响 | 应对 |
|---|---|---|
| 行情源服务器限流 | 上游请求失败、超时 | 缓存削峰、多节点分散、限流、熔断、降低刷新频率 |
| 单个 gotdx Client 串行 | 并发吞吐不足 | 每个连接池创建多个独立 `*gotdx.Client`，按接口类型隔离 |
| gotdx 协议字段变化 | 解析异常 | Provider 层隔离，保留原始包样本、响应样本和回归测试 |
| 热点请求集中 | 缓存击穿、上游压力增大 | singleflight、热点预热、短 TTL 缓存 |
| 历史数据 miss 集中爆发 | 回补队列堆积、回测阻塞 | 覆盖表、优先级队列、预热任务、同步/异步 miss 策略 |
| 历史分笔数据量大 | 查询变慢、存储膨胀 | 按交易日分区、冷热分层、分页限制、列式库预案 |
| 多节点质量不稳定 | 延迟波动 | 节点健康评分、动态权重、熔断半开探测 |
| 1000 QPS 混合流量比例不明确 | 压测失真 | 以回测历史请求高占比作为主压测模型，并保留快照热点模型 |
| 本地缓存与行情源数据短暂不一致 | 用户看到旧数据 | 返回数据时间戳、覆盖状态和缓存标记，关键接口支持强制刷新参数 |
| 数据库成为瓶颈 | 历史分笔/K线查询变慢 | 缓存、索引优化、读写分离、必要时引入列式存储 |

## 10. 交付物

1. 行情服务后端程序；
2. gotdx 接入模块；
3. gotdx client 连接池与节点健康模块；
4. MarketDataProvider 统一接口与模型转换模块；
5. 历史分笔/K线本地存储表结构；
6. 历史覆盖表与回补任务模块；
7. API 接口文档；
8. 缓存与限流配置说明；
9. 部署文档；
10. 运维监控说明；
11. 压测脚本；
12. 1000 QPS 历史高占比压测报告；
13. 故障演练记录；
14. 验收报告。

## 11. 实施建议

一期实施建议采用“先 gotdx 基础链路和历史数据闭环，后扩展能力”的方式推进：

1. 先打通 gotdx 多行情服务器连接、节点探测、独立 client 池和 Provider 封装；
2. 同步完成历史分笔/K线/覆盖表/回补任务表设计，保证回测主链路本地优先；
3. 再实现快照、五档、当日分笔、历史分笔、K线、复权和 XDXR 接口；
4. 然后补齐缓存、请求合并、限流、熔断和 gotdx 错误分类指标；
5. 最后进行历史数据占比不低于 70% 的 1000 QPS 混合压测和故障演练。

系统设计重点不是把所有请求直接透传给行情源，而是通过本地历史库、缓存、削峰、聚合和容错，使回测侧获得稳定的高频历史数据读取能力，同时把 gotdx 上游连接压力控制在安全范围内。
