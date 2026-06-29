# TDX Go 库框架与接口对比：`injoyai/tdx` vs `bensema/gotdx`

日期：2026-06-24  
分析对象：

- `/Users/regan/work/go/src/github.com/injoyai/tdx`
- `/Users/regan/work/go/src/github.com/bensema/gotdx`

## 0. 总结结论

`injoyai/tdx` 和 `bensema/gotdx` 都是 Go 语言 TDX 客户端库，但定位不同：

- `injoyai/tdx` 更像“TDX SDK + 数据工程/业务层”。除协议请求外，它提供代码表、交易日、股本变迁 `gbbq`、本地复权、换手率、任务管理、连接池等上层能力。
- `bensema/gotdx` 更像“协议 SDK + 高层统一 wrapper”。它覆盖主行情、扩展行情、MAC、Goods、文件、表格、host 探测等更多协议面，结构更偏低层协议完整性和统一 API 封装。
- `gotdx` 不是“没有五档快照”或“没有复权”。它有五档盘口快照解析，也有服务端 K线前复权/后复权参数和 XDXR 查询。
- 但 `gotdx` 没有看到 `injoyai/tdx` 这种本地 `gbbq` 因子管理、本地仿射复权、流通股本/换手率计算的业务层。

## 1. `gotdx` 的五档快照能力

结论：`gotdx` 提供五档盘口快照。

### 证据

- `/Users/regan/work/go/src/github.com/bensema/gotdx/proto/get_security_quotes.go:24-50`
  - `GetSecurityQuotesReply.List []SecurityQuote`
  - `SecurityQuote.BidLevels []Level`
  - `SecurityQuote.AskLevels []Level`
  - `Bid1..Bid5`
  - `Ask1..Ask5`
  - `BidVol1..BidVol5`
  - `AskVol1..AskVol5`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/proto/get_security_quotes.go:198-230`
  - 将五档买卖盘解析到 `BidLevels` 和 `AskLevels`。
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_quote.go:80-82`
  - `GetSecurityQuotes`，注释描述为获取盘口五档报价。

### 判断

`gotdx` 的主行情快照不是只有最新价/涨跌幅，也包含买一到买五、卖一到卖五及对应量。

## 2. `gotdx` 的复权能力

结论：`gotdx` 有服务端 K线复权参数，支持前复权和后复权；同时有 XDXR 查询接口。但它没有看到 `injoyai/tdx` 风格的本地 `gbbq` 因子管理和本地复权业务层。

### 证据

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_quote.go:118-127`
  - `GetKLine(category, market, code, start, count, times, adjust)`
  - 将 `adjust` 传入 `GetSecurityBarsRequest.Adjust`。
- `/Users/regan/work/go/src/github.com/bensema/gotdx/types/constants.go:224-228`
  - `AdjustNone uint16 = 0`
  - `AdjustQFQ uint16 = 1`
  - `AdjustHFQ uint16 = 2`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_company.go:35-41`
  - `GetXDXRInfo(market, code)` 查询除权除息信息。

### 判断

`gotdx` 支持：

- 不复权 K线；
- 服务端前复权 K线；
- 服务端后复权 K线；
- 单股票 XDXR 除权除息信息查询。

`gotdx` 未体现：

- 本地维护全市场 `gbbq`；
- 本地计算复权因子；
- 对任意本地历史 K线做前/后复权；
- 基于本地股本历史计算换手率。

这些能力更接近 `injoyai/tdx` 的业务层。

## 3. `gbbq` 的含义及其与复权/除权除息的关系

`gbbq` 通常指“股本变迁”。在 TDX 语境中，它常对应 `gbbq.zip/gbbq` 数据，记录股票股本变化和除权除息事件。

典型记录字段形态：

```text
code + date + category + C1 + C2 + C3 + C4
```

常见 `category` 语义：

| category | 含义 | 常见字段语义 |
|---|---|---|
| 1 | 除权除息 / XRXD | 分红 元/10股、配股价、送转股/10股、配股/10股 |
| 2 / 3 / 5 / 7 / 8 / 9 / 10 | 股本快照 | 盘前流通、前总股本、盘后流通、后总股本 |
| 6 | 增发新股 | 增发相关字段 |
| 11 / 12 | 扩股 / 缩股 | 扩缩股比例相关字段 |
| 13 / 14 | 权证 | 权证相关字段 |

`gbbq` 的主要用途：

1. 本地前复权 / 后复权；
2. 构建或更新本地复权因子；
3. 查询历史流通股本；
4. 计算换手率；
5. 缓存全市场股本变化和除权除息事件。

### 两库差异

- `injoyai/tdx`：用 `Gbbq` 做本地复权、股本查询、换手率和缓存更新。
- `gotdx`：用 `GetXDXRInfo` 查除权除息，用 `GetKLine(... adjust)` 请求服务端复权 K线。

因此：

```text
gbbq ≈ 全市场股本变迁/除权除息事件库
XDXR ≈ 单股票除权除息查询接口
server-side adjust Kline ≈ 服务端直接返回复权后的 K线
```

## 4. 连接 / Client / Host 对比

### `injoyai/tdx`

主要 API：

- `DialDefault`
- `Dial`
- `DialHosts`
- `DialHostsRandom`
- `DialHostsRange`
- `DialWith`
- `DialExHqDefault`
- `DialExHq`
- `DialExHqHosts`
- `WithDebug`
- `WithLevel`
- `WithRedial`
- `SetTimeout`
- `SendFrame`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:35-80`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:80-116`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:247-262`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/client_exhq.go:14-47`

特点：

- 主行情和扩展行情分别 dial；
- 支持 hosts、随机 host、range host；
- `SendFrame` 暴露较底层 frame 调用；
- 内置 `Pool` 和 `Manage` 可做多连接。

### `gotdx`

主要 API：

- `New`
- `NewEx`
- `NewMAC`
- `NewMACEx`
- `Connect`
- `ConnectEx`
- `ConnectMAC`
- `Disconnect`
- `CurrentAddress`
- `ProbeHosts`
- `FastestHost`
- `MainHosts`
- `BrokerHosts`
- `ExHosts`
- `MACHosts`
- `MACExHosts`
- `MainHostAddresses`
- `BrokerHostAddresses`
- `ExHostAddresses`
- `MACHostAddresses`
- `MACExHostAddresses`
- `WithTCPAddress`
- `WithTCPAddressPool`
- `WithExTCPAddress`
- `WithExTCPAddressPool`
- `WithMacTCPAddress`
- `WithMacTCPAddressPool`
- `WithMacExTCPAddress`
- `WithMacExTCPAddressPool`
- `WithTimeoutSec`
- `WithAutoSelectFastest`

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client.go:27-80`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client.go:191-390`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/hosts.go:13-330`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/options.go:12-117`

特点：

- 主行情、扩展行情、MAC、MACEx 模式更细；
- host/address pool 和最快 host 探测更完整；
- 单 `Client` 内部协议调用串行化，稳定但不提供请求连接池。

## 5. 主行情：代码、列表、快照

### `injoyai/tdx`

主要 API：

- `GetCount`
- `GetCode`
- `GetCodeAll`
- `GetStockCodeAll`
- `GetETFCodeAll`
- `GetIndexCodeAll`
- `GetQuote`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:265-429`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/README.md:20-38`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/README.md:96-126`

特点：

- 聚焦 A 股主行情；
- 代码表 API 与后续 `Codes` 管理层打通；
- `GetQuote` 可查行情快照。

### `gotdx`

主要 API：

低层/协议层 wrapper：

- `GetSecurityCount`
- `GetSecurityList`
- `GetSecurityListOld`
- `GetSecurityListRange`
- `GetSecurityFeature452`
- `GetSecurityQuotes`
- `GetQuotesDetail`
- `GetQuotes`
- `GetQuotesEncrypt`
- `GetQuotesList`

高层统一接口：

- `StockCount`
- `StockList`
- `StockAll`
- `StockListOld`
- `StockFeature452`
- `StockQuotesDetail`
- `StockQuotesList`
- `StockQuotes`
- `StockQuotesEncrypt`

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_quote.go:75-314`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_unified.go:62-145`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/README.md:270-281`

特点：

- 主行情 API 形态更细；
- 同时保留低层 `Get*` 和高层 `Stock*`；
- 五档盘口快照字段明确存在。

## 6. K线 / 指数 K线 / 复权

### `injoyai/tdx`

主要 API：

- `GetKline`
- `GetKlineUntil`
- `GetKlineAll`
- `GetKlineMinute`
- `GetKline5Minute`
- `GetKline15Minute`
- `GetKline30Minute`
- `GetKline60Minute`
- `GetKlineHour`
- `GetKlineDay`
- `GetKlineWeek`
- `GetKlineMonth`
- `GetKlineQuarter`
- `GetKlineYear`
- 上述周期的 `All` / `Until` 变体
- `GetIndex`
- `GetIndexUntil`
- `GetIndexAll`
- 指数周期 helper

复权相关：

- `NewGbbq`
- `Gbbq.GetFactors`
- `Gbbq.QFQ`
- `Gbbq.HFQ`
- `Gbbq.QFQKlineDay`
- `Gbbq.HFQKlineDay`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:508-685`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/gbbq.go:137-231`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/README.md:96-126`

特点：

- 周期 helper 非常多；
- 有本地复权因子和本地前/后复权；
- 更适合做本地历史数据仓库。

### `gotdx`

主要 API：

- `GetKLine`
- `GetSecurityBars`
- `GetSecurityBarsOffset`
- `GetIndexBars`
- `StockKLine`
- `StockFullKLine`
- `StockKLineOffset`
- `GetMACSymbolBars`
- `MACSymbolBars`
- `MACKLineOffset`
- `GoodsKLine`

复权常量：

- `types.AdjustNone`
- `types.AdjustQFQ`
- `types.AdjustHFQ`

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_quote.go:118-127`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_unified.go:132-145`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_unified.go:202-231`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_mac.go:30-657`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_goods.go:17-74`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/types/constants.go:224-228`

特点：

- 支持服务端复权参数；
- 覆盖股票、指数、MAC、商品 K线；
- 不提供 `injoyai/tdx` 风格的本地 gbbq 因子管理层。

## 7. 分时 / 分笔 / 成交

### `injoyai/tdx`

主要 API：

- `GetMinute`
- `GetHistoryMinute`
- `GetTrade`
- `GetMinuteTrade`
- `GetTradeAll`
- `GetMinuteTradeAll`
- `GetHistoryTrade`
- `GetHistoryMinuteTrade`
- `GetHistoryTradeDay`
- `GetHistoryMinuteTradeDay`
- `GetHistoryTradeFull`
- `GetHistoryTradeBefore`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:697-837`

特点：

- 历史分笔、分钟成交 API 较完整；
- 命名更贴近 A 股数据采集场景。

### `gotdx`

主要 API：

- `GetMinuteTimeData`
- `GetTickChart`
- `GetHistoryMinuteTimeData`
- `GetHistoryTickChart`
- `GetTransactionData`
- `GetHistoryOrders`
- `GetHistoryTransactionData`
- `GetHistoryTransactionDataWithTrans`
- `StockTickChart`
- `StockHistoryTickChart`
- `StockTransaction`
- `StockFullTransaction`
- `StockHistoryOrders`
- `StockHistoryTransaction`
- `StockHistoryFullTransaction`
- `StockHistoryTransactionWithTrans`
- `StockHistoryFullTransactionWithTrans`

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_quote.go:75-314`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_unified.go:306-349`

特点：

- 同时覆盖 tick chart、transaction、orders；
- 高层 `Stock*` API 对外更统一。

## 8. 财务 / F10 / 除权除息 / 股本变迁

### `injoyai/tdx`

主要 API：

- `GetCompanyCategory`
- `GetCompanyContent`
- `GetFinanceInfo`
- `GetGbbq`
- `GetGbbqAll`
- `NewGbbq`
- `Gbbq.GetFactors`
- `Gbbq.QFQ`
- `Gbbq.HFQ`
- `Gbbq.QFQKlineDay`
- `Gbbq.HFQKlineDay`
- `Gbbq.GetEquity`
- `Gbbq.GetTurnover`
- `Gbbq.Update`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:846-1008`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/gbbq.go:137-231`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/README.md:205-220`

特点：

- 明显有本地业务层；
- 能把 gbbq 和 K线、股本、换手率结合起来；
- 更适合构建本地行情/复权数据库。

### `gotdx`

主要 API：

- `GetCompanyCategories`
- `GetCompanyContent`
- `GetFinanceInfo`
- `GetXDXRInfo`
- `GetCompanyInfo`
- `StockF10`

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_company.go:6-51`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_unified.go:545-615`

特点：

- 提供 F10、财务、XDXR 查询；
- 更偏“查询接口”；
- 不像 `injoyai/tdx` 那样提供完整本地 gbbq 管理和复权计算层。

## 9. 板块 / 报表 / 文件 / 配置

### `injoyai/tdx`

主要 API：

- `GetBlockFileRaw`
- `GetBlockData`
- `GetBlockDataWithIndex`
- `GetReportFile`
- `GetZHBFiles`
- `GetTdxZs`
- `GetTdxBk`
- `GetTdxHy`
- `GetTdxStat`
- `GetTdxStat2`
- `GetXgsg`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client.go:846-1008`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/README.md:205-220`

特点：

- 更贴近通达信本地/报表文件语义；
- 与 A 股数据工程结合紧。

### `gotdx`

主要 API：

- `GetFileMeta`
- `DownloadFile`
- `DownloadFullFile`
- `GetBlockFile`
- `GetParsedBlockFile`
- `GetGroupedBlockFile`
- `GetTableFile`
- `GetCSVFile`
- Ex 文件/表格相关 API
- MAC 文件相关 API

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_extras.go:23-101`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_exquote.go:9-230`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_mac.go:30-657`

特点：

- 文件/表格协议覆盖更广；
- 与扩展行情、MAC 协议结合更完整。

## 10. 扩展行情

### `injoyai/tdx`

主要 API：

- `ExMarkets`
- `ExCount`
- `ExInstruments`
- `ExQuote`
- `ExQuoteList`
- `ExBars`
- `ExMinute`
- `ExHistMinute`
- `ExTrade`
- `ExHistTrade`
- `ExBarsRange`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/client_exhq.go:71-167`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/README.md:205-220`

特点：

- 有 TdxExHq 扩展行情；
- API 数量相对克制，重点是行情、K线、分时、成交。

### `gotdx`

主要 API：

- `GetExServerInfo`
- `ExGetCount`
- `ExGetCategoryList`
- `ExGetList`
- `ExGetListExtra`
- `ExGetQuotesList`
- `ExGetQuote`
- `ExGetQuotes`
- `ExGetQuotes2`
- `ExGetKLine`
- `ExGetKLine2`
- `ExGetHistoryTransaction`
- `ExGetTickChart`
- `ExGetHistoryTickChart`
- `ExGetChartSampling`
- `ExGetBoardList`
- `ExGetMapping2562`
- `ExGetTable`
- `ExGetTableDetail`
- 高层：`ExCount`、`ExCategoryList`、`ExList`、`ExListExtra`、`ExQuotesList`、`ExQuote`、`ExQuotes`、`ExQuotes2`、`ExKLine`、`ExKLine2`、`ExHistoryTransaction`、`ExTickChart`、`ExChartSampling`、`ExBoardList`、`ExMapping2562`、`ExTable`、`ExTableDetail`

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_exquote.go:9-230`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_unified.go:617-838`

特点：

- 扩展行情覆盖面明显更广；
- 表格、mapping、board、chart sampling 等接口更多。

## 11. MAC 协议 / 商品语义

### `injoyai/tdx`

当前对比中未看到独立 MAC 协议层，也未看到 `Goods*` 商品语义高层 API。它的扩展行情集中在 `client_exhq.go`。

### `gotdx`

MAC 主要 API：

- `ConnectMAC`
- `MACBoardCount`
- `MACBoardList`
- `MACBoardMembers`
- `MACBoardMembersWithSort`
- `MACBoardMembersQuotes`
- `MACBoardMembersQuotesWithSort`
- `MACBoardMembersQuotesDynamic`
- `MACSymbolBelongBoard`
- `MACQuotes`
- `MACQuotesWithDate`
- `MACSymbolQuotes`
- `MACTransactions`
- `MACTransactionsWithDate`
- `MACAuction`
- `MACTickCharts`
- `MACSymbolInfo`
- `MACCapitalFlow`
- `MACServerInfo`
- `MACKLineOffset`
- `MACMarketMonitor`
- `MACSymbolBars`
- 以及对应低层 `GetMAC*` 函数

Goods 主要 API：

- `GoodsCount`
- `GoodsCategoryList`
- `GoodsList`
- `GoodsVarieties`
- `GoodsQuote`
- `GoodsQuotes`
- `GoodsQuotesList`
- `GoodsKLine`
- `GoodsTickChart`
- `GoodsChartSampling`
- `GoodsHistoryTransaction`

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_mac.go:30-657`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_goods.go:17-74`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/README.md:124-131`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/README.md:302-315`

特点：

- `gotdx` 明显覆盖了更多非普通 A 股主行情协议面；
- 如果要做 MAC 板块、MAC 商品、商品行情，它更完整。

## 12. 数据工程层：代码表 / 交易日 / 股本缓存 / 管理器

### `injoyai/tdx`

主要 API：

- `NewCodes`
- `NewCodesMysql`
- `NewCodesSqlite`
- `Codes.Update`
- `CodesBase.Get*`
- `NewWorkday`
- `NewWorkdayMysql`
- `NewWorkdaySqlite`
- `Workday.Update`
- `Workday.Is`
- `Workday.TodayIs`
- `Workday.Range*`
- `Workday.Iter*`
- `NewPool`
- `Pool.Get`
- `Pool.Put`
- `Pool.Do`
- `Pool.Go`
- `NewManage`
- `NewManageMysql`
- `Manage.RangeStocks`
- `Manage.RangeETFs`
- `Manage.RangeIndexes`
- `Manage.AddWorkdayTask`

证据：

- `/Users/regan/work/go/src/github.com/injoyai/tdx/codes.go:1-220`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/workday.go:1-180`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/pool.go:1-90`
- `/Users/regan/work/go/src/github.com/injoyai/tdx/manage.go:1-220`

特点：

- 这是 `injoyai/tdx` 的核心差异：它不仅是协议 client，还提供数据工程组件；
- 对本地库、MySQL、SQLite、任务调度、代码表、交易日、股本变迁有封装；
- 更像可以直接用于行情数据采集系统的基础框架。

### `gotdx`

`gotdx` 的数据工程层较轻。它主要提供协议调用、高层 wrapper、host 探测、文件/表格解析，不提供 `Codes`、`Workday`、`Gbbq`、`Manage` 这类本地业务管理组件。

证据：

- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_unified.go:62-838`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/client_extras.go:23-101`
- `/Users/regan/work/go/src/github.com/bensema/gotdx/hosts.go:13-330`

## 13. 实用 API 映射

| 需求 | `injoyai/tdx` | `gotdx` | 判断 |
|---|---|---|---|
| 连接主行情 | `DialDefault` / `DialHosts*` | `New` + `Connect` | 两者都有 |
| 连接扩展行情 | `DialExHq*` | `NewEx` / `ConnectEx` | 两者都有 |
| host 探测/最快 host | host range/random | `ProbeHosts` / `FastestHost` | `gotdx` 更完整 |
| 股票数量 | `GetCount` | `GetSecurityCount` / `StockCount` | 两者都有 |
| 股票列表 | `GetCode` / `GetCodeAll` | `GetSecurityList` / `StockList` / `StockAll` | 两者都有 |
| 快照行情 | `GetQuote` | `GetSecurityQuotes` / `StockQuotes*` | 两者都有 |
| 五档盘口 | `GetQuote` 返回模型中包含价格档位能力 | `SecurityQuote.BidLevels` / `AskLevels` | `gotdx` 明确字段更直观 |
| 普通 K线 | `GetKline*` | `GetKLine` / `StockKLine` | 两者都有 |
| 指数 K线 | `GetIndex*` | `GetIndexBars` | 两者都有 |
| 服务端复权 K线 | 未作为主要路径体现 | `AdjustQFQ` / `AdjustHFQ` | `gotdx` 有明确参数 |
| 本地复权因子 | `Gbbq.GetFactors` | 未见同等层 | `injoyai/tdx` 更完整 |
| 本地前/后复权 | `Gbbq.QFQ` / `Gbbq.HFQ` | 未见同等层 | `injoyai/tdx` 更完整 |
| XDXR/除权除息查询 | `GetGbbq` / `Gbbq` | `GetXDXRInfo` | 两者路径不同 |
| 历史分笔 | `GetHistoryTrade*` | `StockHistoryTransaction*` | 两者都有 |
| 分时 | `GetMinute` / `GetHistoryMinute` | `GetMinuteTimeData` / `GetHistoryMinuteTimeData` | 两者都有 |
| F10 | `GetCompanyCategory` / `GetCompanyContent` | `GetCompanyCategories` / `GetCompanyContent` / `StockF10` | 两者都有 |
| 财务 | `GetFinanceInfo` | `GetFinanceInfo` | 两者都有 |
| 板块文件 | `GetBlock*` | `GetBlockFile` / parsed/grouped | 两者都有，`gotdx` 文件解析更细 |
| 扩展行情 | `Ex*` | `Ex*` / `ExGet*` | `gotdx` 覆盖更广 |
| MAC | 未见同等独立层 | `MAC*` / `GetMAC*` | `gotdx` 明显更完整 |
| Goods | 未见同等高层 | `Goods*` | `gotdx` 明显更完整 |
| 代码表本地管理 | `Codes` | 未见同等层 | `injoyai/tdx` 更完整 |
| 交易日本地管理 | `Workday` | 未见同等层 | `injoyai/tdx` 更完整 |
| 连接池 | `Pool` / `Manage.WithClients` | 无请求连接池 | `injoyai/tdx` 内置 |

## 14. 选择建议

### 选 `injoyai/tdx` 的场景

适合：

- 构建本地 A 股行情数据库；
- 需要本地维护代码表、交易日、股本变迁；
- 需要本地前复权/后复权；
- 需要基于股本历史计算换手率；
- 需要任务管理和连接池；
- 主要聚焦 A 股主行情 + 扩展行情 + 本地数据工程。

一句话：如果目标是“采集 + 落库 + 本地复权 + 数据管理”，`injoyai/tdx` 更完整。

### 选 `gotdx` 的场景

适合：

- 需要更广的 TDX 协议覆盖；
- 需要主行情、扩展行情、MAC、Goods；
- 需要五档快照、服务端复权 K线；
- 需要 host 探测、最快 host 选择；
- 希望低层协议对象和高层统一 wrapper 并存；
- 自己会在业务层实现连接池、缓存、数据管理和本地复权。

一句话：如果目标是“协议覆盖面广 + 统一查询 SDK”，`gotdx` 更适合。

## 15. 最短结论

- `gotdx` 有五档快照，也有服务端前复权/后复权 K线支持。
- `gbbq` 是股本变迁数据，和复权、除权除息、流通股本、换手率计算密切相关。
- `injoyai/tdx` 的优势是本地数据工程和本地复权业务层。
- `gotdx` 的优势是协议覆盖面、MAC/Goods/扩展行情、host 探测和统一 wrapper。
- 如果只要拉复权 K线：`gotdx` 可以通过 `AdjustQFQ` / `AdjustHFQ` 完成。
- 如果要本地维护复权因子、处理任意本地历史数据、计算换手率：`injoyai/tdx` 更完整。
