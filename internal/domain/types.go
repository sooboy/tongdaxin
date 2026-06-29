package domain

import (
	"strings"
	"time"
)

// Market is the service-owned exchange/market identifier.
type Market string

const (
	MarketUnknown Market = ""
	MarketSH      Market = "SH"
	MarketSZ      Market = "SZ"
	MarketBJ      Market = "BJ"
	MarketHK      Market = "HK"
	MarketUS      Market = "US"
)

// SecurityType classifies the instrument at the service boundary.
type SecurityType string

const (
	SecurityTypeUnknown SecurityType = "unknown"
	SecurityTypeStock   SecurityType = "stock"
	SecurityTypeETF     SecurityType = "etf"
	SecurityTypeIndex   SecurityType = "index"
	SecurityTypeBond    SecurityType = "bond"
	SecurityTypeFund    SecurityType = "fund"
)

// SecurityStatus describes whether a symbol can be queried normally.
type SecurityStatus string

const (
	SecurityStatusUnknown SecurityStatus = "unknown"
	SecurityStatusActive  SecurityStatus = "active"
	SecurityStatusHalted  SecurityStatus = "halted"
	SecurityStatusDelist  SecurityStatus = "delisted"
)

// Symbol is the canonical security identifier used by service code.
type Symbol struct {
	Market Market
	Code   string
	Name   string
	Type   SecurityType
	Status SecurityStatus
}

// NewSymbol builds a validated symbol with normalized market/code fields.
func NewSymbol(market Market, code string) (Symbol, error) {
	symbol := Symbol{Market: NormalizeMarket(market), Code: NormalizeCode(code)}
	return symbol, symbol.Validate()
}

func NormalizeMarket(market Market) Market {
	return Market(strings.ToUpper(strings.TrimSpace(string(market))))
}

func NormalizeCode(code string) string {
	return strings.TrimSpace(code)
}

func (s Symbol) Validate() error {
	if s.Market == MarketUnknown {
		return ErrInvalidRequest
	}
	if s.Code == "" {
		return ErrInvalidRequest
	}
	return nil
}

func (s Symbol) Key() string {
	market := NormalizeMarket(s.Market)
	code := NormalizeCode(s.Code)
	if market == MarketUnknown {
		return code
	}
	return string(market) + ":" + code
}

// Level is one order-book price level.
type Level struct {
	Price  float64
	Volume int64
}

// Quote is the normalized quote model returned by public service APIs.
type Quote struct {
	Symbol     Symbol
	LastPrice  float64
	Open       float64
	High       float64
	Low        float64
	PreClose   float64
	Volume     int64
	Amount     float64
	BidLevels  []Level
	AskLevels  []Level
	QuoteTime  time.Time
	SourceTime time.Time
	Cached     bool
}

// OrderBook carries five-level order-book data.
type OrderBook struct {
	Symbol     Symbol
	BidLevels  []Level
	AskLevels  []Level
	SourceTime time.Time
	Cached     bool
}

// TickDirection is a normalized transaction side marker.
type TickDirection string

const (
	TickDirectionUnknown TickDirection = "unknown"
	TickDirectionBuy     TickDirection = "buy"
	TickDirectionSell    TickDirection = "sell"
	TickDirectionNeutral TickDirection = "neutral"
)

// Tick is a normalized transaction record. TradeDate is midnight-local for historical ticks.
type Tick struct {
	Symbol    Symbol
	TradeDate time.Time
	TradeTime time.Time
	Price     float64
	Volume    int64
	Amount    float64
	Direction TickDirection
	Sequence  int64
	Source    string
	Cached    bool
}

// Period is the service-owned K-line period identifier.
type Period string

const (
	PeriodUnknown Period = ""
	Period1Min    Period = "1m"
	Period5Min    Period = "5m"
	Period15Min   Period = "15m"
	Period30Min   Period = "30m"
	Period1Hour   Period = "1h"
	PeriodDay     Period = "day"
	PeriodWeek    Period = "week"
	PeriodMonth   Period = "month"
	PeriodQuarter Period = "quarter"
	PeriodYear    Period = "year"
)

// AdjustType is the normalized adjustment mode for bars.
type AdjustType string

const (
	AdjustNone AdjustType = "none"
	AdjustQFQ  AdjustType = "qfq"
	AdjustHFQ  AdjustType = "hfq"
)

// Bar is the normalized K-line model.
type Bar struct {
	Symbol     Symbol
	Period     Period
	AdjustType AdjustType
	Time       time.Time
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     float64
	Amount     float64
	Source     string
}

// XDXREvent is a normalized ex-right/ex-dividend event.
type XDXREvent struct {
	Symbol         Symbol
	EventDate      time.Time
	EventType      string
	CashDividend   float64
	BonusShare     float64
	AllotmentPrice float64
	AllotmentRatio float64
	RawFields      map[string]any
}

// TradingSession describes one continuous market trading session.
type TradingSession struct {
	OpenMinutes  uint16
	CloseMinutes uint16
	Open         string
	Close        string
}

// TradingDayInfo carries low-frequency server trading-day metadata.
type TradingDayInfo struct {
	Today                    time.Time
	TodayString              string
	IsTodayTradingDay        bool
	LatestTradingDay         time.Time
	LatestTradingDayString   string
	PreviousTradingDay       time.Time
	PreviousTradingDayString string
	TradingSessions          []TradingSession
	AlternateTradingSessions []TradingSession
	SourceTime               time.Time
	Cached                   bool
}

// FinanceInfo carries low-frequency finance/F10 fields without exposing provider structs.
type FinanceInfo struct {
	Symbol     Symbol
	Fields     map[string]any
	SourceTime time.Time
	Cached     bool
}

// SecurityInfo carries normalized code-table rows.
type SecurityInfo struct {
	Symbol Symbol
	Fields map[string]any
	Cached bool
}

// CoverageStatus describes local history coverage for a dataset/range.
type CoverageStatus string

const (
	CoverageMissing CoverageStatus = "missing"
	CoveragePartial CoverageStatus = "partial"
	CoverageCovered CoverageStatus = "covered"
	CoverageFailed  CoverageStatus = "failed"
)

// Dataset identifies a locally materialized history dataset.
type Dataset string

const (
	DatasetHistoryTick   Dataset = "history_tick"
	DatasetKLine         Dataset = "kline"
	DatasetAdjustedKLine Dataset = "adjusted_kline"
)

// HistoryCoverage records local completeness for a historical query range.
type HistoryCoverage struct {
	Dataset       Dataset
	Symbol        Symbol
	TradeDate     time.Time
	Period        Period
	AdjustType    AdjustType
	Status        CoverageStatus
	RowCount      int
	Checksum      string
	SourceAddress string
	LastFetchTime time.Time
	LastError     string
}

// BackfillStatus is the lifecycle state for a data backfill task.
type BackfillStatus string

const (
	BackfillPending  BackfillStatus = "pending"
	BackfillRunning  BackfillStatus = "running"
	BackfillSuccess  BackfillStatus = "success"
	BackfillFailed   BackfillStatus = "failed"
	BackfillRetrying BackfillStatus = "retrying"
)

// BackfillTask is a local-first gap-fill unit for history-heavy backtests.
type BackfillTask struct {
	TaskID        string
	Dataset       Dataset
	Symbol        Symbol
	StartDate     time.Time
	EndDate       time.Time
	Period        Period
	AdjustType    AdjustType
	Priority      int
	Status        BackfillStatus
	RetryCount    int
	NextRetryTime time.Time
	ErrorMessage  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
