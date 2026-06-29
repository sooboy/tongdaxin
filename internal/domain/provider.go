package domain

import (
	"context"
	"time"
)

// QuoteRequest controls batch quote lookup.
type QuoteRequest struct {
	Symbols      []Symbol
	ForceRefresh bool
}

// OrderBookRequest controls batch order-book lookup.
type OrderBookRequest struct {
	Symbols      []Symbol
	ForceRefresh bool
}

// TickRequest controls same-day transaction lookup.
type TickRequest struct {
	Start        uint16
	Count        uint16
	Cursor       string
	Full         bool
	ForceRefresh bool
}

// HistoryTickRequest controls historical transaction lookup for one trading day.
type HistoryTickRequest struct {
	TradeDate           time.Time
	Start               uint16
	Count               uint16
	Full                bool
	WithTransactionFlag bool
	ForceRefresh        bool
}

// KLineRequest controls K-line lookup.
type KLineRequest struct {
	Period       Period
	Start        uint16
	Count        uint16
	Times        uint16
	StartDate    time.Time
	EndDate      time.Time
	ForceRefresh bool
}

// AdjustedKLineRequest controls adjusted K-line lookup.
type AdjustedKLineRequest struct {
	KLineRequest
	AdjustType AdjustType
}

// SecurityQuery controls code-table lookup.
type SecurityQuery struct {
	Markets []Market
	Symbols []Symbol
	Types   []SecurityType
	Start   uint32
	Count   uint32
	Refresh bool
}

// MarketDataProvider is the internal service boundary; adapters must not leak vendor types through it.
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
	GetTradingDay(ctx context.Context) (*TradingDayInfo, error)
}

// HistoryStore is the local-first query path for backtesting data.
type HistoryStore interface {
	Coverage(ctx context.Context, req CoverageRequest) (HistoryCoverage, error)
	PutCoverage(ctx context.Context, coverage HistoryCoverage) error
	PutTicks(ctx context.Context, ticks []Tick) error
	QueryTicks(ctx context.Context, req HistoryTickQuery) ([]Tick, error)
	PutBars(ctx context.Context, bars []Bar) error
	QueryBars(ctx context.Context, req BarQuery) ([]Bar, error)
}

// BackfillQueue schedules upstream gap-fill work without blocking the local query API.
type BackfillQueue interface {
	Enqueue(ctx context.Context, task BackfillTask) (BackfillTask, bool, error)
	Next(ctx context.Context) (BackfillTask, bool, error)
	Update(ctx context.Context, task BackfillTask) error
	List(ctx context.Context, filter BackfillFilter) ([]BackfillTask, error)
}

// CoverageRequest identifies one local history coverage row.
type CoverageRequest struct {
	Dataset    Dataset
	Symbol     Symbol
	TradeDate  time.Time
	Period     Period
	AdjustType AdjustType
}

// HistoryTickQuery queries historical ticks from local storage.
type HistoryTickQuery struct {
	Symbol    Symbol
	TradeDate time.Time
	Start     int
	Limit     int
}

// BarQuery queries bars from local storage.
type BarQuery struct {
	Symbol     Symbol
	Period     Period
	AdjustType AdjustType
	Start      time.Time
	End        time.Time
}

// BackfillFilter narrows task list queries.
type BackfillFilter struct {
	Dataset Dataset
	Symbol  Symbol
	Status  BackfillStatus
}
