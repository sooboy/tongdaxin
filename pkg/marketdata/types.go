// Package marketdata exposes the public library surface for embedding the
// Tongdaxin market-data service in third-party Go applications.
package marketdata

import (
	"context"
	"errors"

	"github.com/sooboy/tongdaxin/internal/domain"
)

// Public error values returned by service implementations and adapters.
var (
	ErrUnsupportedCapability = domain.ErrUnsupportedCapability
	ErrInvalidRequest        = domain.ErrInvalidRequest
	ErrRateLimited           = domain.ErrRateLimited
	ErrNoData                = domain.ErrNoData
	ErrUpstreamUnavailable   = domain.ErrUpstreamUnavailable
)

// Market and symbol types.
type (
	Market         = domain.Market
	SecurityType   = domain.SecurityType
	SecurityStatus = domain.SecurityStatus
	Symbol         = domain.Symbol
)

const (
	MarketSH = domain.MarketSH
	MarketSZ = domain.MarketSZ
	MarketBJ = domain.MarketBJ
)

// Tick, bar and market-data domain models.
type (
	Level                = domain.Level
	Quote                = domain.Quote
	OrderBook            = domain.OrderBook
	TickDirection        = domain.TickDirection
	Tick                 = domain.Tick
	Period               = domain.Period
	AdjustType           = domain.AdjustType
	Bar                  = domain.Bar
	XDXREvent            = domain.XDXREvent
	SecurityInfo         = domain.SecurityInfo
	FinanceInfo          = domain.FinanceInfo
	TradingSession       = domain.TradingSession
	TradingDayInfo       = domain.TradingDayInfo
	QuoteRequest         = domain.QuoteRequest
	OrderBookRequest     = domain.OrderBookRequest
	TickRequest          = domain.TickRequest
	HistoryTickRequest   = domain.HistoryTickRequest
	KLineRequest         = domain.KLineRequest
	AdjustedKLineRequest = domain.AdjustedKLineRequest
	SecurityQuery        = domain.SecurityQuery
)

const (
	TickDirectionUnknown = domain.TickDirectionUnknown
	TickDirectionBuy     = domain.TickDirectionBuy
	TickDirectionSell    = domain.TickDirectionSell
	TickDirectionNeutral = domain.TickDirectionNeutral

	PeriodUnknown = domain.PeriodUnknown
	Period1Min    = domain.Period1Min
	Period5Min    = domain.Period5Min
	Period15Min   = domain.Period15Min
	Period30Min   = domain.Period30Min
	Period1Hour   = domain.Period1Hour
	PeriodDay     = domain.PeriodDay
	PeriodWeek    = domain.PeriodWeek
	PeriodMonth   = domain.PeriodMonth
	PeriodQuarter = domain.PeriodQuarter
	PeriodYear    = domain.PeriodYear

	AdjustNone = domain.AdjustNone
	AdjustQFQ  = domain.AdjustQFQ
	AdjustHFQ  = domain.AdjustHFQ
)

// Service is the interface third-party applications implement or wrap when
// exposing the Gin/gRPC adapters from this module.
type Service interface {
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

func NewSymbol(market Market, code string) (Symbol, error) { return domain.NewSymbol(market, code) }

func ParseMarket(value string) (Market, error) { return domain.ParseMarket(value) }

func ParsePeriod(value string) (Period, error) { return domain.ParsePeriod(value) }

func ParseAdjustType(value string) (AdjustType, error) { return domain.ParseAdjustType(value) }

func NormalizeMarket(value Market) Market { return domain.NormalizeMarket(value) }

func NormalizeCode(value string) string { return domain.NormalizeCode(value) }

func Is(err error, target error) bool { return errors.Is(err, target) }
