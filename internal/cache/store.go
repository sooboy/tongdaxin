package cache

import (
	"context"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

// Cache stores hot market-data responses behind an interchangeable backend.
type Cache interface {
	Close() error
	GetQuotes(ctx context.Context, symbols []domain.Symbol) (map[string]domain.Quote, []domain.Symbol, error)
	PutQuotes(ctx context.Context, quotes []domain.Quote) error
	GetOrderBooks(ctx context.Context, symbols []domain.Symbol) (map[string]domain.OrderBook, []domain.Symbol, error)
	PutOrderBooks(ctx context.Context, books []domain.OrderBook) error
	GetTickPage(ctx context.Context, key string) ([]domain.Tick, bool, error)
	PutTickPage(ctx context.Context, key string, ticks []domain.Tick, ttl time.Duration) error
	PutHistoryTickPage(ctx context.Context, key string, ticks []domain.Tick) error
	GetBars(ctx context.Context, key string) ([]domain.Bar, bool, error)
	PutBars(ctx context.Context, key string, bars []domain.Bar) error
	GetXDXR(ctx context.Context, symbol domain.Symbol) ([]domain.XDXREvent, bool, error)
	PutXDXR(ctx context.Context, symbol domain.Symbol, events []domain.XDXREvent) error
	GetSecurities(ctx context.Context, key string) ([]domain.SecurityInfo, bool, error)
	PutSecurities(ctx context.Context, key string, items []domain.SecurityInfo) error
	GetFinance(ctx context.Context, symbol domain.Symbol) (*domain.FinanceInfo, bool, error)
	PutFinance(ctx context.Context, symbol domain.Symbol, info *domain.FinanceInfo) error
	GetTradingDay(ctx context.Context) (*domain.TradingDayInfo, bool, error)
	PutTradingDay(ctx context.Context, info *domain.TradingDayInfo) error
}

var (
	_ Cache = (*Memory)(nil)
	_ Cache = (*Redis)(nil)
)
