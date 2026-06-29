package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestRedisQuoteCacheRoundTripAndExpiry(t *testing.T) {
	t.Parallel()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(srv.Close)

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cache := NewRedis(client, Config{QuoteTTL: 50 * time.Millisecond})
	symbol := mustRedisSymbol(t)

	if err := cache.PutQuotes(context.Background(), []domain.Quote{{Symbol: symbol, LastPrice: 12.5}}); err != nil {
		t.Fatalf("PutQuotes: %v", err)
	}
	hits, misses, err := cache.GetQuotes(context.Background(), []domain.Symbol{symbol})
	if err != nil {
		t.Fatalf("GetQuotes: %v", err)
	}
	if len(misses) != 0 || len(hits) != 1 {
		t.Fatalf("hits=%+v misses=%+v", hits, misses)
	}
	got := hits[symbol.Key()]
	if !got.Cached || got.LastPrice != 12.5 {
		t.Fatalf("quote = %+v", got)
	}

	srv.FastForward(100 * time.Millisecond)
	hits, misses, err = cache.GetQuotes(context.Background(), []domain.Symbol{symbol})
	if err != nil {
		t.Fatalf("GetQuotes expired: %v", err)
	}
	if len(hits) != 0 || len(misses) != 1 {
		t.Fatalf("expired hits=%+v misses=%+v", hits, misses)
	}
}

func TestRedisFinanceRoundTrip(t *testing.T) {
	t.Parallel()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(srv.Close)

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cache := NewRedis(client, Config{FinanceTTL: time.Minute})
	symbol := mustRedisSymbol(t)
	info := &domain.FinanceInfo{Symbol: symbol, Fields: map[string]any{"name": "demo", "pe": 12.5}}

	if err := cache.PutFinance(context.Background(), symbol, info); err != nil {
		t.Fatalf("PutFinance: %v", err)
	}
	got, ok, err := cache.GetFinance(context.Background(), symbol)
	if err != nil {
		t.Fatalf("GetFinance: %v", err)
	}
	if !ok || got == nil || !got.Cached {
		t.Fatalf("finance = %+v ok=%v", got, ok)
	}
	if got.Symbol.Key() != symbol.Key() {
		t.Fatalf("symbol = %+v", got.Symbol)
	}
	if got.Fields["name"] != "demo" {
		t.Fatalf("fields = %+v", got.Fields)
	}
}

func mustRedisSymbol(t *testing.T) domain.Symbol {
	t.Helper()
	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	return symbol
}
