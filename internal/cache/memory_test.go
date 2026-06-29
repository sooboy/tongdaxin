package cache

import (
	"context"
	"testing"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestMemoryQuoteCacheExpiresAndMarksCached(t *testing.T) {
	now := time.Date(2026, 6, 25, 10, 0, 0, 0, time.Local)
	cache := NewMemory(Config{QuoteTTL: time.Second, Clock: func() time.Time { return now }})
	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	if err := cache.PutQuotes(context.Background(), []domain.Quote{{Symbol: symbol, LastPrice: 10}}); err != nil {
		t.Fatalf("PutQuotes: %v", err)
	}

	hits, misses, err := cache.GetQuotes(context.Background(), []domain.Symbol{symbol})
	if err != nil {
		t.Fatalf("GetQuotes: %v", err)
	}
	if len(misses) != 0 || hits[symbol.Key()].LastPrice != 10 || !hits[symbol.Key()].Cached {
		t.Fatalf("hits=%+v misses=%+v", hits, misses)
	}

	now = now.Add(2 * time.Second)
	hits, misses, err = cache.GetQuotes(context.Background(), []domain.Symbol{symbol})
	if err != nil {
		t.Fatalf("GetQuotes expired: %v", err)
	}
	if len(hits) != 0 || len(misses) != 1 || misses[0].Key() != symbol.Key() {
		t.Fatalf("expired hits=%+v misses=%+v", hits, misses)
	}
}

func TestMemoryTickPageCacheCopiesAndMarksCached(t *testing.T) {
	now := time.Date(2026, 6, 25, 10, 0, 0, 0, time.Local)
	cache := NewMemory(Config{TickTTL: time.Second, Clock: func() time.Time { return now }})
	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	ticks := []domain.Tick{{Symbol: symbol, Price: 10}}
	if err := cache.PutTickPage(context.Background(), "key", ticks, 0); err != nil {
		t.Fatalf("PutTickPage: %v", err)
	}
	ticks[0].Price = 99

	got, ok, err := cache.GetTickPage(context.Background(), "key")
	if err != nil {
		t.Fatalf("GetTickPage: %v", err)
	}
	if !ok || len(got) != 1 || got[0].Price != 10 || !got[0].Cached {
		t.Fatalf("got=%+v ok=%v", got, ok)
	}
}

func TestGroupCoalescesConcurrentLoads(t *testing.T) {
	t.Parallel()

	group := NewGroup[int]()
	existing := &call[int]{}
	existing.wg.Add(1)
	group.calls["same"] = existing

	done := make(chan struct {
		value  int
		shared bool
		err    error
	}, 1)
	called := make(chan struct{}, 1)
	go func() {
		value, shared, err := group.Do(context.Background(), "same", func(context.Context) (int, error) {
			called <- struct{}{}
			return 0, nil
		})
		done <- struct {
			value  int
			shared bool
			err    error
		}{value: value, shared: shared, err: err}
	}()

	existing.value = 7
	existing.wg.Done()
	result := <-done
	if result.err != nil || result.value != 7 || !result.shared {
		t.Fatalf("result = %+v", result)
	}
	select {
	case <-called:
		t.Fatal("load function should be coalesced")
	default:
	}
}
