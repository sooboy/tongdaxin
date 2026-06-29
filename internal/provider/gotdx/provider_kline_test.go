package gotdxadapter

import (
	"context"
	"testing"
	"time"

	"github.com/bensema/gotdx/proto"
	gotdxtypes "github.com/bensema/gotdx/types"

	"github.com/sooboy/tongdaxin/internal/domain"
	"github.com/sooboy/tongdaxin/internal/source"
)

func TestProviderGetKLineSplitsDefaultCountBelowGotdxLimit(t *testing.T) {
	t.Parallel()

	var calls []struct {
		start uint16
		count uint16
	}
	client := &fakeGotdxClient{
		stockKLine: func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
			calls = append(calls, struct {
				start uint16
				count uint16
			}{start: start, count: count})
			bars := make([]proto.SecurityBar, count)
			for i := range bars {
				bars[i] = proto.SecurityBar{DateTime: time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local).AddDate(0, 0, int(start)+i)}
			}
			return bars, nil
		},
	}
	pool := newStaticClientPool(t, client)
	provider := NewProvider(pool, WithKLinePool(pool))

	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	bars, err := provider.GetKLine(context.Background(), symbol, domain.KLineRequest{Period: domain.PeriodDay})
	if err != nil {
		t.Fatalf("GetKLine: %v", err)
	}
	if len(bars) != defaultKLineCount {
		t.Fatalf("bars len = %d, want %d", len(bars), defaultKLineCount)
	}
	if len(calls) != 2 || calls[0].start != 0 || calls[0].count != maxGotdxKLineCount || calls[1].start != maxGotdxKLineCount || calls[1].count != 1 {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestProviderGetKLineKeepsSafeCountSinglePage(t *testing.T) {
	t.Parallel()

	var gotCount uint16
	client := &fakeGotdxClient{
		stockKLine: func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
			gotCount = count
			if category != gotdxtypes.KLINE_TYPE_DAILY {
				t.Fatalf("category = %d, want daily", category)
			}
			return make([]proto.SecurityBar, count), nil
		},
	}
	pool := newStaticClientPool(t, client)
	provider := NewProvider(pool, WithKLinePool(pool))

	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	bars, err := provider.GetKLine(context.Background(), symbol, domain.KLineRequest{Period: domain.PeriodDay, Count: 10})
	if err != nil {
		t.Fatalf("GetKLine: %v", err)
	}
	if gotCount != 10 || len(bars) != 10 {
		t.Fatalf("gotCount=%d bars=%d, want 10", gotCount, len(bars))
	}
}

func TestProviderGetKLineAppliesCountAfterDateRange(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local)
	var gotAdjust uint16
	client := &fakeGotdxClient{
		stockKLine: func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
			gotAdjust = adjust
			bars := make([]proto.SecurityBar, 0, 6)
			for i := 0; i < 6; i++ {
				bars = append(bars, proto.SecurityBar{DateTime: base.AddDate(0, 0, i), Close: float64(i)})
			}
			return bars, nil
		},
	}
	pool := newStaticClientPool(t, client)
	provider := NewProvider(pool, WithKLinePool(pool))

	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	bars, err := provider.GetAdjustedKLine(context.Background(), symbol, domain.AdjustedKLineRequest{
		KLineRequest: domain.KLineRequest{
			Period:    domain.PeriodDay,
			Start:     1,
			Count:     2,
			StartDate: base,
			EndDate:   base.AddDate(0, 0, 5),
		},
		AdjustType: domain.AdjustQFQ,
	})
	if err != nil {
		t.Fatalf("GetKLine: %v", err)
	}
	if len(bars) != 2 || bars[0].Close != 1 || bars[1].Close != 2 {
		t.Fatalf("bars = %+v", bars)
	}
	if gotAdjust != gotdxtypes.AdjustQFQ {
		t.Fatalf("adjust = %d, want qfq", gotAdjust)
	}
}

func newStaticClientPool(t *testing.T, client Client) *source.Pool[Client] {
	t.Helper()
	pool, err := source.NewPool[Client](context.Background(), source.Config[Client]{
		Name:  "test-pool",
		Hosts: []source.HostConfig{{Address: "fake", Clients: 1}},
		Factory: func(context.Context, source.HostConfig, int) (Client, error) {
			return client, nil
		},
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return pool
}
