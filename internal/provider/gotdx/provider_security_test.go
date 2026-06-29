package gotdxadapter

import (
	"context"
	"testing"

	"github.com/bensema/gotdx/proto"
	gotdxtypes "github.com/bensema/gotdx/types"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestProviderGetSecurityInfoUsesPagedStockListForCount(t *testing.T) {
	t.Parallel()

	var calls []struct {
		start uint32
		count uint32
	}
	client := &fakeGotdxClient{
		stockList: func(market uint8, start uint32, count uint32) ([]proto.Security, error) {
			calls = append(calls, struct {
				start uint32
				count uint32
			}{start: start, count: count})
			out := make([]proto.Security, count)
			for i := range out {
				out[i] = proto.Security{Code: "600000", Name: "demo"}
			}
			return out, nil
		},
		stockAll: func(market uint8) ([]proto.Security, error) {
			t.Fatal("StockAll should not be used")
			return nil, nil
		},
	}
	pool := newStaticClientPool(t, client)
	provider := NewProvider(pool, WithStaticPool(pool))

	items, err := provider.GetSecurityInfo(context.Background(), domain.SecurityQuery{Markets: []domain.Market{domain.MarketSH}, Start: 5, Count: 10})
	if err != nil {
		t.Fatalf("GetSecurityInfo: %v", err)
	}
	if len(items) != 10 {
		t.Fatalf("items len = %d, want 10", len(items))
	}
	if len(calls) != 1 || calls[0].start != 5 || calls[0].count != 10 {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestProviderGetSecurityInfoSplitsLargeAndDoesNotCallStockAll(t *testing.T) {
	t.Parallel()

	var calls []struct {
		start uint32
		count uint32
	}
	client := &fakeGotdxClient{
		stockList: func(market uint8, start uint32, count uint32) ([]proto.Security, error) {
			if market != gotdxtypes.MarketSH.Uint8() {
				t.Fatalf("market = %d, want SH", market)
			}
			calls = append(calls, struct {
				start uint32
				count uint32
			}{start: start, count: count})
			out := make([]proto.Security, count)
			for i := range out {
				out[i] = proto.Security{Code: "600000", Name: "demo"}
			}
			return out, nil
		},
		stockAll: func(market uint8) ([]proto.Security, error) {
			t.Fatal("StockAll should not be used")
			return nil, nil
		},
	}
	pool := newStaticClientPool(t, client)
	provider := NewProvider(pool, WithStaticPool(pool))

	items, err := provider.GetSecurityInfo(context.Background(), domain.SecurityQuery{Markets: []domain.Market{domain.MarketSH}, Count: 1001})
	if err != nil {
		t.Fatalf("GetSecurityInfo: %v", err)
	}
	if len(items) != 1001 {
		t.Fatalf("items len = %d, want 1001", len(items))
	}
	if len(calls) != 2 || calls[0].start != 0 || calls[0].count != defaultSecurityPageSize || calls[1].start != defaultSecurityPageSize || calls[1].count != 201 {
		t.Fatalf("calls = %+v", calls)
	}
}
