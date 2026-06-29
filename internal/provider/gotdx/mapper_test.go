package gotdxadapter

import (
	"testing"
	"time"

	"github.com/bensema/gotdx/proto"
	gotdxtypes "github.com/bensema/gotdx/types"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestMarketPeriodAdjustMappings(t *testing.T) {
	t.Parallel()

	market, err := marketToGotdx(domain.MarketSH)
	if err != nil {
		t.Fatalf("marketToGotdx: %v", err)
	}
	if market != gotdxtypes.MarketSH.Uint8() {
		t.Fatalf("market = %d", market)
	}

	period, err := periodToGotdx(domain.PeriodDay)
	if err != nil {
		t.Fatalf("periodToGotdx: %v", err)
	}
	if period != gotdxtypes.KLINE_TYPE_DAILY {
		t.Fatalf("period = %d", period)
	}

	adjust, err := adjustToGotdx(domain.AdjustQFQ)
	if err != nil {
		t.Fatalf("adjustToGotdx: %v", err)
	}
	if adjust != gotdxtypes.AdjustQFQ {
		t.Fatalf("adjust = %d", adjust)
	}
}

func TestQuoteToDomainIncludesFiveLevels(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 10, 0, 0, 0, time.Local)
	quote := quoteToDomain(proto.SecurityQuote{
		Market:   gotdxtypes.MarketSH.Uint8(),
		Code:     "600000",
		Price:    10.5,
		PreClose: 10,
		Vol:      100,
		Amount:   1050,
		BidLevels: []proto.Level{
			{Price: 10.4, Vol: 1}, {Price: 10.3, Vol: 2}, {Price: 10.2, Vol: 3}, {Price: 10.1, Vol: 4}, {Price: 10, Vol: 5},
		},
		AskLevels: []proto.Level{
			{Price: 10.6, Vol: 1}, {Price: 10.7, Vol: 2}, {Price: 10.8, Vol: 3}, {Price: 10.9, Vol: 4}, {Price: 11, Vol: 5},
		},
	}, now)

	if quote.Symbol.Key() != "SH:600000" {
		t.Fatalf("symbol = %s", quote.Symbol.Key())
	}
	if len(quote.BidLevels) != 5 || len(quote.AskLevels) != 5 {
		t.Fatalf("levels = bid %d ask %d", len(quote.BidLevels), len(quote.AskLevels))
	}
	if quote.LastPrice != 10.5 || quote.Volume != 100 || quote.SourceTime != now {
		t.Fatalf("quote = %+v", quote)
	}
}

func TestTransactionMappings(t *testing.T) {
	t.Parallel()

	symbol, _ := domain.NewSymbol(domain.MarketSH, "600000")
	base := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	tick := transactionToDomain(symbol, proto.TransactionData{Time: "09:31", Price: 10, Vol: 100, Action: "BUY"}, 7, base)
	if tick.Direction != domain.TickDirectionBuy || tick.Amount != 1000 || tick.Sequence != 7 {
		t.Fatalf("tick = %+v", tick)
	}
	if tick.TradeTime.Format("2006010215:04") != "2026062509:31" {
		t.Fatalf("trade time = %s", tick.TradeTime)
	}
}

func TestXDXRMapping(t *testing.T) {
	t.Parallel()

	symbol, _ := domain.NewSymbol(domain.MarketSH, "600000")
	fenhong := float32(1.2)
	peigujia := float32(3.4)
	song := float32(5.6)
	peigu := float32(7.8)
	event := xdxrToDomain(symbol, proto.XDXRItem{
		Date:        time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local),
		Category:    1,
		Name:        "除权除息",
		Fenhong:     &fenhong,
		Peigujia:    &peigujia,
		Songzhuangu: &song,
		Peigu:       &peigu,
	})
	if event.EventType != "除权除息" || event.CashDividend == 0 || event.BonusShare == 0 || event.AllotmentPrice == 0 || event.AllotmentRatio == 0 {
		t.Fatalf("event = %+v", event)
	}
	if event.RawFields["category"] != uint8(1) {
		t.Fatalf("raw fields = %+v", event.RawFields)
	}
}
