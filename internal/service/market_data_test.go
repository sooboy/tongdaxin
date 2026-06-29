package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sooboy/tongdaxin/internal/cache"
	"github.com/sooboy/tongdaxin/internal/domain"
	"github.com/sooboy/tongdaxin/internal/history"
)

type fakeProvider struct {
	historyTicks []domain.Tick
	bars         []domain.Bar
	barFunc      func(domain.KLineRequest) []domain.Bar
	barReqs      []domain.KLineRequest
	securities   []domain.SecurityInfo
	securityReqs []domain.SecurityQuery
	tradingDay   *domain.TradingDayInfo
	historyCalls int
	tradingCalls int
	barCalls     int
	err          error
}

func (p *fakeProvider) GetQuotes(context.Context, []domain.Symbol) ([]domain.Quote, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (p *fakeProvider) GetOrderBook(context.Context, []domain.Symbol) ([]domain.OrderBook, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (p *fakeProvider) GetTicks(context.Context, domain.Symbol, domain.TickRequest) ([]domain.Tick, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (p *fakeProvider) GetHistoryTicks(context.Context, domain.Symbol, domain.HistoryTickRequest) ([]domain.Tick, error) {
	p.historyCalls++
	if p.err != nil {
		return nil, p.err
	}
	return append([]domain.Tick(nil), p.historyTicks...), nil
}
func (p *fakeProvider) GetKLine(_ context.Context, _ domain.Symbol, req domain.KLineRequest) ([]domain.Bar, error) {
	p.barCalls++
	p.barReqs = append(p.barReqs, req)
	if p.err != nil {
		return nil, p.err
	}
	if p.barFunc != nil {
		return append([]domain.Bar(nil), p.barFunc(req)...), nil
	}
	return append([]domain.Bar(nil), p.bars...), nil
}
func (p *fakeProvider) GetAdjustedKLine(_ context.Context, _ domain.Symbol, req domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	p.barCalls++
	p.barReqs = append(p.barReqs, req.KLineRequest)
	if p.err != nil {
		return nil, p.err
	}
	if p.barFunc != nil {
		return append([]domain.Bar(nil), p.barFunc(req.KLineRequest)...), nil
	}
	return append([]domain.Bar(nil), p.bars...), nil
}
func (p *fakeProvider) GetXDXR(context.Context, domain.Symbol) ([]domain.XDXREvent, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (p *fakeProvider) GetSecurityInfo(_ context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	p.securityReqs = append(p.securityReqs, req)
	if p.err != nil {
		return nil, p.err
	}
	return append([]domain.SecurityInfo(nil), p.securities...), nil
}
func (p *fakeProvider) GetFinance(context.Context, domain.Symbol) (*domain.FinanceInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (p *fakeProvider) GetTradingDay(context.Context) (*domain.TradingDayInfo, error) {
	p.tradingCalls++
	if p.err != nil {
		return nil, p.err
	}
	return p.tradingDay, nil
}

func TestGetTradingDayCachesProviderResult(t *testing.T) {
	t.Parallel()

	provider := &fakeProvider{tradingDay: &domain.TradingDayInfo{TodayString: "2026-06-29", IsTodayTradingDay: true}}
	svc := NewMarketDataService(provider, nil, nil, cache.NewMemory(cache.Config{SecurityTTL: time.Hour}))

	first, err := svc.GetTradingDay(context.Background())
	if err != nil {
		t.Fatalf("GetTradingDay first: %v", err)
	}
	if first.TodayString != "2026-06-29" || first.Cached {
		t.Fatalf("first = %+v", first)
	}
	provider.err = errors.New("should use cached trading day")
	second, err := svc.GetTradingDay(context.Background())
	if err != nil {
		t.Fatalf("GetTradingDay second: %v", err)
	}
	if second.TodayString != "2026-06-29" || !second.Cached {
		t.Fatalf("second = %+v", second)
	}
	if provider.tradingCalls != 1 {
		t.Fatalf("provider trading calls = %d", provider.tradingCalls)
	}
}

func TestGetSecurityInfoCachesMarketAllAndFiltersLocally(t *testing.T) {
	t.Parallel()

	provider := &fakeProvider{securities: []domain.SecurityInfo{
		{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000", Name: "浦发银行"}},
		{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600001", Name: "邯郸钢铁"}},
	}}
	svc := NewMarketDataService(provider, nil, nil, cache.NewMemory(cache.Config{SecurityTTL: time.Hour}))

	first, err := svc.GetSecurityInfo(context.Background(), domain.SecurityQuery{Symbols: []domain.Symbol{{Market: domain.MarketSH, Code: "600001"}}})
	if err != nil {
		t.Fatalf("GetSecurityInfo first: %v", err)
	}
	if len(first) != 1 || first[0].Symbol.Code != "600001" || first[0].Cached {
		t.Fatalf("first securities = %+v", first)
	}
	if len(provider.securityReqs) != 1 || len(provider.securityReqs[0].Markets) != 1 || provider.securityReqs[0].Markets[0] != domain.MarketSH || provider.securityReqs[0].Start != 0 || provider.securityReqs[0].Count != 0 || len(provider.securityReqs[0].Symbols) != 0 {
		t.Fatalf("provider reqs = %+v", provider.securityReqs)
	}

	provider.err = errors.New("should use cached market all")
	second, err := svc.GetSecurityInfo(context.Background(), domain.SecurityQuery{Symbols: []domain.Symbol{{Market: domain.MarketSH, Code: "600000"}}})
	if err != nil {
		t.Fatalf("GetSecurityInfo second: %v", err)
	}
	if len(second) != 1 || second[0].Symbol.Code != "600000" || !second[0].Cached {
		t.Fatalf("second securities = %+v", second)
	}
	if len(provider.securityReqs) != 1 {
		t.Fatalf("provider called again: %+v", provider.securityReqs)
	}
}

func TestGetSecurityInfoAppliesStartCountAfterMarketFetch(t *testing.T) {
	t.Parallel()

	provider := &fakeProvider{securities: []domain.SecurityInfo{
		{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}},
		{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600001"}},
		{Symbol: domain.Symbol{Market: domain.MarketSZ, Code: "000001"}},
	}}
	svc := NewMarketDataService(provider, nil, nil, cache.NewMemory(cache.Config{SecurityTTL: time.Hour}))

	got, err := svc.GetSecurityInfo(context.Background(), domain.SecurityQuery{Markets: []domain.Market{domain.MarketSH, domain.MarketSZ}, Start: 1, Count: 1})
	if err != nil {
		t.Fatalf("GetSecurityInfo: %v", err)
	}
	if len(got) != 1 || got[0].Symbol.Key() != "SH:600001" {
		t.Fatalf("securities = %+v", got)
	}
	if len(provider.securityReqs) != 1 || provider.securityReqs[0].Start != 0 || provider.securityReqs[0].Count != 0 {
		t.Fatalf("provider reqs = %+v", provider.securityReqs)
	}
}

func TestGetHistoryTicksUsesLocalFirst(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	local := []domain.Tick{{Symbol: symbol, TradeDate: date, TradeTime: date.Add(10 * time.Hour), Price: 10}}
	if err := store.PutTicks(context.Background(), local); err != nil {
		t.Fatalf("PutTicks: %v", err)
	}
	service := MarketDataService{Store: store, Provider: &fakeProvider{err: errors.New("should not call provider")}}

	got, err := service.GetHistoryTicks(context.Background(), symbol, domain.HistoryTickRequest{TradeDate: date, Count: 1})
	if err != nil {
		t.Fatalf("GetHistoryTicks: %v", err)
	}
	if len(got) != 1 || got[0].Price != 10 || !got[0].Cached {
		t.Fatalf("ticks = %+v", got)
	}
}

func TestGetHistoryTicksFetchesAndStoresOnMiss(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	remote := []domain.Tick{{Symbol: symbol, TradeDate: date, TradeTime: date.Add(10 * time.Hour), Price: 11}}
	service := MarketDataService{Store: store, Queue: store, Provider: &fakeProvider{historyTicks: remote}}

	got, err := service.GetHistoryTicks(context.Background(), symbol, domain.HistoryTickRequest{TradeDate: date, Full: true})
	if err != nil {
		t.Fatalf("GetHistoryTicks: %v", err)
	}
	if len(got) != 1 || got[0].Price != 11 {
		t.Fatalf("remote ticks = %+v", got)
	}
	stored, err := store.QueryTicks(context.Background(), domain.HistoryTickQuery{Symbol: symbol, TradeDate: date})
	if err != nil {
		t.Fatalf("stored QueryTicks: %v", err)
	}
	if len(stored) != 1 || stored[0].Price != 11 {
		t.Fatalf("stored = %+v", stored)
	}
	coverage, err := store.Coverage(context.Background(), domain.CoverageRequest{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date})
	if err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	if coverage.Status != domain.CoverageCovered || coverage.RowCount != 1 {
		t.Fatalf("coverage = %+v", coverage)
	}
}

func TestGetHistoryTicksFullIgnoresPartialLocalPage(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	partial := makeTicks(symbol, date, 10, 10)
	if err := store.PutTicks(context.Background(), partial); err != nil {
		t.Fatalf("PutTicks partial: %v", err)
	}
	remote := makeTicks(symbol, date, 20, 20)
	provider := &fakeProvider{historyTicks: remote}
	service := MarketDataService{Store: store, Provider: provider}

	got, err := service.GetHistoryTicks(context.Background(), symbol, domain.HistoryTickRequest{TradeDate: date, Full: true})
	if err != nil {
		t.Fatalf("GetHistoryTicks full: %v", err)
	}
	if len(got) != 20 || got[0].Price != 20 || provider.historyCalls != 1 {
		t.Fatalf("got len=%d first=%+v provider_calls=%d", len(got), got[0], provider.historyCalls)
	}
}

func TestGetHistoryTicksFullIgnoresPollutedCacheWithoutFullCoverage(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	partial := makeTicks(symbol, date, 10, 10)
	if err := store.PutTicks(context.Background(), partial); err != nil {
		t.Fatalf("PutTicks partial: %v", err)
	}
	cacheStore := cache.NewMemory(cache.Config{HistoryTickTTL: time.Hour})
	req := domain.HistoryTickRequest{TradeDate: date, Full: true}
	if err := cacheStore.PutHistoryTickPage(context.Background(), historyTickCacheKey(symbol, req), partial); err != nil {
		t.Fatalf("PutHistoryTickPage: %v", err)
	}
	remote := makeTicks(symbol, date, 20, 20)
	provider := &fakeProvider{historyTicks: remote}
	service := NewMarketDataService(provider, store, nil, cacheStore)

	got, err := service.GetHistoryTicks(context.Background(), symbol, req)
	if err != nil {
		t.Fatalf("GetHistoryTicks full: %v", err)
	}
	if len(got) != 20 || got[0].Price != 20 || provider.historyCalls != 1 {
		t.Fatalf("got len=%d first=%+v provider_calls=%d", len(got), got[0], provider.historyCalls)
	}
}

func TestGetHistoryTicksExpandsCountWhenLocalPageTooShort(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	if err := store.PutTicks(context.Background(), makeTicks(symbol, date, 10, 10)); err != nil {
		t.Fatalf("PutTicks partial: %v", err)
	}
	remote := makeTicks(symbol, date, 20, 20)
	provider := &fakeProvider{historyTicks: remote}
	service := MarketDataService{Store: store, Provider: provider}

	got, err := service.GetHistoryTicks(context.Background(), symbol, domain.HistoryTickRequest{TradeDate: date, Count: 20})
	if err != nil {
		t.Fatalf("GetHistoryTicks count 20: %v", err)
	}
	if len(got) != 20 || got[0].Price != 20 || provider.historyCalls != 1 {
		t.Fatalf("got len=%d first=%+v provider_calls=%d", len(got), got[0], provider.historyCalls)
	}
}

func TestGetHistoryTicksUsesCoveredLocalForFull(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	local := makeTicks(symbol, date, 2, 10)
	if err := store.PutTicks(context.Background(), local); err != nil {
		t.Fatalf("PutTicks full: %v", err)
	}
	if err := store.PutCoverage(context.Background(), domain.HistoryCoverage{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date, Status: domain.CoverageCovered, Checksum: historyTickCoverageFullMarker, RowCount: len(local)}); err != nil {
		t.Fatalf("PutCoverage: %v", err)
	}
	service := MarketDataService{Store: store, Provider: &fakeProvider{err: errors.New("should not call provider")}}

	got, err := service.GetHistoryTicks(context.Background(), symbol, domain.HistoryTickRequest{TradeDate: date, Full: true})
	if err != nil {
		t.Fatalf("GetHistoryTicks full local: %v", err)
	}
	if len(got) != 2 || got[0].Price != 10 || !got[0].Cached {
		t.Fatalf("ticks = %+v", got)
	}
}

func TestGetKLineEnqueuesOnProviderFailure(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	store := history.NewMemoryStore()
	service := MarketDataService{Store: store, Queue: store, Provider: &fakeProvider{err: domain.ErrUpstreamUnavailable}}
	_, err := service.GetKLine(context.Background(), symbol, domain.KLineRequest{Period: domain.PeriodDay})
	if !errors.Is(err, domain.ErrUpstreamUnavailable) {
		t.Fatalf("GetKLine error = %v", err)
	}
	tasks, err := store.List(context.Background(), domain.BackfillFilter{Dataset: domain.DatasetKLine, Symbol: symbol})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Status != domain.BackfillPending {
		t.Fatalf("tasks = %+v", tasks)
	}
}

func TestGetKLineAppliesStartCountToLocalStoreResults(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	bars := make([]domain.Bar, 0, 5)
	for i := 0; i < 5; i++ {
		bars = append(bars, domain.Bar{Symbol: symbol, Period: domain.PeriodDay, Time: base.AddDate(0, 0, i), Close: float64(i)})
	}
	if err := store.PutBars(context.Background(), bars); err != nil {
		t.Fatalf("PutBars: %v", err)
	}
	service := MarketDataService{Store: store, Provider: &fakeProvider{err: errors.New("should not call provider")}}

	got, err := service.GetKLine(context.Background(), symbol, domain.KLineRequest{
		Period:    domain.PeriodDay,
		Start:     1,
		Count:     2,
		StartDate: base,
		EndDate:   base.AddDate(0, 0, 4),
	})
	if err != nil {
		t.Fatalf("GetKLine: %v", err)
	}
	if len(got) != 2 || got[0].Close != 1 || got[1].Close != 2 {
		t.Fatalf("bars = %+v", got)
	}
}

func TestGetKLineRefetchesWhenLocalStoreStartsAfterRequestedStartDate(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	may := time.Date(2026, 5, 29, 0, 0, 0, 0, time.Local)
	june := time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	if err := store.PutBars(context.Background(), []domain.Bar{
		{Symbol: symbol, Period: domain.PeriodDay, Time: june, Close: 6},
	}); err != nil {
		t.Fatalf("PutBars: %v", err)
	}
	provider := &fakeProvider{bars: []domain.Bar{
		{Symbol: symbol, Period: domain.PeriodDay, Time: may, Close: 5},
		{Symbol: symbol, Period: domain.PeriodDay, Time: june, Close: 6},
	}}
	service := MarketDataService{Store: store, Provider: provider}

	got, err := service.GetKLine(context.Background(), symbol, domain.KLineRequest{
		Period:    domain.PeriodDay,
		StartDate: may,
		EndDate:   june,
	})
	if err != nil {
		t.Fatalf("GetKLine: %v", err)
	}
	if provider.barCalls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.barCalls)
	}
	if len(got) != 2 || !got[0].Time.Equal(may) {
		t.Fatalf("bars = %+v", got)
	}
}

func TestGetKLineRefetchesWhenLocalStoreHasFewerRowsThanRequestedCount(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	if err := store.PutBars(context.Background(), []domain.Bar{
		{Symbol: symbol, Period: domain.PeriodDay, Time: base, Close: 0},
		{Symbol: symbol, Period: domain.PeriodDay, Time: base.AddDate(0, 0, 1), Close: 1},
	}); err != nil {
		t.Fatalf("PutBars: %v", err)
	}
	remote := make([]domain.Bar, 0, 4)
	for i := 0; i < 4; i++ {
		remote = append(remote, domain.Bar{Symbol: symbol, Period: domain.PeriodDay, Time: base.AddDate(0, 0, i), Close: float64(i)})
	}
	provider := &fakeProvider{bars: remote}
	service := MarketDataService{Store: store, Provider: provider}

	got, err := service.GetKLine(context.Background(), symbol, domain.KLineRequest{
		Period:    domain.PeriodDay,
		Count:     4,
		StartDate: base,
		EndDate:   base.AddDate(0, 0, 3),
	})
	if err != nil {
		t.Fatalf("GetKLine: %v", err)
	}
	if provider.barCalls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.barCalls)
	}
	if len(got) != 4 {
		t.Fatalf("bars len = %d, want 4: %+v", len(got), got)
	}
}

func TestGetKLineFillsMiddleGapOnly(t *testing.T) {
	t.Parallel()

	symbol := mustSymbol(t)
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.Local)
	leftEnd := time.Date(2020, 1, 10, 0, 0, 0, 0, time.Local)
	gapStart := time.Date(2020, 1, 11, 0, 0, 0, 0, time.Local)
	gapEnd := time.Date(2020, 2, 29, 0, 0, 0, 0, time.Local)
	rightStart := time.Date(2020, 3, 1, 0, 0, 0, 0, time.Local)
	end := time.Date(2020, 3, 10, 0, 0, 0, 0, time.Local)
	store := history.NewMemoryStore()
	if err := store.PutBars(context.Background(), append(
		makeDailyBars(symbol, start, leftEnd, 1),
		makeDailyBars(symbol, rightStart, end, 100)...,
	)); err != nil {
		t.Fatalf("PutBars: %v", err)
	}
	provider := &fakeProvider{barFunc: func(req domain.KLineRequest) []domain.Bar {
		return makeDailyBars(symbol, req.StartDate, req.EndDate, 50)
	}}
	service := MarketDataService{Store: store, Provider: provider}

	got, err := service.GetKLine(context.Background(), symbol, domain.KLineRequest{
		Period:    domain.PeriodDay,
		StartDate: start,
		EndDate:   end,
	})
	if err != nil {
		t.Fatalf("GetKLine: %v", err)
	}
	if provider.barCalls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.barCalls)
	}
	if len(provider.barReqs) != 1 || !sameDay(provider.barReqs[0].StartDate, gapStart) || !sameDay(provider.barReqs[0].EndDate, gapEnd) {
		t.Fatalf("provider reqs = %+v, want gap %s~%s", provider.barReqs, gapStart.Format("2006-01-02"), gapEnd.Format("2006-01-02"))
	}
	if len(got) != int(end.Sub(start).Hours()/24)+1 {
		t.Fatalf("bars len = %d, want full natural-day range", len(got))
	}
	if !sameDay(got[0].Time, start) || !sameDay(got[len(got)-1].Time, end) {
		t.Fatalf("bars range = %s~%s", got[0].Time, got[len(got)-1].Time)
	}
}

func makeDailyBars(symbol domain.Symbol, start time.Time, end time.Time, closeBase float64) []domain.Bar {
	out := make([]domain.Bar, 0)
	for day, i := domain.NormalizeDate(start), 0; !day.After(domain.NormalizeDate(end)); day, i = day.AddDate(0, 0, 1), i+1 {
		out = append(out, domain.Bar{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: day, Close: closeBase + float64(i)})
	}
	return out
}

func sameDay(left time.Time, right time.Time) bool {
	return domain.NormalizeDate(left).Equal(domain.NormalizeDate(right))
}

func makeTicks(symbol domain.Symbol, date time.Time, n int, priceBase float64) []domain.Tick {
	ticks := make([]domain.Tick, 0, n)
	for i := 0; i < n; i++ {
		ticks = append(ticks, domain.Tick{
			Symbol:    symbol,
			TradeDate: date,
			TradeTime: date.Add(time.Duration(9*60+30+i) * time.Minute),
			Price:     priceBase + float64(i),
			Sequence:  int64(i),
		})
	}
	return ticks
}

func mustSymbol(t *testing.T) domain.Symbol {
	t.Helper()
	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	return symbol
}
