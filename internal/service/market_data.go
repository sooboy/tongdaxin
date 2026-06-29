package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sooboy/tongdaxin/internal/cache"
	"github.com/sooboy/tongdaxin/internal/domain"
)

// MarketDataService coordinates cache, local-first history storage and provider-backed fills.
type MarketDataService struct {
	Provider domain.MarketDataProvider
	Store    domain.HistoryStore
	Queue    domain.BackfillQueue
	Cache    cache.Cache
	Clock    func() time.Time

	quoteGroup       *cache.Group[[]domain.Quote]
	orderBookGroup   *cache.Group[[]domain.OrderBook]
	tickGroup        *cache.Group[[]domain.Tick]
	historyTickGroup *cache.Group[[]domain.Tick]
	barGroup         *cache.Group[[]domain.Bar]
	xdxrGroup        *cache.Group[[]domain.XDXREvent]
	securityGroup    *cache.Group[[]domain.SecurityInfo]
	financeGroup     *cache.Group[*domain.FinanceInfo]
	tradingDayGroup  *cache.Group[*domain.TradingDayInfo]
}

const (
	maxDailyBarGap          = 21 * 24 * time.Hour
	defaultHistoryTickCount = 600
)

const historyTickCoverageFullMarker = "full"

func NewMarketDataService(provider domain.MarketDataProvider, store domain.HistoryStore, queue domain.BackfillQueue, cacheStore cache.Cache) *MarketDataService {
	return &MarketDataService{
		Provider:         provider,
		Store:            store,
		Queue:            queue,
		Cache:            cacheStore,
		Clock:            time.Now,
		quoteGroup:       cache.NewGroup[[]domain.Quote](),
		orderBookGroup:   cache.NewGroup[[]domain.OrderBook](),
		tickGroup:        cache.NewGroup[[]domain.Tick](),
		historyTickGroup: cache.NewGroup[[]domain.Tick](),
		barGroup:         cache.NewGroup[[]domain.Bar](),
		xdxrGroup:        cache.NewGroup[[]domain.XDXREvent](),
		securityGroup:    cache.NewGroup[[]domain.SecurityInfo](),
		financeGroup:     cache.NewGroup[*domain.FinanceInfo](),
		tradingDayGroup:  cache.NewGroup[*domain.TradingDayInfo](),
	}
}

func (s *MarketDataService) GetQuotes(ctx context.Context, symbols []domain.Symbol) ([]domain.Quote, error) {
	if err := validateSymbols(symbols); err != nil {
		return nil, err
	}
	hits := map[string]domain.Quote{}
	misses := symbols
	if s.Cache != nil {
		var err error
		hits, misses, err = s.Cache.GetQuotes(ctx, symbols)
		if err != nil {
			return nil, err
		}
		if len(misses) == 0 {
			return orderQuotes(symbols, hits, nil), nil
		}
	}

	quotes, err := doGroup(ctx, s.quoteGroup, symbolsKey("quotes", misses), func(context.Context) ([]domain.Quote, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetQuotes(ctx, misses)
	})
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if err := s.Cache.PutQuotes(ctx, quotes); err != nil {
			return nil, err
		}
	}
	return orderQuotes(symbols, hits, quotes), nil
}

func (s *MarketDataService) GetOrderBook(ctx context.Context, symbols []domain.Symbol) ([]domain.OrderBook, error) {
	if err := validateSymbols(symbols); err != nil {
		return nil, err
	}
	hits := map[string]domain.OrderBook{}
	misses := symbols
	if s.Cache != nil {
		var err error
		hits, misses, err = s.Cache.GetOrderBooks(ctx, symbols)
		if err != nil {
			return nil, err
		}
		if len(misses) == 0 {
			return orderBooks(symbols, hits, nil), nil
		}
	}

	books, err := doGroup(ctx, s.orderBookGroup, symbolsKey("orderbook", misses), func(context.Context) ([]domain.OrderBook, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetOrderBook(ctx, misses)
	})
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if err := s.Cache.PutOrderBooks(ctx, books); err != nil {
			return nil, err
		}
	}
	return orderBooks(symbols, hits, books), nil
}

func (s *MarketDataService) GetTicks(ctx context.Context, symbol domain.Symbol, req domain.TickRequest) ([]domain.Tick, error) {
	if err := symbol.Validate(); err != nil {
		return nil, err
	}
	key := tickCacheKey("ticks", symbol, req.Start, req.Count, req.Full, req.ForceRefresh)
	if s.Cache != nil && !req.ForceRefresh {
		if ticks, ok, err := s.Cache.GetTickPage(ctx, key); err != nil {
			return nil, err
		} else if ok {
			return ticks, nil
		}
	}
	ticks, err := doGroup(ctx, s.tickGroup, key, func(context.Context) ([]domain.Tick, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetTicks(ctx, symbol, req)
	})
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if err := s.Cache.PutTickPage(ctx, key, ticks, 0); err != nil {
			return nil, err
		}
	}
	return ticks, nil
}

// GetHistoryTicks serves backtest-heavy historical tick requests from local cache/storage first.
func (s *MarketDataService) GetHistoryTicks(ctx context.Context, symbol domain.Symbol, req domain.HistoryTickRequest) ([]domain.Tick, error) {
	if err := symbol.Validate(); err != nil {
		return nil, err
	}
	date := domain.NormalizeDate(req.TradeDate)
	if date.IsZero() {
		return nil, domain.ErrInvalidRequest
	}
	key := historyTickCacheKey(symbol, req)
	if s.Cache != nil && !req.ForceRefresh {
		if ticks, ok, err := s.Cache.GetTickPage(ctx, key); err != nil {
			return nil, err
		} else if ok {
			covers, err := s.historyTicksCoverRequest(ctx, symbol, date, ticks, req)
			if err != nil {
				return nil, err
			}
			if covers {
				return ticks, nil
			}
		}
	}

	if s.Store != nil && !req.ForceRefresh {
		local, err := s.Store.QueryTicks(ctx, domain.HistoryTickQuery{Symbol: symbol, TradeDate: date, Start: int(req.Start), Limit: historyTickLocalLimit(req)})
		if err == nil {
			covers, err := s.historyTicksCoverRequest(ctx, symbol, date, local, req)
			if err != nil {
				return nil, err
			}
			if covers {
				markTicksCached(local)
				if s.Cache != nil {
					if err := s.Cache.PutHistoryTickPage(ctx, key, local); err != nil {
						return nil, err
					}
				}
				return local, nil
			}
		}
		if err != nil && !errorsIsNoData(err) {
			return nil, err
		}
	}

	remote, err := doGroup(ctx, s.historyTickGroup, key, func(context.Context) ([]domain.Tick, error) {
		if s.Provider == nil {
			_ = s.enqueue(ctx, domain.BackfillTask{Dataset: domain.DatasetHistoryTick, Symbol: symbol, StartDate: date, EndDate: date, Priority: 100})
			return nil, domain.ErrNoData
		}
		return s.Provider.GetHistoryTicks(ctx, symbol, req)
	})
	if err != nil {
		_ = s.enqueue(ctx, domain.BackfillTask{Dataset: domain.DatasetHistoryTick, Symbol: symbol, StartDate: date, EndDate: date, Priority: 100, ErrorMessage: err.Error()})
		return nil, err
	}
	if len(remote) == 0 {
		_ = s.putCoverage(ctx, domain.HistoryCoverage{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date, Status: domain.CoverageMissing})
		return nil, domain.ErrNoData
	}
	if s.Store != nil {
		if err := s.Store.PutTicks(ctx, remote); err != nil {
			return nil, err
		}
		if req.Full {
			if err := s.putCoverage(ctx, domain.HistoryCoverage{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date, Status: domain.CoverageCovered, RowCount: len(remote), Checksum: historyTickCoverageFullMarker, LastFetchTime: s.now()}); err != nil {
				return nil, err
			}
		}
	}
	if s.Cache != nil {
		if err := s.Cache.PutHistoryTickPage(ctx, key, remote); err != nil {
			return nil, err
		}
	}
	return remote, nil
}

func (s *MarketDataService) GetKLine(ctx context.Context, symbol domain.Symbol, req domain.KLineRequest) ([]domain.Bar, error) {
	return s.getBars(ctx, symbol, req, domain.AdjustNone, func(ctx context.Context, symbol domain.Symbol, req domain.KLineRequest) ([]domain.Bar, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetKLine(ctx, symbol, req)
	})
}

func (s *MarketDataService) GetAdjustedKLine(ctx context.Context, symbol domain.Symbol, req domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	return s.getBars(ctx, symbol, req.KLineRequest, req.AdjustType, func(ctx context.Context, symbol domain.Symbol, kreq domain.KLineRequest) ([]domain.Bar, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetAdjustedKLine(ctx, symbol, domain.AdjustedKLineRequest{KLineRequest: kreq, AdjustType: req.AdjustType})
	})
}

func (s *MarketDataService) GetXDXR(ctx context.Context, symbol domain.Symbol) ([]domain.XDXREvent, error) {
	if err := symbol.Validate(); err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if events, ok, err := s.Cache.GetXDXR(ctx, symbol); err != nil {
			return nil, err
		} else if ok {
			return events, nil
		}
	}
	events, err := doGroup(ctx, s.xdxrGroup, "xdxr|"+symbol.Key(), func(context.Context) ([]domain.XDXREvent, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetXDXR(ctx, symbol)
	})
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if err := s.Cache.PutXDXR(ctx, symbol, events); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func (s *MarketDataService) GetSecurityInfo(ctx context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	fetchReq := securityFetchQuery(req)
	fetchKey := securityCacheKey(fetchReq)
	if s.Cache != nil && !req.Refresh {
		if items, ok, err := s.Cache.GetSecurities(ctx, fetchKey); err != nil {
			return nil, err
		} else if ok {
			return filterSecurityInfo(items, req)
		}
	}

	items, err := doGroup(ctx, s.securityGroup, fetchKey, func(context.Context) ([]domain.SecurityInfo, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetSecurityInfo(ctx, fetchReq)
	})
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if err := s.Cache.PutSecurities(ctx, fetchKey, items); err != nil {
			return nil, err
		}
	}
	return filterSecurityInfo(items, req)
}

func (s *MarketDataService) GetTradingDay(ctx context.Context) (*domain.TradingDayInfo, error) {
	if s.Cache != nil {
		if info, ok, err := s.Cache.GetTradingDay(ctx); err != nil {
			return nil, err
		} else if ok {
			return info, nil
		}
	}
	info, err := doGroup(ctx, s.tradingDayGroup, "trading_day|status", func(context.Context) (*domain.TradingDayInfo, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetTradingDay(ctx)
	})
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if err := s.Cache.PutTradingDay(ctx, info); err != nil {
			return nil, err
		}
	}
	return info, nil
}

func (s *MarketDataService) GetFinance(ctx context.Context, symbol domain.Symbol) (*domain.FinanceInfo, error) {
	if err := symbol.Validate(); err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if info, ok, err := s.Cache.GetFinance(ctx, symbol); err != nil {
			return nil, err
		} else if ok {
			return info, nil
		}
	}
	info, err := doGroup(ctx, s.financeGroup, "finance|"+symbol.Key(), func(context.Context) (*domain.FinanceInfo, error) {
		if s.Provider == nil {
			return nil, domain.ErrUpstreamUnavailable
		}
		return s.Provider.GetFinance(ctx, symbol)
	})
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		if err := s.Cache.PutFinance(ctx, symbol, info); err != nil {
			return nil, err
		}
	}
	return info, nil
}

func (s *MarketDataService) getBars(ctx context.Context, symbol domain.Symbol, req domain.KLineRequest, adjust domain.AdjustType, fetch func(context.Context, domain.Symbol, domain.KLineRequest) ([]domain.Bar, error)) ([]domain.Bar, error) {
	if err := symbol.Validate(); err != nil {
		return nil, err
	}
	if req.Period == domain.PeriodUnknown {
		return nil, domain.ErrInvalidRequest
	}
	if adjust == "" {
		adjust = domain.AdjustNone
	}
	key := barCacheKey(symbol, req, adjust)
	if s.Cache != nil && !req.ForceRefresh {
		if bars, ok, err := s.Cache.GetBars(ctx, key); err != nil {
			return nil, err
		} else if ok && cachedBarsCoverRequest(bars, req) {
			return sliceBarsByStartCount(bars, req.Start, req.Count), nil
		}
	}

	if s.Store != nil && !req.ForceRefresh {
		localAll, err := s.Store.QueryBars(ctx, domain.BarQuery{Symbol: symbol, Period: req.Period, AdjustType: adjust, End: req.EndDate})
		if err == nil {
			local := filterBarsByDate(localAll, req.StartDate, req.EndDate)
			if localBarsCoverRequest(localAll, local, req) {
				local = sliceBarsByStartCount(local, req.Start, req.Count)
				if s.Cache != nil {
					if err := s.Cache.PutBars(ctx, key, local); err != nil {
						return nil, err
					}
				}
				return local, nil
			}
			if req.Period == domain.PeriodDay && !req.StartDate.IsZero() && !req.EndDate.IsZero() {
				gaps := barGapsForRequest(localAll, req)
				if len(gaps) > 0 {
					remote, err := fetchBarGaps(ctx, gaps, symbol, req, fetch)
					if err != nil {
						start, end := gapTaskRange(gaps)
						_ = s.enqueue(ctx, domain.BackfillTask{Dataset: datasetForAdjust(adjust), Symbol: symbol, StartDate: start, EndDate: end, Period: req.Period, AdjustType: adjust, Priority: 90, ErrorMessage: err.Error()})
						return nil, err
					}
					if len(remote) > 0 {
						if err := s.Store.PutBars(ctx, remote); err != nil {
							return nil, err
						}
					}
					merged := mergeBars(local, filterBarsByDate(remote, req.StartDate, req.EndDate))
					merged = sliceBarsByStartCount(merged, req.Start, req.Count)
					if s.Cache != nil && cachedBarsCoverRequest(merged, req) {
						if err := s.Cache.PutBars(ctx, key, merged); err != nil {
							return nil, err
						}
					}
					return merged, nil
				}
			}
		}
		if err != nil && !errorsIsNoData(err) {
			return nil, err
		}
	}

	remote, err := doGroup(ctx, s.barGroup, key, func(context.Context) ([]domain.Bar, error) { return fetch(ctx, symbol, req) })
	start, end := barTaskRange(req, remote)
	if err != nil {
		_ = s.enqueue(ctx, domain.BackfillTask{Dataset: datasetForAdjust(adjust), Symbol: symbol, StartDate: start, EndDate: end, Period: req.Period, AdjustType: adjust, Priority: 90, ErrorMessage: err.Error()})
		return nil, err
	}
	if len(remote) == 0 {
		_ = s.enqueue(ctx, domain.BackfillTask{Dataset: datasetForAdjust(adjust), Symbol: symbol, StartDate: start, EndDate: end, Period: req.Period, AdjustType: adjust, Priority: 90})
		return nil, domain.ErrNoData
	}
	if s.Store != nil {
		if err := s.Store.PutBars(ctx, remote); err != nil {
			return nil, err
		}
		if err := s.putCoverage(ctx, domain.HistoryCoverage{Dataset: datasetForAdjust(adjust), Symbol: symbol, TradeDate: start, Period: req.Period, AdjustType: adjust, Status: domain.CoverageCovered, RowCount: len(remote), LastFetchTime: s.now()}); err != nil {
			return nil, err
		}
	}
	if s.Cache != nil {
		if err := s.Cache.PutBars(ctx, key, remote); err != nil {
			return nil, err
		}
	}
	return sliceBarsByStartCount(remote, req.Start, req.Count), nil
}

func filterBarsByDate(bars []domain.Bar, startDate time.Time, endDate time.Time) []domain.Bar {
	start := domain.NormalizeDate(startDate)
	end := endDate
	out := make([]domain.Bar, 0, len(bars))
	for _, bar := range bars {
		if !start.IsZero() && bar.Time.Before(start) {
			continue
		}
		if !end.IsZero() && bar.Time.After(end) {
			continue
		}
		out = append(out, bar)
	}
	return out
}

type barGap struct {
	start time.Time
	end   time.Time
}

func fetchBarGaps(ctx context.Context, gaps []barGap, symbol domain.Symbol, req domain.KLineRequest, fetch func(context.Context, domain.Symbol, domain.KLineRequest) ([]domain.Bar, error)) ([]domain.Bar, error) {
	out := make([]domain.Bar, 0)
	for _, gap := range gaps {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		gapReq := req
		gapReq.Start = 0
		gapReq.Count = 0
		gapReq.StartDate = gap.start
		gapReq.EndDate = gap.end
		bars, err := fetch(ctx, symbol, gapReq)
		if err != nil {
			return nil, err
		}
		out = append(out, bars...)
	}
	return out, nil
}

func gapTaskRange(gaps []barGap) (time.Time, time.Time) {
	if len(gaps) == 0 {
		now := domain.NormalizeDate(time.Now())
		return now, now
	}
	return gaps[0].start, gaps[len(gaps)-1].end
}

func mergeBars(left []domain.Bar, right []domain.Bar) []domain.Bar {
	merged := make(map[int64]domain.Bar, len(left)+len(right))
	for _, bar := range left {
		merged[bar.Time.UnixNano()] = bar
	}
	for _, bar := range right {
		merged[bar.Time.UnixNano()] = bar
	}
	out := make([]domain.Bar, 0, len(merged))
	for _, bar := range merged {
		out = append(out, bar)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Time.Before(out[j].Time)
	})
	return out
}

func historyTickLocalLimit(req domain.HistoryTickRequest) int {
	if req.Full {
		return 0
	}
	count := req.Count
	if count == 0 {
		count = defaultHistoryTickCount
	}
	return int(count)
}

func (s *MarketDataService) historyTicksCoverRequest(ctx context.Context, symbol domain.Symbol, date time.Time, ticks []domain.Tick, req domain.HistoryTickRequest) (bool, error) {
	if len(ticks) == 0 {
		return false, nil
	}
	full, err := s.historyTicksHaveFullCoverage(ctx, symbol, date)
	if err != nil {
		return false, err
	}
	if req.Full {
		return full, nil
	}
	limit := historyTickLocalLimit(req)
	if limit > 0 && len(ticks) >= limit {
		return true, nil
	}
	return full, nil
}

func (s *MarketDataService) historyTicksHaveFullCoverage(ctx context.Context, symbol domain.Symbol, date time.Time) (bool, error) {
	if s.Store == nil {
		return true, nil
	}
	coverage, err := s.Store.Coverage(ctx, domain.CoverageRequest{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date})
	if err != nil {
		return false, err
	}
	return coverage.Status == domain.CoverageCovered && coverage.Checksum == historyTickCoverageFullMarker, nil
}

func localBarsCoverRequest(allBars []domain.Bar, filteredBars []domain.Bar, req domain.KLineRequest) bool {
	if len(allBars) == 0 || len(filteredBars) == 0 {
		return false
	}
	startDate := domain.NormalizeDate(req.StartDate)
	if !startDate.IsZero() {
		first := domain.NormalizeDate(allBars[0].Time)
		if first.IsZero() || first.After(startDate) {
			return false
		}
	}
	if req.Count > 0 && int(req.Start)+int(req.Count) > len(filteredBars) {
		return false
	}
	if len(barGapsForRequest(allBars, req)) > 0 {
		return false
	}
	return true
}

func cachedBarsCoverRequest(bars []domain.Bar, req domain.KLineRequest) bool {
	if len(bars) == 0 {
		return false
	}
	if req.Count > 0 && int(req.Start)+int(req.Count) > len(bars) {
		return false
	}
	if !req.StartDate.IsZero() {
		first := domain.NormalizeDate(bars[0].Time)
		start := domain.NormalizeDate(req.StartDate)
		if first.IsZero() || first.After(start) {
			return false
		}
	}
	if req.Period == domain.PeriodDay && !req.StartDate.IsZero() && !req.EndDate.IsZero() && len(barGapsForRequest(bars, req)) > 0 {
		return false
	}
	return true
}

func barGapsForRequest(bars []domain.Bar, req domain.KLineRequest) []barGap {
	if req.Period != domain.PeriodDay || req.StartDate.IsZero() || req.EndDate.IsZero() {
		return nil
	}
	start := domain.NormalizeDate(req.StartDate)
	end := domain.NormalizeDate(req.EndDate)
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return nil
	}
	inRange := filterBarsByDate(bars, start, end)
	if len(inRange) == 0 {
		return []barGap{{start: start, end: end}}
	}
	gaps := make([]barGap, 0)
	first := domain.NormalizeDate(inRange[0].Time)
	if first.After(start) {
		gaps = append(gaps, barGap{start: start, end: first.AddDate(0, 0, -1)})
	}
	for i := 1; i < len(inRange); i++ {
		prev := domain.NormalizeDate(inRange[i-1].Time)
		next := domain.NormalizeDate(inRange[i].Time)
		if prev.IsZero() || next.IsZero() || !next.After(prev) {
			continue
		}
		if next.Sub(prev) > maxDailyBarGap {
			gaps = append(gaps, barGap{start: prev.AddDate(0, 0, 1), end: next.AddDate(0, 0, -1)})
		}
	}
	last := domain.NormalizeDate(inRange[len(inRange)-1].Time)
	if end.After(last) && end.Sub(last) > maxDailyBarGap {
		gaps = append(gaps, barGap{start: last.AddDate(0, 0, 1), end: end})
	}
	return gaps
}

func (s *MarketDataService) enqueue(ctx context.Context, task domain.BackfillTask) error {
	if s.Queue == nil {
		return nil
	}
	if task.Status == "" {
		task.Status = domain.BackfillPending
	}
	_, _, err := s.Queue.Enqueue(ctx, task)
	return err
}

func (s *MarketDataService) putCoverage(ctx context.Context, coverage domain.HistoryCoverage) error {
	if s.Store == nil {
		return nil
	}
	return s.Store.PutCoverage(ctx, coverage)
}

func (s *MarketDataService) now() time.Time {
	if s.Clock == nil {
		return time.Now()
	}
	return s.Clock()
}

func doGroup[T any](ctx context.Context, group *cache.Group[T], key string, fn func(context.Context) (T, error)) (T, error) {
	if group == nil {
		return fn(ctx)
	}
	value, _, err := group.Do(ctx, key, fn)
	return value, err
}

func validateSymbols(symbols []domain.Symbol) error {
	if len(symbols) == 0 {
		return domain.ErrInvalidRequest
	}
	for _, symbol := range symbols {
		if err := symbol.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func orderQuotes(symbols []domain.Symbol, hits map[string]domain.Quote, remote []domain.Quote) []domain.Quote {
	items := make(map[string]domain.Quote, len(hits)+len(remote))
	for key, quote := range hits {
		items[key] = quote
	}
	for _, quote := range remote {
		items[quote.Symbol.Key()] = quote
	}
	out := make([]domain.Quote, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, symbol := range symbols {
		key := symbol.Key()
		if quote, ok := items[key]; ok {
			out = append(out, quote)
			seen[key] = struct{}{}
		}
	}
	for _, quote := range remote {
		key := quote.Symbol.Key()
		if _, ok := seen[key]; !ok {
			out = append(out, quote)
		}
	}
	return out
}

func orderBooks(symbols []domain.Symbol, hits map[string]domain.OrderBook, remote []domain.OrderBook) []domain.OrderBook {
	items := make(map[string]domain.OrderBook, len(hits)+len(remote))
	for key, book := range hits {
		items[key] = book
	}
	for _, book := range remote {
		items[book.Symbol.Key()] = book
	}
	out := make([]domain.OrderBook, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, symbol := range symbols {
		key := symbol.Key()
		if book, ok := items[key]; ok {
			out = append(out, book)
			seen[key] = struct{}{}
		}
	}
	for _, book := range remote {
		key := book.Symbol.Key()
		if _, ok := seen[key]; !ok {
			out = append(out, book)
		}
	}
	return out
}

func markTicksCached(ticks []domain.Tick) {
	for i := range ticks {
		ticks[i].Cached = true
	}
}

func datasetForAdjust(adjust domain.AdjustType) domain.Dataset {
	if adjust == domain.AdjustNone || adjust == "" {
		return domain.DatasetKLine
	}
	return domain.DatasetAdjustedKLine
}

func barTaskRange(req domain.KLineRequest, bars []domain.Bar) (time.Time, time.Time) {
	start := domain.NormalizeDate(req.StartDate)
	end := domain.NormalizeDate(req.EndDate)
	if len(bars) > 0 {
		if start.IsZero() {
			start = domain.NormalizeDate(bars[0].Time)
		}
		if end.IsZero() {
			end = domain.NormalizeDate(bars[len(bars)-1].Time)
		}
	}
	if start.IsZero() {
		start = domain.NormalizeDate(time.Now())
	}
	if end.IsZero() {
		end = start
	}
	return start, end
}

func symbolsKey(prefix string, symbols []domain.Symbol) string {
	keys := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		keys = append(keys, symbol.Key())
	}
	sort.Strings(keys)
	return prefix + "|" + strings.Join(keys, ",")
}

func sliceBarsByStartCount(bars []domain.Bar, start uint16, count uint16) []domain.Bar {
	if len(bars) == 0 {
		return bars
	}
	from := int(start)
	if from > len(bars) {
		return []domain.Bar{}
	}
	to := len(bars)
	if count > 0 {
		limit := from + int(count)
		if limit < to {
			to = limit
		}
	}
	out := make([]domain.Bar, to-from)
	copy(out, bars[from:to])
	return out
}

func tickCacheKey(prefix string, symbol domain.Symbol, start uint16, count uint16, full bool, force bool) string {
	return fmt.Sprintf("%s|%s|start=%d|count=%d|full=%t|force=%t", prefix, symbol.Key(), start, count, full, force)
}

func historyTickCacheKey(symbol domain.Symbol, req domain.HistoryTickRequest) string {
	return fmt.Sprintf("history_tick_v2|%s|date=%s|start=%d|count=%d|full=%t|trans=%t", symbol.Key(), domain.NormalizeDate(req.TradeDate).Format("20060102"), req.Start, req.Count, req.Full, req.WithTransactionFlag)
}

func barCacheKey(symbol domain.Symbol, req domain.KLineRequest, adjust domain.AdjustType) string {
	return fmt.Sprintf("bar|%s|period=%s|adjust=%s|start=%d|count=%d|times=%d|from=%s|to=%s", symbol.Key(), req.Period, adjust, req.Start, req.Count, req.Times, domain.NormalizeDate(req.StartDate).Format("20060102"), domain.NormalizeDate(req.EndDate).Format("20060102"))
}

func securityCacheKey(req domain.SecurityQuery) string {
	markets := make([]string, 0, len(req.Markets))
	for _, market := range req.Markets {
		markets = append(markets, string(domain.NormalizeMarket(market)))
	}
	sort.Strings(markets)
	symbols := make([]string, 0, len(req.Symbols))
	for _, symbol := range req.Symbols {
		symbols = append(symbols, symbol.Key())
	}
	sort.Strings(symbols)
	return fmt.Sprintf("securities|markets=%s|symbols=%s|start=%d|count=%d", strings.Join(markets, ","), strings.Join(symbols, ","), req.Start, req.Count)
}

func securityFetchQuery(req domain.SecurityQuery) domain.SecurityQuery {
	markets := req.Markets
	if len(markets) == 0 && len(req.Symbols) > 0 {
		seen := make(map[domain.Market]struct{}, len(req.Symbols))
		for _, symbol := range req.Symbols {
			market := domain.NormalizeMarket(symbol.Market)
			if market == domain.MarketUnknown {
				continue
			}
			if _, ok := seen[market]; ok {
				continue
			}
			seen[market] = struct{}{}
			markets = append(markets, market)
		}
	}
	return domain.SecurityQuery{Markets: markets, Refresh: req.Refresh}
}

func filterSecurityInfo(items []domain.SecurityInfo, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	markets := make(map[domain.Market]struct{}, len(req.Markets))
	for _, market := range req.Markets {
		markets[domain.NormalizeMarket(market)] = struct{}{}
	}
	symbols := make(map[string]struct{}, len(req.Symbols))
	for _, symbol := range req.Symbols {
		if err := symbol.Validate(); err != nil {
			return nil, err
		}
		symbols[symbol.Key()] = struct{}{}
	}
	out := make([]domain.SecurityInfo, 0, len(items))
	for _, item := range items {
		if len(markets) > 0 {
			if _, ok := markets[domain.NormalizeMarket(item.Symbol.Market)]; !ok {
				continue
			}
		}
		if len(symbols) > 0 {
			if _, ok := symbols[item.Symbol.Key()]; !ok {
				continue
			}
		}
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Symbol.Market != out[j].Symbol.Market {
			return out[i].Symbol.Market < out[j].Symbol.Market
		}
		return out[i].Symbol.Code < out[j].Symbol.Code
	})
	if req.Start > uint32(len(out)) {
		return []domain.SecurityInfo{}, nil
	}
	from := int(req.Start)
	to := len(out)
	if req.Count > 0 {
		limit := from + int(req.Count)
		if limit < to {
			to = limit
		}
	}
	return append([]domain.SecurityInfo(nil), out[from:to]...), nil
}

func errorsIsNoData(err error) bool {
	return err == domain.ErrNoData
}
