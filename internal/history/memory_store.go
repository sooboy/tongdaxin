package history

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

// MemoryStore is a concurrency-safe local history store for the initial backtest path.
// It is intentionally deterministic and can later be replaced by a SQL/columnar implementation.
type MemoryStore struct {
	mu sync.RWMutex

	coverage   map[string]domain.HistoryCoverage
	securities map[string]domain.SecurityInfo
	ticks      map[string][]domain.Tick
	bars       map[string][]domain.Bar

	tasks     map[string]domain.BackfillTask
	taskByID  map[string]string
	taskOrder []string
	clock     func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		coverage:   make(map[string]domain.HistoryCoverage),
		securities: make(map[string]domain.SecurityInfo),
		ticks:      make(map[string][]domain.Tick),
		bars:       make(map[string][]domain.Bar),
		tasks:      make(map[string]domain.BackfillTask),
		taskByID:   make(map[string]string),
		clock:      time.Now,
	}
}

func (s *MemoryStore) Coverage(ctx context.Context, req domain.CoverageRequest) (domain.HistoryCoverage, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.HistoryCoverage{}, err
	}
	if err := req.Symbol.Validate(); err != nil {
		return domain.HistoryCoverage{}, err
	}

	key := coverageKey(req.Dataset, req.Symbol, req.TradeDate, req.Period, req.AdjustType)
	s.mu.RLock()
	coverage, ok := s.coverage[key]
	s.mu.RUnlock()
	if ok {
		return coverage, nil
	}
	return domain.HistoryCoverage{
		Dataset:    req.Dataset,
		Symbol:     req.Symbol,
		TradeDate:  domain.NormalizeDate(req.TradeDate),
		Period:     req.Period,
		AdjustType: req.AdjustType,
		Status:     domain.CoverageMissing,
	}, nil
}

func (s *MemoryStore) PutCoverage(ctx context.Context, coverage domain.HistoryCoverage) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if err := coverage.Symbol.Validate(); err != nil {
		return err
	}
	coverage.TradeDate = domain.NormalizeDate(coverage.TradeDate)
	if coverage.Status == "" {
		coverage.Status = domain.CoverageCovered
	}
	key := coverageKey(coverage.Dataset, coverage.Symbol, coverage.TradeDate, coverage.Period, coverage.AdjustType)
	s.mu.Lock()
	s.coverage[key] = coverage
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) PutSecurities(ctx context.Context, items []domain.SecurityInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range items {
		if err := item.Symbol.Validate(); err != nil {
			return err
		}
		item.Cached = false
		s.securities[item.Symbol.Key()] = item
	}
	return nil
}

func (s *MemoryStore) QuerySecurities(ctx context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	items := make([]domain.SecurityInfo, 0, len(s.securities))
	for _, item := range s.securities {
		item.Cached = true
		items = append(items, item)
	}
	s.mu.RUnlock()
	return filterSecurities(items, req)
}

func (s *MemoryStore) PutTicks(ctx context.Context, ticks []domain.Tick) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if len(ticks) == 0 {
		return nil
	}

	grouped := make(map[string][]domain.Tick)
	for _, tick := range ticks {
		if err := tick.Symbol.Validate(); err != nil {
			return err
		}
		if tick.TradeDate.IsZero() {
			tick.TradeDate = domain.NormalizeDate(tick.TradeTime)
		} else {
			tick.TradeDate = domain.NormalizeDate(tick.TradeDate)
		}
		if tick.TradeDate.IsZero() {
			return domain.ErrInvalidRequest
		}
		key := tickKey(tick.Symbol, tick.TradeDate)
		grouped[key] = append(grouped[key], tick)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key, items := range grouped {
		merged := make(map[string]domain.Tick, len(s.ticks[key])+len(items))
		for _, tick := range s.ticks[key] {
			merged[tickRowKey(tick)] = tick
		}
		for _, tick := range items {
			merged[tickRowKey(tick)] = tick
		}
		out := make([]domain.Tick, 0, len(merged))
		for _, tick := range merged {
			out = append(out, tick)
		}
		sortTicks(out)
		s.ticks[key] = out
	}
	return nil
}

func (s *MemoryStore) QueryTicks(ctx context.Context, req domain.HistoryTickQuery) ([]domain.Tick, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if err := req.Symbol.Validate(); err != nil {
		return nil, err
	}
	date := domain.NormalizeDate(req.TradeDate)
	if date.IsZero() {
		return nil, domain.ErrInvalidRequest
	}
	if req.Start < 0 || req.Limit < 0 {
		return nil, domain.ErrInvalidRequest
	}

	s.mu.RLock()
	items := append([]domain.Tick(nil), s.ticks[tickKey(req.Symbol, date)]...)
	s.mu.RUnlock()
	if len(items) == 0 {
		return nil, domain.ErrNoData
	}
	start := req.Start
	if start > len(items) {
		return []domain.Tick{}, nil
	}
	end := len(items)
	if req.Limit > 0 && start+req.Limit < end {
		end = start + req.Limit
	}
	return append([]domain.Tick(nil), items[start:end]...), nil
}

func (s *MemoryStore) PutBars(ctx context.Context, bars []domain.Bar) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if len(bars) == 0 {
		return nil
	}

	grouped := make(map[string][]domain.Bar)
	for _, bar := range bars {
		if err := bar.Symbol.Validate(); err != nil {
			return err
		}
		if bar.Period == domain.PeriodUnknown || bar.Time.IsZero() {
			return domain.ErrInvalidRequest
		}
		if bar.AdjustType == "" {
			bar.AdjustType = domain.AdjustNone
		}
		key := barKey(bar.Symbol, bar.Period, bar.AdjustType)
		grouped[key] = append(grouped[key], bar)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key, items := range grouped {
		merged := make(map[int64]domain.Bar, len(s.bars[key])+len(items))
		for _, bar := range s.bars[key] {
			merged[bar.Time.UnixNano()] = bar
		}
		for _, bar := range items {
			merged[bar.Time.UnixNano()] = bar
		}
		bars := make([]domain.Bar, 0, len(merged))
		for _, bar := range merged {
			bars = append(bars, bar)
		}
		sortBars(bars)
		s.bars[key] = bars
	}
	return nil
}

func (s *MemoryStore) QueryBars(ctx context.Context, req domain.BarQuery) ([]domain.Bar, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if err := req.Symbol.Validate(); err != nil {
		return nil, err
	}
	if req.Period == domain.PeriodUnknown {
		return nil, domain.ErrInvalidRequest
	}
	adjust := req.AdjustType
	if adjust == "" {
		adjust = domain.AdjustNone
	}

	s.mu.RLock()
	items := append([]domain.Bar(nil), s.bars[barKey(req.Symbol, req.Period, adjust)]...)
	s.mu.RUnlock()
	if len(items) == 0 {
		return nil, domain.ErrNoData
	}
	out := make([]domain.Bar, 0, len(items))
	for _, bar := range items {
		if !req.Start.IsZero() && bar.Time.Before(req.Start) {
			continue
		}
		if !req.End.IsZero() && bar.Time.After(req.End) {
			continue
		}
		out = append(out, bar)
	}
	if len(out) == 0 {
		return nil, domain.ErrNoData
	}
	return out, nil
}

func (s *MemoryStore) Enqueue(ctx context.Context, task domain.BackfillTask) (domain.BackfillTask, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.BackfillTask{}, false, err
	}
	if err := task.Symbol.Validate(); err != nil {
		return domain.BackfillTask{}, false, err
	}
	if task.Dataset == "" || task.StartDate.IsZero() || task.EndDate.IsZero() || task.EndDate.Before(task.StartDate) {
		return domain.BackfillTask{}, false, domain.ErrInvalidRequest
	}
	if task.AdjustType == "" {
		task.AdjustType = domain.AdjustNone
	}
	if task.Status == "" {
		task.Status = domain.BackfillPending
	}
	task.StartDate = domain.NormalizeDate(task.StartDate)
	task.EndDate = domain.NormalizeDate(task.EndDate)
	key := taskKey(task)

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.tasks[key]; ok {
		return existing, false, nil
	}
	now := s.now()
	if task.TaskID == "" {
		task.TaskID = stableTaskID(key)
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	task.UpdatedAt = now
	s.tasks[key] = task
	s.taskByID[task.TaskID] = key
	s.taskOrder = append(s.taskOrder, key)
	return task, true, nil
}

func (s *MemoryStore) Next(ctx context.Context) (domain.BackfillTask, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.BackfillTask{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	bestIndex := -1
	var best domain.BackfillTask
	for i, key := range s.taskOrder {
		task := s.tasks[key]
		if task.Status != domain.BackfillPending && task.Status != domain.BackfillRetrying {
			continue
		}
		if bestIndex == -1 || task.Priority > best.Priority || (task.Priority == best.Priority && task.CreatedAt.Before(best.CreatedAt)) {
			bestIndex = i
			best = task
		}
	}
	if bestIndex == -1 {
		return domain.BackfillTask{}, false, nil
	}
	best.Status = domain.BackfillRunning
	best.UpdatedAt = s.now()
	key := s.taskOrder[bestIndex]
	s.tasks[key] = best
	return best, true, nil
}

func (s *MemoryStore) Update(ctx context.Context, task domain.BackfillTask) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if task.TaskID == "" {
		return domain.ErrInvalidRequest
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.taskByID[task.TaskID]
	if !ok {
		return domain.ErrNoData
	}
	task.UpdatedAt = s.now()
	s.tasks[key] = task
	return nil
}

func (s *MemoryStore) List(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillTask, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.BackfillTask, 0, len(s.tasks))
	for _, key := range s.taskOrder {
		task := s.tasks[key]
		if filter.Dataset != "" && task.Dataset != filter.Dataset {
			continue
		}
		if filter.Status != "" && task.Status != filter.Status {
			continue
		}
		if filter.Symbol.Code != "" && task.Symbol.Key() != filter.Symbol.Key() {
			continue
		}
		out = append(out, task)
	}
	return out, nil
}

func (s *MemoryStore) now() time.Time {
	if s.clock == nil {
		return time.Now()
	}
	return s.clock()
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func coverageKey(dataset domain.Dataset, symbol domain.Symbol, tradeDate time.Time, period domain.Period, adjust domain.AdjustType) string {
	return strings.Join([]string{string(dataset), symbol.Key(), dateKey(tradeDate), string(period), string(adjust)}, "|")
}

func tickRowKey(tick domain.Tick) string {
	return strconv.FormatInt(tick.TradeTime.UnixNano(), 10) + "|" + strconv.FormatInt(tick.Sequence, 10)
}

func tickKey(symbol domain.Symbol, tradeDate time.Time) string {
	return strings.Join([]string{symbol.Key(), dateKey(tradeDate)}, "|")
}

func barKey(symbol domain.Symbol, period domain.Period, adjust domain.AdjustType) string {
	return strings.Join([]string{symbol.Key(), string(period), string(adjust)}, "|")
}

func taskKey(task domain.BackfillTask) string {
	return strings.Join([]string{
		string(task.Dataset),
		task.Symbol.Key(),
		dateKey(task.StartDate),
		dateKey(task.EndDate),
		string(task.Period),
		string(task.AdjustType),
	}, "|")
}

func dateKey(t time.Time) string {
	date := domain.NormalizeDate(t)
	if date.IsZero() {
		return ""
	}
	return date.Format("20060102")
}

func stableTaskID(key string) string {
	sum := sha1.Sum([]byte(key))
	return hex.EncodeToString(sum[:8])
}

func sortTicks(ticks []domain.Tick) {
	sort.SliceStable(ticks, func(i, j int) bool {
		if !ticks[i].TradeTime.Equal(ticks[j].TradeTime) {
			return ticks[i].TradeTime.Before(ticks[j].TradeTime)
		}
		return ticks[i].Sequence < ticks[j].Sequence
	})
}

func sortBars(bars []domain.Bar) {
	sort.SliceStable(bars, func(i, j int) bool {
		return bars[i].Time.Before(bars[j].Time)
	})
}

func filterSecurities(items []domain.SecurityInfo, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	if len(items) == 0 {
		return nil, domain.ErrNoData
	}
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

func Itoa(v int) string { return strconv.Itoa(v) }
