package cache

import (
	"context"
	"sync"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

const (
	defaultQuoteTTL       = 500 * time.Millisecond
	defaultOrderBookTTL   = 500 * time.Millisecond
	defaultTickTTL        = 2 * time.Second
	defaultHistoryTickTTL = 5 * time.Minute
	defaultBarTTL         = 5 * time.Minute
	defaultXDXRTTL        = 24 * time.Hour
	defaultSecurityTTL    = 24 * time.Hour
	defaultFinanceTTL     = 24 * time.Hour
)

// Config controls cache lifetimes shared by the in-process and Redis backends.
type Config struct {
	QuoteTTL       time.Duration
	OrderBookTTL   time.Duration
	TickTTL        time.Duration
	HistoryTickTTL time.Duration
	BarTTL         time.Duration
	XDXRTTL        time.Duration
	SecurityTTL    time.Duration
	FinanceTTL     time.Duration
	Clock          func() time.Time
}

func DefaultConfig() Config {
	return Config{
		QuoteTTL:       defaultQuoteTTL,
		OrderBookTTL:   defaultOrderBookTTL,
		TickTTL:        defaultTickTTL,
		HistoryTickTTL: defaultHistoryTickTTL,
		BarTTL:         defaultBarTTL,
		XDXRTTL:        defaultXDXRTTL,
		SecurityTTL:    defaultSecurityTTL,
		FinanceTTL:     defaultFinanceTTL,
		Clock:          time.Now,
	}
}

// Memory is the first-phase hot cache for quotes, order books, history pages, bars and low-frequency metadata.
type Memory struct {
	mu sync.RWMutex

	cfg Config

	quotes     map[string]entry[domain.Quote]
	orderBooks map[string]entry[domain.OrderBook]
	tickPages  map[string]entry[[]domain.Tick]
	barRanges  map[string]entry[[]domain.Bar]
	xdxr       map[string]entry[[]domain.XDXREvent]
	securities map[string]entry[[]domain.SecurityInfo]
	finance    map[string]entry[*domain.FinanceInfo]
	tradingDay entry[*domain.TradingDayInfo]
}

type entry[T any] struct {
	value     T
	expiresAt time.Time
}

func NewMemory(cfg Config) *Memory {
	cfg = normalizeConfig(cfg)
	return &Memory{
		cfg:        cfg,
		quotes:     make(map[string]entry[domain.Quote]),
		orderBooks: make(map[string]entry[domain.OrderBook]),
		tickPages:  make(map[string]entry[[]domain.Tick]),
		barRanges:  make(map[string]entry[[]domain.Bar]),
		xdxr:       make(map[string]entry[[]domain.XDXREvent]),
		securities: make(map[string]entry[[]domain.SecurityInfo]),
		finance:    make(map[string]entry[*domain.FinanceInfo]),
	}
}

func normalizeConfig(cfg Config) Config {
	defaults := DefaultConfig()
	if cfg.QuoteTTL == 0 {
		cfg.QuoteTTL = defaults.QuoteTTL
	}
	if cfg.OrderBookTTL == 0 {
		cfg.OrderBookTTL = defaults.OrderBookTTL
	}
	if cfg.TickTTL == 0 {
		cfg.TickTTL = defaults.TickTTL
	}
	if cfg.HistoryTickTTL == 0 {
		cfg.HistoryTickTTL = defaults.HistoryTickTTL
	}
	if cfg.BarTTL == 0 {
		cfg.BarTTL = defaults.BarTTL
	}
	if cfg.XDXRTTL == 0 {
		cfg.XDXRTTL = defaults.XDXRTTL
	}
	if cfg.SecurityTTL == 0 {
		cfg.SecurityTTL = defaults.SecurityTTL
	}
	if cfg.FinanceTTL == 0 {
		cfg.FinanceTTL = defaults.FinanceTTL
	}
	if cfg.Clock == nil {
		cfg.Clock = defaults.Clock
	}
	return cfg
}

func (m *Memory) GetQuotes(ctx context.Context, symbols []domain.Symbol) (map[string]domain.Quote, []domain.Symbol, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	now := m.now()
	hits := make(map[string]domain.Quote, len(symbols))
	misses := make([]domain.Symbol, 0)
	m.mu.RLock()
	for _, symbol := range symbols {
		key := symbol.Key()
		item, ok := m.quotes[key]
		if ok && now.Before(item.expiresAt) {
			quote := item.value
			quote.Cached = true
			hits[key] = quote
			continue
		}
		misses = append(misses, symbol)
	}
	m.mu.RUnlock()
	return hits, misses, nil
}

func (m *Memory) PutQuotes(ctx context.Context, quotes []domain.Quote) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	expiresAt := m.now().Add(m.cfg.QuoteTTL)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, quote := range quotes {
		if quote.Symbol.Code == "" {
			continue
		}
		quote.Cached = false
		m.quotes[quote.Symbol.Key()] = entry[domain.Quote]{value: quote, expiresAt: expiresAt}
	}
	return nil
}

func (m *Memory) GetOrderBooks(ctx context.Context, symbols []domain.Symbol) (map[string]domain.OrderBook, []domain.Symbol, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	now := m.now()
	hits := make(map[string]domain.OrderBook, len(symbols))
	misses := make([]domain.Symbol, 0)
	m.mu.RLock()
	for _, symbol := range symbols {
		key := symbol.Key()
		item, ok := m.orderBooks[key]
		if ok && now.Before(item.expiresAt) {
			book := item.value
			book.Cached = true
			hits[key] = book
			continue
		}
		misses = append(misses, symbol)
	}
	m.mu.RUnlock()
	return hits, misses, nil
}

func (m *Memory) PutOrderBooks(ctx context.Context, books []domain.OrderBook) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	expiresAt := m.now().Add(m.cfg.OrderBookTTL)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, book := range books {
		if book.Symbol.Code == "" {
			continue
		}
		book.Cached = false
		m.orderBooks[book.Symbol.Key()] = entry[domain.OrderBook]{value: book, expiresAt: expiresAt}
	}
	return nil
}

func (m *Memory) GetTickPage(ctx context.Context, key string) ([]domain.Tick, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	item, ok := m.tickPages[key]
	m.mu.RUnlock()
	if !ok || !m.now().Before(item.expiresAt) {
		return nil, false, nil
	}
	return markCachedTicks(copyTicks(item.value)), true, nil
}

func (m *Memory) PutTickPage(ctx context.Context, key string, ticks []domain.Tick, ttl time.Duration) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if ttl == 0 {
		ttl = m.cfg.TickTTL
	}
	m.mu.Lock()
	m.tickPages[key] = entry[[]domain.Tick]{value: copyTicks(ticks), expiresAt: m.now().Add(ttl)}
	m.mu.Unlock()
	return nil
}

func (m *Memory) PutHistoryTickPage(ctx context.Context, key string, ticks []domain.Tick) error {
	return m.PutTickPage(ctx, key, ticks, m.cfg.HistoryTickTTL)
}

func (m *Memory) GetBars(ctx context.Context, key string) ([]domain.Bar, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	item, ok := m.barRanges[key]
	m.mu.RUnlock()
	if !ok || !m.now().Before(item.expiresAt) {
		return nil, false, nil
	}
	return append([]domain.Bar(nil), item.value...), true, nil
}

func (m *Memory) PutBars(ctx context.Context, key string, bars []domain.Bar) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	m.barRanges[key] = entry[[]domain.Bar]{value: append([]domain.Bar(nil), bars...), expiresAt: m.now().Add(m.cfg.BarTTL)}
	m.mu.Unlock()
	return nil
}

func (m *Memory) GetXDXR(ctx context.Context, symbol domain.Symbol) ([]domain.XDXREvent, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	item, ok := m.xdxr[symbol.Key()]
	m.mu.RUnlock()
	if !ok || !m.now().Before(item.expiresAt) {
		return nil, false, nil
	}
	return append([]domain.XDXREvent(nil), item.value...), true, nil
}

func (m *Memory) PutXDXR(ctx context.Context, symbol domain.Symbol, events []domain.XDXREvent) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	m.xdxr[symbol.Key()] = entry[[]domain.XDXREvent]{value: append([]domain.XDXREvent(nil), events...), expiresAt: m.now().Add(m.cfg.XDXRTTL)}
	m.mu.Unlock()
	return nil
}

func (m *Memory) GetSecurities(ctx context.Context, key string) ([]domain.SecurityInfo, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	item, ok := m.securities[key]
	m.mu.RUnlock()
	if !ok || !m.now().Before(item.expiresAt) {
		return nil, false, nil
	}
	items := append([]domain.SecurityInfo(nil), item.value...)
	markSecuritiesCached(items)
	return items, true, nil
}

func (m *Memory) PutSecurities(ctx context.Context, key string, items []domain.SecurityInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	items = append([]domain.SecurityInfo(nil), items...)
	clearSecuritiesCached(items)
	m.mu.Lock()
	m.securities[key] = entry[[]domain.SecurityInfo]{value: items, expiresAt: m.now().Add(m.cfg.SecurityTTL)}
	m.mu.Unlock()
	return nil
}

func markSecuritiesCached(items []domain.SecurityInfo) {
	for i := range items {
		items[i].Cached = true
	}
}

func clearSecuritiesCached(items []domain.SecurityInfo) {
	for i := range items {
		items[i].Cached = false
	}
}

func (m *Memory) GetFinance(ctx context.Context, symbol domain.Symbol) (*domain.FinanceInfo, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	item, ok := m.finance[symbol.Key()]
	m.mu.RUnlock()
	if !ok || !m.now().Before(item.expiresAt) || item.value == nil {
		return nil, false, nil
	}
	info := *item.value
	info.Cached = true
	return &info, true, nil
}

func (m *Memory) PutFinance(ctx context.Context, symbol domain.Symbol, info *domain.FinanceInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	copyInfo := *info
	copyInfo.Cached = false
	m.mu.Lock()
	m.finance[symbol.Key()] = entry[*domain.FinanceInfo]{value: &copyInfo, expiresAt: m.now().Add(m.cfg.FinanceTTL)}
	m.mu.Unlock()
	return nil
}

func (m *Memory) GetTradingDay(ctx context.Context) (*domain.TradingDayInfo, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	item := m.tradingDay
	m.mu.RUnlock()
	if item.value == nil || !m.now().Before(item.expiresAt) {
		return nil, false, nil
	}
	info := *item.value
	info.Cached = true
	return &info, true, nil
}

func (m *Memory) PutTradingDay(ctx context.Context, info *domain.TradingDayInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	copyInfo := *info
	copyInfo.Cached = false
	m.mu.Lock()
	m.tradingDay = entry[*domain.TradingDayInfo]{value: &copyInfo, expiresAt: m.now().Add(m.cfg.SecurityTTL)}
	m.mu.Unlock()
	return nil
}

func (m *Memory) Close() error {
	return nil
}

func (m *Memory) now() time.Time {
	return m.cfg.Clock()
}

func copyTicks(ticks []domain.Tick) []domain.Tick {
	return append([]domain.Tick(nil), ticks...)
}

func markCachedTicks(ticks []domain.Tick) []domain.Tick {
	for i := range ticks {
		ticks[i].Cached = true
	}
	return ticks
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
