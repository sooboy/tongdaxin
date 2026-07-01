package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sooboy/tongdaxin/internal/domain"
)

const defaultRedisKeyPrefix = "marketdata:cache:v1"

type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Close() error
}

// Redis is the optional distributed cache backend.
type Redis struct {
	client redisClient
	cfg    Config
	prefix string

	closeOnce sync.Once
	closeErr  error
}

// OpenRedis connects to Redis at rawURL and verifies the cache is reachable.
func OpenRedis(ctx context.Context, rawURL string, cfg Config) (*Redis, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return NewRedis(client, cfg), nil
}

// NewRedis wraps an existing Redis client.
func NewRedis(client redisClient, cfg Config) *Redis {
	cfg = normalizeConfig(cfg)
	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = defaultRedisKeyPrefix
	}
	return &Redis{client: client, cfg: cfg, prefix: prefix}
}

// Close releases the underlying Redis client.

func (r *Redis) GetTradingDay(ctx context.Context) (*domain.TradingDayInfo, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	info, ok, err := getJSON[*domain.TradingDayInfo](ctx, r.client, r.key("trading_day_v2", "status"))
	if err != nil || !ok || info == nil {
		return info, ok, err
	}
	info.Cached = true
	return info, true, nil
}

func (r *Redis) PutTradingDay(ctx context.Context, info *domain.TradingDayInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	copyInfo := *info
	copyInfo.Cached = false
	return putJSON(ctx, r.client, r.key("trading_day_v2", "status"), &copyInfo, r.cfg.SecurityTTL)
}

func (r *Redis) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.closeErr = r.client.Close()
	})
	return r.closeErr
}

func (r *Redis) GetQuotes(ctx context.Context, symbols []domain.Symbol) (map[string]domain.Quote, []domain.Symbol, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	hits := make(map[string]domain.Quote, len(symbols))
	misses := make([]domain.Symbol, 0, len(symbols))
	for _, symbol := range symbols {
		quote, ok, err := getJSON[domain.Quote](ctx, r.client, r.key("quotes", symbol.Key()))
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			misses = append(misses, symbol)
			continue
		}
		quote.Cached = true
		hits[symbol.Key()] = quote
	}
	return hits, misses, nil
}

func (r *Redis) PutQuotes(ctx context.Context, quotes []domain.Quote) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	for _, quote := range quotes {
		if quote.Symbol.Code == "" {
			continue
		}
		quote.Cached = false
		if err := putJSON(ctx, r.client, r.key("quotes", quote.Symbol.Key()), quote, r.cfg.QuoteTTL); err != nil {
			return err
		}
	}
	return nil
}

func (r *Redis) GetOrderBooks(ctx context.Context, symbols []domain.Symbol) (map[string]domain.OrderBook, []domain.Symbol, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	hits := make(map[string]domain.OrderBook, len(symbols))
	misses := make([]domain.Symbol, 0, len(symbols))
	for _, symbol := range symbols {
		book, ok, err := getJSON[domain.OrderBook](ctx, r.client, r.key("orderbooks", symbol.Key()))
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			misses = append(misses, symbol)
			continue
		}
		book.Cached = true
		hits[symbol.Key()] = book
	}
	return hits, misses, nil
}

func (r *Redis) PutOrderBooks(ctx context.Context, books []domain.OrderBook) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	for _, book := range books {
		if book.Symbol.Code == "" {
			continue
		}
		book.Cached = false
		if err := putJSON(ctx, r.client, r.key("orderbooks", book.Symbol.Key()), book, r.cfg.OrderBookTTL); err != nil {
			return err
		}
	}
	return nil
}

func (r *Redis) GetTickPage(ctx context.Context, key string) ([]domain.Tick, bool, error) {
	return r.getTicks(ctx, key)
}

func (r *Redis) PutTickPage(ctx context.Context, key string, ticks []domain.Tick, ttl time.Duration) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if ttl == 0 {
		ttl = r.cfg.TickTTL
	}
	return putJSON(ctx, r.client, r.key("tick-pages", key), ticks, ttl)
}

func (r *Redis) PutHistoryTickPage(ctx context.Context, key string, ticks []domain.Tick) error {
	return r.PutTickPage(ctx, key, ticks, r.cfg.HistoryTickTTL)
}

func (r *Redis) GetBars(ctx context.Context, key string) ([]domain.Bar, bool, error) {
	return r.getBars(ctx, key)
}

func (r *Redis) PutBars(ctx context.Context, key string, bars []domain.Bar) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	return putJSON(ctx, r.client, r.key("bars", key), bars, r.cfg.BarTTL)
}

func (r *Redis) GetXDXR(ctx context.Context, symbol domain.Symbol) ([]domain.XDXREvent, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	events, ok, err := getJSON[[]domain.XDXREvent](ctx, r.client, r.key("xdxr", symbol.Key()))
	if err != nil || !ok {
		return events, ok, err
	}
	return events, true, nil
}

func (r *Redis) PutXDXR(ctx context.Context, symbol domain.Symbol, events []domain.XDXREvent) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	return putJSON(ctx, r.client, r.key("xdxr", symbol.Key()), events, r.cfg.XDXRTTL)
}

func (r *Redis) GetSecurities(ctx context.Context, key string) ([]domain.SecurityInfo, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	items, ok, err := getJSON[[]domain.SecurityInfo](ctx, r.client, r.key("securities", key))
	if err != nil || !ok {
		return items, ok, err
	}
	markSecuritiesCached(items)
	return items, true, nil
}

func (r *Redis) PutSecurities(ctx context.Context, key string, items []domain.SecurityInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	items = append([]domain.SecurityInfo(nil), items...)
	clearSecuritiesCached(items)
	return putJSON(ctx, r.client, r.key("securities", key), items, r.cfg.SecurityTTL)
}

func (r *Redis) GetFinance(ctx context.Context, symbol domain.Symbol) (*domain.FinanceInfo, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	info, ok, err := getJSON[*domain.FinanceInfo](ctx, r.client, r.key("finance", symbol.Key()))
	if err != nil || !ok || info == nil {
		return info, ok, err
	}
	info.Cached = true
	return info, true, nil
}

func (r *Redis) PutFinance(ctx context.Context, symbol domain.Symbol, info *domain.FinanceInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	copyInfo := *info
	copyInfo.Cached = false
	return putJSON(ctx, r.client, r.key("finance", symbol.Key()), &copyInfo, r.cfg.FinanceTTL)
}

func (r *Redis) getTicks(ctx context.Context, key string) ([]domain.Tick, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	ticks, ok, err := getJSON[[]domain.Tick](ctx, r.client, r.key("tick-pages", key))
	if err != nil || !ok {
		return ticks, ok, err
	}
	return markCachedTicks(copyTicks(ticks)), true, nil
}

func (r *Redis) getBars(ctx context.Context, key string) ([]domain.Bar, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	bars, ok, err := getJSON[[]domain.Bar](ctx, r.client, r.key("bars", key))
	if err != nil || !ok {
		return bars, ok, err
	}
	return append([]domain.Bar(nil), bars...), true, nil
}

func getJSON[T any](ctx context.Context, client redisClient, key string) (T, bool, error) {
	var zero T
	payload, err := client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return zero, false, nil
		}
		return zero, false, err
	}
	var value T
	if err := json.Unmarshal(payload, &value); err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func putJSON[T any](ctx context.Context, client redisClient, key string, value T, ttl time.Duration) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return client.Set(ctx, key, payload, ttl).Err()
}

func (r *Redis) TryLock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return "", false, err
	}
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	token := time.Now().UTC().Format(time.RFC3339Nano)
	ok, err := r.client.SetNX(ctx, r.key("locks", key), token, ttl).Result()
	if err != nil {
		return "", false, err
	}
	return token, ok, nil
}

func (r *Redis) Unlock(ctx context.Context, key string, _ string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	return r.client.Del(ctx, r.key("locks", key)).Err()
}

func (r *Redis) key(kind, key string) string {
	return r.prefix + ":" + kind + ":" + key
}
