package gotdxadapter

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	gotdx "github.com/bensema/gotdx"
	"github.com/bensema/gotdx/proto"

	"github.com/sooboy/tongdaxin/internal/domain"
	"github.com/sooboy/tongdaxin/internal/source"
)

const (
	defaultTimeoutSec       = 3
	defaultClientsPerHost   = 1
	defaultMaxHostsPerPool  = 4
	defaultKLineCount       = 800
	maxGotdxKLineCount      = 799
	defaultSecurityPageSize = 800
	minReadyLiveClients     = 1
)

// Client is the gotdx surface used by this adapter. Keeping this interface here
// lets provider tests use deterministic clients without live TDX network access.
type Client interface {
	StockQuotesDetail(markets []uint8, codes []string) ([]proto.SecurityQuote, error)
	StockTransaction(market uint8, code string, start uint16, count uint16) ([]proto.TransactionData, error)
	StockFullTransaction(market uint8, code string) ([]proto.TransactionData, error)
	StockHistoryTransaction(date uint32, market uint8, code string, start uint16, count uint16) ([]proto.HistoryTransactionData, error)
	StockHistoryFullTransaction(date uint32, market uint8, code string) ([]proto.HistoryTransactionData, error)
	StockHistoryTransactionWithTrans(date uint32, market uint8, code string, start uint16, count uint16) ([]proto.HistoryTransactionDataWithTrans, error)
	StockHistoryFullTransactionWithTrans(date uint32, market uint8, code string) ([]proto.HistoryTransactionDataWithTrans, error)
	StockKLine(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error)
	StockFullKLine(category uint16, market uint8, code string, times uint16, adjust uint16, f func(kline proto.SecurityBar) bool) ([]proto.SecurityBar, error)
	GetXDXRInfo(market uint8, code string) (*proto.GetXDXRInfoReply, error)
	StockList(market uint8, start uint32, count uint32) ([]proto.Security, error)
	StockAll(market uint8) ([]proto.Security, error)
	GetFinanceInfo(market uint8, code string) (*proto.GetFinanceInfoReply, error)
	Disconnect() error
}

type macClient interface {
	MACServerInfo() (*proto.MACServerInfoReply, error)
	Disconnect() error
}

// LiveConfig creates gotdx-backed pools for all first-phase interface classes.
type LiveConfig struct {
	QuoteHosts      []string
	TickHosts       []string
	HistoryHosts    []string
	KLineHosts      []string
	AdjustHosts     []string
	StaticHosts     []string
	MACHosts        []string
	TimeoutSec      int
	ClientsPerHost  int
	MaxHostsPerPool int
}

// Provider is a gotdx-backed MarketDataProvider implementation.
type Provider struct {
	quotePool   *source.Pool[Client]
	tickPool    *source.Pool[Client]
	historyPool *source.Pool[Client]
	klinePool   *source.Pool[Client]
	adjustPool  *source.Pool[Client]
	staticPool  *source.Pool[Client]
	macPool     *source.Pool[macClient]
	clock       func() time.Time
}

// NewProvider wires prebuilt pools. Nil specialized pools fall back to quotePool.
func NewProvider(quotePool *source.Pool[Client], opts ...Option) *Provider {
	provider := &Provider{quotePool: quotePool, clock: time.Now}
	for _, opt := range opts {
		opt(provider)
	}
	return provider
}

// Option customizes Provider wiring.
type Option func(*Provider)

func WithTickPool(pool *source.Pool[Client]) Option {
	return func(provider *Provider) { provider.tickPool = pool }
}

func WithHistoryPool(pool *source.Pool[Client]) Option {
	return func(provider *Provider) { provider.historyPool = pool }
}

func WithKLinePool(pool *source.Pool[Client]) Option {
	return func(provider *Provider) { provider.klinePool = pool }
}

func WithAdjustPool(pool *source.Pool[Client]) Option {
	return func(provider *Provider) { provider.adjustPool = pool }
}

func WithStaticPool(pool *source.Pool[Client]) Option {
	return func(provider *Provider) { provider.staticPool = pool }
}

func WithMACPool(pool *source.Pool[macClient]) Option {
	return func(provider *Provider) { provider.macPool = pool }
}

func WithClock(clock func() time.Time) Option {
	return func(provider *Provider) {
		if clock != nil {
			provider.clock = clock
		}
	}
}

// NewLiveProvider creates gotdx clients eagerly, one independent client per pool slot.
func NewLiveProvider(ctx context.Context, cfg LiveConfig) (*Provider, error) {
	if len(cfg.QuoteHosts) == 0 {
		cfg.QuoteHosts = gotdx.MainHostAddresses()
	}
	if len(cfg.TickHosts) == 0 {
		cfg.TickHosts = cfg.QuoteHosts
	}
	if len(cfg.HistoryHosts) == 0 {
		cfg.HistoryHosts = cfg.QuoteHosts
	}
	if len(cfg.KLineHosts) == 0 {
		cfg.KLineHosts = cfg.QuoteHosts
	}
	if len(cfg.AdjustHosts) == 0 {
		cfg.AdjustHosts = cfg.QuoteHosts
	}
	if len(cfg.StaticHosts) == 0 {
		cfg.StaticHosts = cfg.QuoteHosts
	}
	if len(cfg.MACHosts) == 0 {
		cfg.MACHosts = gotdx.MACHostAddresses()
	}
	if cfg.TimeoutSec == 0 {
		cfg.TimeoutSec = defaultTimeoutSec
	}
	if cfg.ClientsPerHost == 0 {
		cfg.ClientsPerHost = defaultClientsPerHost
	}
	if cfg.MaxHostsPerPool == 0 {
		cfg.MaxHostsPerPool = defaultMaxHostsPerPool
	}
	log.Printf("gotdx live provider start quote_hosts=%d tick_hosts=%d history_hosts=%d kline_hosts=%d adjust_hosts=%d static_hosts=%d mac_hosts=%d clients_per_host=%d max_hosts_per_pool=%d timeout_sec=%d", len(cfg.QuoteHosts), len(cfg.TickHosts), len(cfg.HistoryHosts), len(cfg.KLineHosts), len(cfg.AdjustHosts), len(cfg.StaticHosts), len(cfg.MACHosts), cfg.ClientsPerHost, cfg.MaxHostsPerPool, cfg.TimeoutSec)

	selector := newLiveHostSelector(ctx, cfg.TimeoutSec, cfg.MaxHostsPerPool)
	quoteHosts := selector.selectFor("quote-pool", cfg.QuoteHosts)
	tickHosts := selector.selectFor("tick-pool", cfg.TickHosts)
	historyHosts := selector.selectFor("history-pool", cfg.HistoryHosts)
	klineHosts := selector.selectFor("kline-pool", cfg.KLineHosts)
	adjustHosts := selector.selectFor("adjust-pool", cfg.AdjustHosts)
	staticHosts := selector.selectFor("static-pool", cfg.StaticHosts)
	macHosts := selectLimitedHosts("mac-pool", cfg.MACHosts, cfg.MaxHostsPerPool)

	quotePool, err := newLivePool(ctx, "quote-pool", quoteHosts, cfg.ClientsPerHost, cfg.TimeoutSec)
	if err != nil {
		return nil, err
	}
	tickPool, err := newLivePool(ctx, "tick-pool", tickHosts, cfg.ClientsPerHost, cfg.TimeoutSec)
	if err != nil {
		_ = quotePool.Close(context.Background())
		return nil, err
	}
	historyPool, err := newLivePool(ctx, "history-pool", historyHosts, cfg.ClientsPerHost, cfg.TimeoutSec)
	if err != nil {
		_ = quotePool.Close(context.Background())
		_ = tickPool.Close(context.Background())
		return nil, err
	}
	klinePool, err := newLivePool(ctx, "kline-pool", klineHosts, cfg.ClientsPerHost, cfg.TimeoutSec)
	if err != nil {
		_ = quotePool.Close(context.Background())
		_ = tickPool.Close(context.Background())
		_ = historyPool.Close(context.Background())
		return nil, err
	}
	adjustPool, err := newLivePool(ctx, "adjust-pool", adjustHosts, 1, cfg.TimeoutSec)
	if err != nil {
		_ = quotePool.Close(context.Background())
		_ = tickPool.Close(context.Background())
		_ = historyPool.Close(context.Background())
		_ = klinePool.Close(context.Background())
		return nil, err
	}
	staticPool, err := newLivePool(ctx, "static-pool", staticHosts, 1, cfg.TimeoutSec)
	if err != nil {
		_ = quotePool.Close(context.Background())
		_ = tickPool.Close(context.Background())
		_ = historyPool.Close(context.Background())
		_ = klinePool.Close(context.Background())
		_ = adjustPool.Close(context.Background())
		return nil, err
	}
	macPool, err := newMACLivePool(ctx, "mac-pool", macHosts, 1, cfg.TimeoutSec)
	if err != nil {
		_ = quotePool.Close(context.Background())
		_ = tickPool.Close(context.Background())
		_ = historyPool.Close(context.Background())
		_ = klinePool.Close(context.Background())
		_ = adjustPool.Close(context.Background())
		_ = staticPool.Close(context.Background())
		return nil, err
	}

	log.Print("gotdx live provider ready")

	return NewProvider(
		quotePool,
		WithTickPool(tickPool),
		WithHistoryPool(historyPool),
		WithKLinePool(klinePool),
		WithAdjustPool(adjustPool),
		WithStaticPool(staticPool),
		WithMACPool(macPool),
	), nil
}

func newLivePool(ctx context.Context, name string, hosts []string, clientsPerHost int, timeoutSec int) (*source.Pool[Client], error) {
	hostConfigs := make([]source.HostConfig, 0, len(hosts))
	for _, host := range hosts {
		hostConfigs = append(hostConfigs, source.HostConfig{Address: host, Clients: clientsPerHost})
	}
	if len(hostConfigs) == 0 {
		return nil, fmt.Errorf("gotdx %s requires at least one host", name)
	}
	log.Printf("gotdx pool initializing name=%s hosts=%d clients_per_host=%d timeout_sec=%d", name, len(hostConfigs), clientsPerHost, timeoutSec)
	return source.NewPool[Client](ctx, source.Config[Client]{
		Name:            name,
		Hosts:           hostConfigs,
		MinReadyClients: minReadyLiveClients,
		Factory: func(ctx context.Context, host source.HostConfig, index int) (Client, error) {
			started := time.Now()
			log.Printf("gotdx client connect start pool=%s host=%s index=%d timeout_sec=%d", name, host.Address, index, timeoutSec)
			client, err := newConnectedLiveClient(host.Address, timeoutSec)
			if err != nil {
				log.Printf("gotdx client connect failed pool=%s host=%s index=%d duration=%s err=%v", name, host.Address, index, time.Since(started), err)
				return nil, err
			}
			log.Printf("gotdx client connect success pool=%s host=%s index=%d duration=%s", name, host.Address, index, time.Since(started))
			return client, nil
		},
		Close: func(client Client) error { return client.Disconnect() },
	})
}

func newMACLivePool(ctx context.Context, name string, hosts []string, clientsPerHost int, timeoutSec int) (*source.Pool[macClient], error) {
	hostConfigs := make([]source.HostConfig, 0, len(hosts))
	for _, host := range hosts {
		hostConfigs = append(hostConfigs, source.HostConfig{Address: host, Clients: clientsPerHost})
	}
	if len(hostConfigs) == 0 {
		return nil, fmt.Errorf("gotdx %s requires at least one host", name)
	}
	log.Printf("gotdx mac pool initializing name=%s hosts=%d clients_per_host=%d timeout_sec=%d", name, len(hostConfigs), clientsPerHost, timeoutSec)
	return source.NewPool[macClient](ctx, source.Config[macClient]{
		Name:            name,
		Hosts:           hostConfigs,
		MinReadyClients: minReadyLiveClients,
		Factory: func(ctx context.Context, host source.HostConfig, index int) (macClient, error) {
			started := time.Now()
			log.Printf("gotdx mac client connect start pool=%s host=%s index=%d timeout_sec=%d", name, host.Address, index, timeoutSec)
			client := gotdx.NewMAC(
				gotdx.WithMacTCPAddress(host.Address),
				gotdx.WithMacTCPAddressPool(),
				gotdx.WithTimeoutSec(timeoutSec),
			)
			if err := client.ConnectMAC(); err != nil {
				log.Printf("gotdx mac client connect failed pool=%s host=%s index=%d duration=%s err=%v", name, host.Address, index, time.Since(started), err)
				return nil, err
			}
			log.Printf("gotdx mac client connect success pool=%s host=%s index=%d duration=%s", name, host.Address, index, time.Since(started))
			return client, nil
		},
		Close: func(client macClient) error { return client.Disconnect() },
	})
}

func selectLimitedHosts(name string, hosts []string, maxHosts int) []string {
	uniqueHosts := uniqueNonEmptyHosts(hosts)
	if maxHosts == 0 {
		maxHosts = defaultMaxHostsPerPool
	}
	if maxHosts > 0 && len(uniqueHosts) > maxHosts {
		uniqueHosts = uniqueHosts[:maxHosts]
	}
	log.Printf("gotdx host selection limited name=%s selected=%d max_hosts=%d hosts=%v", name, len(uniqueHosts), maxHosts, uniqueHosts)
	return uniqueHosts
}

type liveHostSelector struct {
	ctx        context.Context
	timeoutSec int
	maxHosts   int
	cache      map[string][]string
}

func newLiveHostSelector(ctx context.Context, timeoutSec int, maxHosts int) *liveHostSelector {
	return &liveHostSelector{ctx: ctx, timeoutSec: timeoutSec, maxHosts: maxHosts, cache: make(map[string][]string)}
}

func (s *liveHostSelector) selectFor(name string, hosts []string) []string {
	uniqueHosts := uniqueNonEmptyHosts(hosts)
	key := fmt.Sprintf("timeout=%d;max=%d;hosts=%s", s.timeoutSec, s.maxHosts, strings.Join(uniqueHosts, "\x00"))
	if selected, ok := s.cache[key]; ok {
		out := append([]string(nil), selected...)
		log.Printf("gotdx pool host selection reused name=%s selected=%d hosts=%v", name, len(out), out)
		return out
	}
	selected := selectLiveHosts(s.ctx, name, uniqueHosts, s.timeoutSec, s.maxHosts)
	s.cache[key] = append([]string(nil), selected...)
	return selected
}

func selectLiveHosts(ctx context.Context, name string, hosts []string, timeoutSec int, maxHosts int) []string {
	uniqueHosts := uniqueNonEmptyHosts(hosts)
	if len(uniqueHosts) == 0 {
		return nil
	}
	if maxHosts < 0 || maxHosts >= len(uniqueHosts) {
		return uniqueHosts
	}
	if maxHosts == 0 {
		maxHosts = defaultMaxHostsPerPool
	}
	if maxHosts >= len(uniqueHosts) {
		return uniqueHosts
	}
	if err := ctx.Err(); err != nil {
		return uniqueHosts[:maxHosts]
	}

	probeStarted := time.Now()
	results := probeLiveHosts(uniqueHosts, timeoutSec)
	selected := make([]string, 0, maxHosts)
	failed := 0
	for _, result := range results {
		if !result.reachable {
			failed++
			continue
		}
		selected = append(selected, result.address)
		if len(selected) >= maxHosts {
			break
		}
	}
	if len(selected) == 0 {
		selected = uniqueHosts[:maxHosts]
	}
	log.Printf("gotdx pool host selection name=%s candidates=%d selected=%d failed=%d max_hosts=%d duration=%s hosts=%v", name, len(uniqueHosts), len(selected), failed, maxHosts, time.Since(probeStarted), selected)
	return selected
}

func uniqueNonEmptyHosts(hosts []string) []string {
	seen := make(map[string]struct{}, len(hosts))
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	return out
}

type liveHostProbeResult struct {
	address   string
	latency   time.Duration
	reachable bool
	err       string
}

func probeLiveHosts(hosts []string, timeoutSec int) []liveHostProbeResult {
	results := make([]liveHostProbeResult, 0, len(hosts))
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, host := range hosts {
		host := host
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			client := gotdx.New(
				gotdx.WithTCPAddress(host),
				gotdx.WithTCPAddressPool(),
				gotdx.WithTimeoutSec(timeoutSec),
			)
			_, err := client.Connect()
			result := liveHostProbeResult{address: host, latency: time.Since(started)}
			if err != nil {
				result.err = err.Error()
			} else {
				result.reachable = true
				_ = client.Disconnect()
			}
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool {
		if results[i].reachable != results[j].reachable {
			return results[i].reachable
		}
		if results[i].latency != results[j].latency {
			return results[i].latency < results[j].latency
		}
		return results[i].address < results[j].address
	})
	return results
}

type liveClient struct {
	mu         sync.Mutex
	address    string
	timeoutSec int
	client     Client
	connector  liveClientConnector
}

type liveClientConnector func(address string, timeoutSec int) (Client, error)

func newConnectedLiveClient(address string, timeoutSec int) (*liveClient, error) {
	client := &liveClient{address: address, timeoutSec: timeoutSec}
	if err := client.connectLocked(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *liveClient) connectLocked() error {
	connector := c.connector
	if connector == nil {
		connector = newGotdxLiveClient
	}
	client, err := connector(c.address, c.timeoutSec)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func newGotdxLiveClient(address string, timeoutSec int) (Client, error) {
	client := gotdx.New(
		gotdx.WithTCPAddress(address),
		gotdx.WithTCPAddressPool(),
		gotdx.WithTimeoutSec(timeoutSec),
	)
	if _, err := client.Connect(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *liveClient) reconnectLocked(operation string, reason error) error {
	if c.client != nil {
		_ = c.client.Disconnect()
		c.client = nil
	}
	started := time.Now()
	log.Printf("gotdx client reconnect start host=%s operation=%s timeout_sec=%d reason=%v", c.address, operation, c.timeoutSec, reason)
	if err := c.connectLocked(); err != nil {
		log.Printf("gotdx client reconnect failed host=%s operation=%s duration=%s err=%v", c.address, operation, time.Since(started), err)
		return err
	}
	log.Printf("gotdx client reconnect success host=%s operation=%s duration=%s", c.address, operation, time.Since(started))
	return nil
}

func (c *liveClient) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		return nil
	}
	err := c.client.Disconnect()
	c.client = nil
	return err
}

func withLiveClient[T any](c *liveClient, operation string, fn func(Client) (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		if err := c.connectLocked(); err != nil {
			var zero T
			return zero, fmt.Errorf("%w: gotdx %s connect failed: %v", domain.ErrUpstreamUnavailable, operation, err)
		}
	}
	result, err := callLiveClient(c.client, operation, fn)
	if err == nil {
		return result, nil
	}
	if reconnectErr := c.reconnectLocked(operation, err); reconnectErr != nil {
		var zero T
		return zero, fmt.Errorf("%w: gotdx %s failed: %v; reconnect failed: %v", domain.ErrUpstreamUnavailable, operation, err, reconnectErr)
	}
	result, err = callLiveClient(c.client, operation, fn)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("%w: gotdx %s failed after reconnect: %v", domain.ErrUpstreamUnavailable, operation, err)
	}
	return result, nil
}

func callLiveClient[T any](client Client, operation string, fn func(Client) (T, error)) (result T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var zero T
			result = zero
			log.Printf("gotdx client operation panic operation=%s panic=%v stack=%s", operation, recovered, debug.Stack())
			err = fmt.Errorf("%w: gotdx %s panic: %v", domain.ErrUpstreamUnavailable, operation, recovered)
		}
	}()
	return fn(client)
}

func (c *liveClient) StockQuotesDetail(markets []uint8, codes []string) ([]proto.SecurityQuote, error) {
	return withLiveClient(c, "StockQuotesDetail", func(client Client) ([]proto.SecurityQuote, error) {
		return client.StockQuotesDetail(markets, codes)
	})
}

func (c *liveClient) StockTransaction(market uint8, code string, start uint16, count uint16) ([]proto.TransactionData, error) {
	return withLiveClient(c, "StockTransaction", func(client Client) ([]proto.TransactionData, error) {
		return client.StockTransaction(market, code, start, count)
	})
}

func (c *liveClient) StockFullTransaction(market uint8, code string) ([]proto.TransactionData, error) {
	return withLiveClient(c, "StockFullTransaction", func(client Client) ([]proto.TransactionData, error) {
		return client.StockFullTransaction(market, code)
	})
}

func (c *liveClient) StockHistoryTransaction(date uint32, market uint8, code string, start uint16, count uint16) ([]proto.HistoryTransactionData, error) {
	return withLiveClient(c, "StockHistoryTransaction", func(client Client) ([]proto.HistoryTransactionData, error) {
		return client.StockHistoryTransaction(date, market, code, start, count)
	})
}

func (c *liveClient) StockHistoryFullTransaction(date uint32, market uint8, code string) ([]proto.HistoryTransactionData, error) {
	return withLiveClient(c, "StockHistoryFullTransaction", func(client Client) ([]proto.HistoryTransactionData, error) {
		return client.StockHistoryFullTransaction(date, market, code)
	})
}

func (c *liveClient) StockHistoryTransactionWithTrans(date uint32, market uint8, code string, start uint16, count uint16) ([]proto.HistoryTransactionDataWithTrans, error) {
	return withLiveClient(c, "StockHistoryTransactionWithTrans", func(client Client) ([]proto.HistoryTransactionDataWithTrans, error) {
		return client.StockHistoryTransactionWithTrans(date, market, code, start, count)
	})
}

func (c *liveClient) StockHistoryFullTransactionWithTrans(date uint32, market uint8, code string) ([]proto.HistoryTransactionDataWithTrans, error) {
	return withLiveClient(c, "StockHistoryFullTransactionWithTrans", func(client Client) ([]proto.HistoryTransactionDataWithTrans, error) {
		return client.StockHistoryFullTransactionWithTrans(date, market, code)
	})
}

func (c *liveClient) StockKLine(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
	return withLiveClient(c, "StockKLine", func(client Client) ([]proto.SecurityBar, error) {
		return client.StockKLine(category, market, code, start, count, times, adjust)
	})
}

func (c *liveClient) StockFullKLine(category uint16, market uint8, code string, times uint16, adjust uint16, f func(kline proto.SecurityBar) bool) ([]proto.SecurityBar, error) {
	return withLiveClient(c, "StockFullKLine", func(client Client) ([]proto.SecurityBar, error) {
		return client.StockFullKLine(category, market, code, times, adjust, f)
	})
}

func (c *liveClient) GetXDXRInfo(market uint8, code string) (*proto.GetXDXRInfoReply, error) {
	return withLiveClient(c, "GetXDXRInfo", func(client Client) (*proto.GetXDXRInfoReply, error) {
		return client.GetXDXRInfo(market, code)
	})
}

func (c *liveClient) StockList(market uint8, start uint32, count uint32) ([]proto.Security, error) {
	return withLiveClient(c, "StockList", func(client Client) ([]proto.Security, error) {
		return client.StockList(market, start, count)
	})
}

func (c *liveClient) StockAll(market uint8) ([]proto.Security, error) {
	return withLiveClient(c, "StockAll", func(client Client) ([]proto.Security, error) {
		return client.StockAll(market)
	})
}

func (c *liveClient) GetFinanceInfo(market uint8, code string) (*proto.GetFinanceInfoReply, error) {
	return withLiveClient(c, "GetFinanceInfo", func(client Client) (*proto.GetFinanceInfoReply, error) {
		return client.GetFinanceInfo(market, code)
	})
}

var _ Client = (*liveClient)(nil)

func (p *Provider) GetQuotes(ctx context.Context, symbols []domain.Symbol) ([]domain.Quote, error) {
	markets, codes, err := symbolsToGotdx(symbols)
	if err != nil {
		return nil, err
	}
	lease, err := p.acquire(ctx, p.quotePool)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	items, err := lease.Client.StockQuotesDetail(markets, codes)
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()
	out := make([]domain.Quote, 0, len(items))
	now := p.now()
	for _, item := range items {
		out = append(out, quoteToDomain(item, now))
	}
	return out, nil
}

func (p *Provider) GetOrderBook(ctx context.Context, symbols []domain.Symbol) ([]domain.OrderBook, error) {
	markets, codes, err := symbolsToGotdx(symbols)
	if err != nil {
		return nil, err
	}
	lease, err := p.acquire(ctx, p.quotePool)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	items, err := lease.Client.StockQuotesDetail(markets, codes)
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()
	out := make([]domain.OrderBook, 0, len(items))
	now := p.now()
	for _, item := range items {
		out = append(out, quoteToOrderBook(item, now))
	}
	return out, nil
}

func (p *Provider) GetTicks(ctx context.Context, symbol domain.Symbol, req domain.TickRequest) ([]domain.Tick, error) {
	market, err := marketToGotdx(symbol.Market)
	if err != nil {
		return nil, err
	}
	lease, err := p.acquire(ctx, p.pickPool(p.tickPool))
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	var items []proto.TransactionData
	if req.Full {
		items, err = lease.Client.StockFullTransaction(market, symbol.Code)
	} else {
		count := req.Count
		if count == 0 {
			count = 600
		}
		items, err = lease.Client.StockTransaction(market, symbol.Code, req.Start, count)
	}
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()
	out := make([]domain.Tick, 0, len(items))
	now := p.now()
	for i, item := range items {
		out = append(out, transactionToDomain(symbol, item, int64(i), now))
	}
	return out, nil
}

func (p *Provider) GetHistoryTicks(ctx context.Context, symbol domain.Symbol, req domain.HistoryTickRequest) ([]domain.Tick, error) {
	market, err := marketToGotdx(symbol.Market)
	if err != nil {
		return nil, err
	}
	date := yyyymmdd(req.TradeDate)
	if date == 0 {
		return nil, domain.ErrInvalidRequest
	}
	lease, err := p.acquire(ctx, p.pickPool(p.historyPool))
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	if req.WithTransactionFlag {
		var items []proto.HistoryTransactionDataWithTrans
		if req.Full {
			items, err = lease.Client.StockHistoryFullTransactionWithTrans(date, market, symbol.Code)
		} else {
			count := req.Count
			if count == 0 {
				count = 600
			}
			items, err = lease.Client.StockHistoryTransactionWithTrans(date, market, symbol.Code, req.Start, count)
		}
		if err != nil {
			_ = lease.ReportFailure(err)
			return nil, err
		}
		_ = lease.ReportSuccess()
		out := make([]domain.Tick, 0, len(items))
		for i, item := range items {
			out = append(out, historyTransactionWithTransToDomain(symbol, item, int64(i)))
		}
		return out, nil
	}

	var items []proto.HistoryTransactionData
	if req.Full {
		items, err = lease.Client.StockHistoryFullTransaction(date, market, symbol.Code)
	} else {
		count := req.Count
		if count == 0 {
			count = 600
		}
		items, err = lease.Client.StockHistoryTransaction(date, market, symbol.Code, req.Start, count)
	}
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()
	out := make([]domain.Tick, 0, len(items))
	for i, item := range items {
		out = append(out, historyTransactionToDomain(symbol, item, int64(i)))
	}
	return out, nil
}

func (p *Provider) GetKLine(ctx context.Context, symbol domain.Symbol, req domain.KLineRequest) ([]domain.Bar, error) {
	return p.getBars(ctx, symbol, req, domain.AdjustNone)
}

func (p *Provider) GetAdjustedKLine(ctx context.Context, symbol domain.Symbol, req domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	return p.getBars(ctx, symbol, req.KLineRequest, req.AdjustType)
}

func (p *Provider) GetXDXR(ctx context.Context, symbol domain.Symbol) ([]domain.XDXREvent, error) {
	market, err := marketToGotdx(symbol.Market)
	if err != nil {
		return nil, err
	}
	lease, err := p.acquire(ctx, p.pickPool(p.adjustPool))
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	reply, err := lease.Client.GetXDXRInfo(market, symbol.Code)
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()
	out := make([]domain.XDXREvent, 0, len(reply.List))
	for _, item := range reply.List {
		out = append(out, xdxrToDomain(symbol, item))
	}
	return out, nil
}

func (p *Provider) GetSecurityInfo(ctx context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	markets := req.Markets
	if len(markets) == 0 {
		markets = []domain.Market{domain.MarketSH, domain.MarketSZ, domain.MarketBJ}
	}
	lease, err := p.acquire(ctx, p.pickPool(p.staticPool))
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	out := make([]domain.SecurityInfo, 0)
	for _, domainMarket := range markets {
		market, err := marketToGotdx(domainMarket)
		if err != nil {
			_ = lease.ReportFailure(err)
			return nil, err
		}
		securities, err := fetchSecurities(ctx, lease.Client, market, req.Start, req.Count)
		if err != nil {
			_ = lease.ReportFailure(err)
			return nil, err
		}
		for _, item := range securities {
			out = append(out, securityToDomain(domainMarket, item))
		}
	}
	_ = lease.ReportSuccess()
	return out, nil
}

func fetchSecurities(ctx context.Context, client Client, market uint8, start uint32, count uint32) ([]proto.Security, error) {
	if client == nil {
		return nil, domain.ErrUpstreamUnavailable
	}
	if count > 0 {
		return fetchSecurityRange(ctx, client, market, start, count)
	}
	out := make([]proto.Security, 0)
	for offset := uint32(0); ; offset += defaultSecurityPageSize {
		page, err := fetchSecurityRange(ctx, client, market, offset, defaultSecurityPageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < defaultSecurityPageSize {
			break
		}
	}
	return out, nil
}

func fetchSecurityRange(ctx context.Context, client Client, market uint8, start uint32, count uint32) ([]proto.Security, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]proto.Security, 0, count)
	remaining := count
	offset := start
	for remaining > 0 {
		pageSize := remaining
		if pageSize > defaultSecurityPageSize {
			pageSize = defaultSecurityPageSize
		}
		page, err := client.StockList(market, offset, pageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < int(pageSize) {
			break
		}
		remaining -= pageSize
		offset += pageSize
	}
	return out, nil
}

func (p *Provider) GetFinance(ctx context.Context, symbol domain.Symbol) (*domain.FinanceInfo, error) {
	market, err := marketToGotdx(symbol.Market)
	if err != nil {
		return nil, err
	}
	lease, err := p.acquire(ctx, p.pickPool(p.staticPool))
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	reply, err := lease.Client.GetFinanceInfo(market, symbol.Code)
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()
	return financeToDomain(symbol, reply), nil
}

func (p *Provider) Close(ctx context.Context) error {
	log.Print("gotdx provider close start")
	var err error
	seen := make(map[*source.Pool[Client]]struct{})
	for _, pool := range []*source.Pool[Client]{p.quotePool, p.tickPool, p.historyPool, p.klinePool, p.adjustPool, p.staticPool} {
		if pool == nil {
			continue
		}
		if _, ok := seen[pool]; ok {
			continue
		}
		seen[pool] = struct{}{}
		log.Printf("gotdx provider closing pool=%p", pool)
		if closeErr := pool.Close(ctx); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if p.macPool != nil {
		log.Printf("gotdx provider closing mac_pool=%p", p.macPool)
		if closeErr := p.macPool.Close(ctx); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	log.Print("gotdx provider close done")
	return err
}

func (p *Provider) getBars(ctx context.Context, symbol domain.Symbol, req domain.KLineRequest, adjust domain.AdjustType) ([]domain.Bar, error) {
	market, err := marketToGotdx(symbol.Market)
	if err != nil {
		return nil, err
	}
	category, err := periodToGotdx(req.Period)
	if err != nil {
		return nil, err
	}
	adjustValue, err := adjustToGotdx(adjust)
	if err != nil {
		return nil, err
	}
	lease, err := p.acquire(ctx, p.pickPool(p.klinePool))
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	var items []proto.SecurityBar
	if req.StartDate.IsZero() && req.EndDate.IsZero() {
		items, err = fetchKLinePage(ctx, lease.Client, category, market, symbol.Code, req.Start, normalizeKLineCount(req.Count), req.Times, adjustValue)
	} else {
		startDate := domain.NormalizeDate(req.StartDate)
		endDate := domain.NormalizeDate(req.EndDate)
		items, err = fetchKLineRange(ctx, lease.Client, category, market, symbol.Code, startDate, endDate, req.Times, adjustValue)
	}
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()

	endDate := req.EndDate
	startDate := domain.NormalizeDate(req.StartDate)
	out := make([]domain.Bar, 0, len(items))
	for _, item := range items {
		if !startDate.IsZero() && item.DateTime.Before(startDate) {
			continue
		}
		if !endDate.IsZero() && item.DateTime.After(endDate) {
			continue
		}
		out = append(out, barToDomain(symbol, req.Period, adjust, item))
	}
	return sliceDomainBarsByStartCount(out, req.Start, req.Count), nil
}

func normalizeKLineCount(count uint16) uint16 {
	if count == 0 {
		return defaultKLineCount
	}
	return count
}

func fetchKLinePage(ctx context.Context, client Client, category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
	out := make([]proto.SecurityBar, 0, count)
	remaining := count
	currentStart := start
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pageCount := remaining
		if pageCount > maxGotdxKLineCount {
			pageCount = maxGotdxKLineCount
		}
		page, err := client.StockKLine(category, market, code, currentStart, pageCount, times, adjust)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if uint16(len(page)) < pageCount {
			break
		}
		remaining -= pageCount
		currentStart += pageCount
	}
	return out, nil
}

func fetchKLineRange(ctx context.Context, client Client, category uint16, market uint8, code string, startDate time.Time, endDate time.Time, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
	stopDate := startDate
	if stopDate.IsZero() {
		stopDate = endDate
	}
	if stopDate.IsZero() {
		return fetchKLinePage(ctx, client, category, market, code, 0, defaultKLineCount, times, adjust)
	}

	var out []proto.SecurityBar
	for start := uint32(0); start <= uint32(^uint16(0)); start += maxGotdxKLineCount {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := fetchKLinePage(ctx, client, category, market, code, uint16(start), maxGotdxKLineCount, times, adjust)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		combined := make([]proto.SecurityBar, 0, len(page)+len(out))
		combined = append(combined, page...)
		combined = append(combined, out...)
		out = combined

		oldest := page[0].DateTime
		if !oldest.After(stopDate) || len(page) < maxGotdxKLineCount {
			break
		}
	}
	return out, nil
}

func sliceDomainBarsByStartCount(bars []domain.Bar, start uint16, count uint16) []domain.Bar {
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

func (p *Provider) acquire(ctx context.Context, pool *source.Pool[Client]) (*source.Lease[Client], error) {
	if pool == nil {
		return nil, domain.ErrUpstreamUnavailable
	}
	return pool.Acquire(ctx)
}

func (p *Provider) pickPool(pool *source.Pool[Client]) *source.Pool[Client] {
	if pool != nil {
		return pool
	}
	return p.quotePool
}

func (p *Provider) now() time.Time {
	if p.clock == nil {
		return time.Now()
	}
	return p.clock()
}

func symbolsToGotdx(symbols []domain.Symbol) ([]uint8, []string, error) {
	if len(symbols) == 0 {
		return nil, nil, domain.ErrInvalidRequest
	}
	markets := make([]uint8, 0, len(symbols))
	codes := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if err := symbol.Validate(); err != nil {
			return nil, nil, err
		}
		market, err := marketToGotdx(symbol.Market)
		if err != nil {
			return nil, nil, err
		}
		markets = append(markets, market)
		codes = append(codes, symbol.Code)
	}
	return markets, codes, nil
}

func yyyymmdd(t time.Time) uint32 {
	if t.IsZero() {
		return 0
	}
	value := t.Format("20060102")
	var out uint32
	_, _ = fmt.Sscanf(value, "%d", &out)
	return out
}

var _ domain.MarketDataProvider = (*Provider)(nil)
