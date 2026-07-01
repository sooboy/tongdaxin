package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/sooboy/tongdaxin/internal/cache"
	"github.com/sooboy/tongdaxin/internal/domain"
	"github.com/sooboy/tongdaxin/internal/storage"
)

type providerStub struct{}

func (providerStub) GetQuotes(context.Context, []domain.Symbol) ([]domain.Quote, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetOrderBook(context.Context, []domain.Symbol) ([]domain.OrderBook, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetTicks(context.Context, domain.Symbol, domain.TickRequest) ([]domain.Tick, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetHistoryTicks(context.Context, domain.Symbol, domain.HistoryTickRequest) ([]domain.Tick, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetKLine(context.Context, domain.Symbol, domain.KLineRequest) ([]domain.Bar, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetAdjustedKLine(context.Context, domain.Symbol, domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetXDXR(context.Context, domain.Symbol) ([]domain.XDXREvent, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetSecurityInfo(context.Context, domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetFinance(context.Context, domain.Symbol) (*domain.FinanceInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (providerStub) GetTradingDay(context.Context) (*domain.TradingDayInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}

type quoteProviderStub struct {
	providerStub
	quotes []domain.Quote
}

func (s *quoteProviderStub) GetQuotes(context.Context, []domain.Symbol) ([]domain.Quote, error) {
	return s.quotes, nil
}

func TestBuildOfflineDoesNotCreateProvider(t *testing.T) {
	t.Parallel()

	called := false
	app, err := Build(context.Background(), Config{
		Addr:        "127.0.0.1:0",
		DisableLive: true,
		ProviderFactory: func(context.Context, Config) (domain.MarketDataProvider, func(context.Context) error, error) {
			called = true
			return providerStub{}, nil, nil
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if called {
		t.Fatal("provider factory called in offline mode")
	}
	if app.Handler == nil || app.Store == nil || app.Server.Addr != "127.0.0.1:0" {
		t.Fatalf("app = %+v", app)
	}
}

func TestBuildSupportsGinRouter(t *testing.T) {
	t.Parallel()

	app, err := Build(context.Background(), Config{Addr: "127.0.0.1:0", DisableLive: true, HTTPRouter: "gin"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	body, status := requestJSON(t, app.Handler, "/api/v1/health")
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("status=%d body=%#v", status, body)
	}
}

func TestBuildEnablesGRPCServer(t *testing.T) {
	t.Parallel()

	app, err := Build(context.Background(), Config{Addr: "127.0.0.1:0", GRPCAddr: "127.0.0.1:0", DisableLive: true})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if app.GRPCServer == nil || app.GRPCAddr != "127.0.0.1:0" {
		t.Fatalf("grpc app = %+v", app)
	}
}

func TestBuildUsesSQLStorage(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.ToSlash(filepath.Join(t.TempDir(), "marketdata.sqlite")) + "?_pragma=foreign_keys(1)&_time_format=sqlite"
	app, err := Build(context.Background(), Config{
		Addr:           "127.0.0.1:0",
		DisableLive:    true,
		StorageDialect: storage.DialectSQLite,
		StorageDSN:     dsn,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, ok := app.Store.(*storage.SQLStore); !ok {
		t.Fatalf("store type = %T", app.Store)
	}
	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestBuildReturnsBeforeProviderFactoryCompletes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	buildDone := make(chan struct{})
	closed := make(chan struct{})
	var releaseOnce sync.Once
	releaseProvider := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	var closeOnce sync.Once
	var app *App
	var buildErr error
	shutdowned := false
	go func() {
		app, buildErr = Build(context.Background(), Config{
			ProviderFactory: func(ctx context.Context, _ Config) (domain.MarketDataProvider, func(context.Context) error, error) {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
				return &quoteProviderStub{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}}, func(context.Context) error {
					closeOnce.Do(func() {
						close(closed)
					})
					return nil
				}, nil
			},
		})
		close(buildDone)
	}()
	t.Cleanup(func() {
		releaseProvider()
		if app != nil && !shutdowned {
			_ = app.Shutdown(context.Background())
		}
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider factory did not start")
	}
	select {
	case <-buildDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Build blocked on provider factory")
	}
	if buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	if app == nil || app.Handler == nil {
		t.Fatalf("app = %+v", app)
	}

	body, status := requestJSON(t, app.Handler, "/api/v1/quotes?market=SH&code=600000")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("pre-release status = %d body = %#v", status, body)
	}

	releaseProvider()
	body, status = eventuallyRequestJSON(t, app.Handler, "/api/v1/quotes?market=SH&code=600000", http.StatusOK, time.Second)
	if status != http.StatusOK {
		t.Fatalf("post-release status = %d body = %#v", status, body)
	}
	data, ok := body["data"].([]any)
	if !ok || len(data) != 1 {
		t.Fatalf("post-release body = %#v", body)
	}
	quote, ok := data[0].(map[string]any)
	if !ok {
		t.Fatalf("post-release quote = %#v", data[0])
	}
	if quote["last_price"] != 10.5 {
		t.Fatalf("post-release quote = %#v", quote)
	}

	shutdowned = true
	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("provider close hook not called")
	}
}

func TestBuildRetriesProviderFactoryUntilReady(t *testing.T) {
	attempts := 0
	closed := make(chan struct{})
	var closeOnce sync.Once
	app, err := Build(context.Background(), Config{
		ProviderFactory: func(context.Context, Config) (domain.MarketDataProvider, func(context.Context) error, error) {
			attempts++
			if attempts == 1 {
				return nil, nil, errors.New("temporary upstream bootstrap failure")
			}
			return &quoteProviderStub{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}}, func(context.Context) error {
				closeOnce.Do(func() { close(closed) })
				return nil
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer app.Shutdown(context.Background())

	body, status := eventuallyRequestJSON(t, app.Handler, "/api/v1/quotes?market=SH&code=600000", http.StatusOK, 2*time.Second)
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if attempts < 2 {
		t.Fatalf("attempts = %d, want retry", attempts)
	}

	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("provider close hook not called")
	}
}

func TestBuildCancelsBlockedProviderFactoryOnShutdown(t *testing.T) {
	started := make(chan struct{})
	factoryDone := make(chan struct{})
	closed := make(chan struct{})
	var closeOnce sync.Once
	var app *App
	var buildErr error
	buildDone := make(chan struct{})
	shutdowned := false
	go func() {
		app, buildErr = Build(context.Background(), Config{
			ProviderFactory: func(ctx context.Context, _ Config) (domain.MarketDataProvider, func(context.Context) error, error) {
				close(started)
				<-ctx.Done()
				close(factoryDone)
				return &quoteProviderStub{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}}, func(context.Context) error {
					closeOnce.Do(func() {
						close(closed)
					})
					return nil
				}, nil
			},
		})
		close(buildDone)
	}()
	t.Cleanup(func() {
		if app != nil && !shutdowned {
			_ = app.Shutdown(context.Background())
		}
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider factory did not start")
	}
	select {
	case <-buildDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Build blocked on provider factory")
	}
	if buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	if app == nil || app.Handler == nil {
		t.Fatalf("app = %+v", app)
	}

	body, status := requestJSON(t, app.Handler, "/api/v1/quotes?market=SH&code=600000")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("pre-shutdown status = %d body = %#v", status, body)
	}

	shutdowned = true
	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-factoryDone:
	case <-time.After(time.Second):
		t.Fatal("provider factory was not canceled")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("provider close hook not called")
	}
}

func TestBuildClosesLateProviderAfterShutdownTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	buildDone := make(chan struct{})
	closed := make(chan struct{})
	var releaseOnce sync.Once
	releaseProvider := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	var closeOnce sync.Once
	var app *App
	var buildErr error
	shutdowned := false
	go func() {
		app, buildErr = Build(context.Background(), Config{
			ShutdownTimeout: 20 * time.Millisecond,
			ProviderFactory: func(context.Context, Config) (domain.MarketDataProvider, func(context.Context) error, error) {
				close(started)
				<-release
				return &quoteProviderStub{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}}, func(ctx context.Context) error {
					if _, ok := ctx.Deadline(); !ok {
						return errors.New("late close missing deadline")
					}
					<-ctx.Done()
					closeOnce.Do(func() {
						close(closed)
					})
					return ctx.Err()
				}, nil
			},
		})
		close(buildDone)
	}()
	t.Cleanup(func() {
		releaseProvider()
		if app != nil && !shutdowned {
			_ = app.Shutdown(context.Background())
		}
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider factory did not start")
	}
	select {
	case <-buildDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Build blocked on provider factory")
	}
	if buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	if app == nil || app.Handler == nil {
		t.Fatalf("app = %+v", app)
	}

	body, status := requestJSON(t, app.Handler, "/api/v1/quotes?market=SH&code=600000")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("pre-shutdown status = %d body = %#v", status, body)
	}

	shutdowned = true
	err := app.Shutdown(context.Background())
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v", err)
	}

	releaseProvider()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("provider close hook not called after late completion")
	}

	body, status = requestJSON(t, app.Handler, "/api/v1/quotes?market=SH&code=600000")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown status = %d body = %#v", status, body)
	}
}

func TestListenAndServeCleansUpOnBindFailure(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	started := make(chan struct{})
	factoryDone := make(chan struct{})
	closed := make(chan struct{})
	var startOnce sync.Once
	var doneOnce sync.Once
	var closeOnce sync.Once
	var mu sync.Mutex
	closeCount := 0

	app, err := Build(context.Background(), Config{
		Addr:            ln.Addr().String(),
		ShutdownTimeout: 20 * time.Millisecond,
		ProviderFactory: func(ctx context.Context, _ Config) (domain.MarketDataProvider, func(context.Context) error, error) {
			startOnce.Do(func() { close(started) })
			<-ctx.Done()
			doneOnce.Do(func() { close(factoryDone) })
			return &quoteProviderStub{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}}, func(context.Context) error {
				mu.Lock()
				closeCount++
				mu.Unlock()
				closeOnce.Do(func() { close(closed) })
				return nil
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider factory did not start")
	}

	listenErrCh := make(chan error, 1)
	go func() {
		listenErrCh <- app.ListenAndServe(context.Background())
	}()

	var listenErr error
	select {
	case listenErr = <-listenErrCh:
	case <-time.After(time.Second):
		t.Fatal("ListenAndServe did not return")
	}
	if listenErr == nil {
		t.Fatal("expected listen failure")
	}

	select {
	case <-factoryDone:
	case <-time.After(time.Second):
		t.Fatal("provider factory was not canceled")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("provider close hook not called")
	}

	mu.Lock()
	if closeCount != 1 {
		mu.Unlock()
		t.Fatalf("closeCount = %d", closeCount)
	}
	mu.Unlock()

	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	mu.Lock()
	if closeCount != 1 {
		mu.Unlock()
		t.Fatalf("closeCount after second shutdown = %d", closeCount)
	}
	mu.Unlock()
}

func TestBuildUsesRedisCache(t *testing.T) {
	t.Parallel()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	defer srv.Close()

	app, err := Build(context.Background(), Config{
		DisableLive:   true,
		CacheRedisURL: "redis://" + srv.Addr() + "/0",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, ok := app.Cache.(*cache.Redis); !ok {
		t.Fatalf("cache type = %T", app.Cache)
	}
	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	if err := app.Cache.PutQuotes(context.Background(), []domain.Quote{{Symbol: symbol, LastPrice: 10}}); err != nil {
		t.Fatalf("PutQuotes: %v", err)
	}
	hits, misses, err := app.Cache.GetQuotes(context.Background(), []domain.Symbol{symbol})
	if err != nil {
		t.Fatalf("GetQuotes: %v", err)
	}
	if len(misses) != 0 || len(hits) != 1 || !hits[symbol.Key()].Cached || hits[symbol.Key()].LastPrice != 10 {
		t.Fatalf("hits=%+v misses=%+v", hits, misses)
	}
	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestBuildWiresAPIRateLimiter(t *testing.T) {
	t.Parallel()

	app, err := Build(context.Background(), Config{DisableLive: true, RateLimitRPS: 1, RateLimitBurst: 1})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	first := httptest.NewRecorder()
	app.Handler.ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d", first.Code)
	}
	second := httptest.NewRecorder()
	app.Handler.ServeHTTP(second, req)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d body = %q", second.Code, second.Body.String())
	}
}

func TestServeHealthEndpoint(t *testing.T) {
	t.Parallel()

	app, err := Build(context.Background(), Config{DisableLive: true, ShutdownTimeout: time.Second})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- app.Serve(ctx, ln) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/api/v1/health")
	if err != nil {
		cancel()
		t.Fatalf("Get health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		cancel()
		t.Fatalf("Decode: %v", err)
	}
	if body["ok"] != true {
		cancel()
		t.Fatalf("body = %#v", body)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve returned: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after cancel")
	}
}

func requestJSON(t *testing.T, handler http.Handler, target string) (map[string]any, int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var body map[string]any
	if rec.Body.Len() != 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}
	return body, rec.Code
}

func eventuallyRequestJSON(t *testing.T, handler http.Handler, target string, want int, timeout time.Duration) (map[string]any, int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var body map[string]any
	var status int
	for {
		body, status = requestJSON(t, handler, target)
		if status == want {
			return body, status
		}
		if time.Now().After(deadline) {
			t.Fatalf("request %q never reached status %d; last status=%d body=%#v", target, want, status, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
