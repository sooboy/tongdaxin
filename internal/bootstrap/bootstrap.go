package bootstrap

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	ginapi "github.com/sooboy/tongdaxin/internal/api/gin"
	grpcapi "github.com/sooboy/tongdaxin/internal/api/grpc"
	httpapi "github.com/sooboy/tongdaxin/internal/api/http"
	"github.com/sooboy/tongdaxin/internal/cache"
	"github.com/sooboy/tongdaxin/internal/domain"
	"github.com/sooboy/tongdaxin/internal/history"
	gotdxadapter "github.com/sooboy/tongdaxin/internal/provider/gotdx"
	"github.com/sooboy/tongdaxin/internal/service"
	"github.com/sooboy/tongdaxin/internal/storage"
	grpcgo "google.golang.org/grpc"
)

// Config wires the runnable market-data server without exposing vendor types.
type Config struct {
	Addr            string
	GRPCAddr        string
	HTTPRouter      string
	DisableLive     bool
	QuoteHosts      []string
	TickHosts       []string
	HistoryHosts    []string
	KLineHosts      []string
	AdjustHosts     []string
	StaticHosts     []string
	TimeoutSec      int
	ClientsPerHost  int
	MaxHostsPerPool int
	ShutdownTimeout time.Duration

	StorageDialect      storage.Dialect
	StorageDSN          string
	StorageMaxOpenConns int
	StorageMaxIdleConns int

	CacheRedisURL  string
	CacheKeyPrefix string
	RateLimitRPS   int
	RateLimitBurst int
	LogDir         string
	LogFilePrefix  string
	LogStdout      bool

	ProviderFactory ProviderFactory
}

// ProviderFactory creates the live adapter and returns its close hook.
type ProviderFactory func(context.Context, Config) (domain.MarketDataProvider, func(context.Context) error, error)

// App owns the HTTP server and optional upstream close hook.
type App struct {
	Server          *http.Server
	GRPCServer      *grpcgo.Server
	GRPCAddr        string
	Handler         http.Handler
	Store           domain.HistoryStore
	Cache           cache.Cache
	shutdownTimeout time.Duration
	shutdownStartFn func()
	closeFn         func(context.Context) error
	shutdownOnce    sync.Once
	shutdownMu      sync.Mutex
	shutdownErr     error
}

// Build creates the service handler and starts the live gotdx-backed provider in the background unless disabled.
func Build(ctx context.Context, cfg Config) (*App, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.ProviderFactory == nil {
		cfg.ProviderFactory = buildLiveProvider
	}
	if cfg.HTTPRouter == "" {
		cfg.HTTPRouter = "nethttp"
	}
	log.Printf("bootstrap build start addr=%s grpc_addr=%s http_router=%s offline=%t storage=%s redis_cache=%t cache_prefix=%q rate_limit_rps=%d", cfg.Addr, cfg.GRPCAddr, cfg.HTTPRouter, cfg.DisableLive, cfg.StorageDialect, cfg.CacheRedisURL != "", cfg.CacheKeyPrefix, cfg.RateLimitRPS)

	store, queue, storeClose, err := buildStore(ctx, cfg)
	if err != nil {
		return nil, err
	}

	closeFns := make([]func(context.Context) error, 0, 4)
	if storeClose != nil {
		closeFns = append(closeFns, storeClose)
	}
	cleanup := func() {
		for i := len(closeFns) - 1; i >= 0; i-- {
			_ = closeFns[i](ctx)
		}
	}

	cacheStore, err := buildCache(ctx, cfg)
	if err != nil {
		cleanup()
		return nil, err
	}
	if cacheStore != nil {
		closeFns = append(closeFns, func(context.Context) error { return cacheStore.Close() })
	}

	var shutdownStartFn func()
	var provider domain.MarketDataProvider
	if cfg.DisableLive {
		log.Print("bootstrap live provider disabled")
	} else {
		log.Print("bootstrap live provider initializing in background")
		runtime := newProviderRuntime(cfg.ShutdownTimeout)
		provider = runtime

		providerCtx, providerCancel := context.WithCancel(ctx)
		shutdownStartFn = providerCancel
		closeFns = append(closeFns, func(shutdownCtx context.Context) error {
			providerCancel()
			return runtime.Close(shutdownCtx)
		})

		go runProviderFactoryUntilReady(providerCtx, runtime, cfg)
	}

	var closeFn func(context.Context) error
	if len(closeFns) > 0 {
		closeFn = func(ctx context.Context) error {
			var errs []error
			for i := len(closeFns) - 1; i >= 0; i-- {
				if err := closeFns[i](ctx); err != nil {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		}
	}

	svc := service.NewMarketDataService(provider, store, queue, cacheStore)
	handlerOpts := httpapi.Options{
		RateLimiter: httpapi.NewRateLimiter(httpapi.RateLimitConfig{RequestsPerSecond: cfg.RateLimitRPS, Burst: cfg.RateLimitBurst}),
		Logger:      log.Default(),
	}
	handler, err := buildHTTPHandler(svc, cfg, handlerOpts)
	if err != nil {
		cleanup()
		return nil, err
	}
	var grpcServer *grpcgo.Server
	if strings.TrimSpace(cfg.GRPCAddr) != "" {
		grpcServer = grpcapi.NewGRPCServer(svc)
	}
	return &App{
		Server: &http.Server{
			Addr:    cfg.Addr,
			Handler: handler,
		},
		GRPCServer:      grpcServer,
		GRPCAddr:        strings.TrimSpace(cfg.GRPCAddr),
		Handler:         handler,
		Store:           store,
		Cache:           cacheStore,
		shutdownTimeout: cfg.ShutdownTimeout,
		shutdownStartFn: shutdownStartFn,
		closeFn:         closeFn,
	}, nil
}

func buildHTTPHandler(svc httpapi.MarketDataService, cfg Config, opts httpapi.Options) (http.Handler, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.HTTPRouter)) {
	case "", "nethttp", "stdlib", "http":
		return httpapi.NewWithOptions(svc, opts), nil
	case "gin":
		return ginapi.NewWithOptions(svc, opts), nil
	default:
		return nil, domain.ErrInvalidRequest
	}
}

func runProviderFactoryUntilReady(ctx context.Context, runtime *providerRuntime, cfg Config) {
	const maxRetryDelay = 30 * time.Second
	retryDelay := time.Second
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			runtime.finish(nil, nil)
			return
		}
		builtProvider, providerClose, err := cfg.ProviderFactory(ctx, cfg)
		if err == nil && builtProvider != nil {
			runtime.finish(builtProvider, providerClose)
			return
		}
		if err == nil {
			err = errors.New("provider factory returned nil provider")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			runtime.finish(nil, nil)
			return
		}
		log.Printf("bootstrap live provider init failed attempt=%d retry_in=%s err=%v", attempt, retryDelay, err)
		select {
		case <-ctx.Done():
			runtime.finish(nil, nil)
			return
		case <-time.After(retryDelay):
		}
		if retryDelay < maxRetryDelay {
			retryDelay *= 2
			if retryDelay > maxRetryDelay {
				retryDelay = maxRetryDelay
			}
		}
	}
}

func buildCache(ctx context.Context, cfg Config) (cache.Cache, error) {
	if cfg.CacheRedisURL == "" {
		return cache.NewMemory(cache.DefaultConfig()), nil
	}
	cacheCfg := cache.DefaultConfig()
	cacheCfg.KeyPrefix = cfg.CacheKeyPrefix
	return cache.OpenRedis(ctx, cfg.CacheRedisURL, cacheCfg)
}

func buildStore(ctx context.Context, cfg Config) (domain.HistoryStore, domain.BackfillQueue, func(context.Context) error, error) {
	if cfg.StorageDialect == "" {
		store := history.NewMemoryStore()
		return store, store, nil, nil
	}
	store, err := storage.Open(ctx, storage.Config{
		Dialect:      cfg.StorageDialect,
		DSN:          cfg.StorageDSN,
		MaxOpenConns: cfg.StorageMaxOpenConns,
		MaxIdleConns: cfg.StorageMaxIdleConns,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return store, store, func(context.Context) error { return store.Close() }, nil
}

func buildLiveProvider(ctx context.Context, cfg Config) (domain.MarketDataProvider, func(context.Context) error, error) {
	liveCfg := gotdxadapter.LiveConfig{
		QuoteHosts:      cfg.QuoteHosts,
		TickHosts:       cfg.TickHosts,
		HistoryHosts:    cfg.HistoryHosts,
		KLineHosts:      cfg.KLineHosts,
		AdjustHosts:     cfg.AdjustHosts,
		StaticHosts:     cfg.StaticHosts,
		TimeoutSec:      cfg.TimeoutSec,
		ClientsPerHost:  cfg.ClientsPerHost,
		MaxHostsPerPool: cfg.MaxHostsPerPool,
	}
	provider, err := gotdxadapter.NewLiveProvider(ctx, liveCfg)
	if err != nil {
		return nil, nil, err
	}
	return provider, provider.Close, nil
}

// Serve runs the server against an already prepared listener.
func (a *App) Serve(ctx context.Context, ln net.Listener) error {
	if a == nil || a.Server == nil {
		return domain.ErrInvalidRequest
	}
	if ctx == nil {
		ctx = context.Background()
	}

	errCh := make(chan error, 1)
	go func() { errCh <- a.Server.Serve(ln) }()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.Join(err, a.Shutdown(context.Background()))
		}
		return nil
	case <-ctx.Done():
		return a.Shutdown(context.Background())
	}
}

// ListenAndServe binds the configured address and serves until ctx cancels or the server exits.
func (a *App) ListenAndServe(ctx context.Context) error {
	if a == nil || a.Server == nil {
		return domain.ErrInvalidRequest
	}
	httpLn, err := net.Listen("tcp", a.Server.Addr)
	if err != nil {
		return errors.Join(err, a.Shutdown(context.Background()))
	}
	log.Printf("market-data HTTP service listening on %s", httpLn.Addr().String())
	defer httpLn.Close()

	var grpcLn net.Listener
	if a.GRPCServer != nil && strings.TrimSpace(a.GRPCAddr) != "" {
		grpcLn, err = net.Listen("tcp", a.GRPCAddr)
		if err != nil {
			return errors.Join(err, a.Shutdown(context.Background()))
		}
		log.Printf("market-data gRPC service listening on %s", grpcLn.Addr().String())
		defer grpcLn.Close()
	}
	return a.serveListeners(ctx, httpLn, grpcLn)
}

func (a *App) serveListeners(ctx context.Context, httpLn net.Listener, grpcLn net.Listener) error {
	if ctx == nil {
		ctx = context.Background()
	}
	errCh := make(chan error, 2)
	go func() { errCh <- a.Server.Serve(httpLn) }()
	if grpcLn != nil && a.GRPCServer != nil {
		go func() { errCh <- a.GRPCServer.Serve(grpcLn) }()
	}

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpcgo.ErrServerStopped) {
			return errors.Join(err, a.Shutdown(context.Background()))
		}
		return nil
	case <-ctx.Done():
		return a.Shutdown(context.Background())
	}
}

func stopGRPCServer(server *grpcgo.Server, ctx context.Context) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		server.Stop()
		<-done
	}
}

// Shutdown closes the HTTP server and the upstream provider, if any.
func (a *App) Shutdown(ctx context.Context) error {
	if a == nil || a.Server == nil {
		return nil
	}
	a.shutdownOnce.Do(func() {
		baseCtx := ctx
		if baseCtx == nil {
			baseCtx = context.Background()
		}
		if a.shutdownStartFn != nil {
			a.shutdownStartFn()
		}
		serverCtx := baseCtx
		if a.shutdownTimeout > 0 {
			var cancel context.CancelFunc
			serverCtx, cancel = context.WithTimeout(baseCtx, a.shutdownTimeout)
			defer cancel()
		}

		var errs []error
		if err := a.Server.Shutdown(serverCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
		if a.GRPCServer != nil {
			stopGRPCServer(a.GRPCServer, serverCtx)
		}
		if a.closeFn != nil {
			cleanupCtx := context.Background()
			if a.shutdownTimeout > 0 {
				var cleanupCancel context.CancelFunc
				cleanupCtx, cleanupCancel = context.WithTimeout(context.Background(), a.shutdownTimeout)
				defer cleanupCancel()
			}
			if err := a.closeFn(cleanupCtx); err != nil {
				errs = append(errs, err)
			}
		}
		shutdownErr := errors.Join(errs...)
		a.shutdownMu.Lock()
		a.shutdownErr = shutdownErr
		a.shutdownMu.Unlock()
	})
	a.shutdownMu.Lock()
	defer a.shutdownMu.Unlock()
	return a.shutdownErr
}
