package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sooboy/tongdaxin/internal/bootstrap"
	"github.com/sooboy/tongdaxin/internal/logging"
	"github.com/sooboy/tongdaxin/internal/storage"
)

func main() {
	err := run(os.Args[1:])
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func run(args []string) (err error) {
	cfg, err := buildConfig(args)
	if err != nil {
		return err
	}
	logCloser, err := configureLogging(cfg)
	if err != nil {
		return err
	}
	if logCloser != nil {
		defer func() {
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("market-data service stopped error=%v", err)
			}
			if closeErr := logCloser.Close(); err == nil {
				err = closeErr
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	if cfg.DisableLive {
		log.Print("market-data service starting in offline mode")
	} else {
		log.Print("market-data service starting live upstream initialization in background; use -offline for local smoke tests")
	}

	app, err := bootstrap.Build(ctx, cfg)
	if err != nil {
		return err
	}
	return app.ListenAndServe(ctx)
}

func buildConfig(args []string) (bootstrap.Config, error) {
	fs := flag.NewFlagSet("marketdata", flag.ContinueOnError)
	addr := fs.String("addr", envDefault("MARKETDATA_ADDR", ":8080"), "HTTP listen address")
	offline := fs.Bool("offline", envBool("MARKETDATA_OFFLINE"), "start without live upstream connections")
	quoteHosts := fs.String("quote-hosts", os.Getenv("MARKETDATA_QUOTE_HOSTS"), "comma-separated quote upstream hosts")
	tickHosts := fs.String("tick-hosts", os.Getenv("MARKETDATA_TICK_HOSTS"), "comma-separated tick upstream hosts")
	historyHosts := fs.String("history-hosts", os.Getenv("MARKETDATA_HISTORY_HOSTS"), "comma-separated history upstream hosts")
	klineHosts := fs.String("kline-hosts", os.Getenv("MARKETDATA_KLINE_HOSTS"), "comma-separated kline upstream hosts")
	adjustHosts := fs.String("adjust-hosts", os.Getenv("MARKETDATA_ADJUST_HOSTS"), "comma-separated adjusted-kline upstream hosts")
	staticHosts := fs.String("static-hosts", os.Getenv("MARKETDATA_STATIC_HOSTS"), "comma-separated static-data upstream hosts")
	timeoutSec := fs.Int("timeout-sec", envInt("MARKETDATA_TIMEOUT_SEC", 3), "upstream request timeout seconds")
	clientsPerHost := fs.Int("clients-per-host", envInt("MARKETDATA_CLIENTS_PER_HOST", 0), "independent clients per selected upstream host; 0 uses provider default")
	maxHostsPerPool := fs.Int("max-hosts-per-pool", envInt("MARKETDATA_MAX_HOSTS_PER_POOL", 0), "maximum reachable upstream hosts selected per pool; 0 uses provider default, negative uses all hosts")
	shutdownTimeout := fs.Duration("shutdown-timeout", envDuration("MARKETDATA_SHUTDOWN_TIMEOUT", 10*time.Second), "graceful shutdown timeout")
	storageDialect := fs.String("storage-dialect", envDefault("MARKETDATA_STORAGE_DIALECT", ""), "history storage backend: empty memory, sqlite, postgres or mysql")
	storageDSN := fs.String("storage-dsn", os.Getenv("MARKETDATA_STORAGE_DSN"), "history storage DSN; sqlite uses a local file when omitted")
	storageMaxOpenConns := fs.Int("storage-max-open-conns", envInt("MARKETDATA_STORAGE_MAX_OPEN_CONNS", 0), "history storage max open SQL connections")
	storageMaxIdleConns := fs.Int("storage-max-idle-conns", envInt("MARKETDATA_STORAGE_MAX_IDLE_CONNS", 0), "history storage max idle SQL connections")
	cacheRedisURL := fs.String("cache-redis-url", envDefault("MARKETDATA_CACHE_REDIS_URL", ""), "Redis cache URL; empty uses in-process memory cache")
	apiRateLimitRPS := fs.Int("api-rate-limit-rps", envInt("MARKETDATA_API_RATE_LIMIT_RPS", 0), "global API request limit per second; 0 disables limiting")
	apiRateLimitBurst := fs.Int("api-rate-limit-burst", envInt("MARKETDATA_API_RATE_LIMIT_BURST", 0), "global API token bucket burst; defaults to rps when omitted")
	logDir := fs.String("log-dir", envDefaultAllowEmpty("MARKETDATA_LOG_DIR", "logs/marketdata"), "directory for daily log files; empty disables file logging")
	logFilePrefix := fs.String("log-file-prefix", envDefault("MARKETDATA_LOG_FILE_PREFIX", "marketdata"), "daily log file prefix")
	logStdout := fs.Bool("log-stdout", envBoolDefault("MARKETDATA_LOG_STDOUT", true), "also write logs to stdout")
	if err := fs.Parse(args); err != nil {
		return bootstrap.Config{}, err
	}

	return bootstrap.Config{
		Addr:                *addr,
		DisableLive:         *offline,
		QuoteHosts:          splitHosts(*quoteHosts),
		TickHosts:           splitHosts(*tickHosts),
		HistoryHosts:        splitHosts(*historyHosts),
		KLineHosts:          splitHosts(*klineHosts),
		AdjustHosts:         splitHosts(*adjustHosts),
		StaticHosts:         splitHosts(*staticHosts),
		TimeoutSec:          *timeoutSec,
		ClientsPerHost:      *clientsPerHost,
		MaxHostsPerPool:     *maxHostsPerPool,
		ShutdownTimeout:     *shutdownTimeout,
		StorageDialect:      parseStorageDialect(*storageDialect),
		StorageDSN:          strings.TrimSpace(*storageDSN),
		StorageMaxOpenConns: *storageMaxOpenConns,
		StorageMaxIdleConns: *storageMaxIdleConns,
		CacheRedisURL:       strings.TrimSpace(*cacheRedisURL),
		RateLimitRPS:        *apiRateLimitRPS,
		RateLimitBurst:      *apiRateLimitBurst,
		LogDir:              strings.TrimSpace(*logDir),
		LogFilePrefix:       strings.TrimSpace(*logFilePrefix),
		LogStdout:           *logStdout,
	}, nil
}

func parseStorageDialect(raw string) storage.Dialect {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "memory", "mem", "inmemory":
		return ""
	case "postgres", "postgresql":
		return storage.DialectPostgres
	case "mysql":
		return storage.DialectMySQL
	case "sqlite", "sqlite3":
		return storage.DialectSQLite
	default:
		return storage.Dialect(strings.ToLower(strings.TrimSpace(raw)))
	}
}

func splitHosts(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	hosts := make([]string, 0, len(parts))
	for _, part := range parts {
		host := strings.TrimSpace(part)
		if host != "" {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

func envDefault(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envDefaultAllowEmpty(name string, fallback string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	return strings.TrimSpace(value)
}

func envBool(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func envBoolDefault(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func configureLogging(cfg bootstrap.Config) (io.Closer, error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	if cfg.LogDir == "" {
		if cfg.LogStdout {
			log.SetOutput(os.Stdout)
		} else {
			log.SetOutput(io.Discard)
		}
		return nil, nil
	}
	writer, err := logging.NewDailyFileWriter(logging.DailyFileWriterConfig{Dir: cfg.LogDir, Prefix: cfg.LogFilePrefix})
	if err != nil {
		return nil, err
	}
	if cfg.LogStdout {
		log.SetOutput(io.MultiWriter(os.Stdout, writer))
	} else {
		log.SetOutput(writer)
	}
	return writer, nil
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
