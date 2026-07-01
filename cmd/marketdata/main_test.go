package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/sooboy/tongdaxin/internal/bootstrap"
	"github.com/sooboy/tongdaxin/internal/storage"
)

func TestSplitHosts(t *testing.T) {
	t.Parallel()

	got := splitHosts(" 1.1.1.1:7709, ,2.2.2.2:7709 ")
	want := []string{"1.1.1.1:7709", "2.2.2.2:7709"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitHosts = %#v", got)
	}
}

func TestBuildConfigParsesStorageFlags(t *testing.T) {
	t.Parallel()

	cfg, err := buildConfig([]string{
		"-offline",
		"-addr", "127.0.0.1:0",
		"-http-router", "gin",
		"-grpc-addr", "127.0.0.1:9090",
		"-max-hosts-per-pool", "3",
		"-clients-per-host", "2",
		"-storage-dialect", "sqlite",
		"-storage-dsn", "file:test.sqlite",
		"-storage-max-open-conns", "4",
		"-storage-max-idle-conns", "2",
	})
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if !cfg.DisableLive || cfg.Addr != "127.0.0.1:0" || cfg.HTTPRouter != "gin" || cfg.GRPCAddr != "127.0.0.1:9090" || cfg.MaxHostsPerPool != 3 || cfg.ClientsPerHost != 2 {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.StorageDialect != storage.DialectSQLite || cfg.StorageDSN != "file:test.sqlite" || cfg.StorageMaxOpenConns != 4 || cfg.StorageMaxIdleConns != 2 {
		t.Fatalf("storage cfg = %+v", cfg)
	}
}
func TestBuildConfigParsesCacheFlags(t *testing.T) {
	t.Parallel()

	cfg, err := buildConfig([]string{
		"-cache-redis-url", "redis://127.0.0.1:6379/1",
		"-cache-key-prefix", "marketdata:test",
	})
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.CacheRedisURL != "redis://127.0.0.1:6379/1" || cfg.CacheKeyPrefix != "marketdata:test" {
		t.Fatalf("cache cfg = %+v", cfg)
	}
}

func TestBuildConfigParsesRateLimitFlags(t *testing.T) {
	t.Parallel()

	cfg, err := buildConfig([]string{
		"-api-rate-limit-rps", "1000",
		"-api-rate-limit-burst", "1500",
	})
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.RateLimitRPS != 1000 || cfg.RateLimitBurst != 1500 {
		t.Fatalf("rate limit cfg = %+v", cfg)
	}
}

func TestBuildConfigParsesLogFlags(t *testing.T) {
	t.Parallel()

	cfg, err := buildConfig([]string{
		"-log-dir", " /tmp/market-logs ",
		"-log-file-prefix", " marketdata-debug ",
		"-log-stdout=false",
	})
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.LogDir != "/tmp/market-logs" || cfg.LogFilePrefix != "marketdata-debug" || cfg.LogStdout {
		t.Fatalf("log cfg = %+v", cfg)
	}
}

func TestBuildConfigAllowsEnvToDisableLogFiles(t *testing.T) {
	t.Setenv("MARKETDATA_LOG_DIR", "")

	cfg, err := buildConfig(nil)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.LogDir != "" {
		t.Fatalf("LogDir = %q, want empty", cfg.LogDir)
	}
}

func TestConfigureLoggingWritesDailyFile(t *testing.T) {
	dir := t.TempDir()
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})

	closer, err := configureLogging(bootstrap.Config{LogDir: dir, LogFilePrefix: "marketdata-test", LogStdout: false})
	if err != nil {
		t.Fatalf("configureLogging: %v", err)
	}
	log.Print("daily file log enabled")
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "marketdata-test-*.log"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("log files = %#v, want one", files)
	}
	payload, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Contains(payload, []byte("daily file log enabled")) {
		t.Fatalf("log file = %q", payload)
	}
}

func TestConfigureLoggingHandlesStdoutOnlyMode(t *testing.T) {
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})

	closer, err := configureLogging(bootstrap.Config{LogStdout: true})
	if err != nil {
		t.Fatalf("configureLogging stdout: %v", err)
	}
	if closer != nil {
		t.Fatalf("closer = %T, want nil", closer)
	}
	if log.Writer() != os.Stdout {
		t.Fatalf("writer = %T, want stdout", log.Writer())
	}

	closer, err = configureLogging(bootstrap.Config{LogStdout: false})
	if err != nil {
		t.Fatalf("configureLogging discard: %v", err)
	}
	if closer != nil {
		t.Fatalf("closer = %T, want nil", closer)
	}
	if log.Writer() != io.Discard {
		t.Fatalf("writer = %T, want discard", log.Writer())
	}
}

func TestEnvBoolDefault(t *testing.T) {
	t.Setenv("MARKETDATA_TEST_BOOL_FALSE", "off")
	t.Setenv("MARKETDATA_TEST_BOOL_BAD", "bad")
	if envBoolDefault("MARKETDATA_TEST_BOOL_MISSING", true) != true {
		t.Fatal("missing env did not keep fallback true")
	}
	if envBoolDefault("MARKETDATA_TEST_BOOL_FALSE", true) != false {
		t.Fatal("off env did not parse false")
	}
	if envBoolDefault("MARKETDATA_TEST_BOOL_BAD", true) != true {
		t.Fatal("bad env did not keep fallback")
	}
}

func TestParseStorageDialect(t *testing.T) {
	t.Parallel()

	cases := map[string]storage.Dialect{
		"":           "",
		"memory":     "",
		"postgresql": storage.DialectPostgres,
		"mysql":      storage.DialectMySQL,
		"sqlite3":    storage.DialectSQLite,
	}
	for raw, want := range cases {
		raw, want := raw, want
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			if got := parseStorageDialect(raw); got != want {
				t.Fatalf("parseStorageDialect(%q) = %q want %q", raw, got, want)
			}
		})
	}
}

func TestEnvHelpers(t *testing.T) {
	t.Setenv("MARKETDATA_TEST_ADDR", " :9000 ")
	t.Setenv("MARKETDATA_TEST_BOOL", "yes")
	t.Setenv("MARKETDATA_TEST_INT", "12")
	t.Setenv("MARKETDATA_TEST_DURATION", "2s")
	if got := envDefault("MARKETDATA_TEST_ADDR", ":8080"); got != ":9000" {
		t.Fatalf("envDefault = %q", got)
	}
	if !envBool("MARKETDATA_TEST_BOOL") {
		t.Fatal("envBool = false")
	}
	if got := envInt("MARKETDATA_TEST_INT", 3); got != 12 {
		t.Fatalf("envInt = %d", got)
	}
	if got := envDuration("MARKETDATA_TEST_DURATION", time.Second); got != 2*time.Second {
		t.Fatalf("envDuration = %s", got)
	}
}

func TestEnvHelpersFallback(t *testing.T) {
	t.Setenv("MARKETDATA_TEST_BAD_INT", "bad")
	t.Setenv("MARKETDATA_TEST_BAD_DURATION", "bad")
	if got := envDefault("MARKETDATA_TEST_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("envDefault fallback = %q", got)
	}
	if envBool("MARKETDATA_TEST_MISSING") {
		t.Fatal("envBool missing = true")
	}
	if got := envInt("MARKETDATA_TEST_BAD_INT", 7); got != 7 {
		t.Fatalf("envInt fallback = %d", got)
	}
	if got := envDuration("MARKETDATA_TEST_BAD_DURATION", time.Second); got != time.Second {
		t.Fatalf("envDuration fallback = %s", got)
	}
}

func TestMainIgnoresIrrelevantEnv(t *testing.T) {
	if _, ok := os.LookupEnv("THIS_ENV_SHOULD_NOT_EXIST_FOR_TEST"); ok {
		t.Fatal("unexpected env present")
	}
}
