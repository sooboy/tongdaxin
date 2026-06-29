package storage

import "fmt"

// Dialect identifies the SQL backend used for first-phase persistent storage.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectMySQL    Dialect = "mysql"
	DialectSQLite   Dialect = "sqlite"
)

type Migration struct {
	ID          string
	Description string
	Statements  []string
}

func RequiredTables() []string {
	return []string{
		"securities",
		"trading_days",
		"history_ticks",
		"kline_bars",
		"history_coverage",
		"xdxr_events",
		"finance_snapshots",
		"backfill_tasks",
	}
}

func Migrations(dialect Dialect) ([]Migration, error) {
	switch dialect {
	case DialectPostgres:
		return postgresMigrations(), nil
	case DialectMySQL:
		return mysqlMigrations(), nil
	case DialectSQLite:
		return sqliteMigrations(), nil
	default:
		return nil, fmt.Errorf("unsupported storage dialect %q", dialect)
	}
}

func postgresMigrations() []Migration {
	return []Migration{
		{
			ID:          "001_core_marketdata_tables",
			Description: "core securities, history, coverage and backfill tables",
			Statements: []string{
				`CREATE TABLE IF NOT EXISTS securities (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL DEFAULT 'unknown',
    fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, code)
);`,
				`CREATE TABLE IF NOT EXISTS trading_days (
    market TEXT NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, trade_date)
);`,
				`CREATE TABLE IF NOT EXISTS history_ticks (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    price NUMERIC(20, 6) NOT NULL,
    volume BIGINT NOT NULL,
    amount NUMERIC(24, 6) NOT NULL DEFAULT 0,
    direction TEXT NOT NULL DEFAULT 'unknown',
    sequence BIGINT NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    source_address TEXT NOT NULL DEFAULT '',
    fetch_time TIMESTAMPTZ,
    checksum TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (market, code, trade_date, trade_time, sequence)
);`,
				`CREATE INDEX IF NOT EXISTS history_ticks_symbol_time_idx ON history_ticks (market, code, trade_date, trade_time);`,
				`CREATE TABLE IF NOT EXISTS kline_bars (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    period TEXT NOT NULL,
    adjust_type TEXT NOT NULL DEFAULT 'none',
    bar_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(20, 6) NOT NULL,
    high NUMERIC(20, 6) NOT NULL,
    low NUMERIC(20, 6) NOT NULL,
    close NUMERIC(20, 6) NOT NULL,
    volume NUMERIC(24, 6) NOT NULL DEFAULT 0,
    amount NUMERIC(24, 6) NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    source_address TEXT NOT NULL DEFAULT '',
    fetch_time TIMESTAMPTZ,
    checksum TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (market, code, period, adjust_type, bar_time)
);`,
				`CREATE INDEX IF NOT EXISTS kline_bars_symbol_range_idx ON kline_bars (market, code, period, adjust_type, bar_time);`,
				`CREATE TABLE IF NOT EXISTS history_coverage (
    dataset TEXT NOT NULL,
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    trade_date DATE NOT NULL DEFAULT DATE '1970-01-01',
    period TEXT NOT NULL DEFAULT '',
    adjust_type TEXT NOT NULL DEFAULT 'none',
    status TEXT NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    source_address TEXT NOT NULL DEFAULT '',
    last_fetch_time TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, market, code, trade_date, period, adjust_type)
);`,
				`CREATE TABLE IF NOT EXISTS xdxr_events (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    event_date DATE NOT NULL,
    event_type TEXT NOT NULL DEFAULT '',
    cash_dividend NUMERIC(20, 6) NOT NULL DEFAULT 0,
    bonus_share NUMERIC(20, 6) NOT NULL DEFAULT 0,
    allotment_price NUMERIC(20, 6) NOT NULL DEFAULT 0,
    allotment_ratio NUMERIC(20, 6) NOT NULL DEFAULT 0,
    raw_fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, code, event_date, event_type)
);`,
				`CREATE TABLE IF NOT EXISTS finance_snapshots (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_time TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, code)
);`,
				`CREATE TABLE IF NOT EXISTS backfill_tasks (
    task_id TEXT PRIMARY KEY,
    dataset TEXT NOT NULL,
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    period TEXT NOT NULL DEFAULT '',
    adjust_type TEXT NOT NULL DEFAULT 'none',
    priority INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_time TIMESTAMPTZ,
    error_message TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset, market, code, start_date, end_date, period, adjust_type)
);`,
				`CREATE INDEX IF NOT EXISTS backfill_tasks_pick_idx ON backfill_tasks (status, priority DESC, created_at);`,
			},
		},
	}
}

func mysqlMigrations() []Migration {
	return []Migration{
		{
			ID:          "001_core_marketdata_tables",
			Description: "core securities, history, coverage and backfill tables",
			Statements: []string{
				`CREATE TABLE IF NOT EXISTS securities (
    market VARCHAR(16) NOT NULL,
    code VARCHAR(32) NOT NULL,
    name VARCHAR(128) NOT NULL DEFAULT '',
    type VARCHAR(32) NOT NULL DEFAULT 'unknown',
    status VARCHAR(32) NOT NULL DEFAULT 'unknown',
    fields JSON NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (market, code)
);`,
				`CREATE TABLE IF NOT EXISTS trading_days (
    market VARCHAR(16) NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (market, trade_date)
);`,
				`CREATE TABLE IF NOT EXISTS history_ticks (
    market VARCHAR(16) NOT NULL,
    code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    trade_time DATETIME(3) NOT NULL,
    price DECIMAL(20, 6) NOT NULL,
    volume BIGINT NOT NULL,
    amount DECIMAL(24, 6) NOT NULL DEFAULT 0,
    direction VARCHAR(32) NOT NULL DEFAULT 'unknown',
    sequence BIGINT NOT NULL DEFAULT 0,
    source VARCHAR(32) NOT NULL DEFAULT '',
    source_address VARCHAR(255) NOT NULL DEFAULT '',
    fetch_time DATETIME(3),
    checksum VARCHAR(128) NOT NULL DEFAULT '',
    PRIMARY KEY (market, code, trade_date, trade_time, sequence),
    KEY history_ticks_symbol_time_idx (market, code, trade_date, trade_time)
);`,
				`CREATE TABLE IF NOT EXISTS kline_bars (
    market VARCHAR(16) NOT NULL,
    code VARCHAR(32) NOT NULL,
    period VARCHAR(16) NOT NULL,
    adjust_type VARCHAR(16) NOT NULL DEFAULT 'none',
    bar_time DATETIME(3) NOT NULL,
    open DECIMAL(20, 6) NOT NULL,
    high DECIMAL(20, 6) NOT NULL,
    low DECIMAL(20, 6) NOT NULL,
    close DECIMAL(20, 6) NOT NULL,
    volume DECIMAL(24, 6) NOT NULL DEFAULT 0,
    amount DECIMAL(24, 6) NOT NULL DEFAULT 0,
    source VARCHAR(32) NOT NULL DEFAULT '',
    source_address VARCHAR(255) NOT NULL DEFAULT '',
    fetch_time DATETIME(3),
    checksum VARCHAR(128) NOT NULL DEFAULT '',
    PRIMARY KEY (market, code, period, adjust_type, bar_time),
    KEY kline_bars_symbol_range_idx (market, code, period, adjust_type, bar_time)
);`,
				`CREATE TABLE IF NOT EXISTS history_coverage (
    dataset VARCHAR(32) NOT NULL,
    market VARCHAR(16) NOT NULL,
    code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL DEFAULT '1970-01-01',
    period VARCHAR(16) NOT NULL DEFAULT '',
    adjust_type VARCHAR(16) NOT NULL DEFAULT 'none',
    status VARCHAR(32) NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(128) NOT NULL DEFAULT '',
    source_address VARCHAR(255) NOT NULL DEFAULT '',
    last_fetch_time DATETIME(3),
    last_error TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, market, code, trade_date, period, adjust_type)
);`,
				`CREATE TABLE IF NOT EXISTS xdxr_events (
    market VARCHAR(16) NOT NULL,
    code VARCHAR(32) NOT NULL,
    event_date DATE NOT NULL,
    event_type VARCHAR(64) NOT NULL DEFAULT '',
    cash_dividend DECIMAL(20, 6) NOT NULL DEFAULT 0,
    bonus_share DECIMAL(20, 6) NOT NULL DEFAULT 0,
    allotment_price DECIMAL(20, 6) NOT NULL DEFAULT 0,
    allotment_ratio DECIMAL(20, 6) NOT NULL DEFAULT 0,
    raw_fields JSON NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (market, code, event_date, event_type)
);`,
				`CREATE TABLE IF NOT EXISTS finance_snapshots (
    market VARCHAR(16) NOT NULL,
    code VARCHAR(32) NOT NULL,
    fields JSON NOT NULL,
    source_time DATETIME(3),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (market, code)
);`,
				`CREATE TABLE IF NOT EXISTS backfill_tasks (
    task_id VARCHAR(64) NOT NULL PRIMARY KEY,
    dataset VARCHAR(32) NOT NULL,
    market VARCHAR(16) NOT NULL,
    code VARCHAR(32) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    period VARCHAR(16) NOT NULL DEFAULT '',
    adjust_type VARCHAR(16) NOT NULL DEFAULT 'none',
    priority INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_time DATETIME(3),
    error_message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY backfill_tasks_dedupe_idx (dataset, market, code, start_date, end_date, period, adjust_type),
    KEY backfill_tasks_pick_idx (status, priority, created_at)
);`,
			},
		},
	}
}

func sqliteMigrations() []Migration {
	return []Migration{
		{
			ID:          "001_core_marketdata_tables",
			Description: "core securities, history, coverage and backfill tables",
			Statements: []string{
				`CREATE TABLE IF NOT EXISTS securities (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL DEFAULT 'unknown',
    fields TEXT NOT NULL DEFAULT '{}',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market, code)
);`,
				`CREATE TABLE IF NOT EXISTS trading_days (
    market TEXT NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market, trade_date)
);`,
				`CREATE TABLE IF NOT EXISTS history_ticks (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_time DATETIME NOT NULL,
    price REAL NOT NULL,
    volume INTEGER NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    direction TEXT NOT NULL DEFAULT 'unknown',
    sequence INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    source_address TEXT NOT NULL DEFAULT '',
    fetch_time DATETIME,
    checksum TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (market, code, trade_date, trade_time, sequence)
);`,
				`CREATE INDEX IF NOT EXISTS history_ticks_symbol_time_idx ON history_ticks (market, code, trade_date, trade_time);`,
				`CREATE TABLE IF NOT EXISTS kline_bars (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    period TEXT NOT NULL,
    adjust_type TEXT NOT NULL DEFAULT 'none',
    bar_time DATETIME NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL DEFAULT 0,
    amount REAL NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    source_address TEXT NOT NULL DEFAULT '',
    fetch_time DATETIME,
    checksum TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (market, code, period, adjust_type, bar_time)
);`,
				`CREATE INDEX IF NOT EXISTS kline_bars_symbol_range_idx ON kline_bars (market, code, period, adjust_type, bar_time);`,
				`CREATE TABLE IF NOT EXISTS history_coverage (
    dataset TEXT NOT NULL,
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    trade_date DATE NOT NULL DEFAULT '1970-01-01',
    period TEXT NOT NULL DEFAULT '',
    adjust_type TEXT NOT NULL DEFAULT 'none',
    status TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    source_address TEXT NOT NULL DEFAULT '',
    last_fetch_time DATETIME,
    last_error TEXT NOT NULL DEFAULT '',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset, market, code, trade_date, period, adjust_type)
);`,
				`CREATE TABLE IF NOT EXISTS xdxr_events (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    event_date DATE NOT NULL,
    event_type TEXT NOT NULL DEFAULT '',
    cash_dividend REAL NOT NULL DEFAULT 0,
    bonus_share REAL NOT NULL DEFAULT 0,
    allotment_price REAL NOT NULL DEFAULT 0,
    allotment_ratio REAL NOT NULL DEFAULT 0,
    raw_fields TEXT NOT NULL DEFAULT '{}',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market, code, event_date, event_type)
);`,
				`CREATE TABLE IF NOT EXISTS finance_snapshots (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    fields TEXT NOT NULL DEFAULT '{}',
    source_time DATETIME,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market, code)
);`,
				`CREATE TABLE IF NOT EXISTS backfill_tasks (
    task_id TEXT PRIMARY KEY,
    dataset TEXT NOT NULL,
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    period TEXT NOT NULL DEFAULT '',
    adjust_type TEXT NOT NULL DEFAULT 'none',
    priority INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_time DATETIME,
    error_message TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset, market, code, start_date, end_date, period, adjust_type)
);`,
				`CREATE INDEX IF NOT EXISTS backfill_tasks_pick_idx ON backfill_tasks (status, priority DESC, created_at);`,
			},
		},
	}
}
