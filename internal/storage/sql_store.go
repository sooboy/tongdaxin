package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"

	"github.com/sooboy/tongdaxin/internal/domain"
)

var (
	_ domain.HistoryStore  = (*SQLStore)(nil)
	_ domain.BackfillQueue = (*SQLStore)(nil)
)

// Config controls the SQL-backed local history store.
type Config struct {
	Dialect      Dialect
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
}

// SQLStore persists local history coverage, history rows and backfill tasks.
type SQLStore struct {
	db      *sql.DB
	dialect Dialect
}

// Open connects to the requested SQL backend, applies the dialect schema and returns a store.
func Open(ctx context.Context, cfg Config) (*SQLStore, error) {
	if cfg.Dialect == "" {
		cfg.Dialect = DialectSQLite
	}
	if cfg.DSN == "" {
		cfg.DSN = DefaultDSN(cfg.Dialect)
	}
	if cfg.DSN == "" {
		return nil, fmt.Errorf("storage dsn required for %s", cfg.Dialect)
	}
	name, err := driverName(cfg.Dialect)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(name, cfg.DSN)
	if err != nil {
		return nil, err
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ApplyMigrations(ctx, db, cfg.Dialect); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLStore{db: db, dialect: cfg.Dialect}, nil
}

// Close releases the underlying database connection pool.
func (s *SQLStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// DefaultDSN returns a reasonable local-development DSN for the requested dialect.
func DefaultDSN(dialect Dialect) string {
	switch dialect {
	case DialectSQLite, "":
		return DefaultSQLiteDSN()
	default:
		return ""
	}
}

// DefaultSQLiteDSN returns the repo-local SQLite file DSN used by the market-data command.
func DefaultSQLiteDSN() string {
	return "file:marketdata.sqlite?_pragma=foreign_keys(1)&_time_format=sqlite"
}

// ApplyMigrations executes the schema statements for the requested dialect.
func ApplyMigrations(ctx context.Context, db *sql.DB, dialect Dialect) error {
	migrations, err := Migrations(dialect)
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply %s: %w", migration.ID, err)
			}
		}
	}
	return nil
}

func driverName(dialect Dialect) (string, error) {
	switch dialect {
	case DialectSQLite, "":
		return "sqlite", nil
	case DialectPostgres:
		return "postgres", nil
	case DialectMySQL:
		return "mysql", nil
	default:
		return "", fmt.Errorf("unsupported storage dialect %q", dialect)
	}
}

// Coverage returns the stored coverage row or a missing marker if no row exists.
func (s *SQLStore) Coverage(ctx context.Context, req domain.CoverageRequest) (domain.HistoryCoverage, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.HistoryCoverage{}, err
	}
	if err := req.Symbol.Validate(); err != nil {
		return domain.HistoryCoverage{}, err
	}
	tradeDate := domain.NormalizeDate(req.TradeDate)
	row := s.db.QueryRowContext(ctx, bind(s.dialect, `
SELECT dataset, market, code, trade_date, period, adjust_type, status, row_count, checksum, source_address, last_fetch_time, last_error
FROM history_coverage
WHERE dataset = ? AND market = ? AND code = ? AND trade_date = ? AND period = ? AND adjust_type = ?`),
		req.Dataset, req.Symbol.Market, req.Symbol.Code, tradeDate, req.Period, req.AdjustType)
	var coverage domain.HistoryCoverage
	var symbolMarket string
	var symbolCode string
	var tradeDateValue timeValue
	var lastFetch timeValue
	if err := row.Scan(&coverage.Dataset, &symbolMarket, &symbolCode, &tradeDateValue, &coverage.Period, &coverage.AdjustType, &coverage.Status, &coverage.RowCount, &coverage.Checksum, &coverage.SourceAddress, &lastFetch, &coverage.LastError); err != nil {
		if errorsIsNoRows(err) {
			return domain.HistoryCoverage{
				Dataset:    req.Dataset,
				Symbol:     req.Symbol,
				TradeDate:  tradeDate,
				Period:     req.Period,
				AdjustType: req.AdjustType,
				Status:     domain.CoverageMissing,
			}, nil
		}
		return domain.HistoryCoverage{}, err
	}
	coverage.Symbol = domain.Symbol{Market: domain.Market(symbolMarket), Code: symbolCode}
	if tradeDateValue.Valid {
		coverage.TradeDate = tradeDateValue.Time
	}
	if lastFetch.Valid {
		coverage.LastFetchTime = lastFetch.Time
	}
	return coverage, nil
}

// PutCoverage stores a coverage row, replacing any existing row for the same key.
func (s *SQLStore) PutCoverage(ctx context.Context, coverage domain.HistoryCoverage) error {
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
	now := time.Now()
	if coverage.LastFetchTime.IsZero() && coverage.Status == domain.CoverageCovered {
		coverage.LastFetchTime = now
	}
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, bind(s.dialect, `DELETE FROM history_coverage WHERE dataset = ? AND market = ? AND code = ? AND trade_date = ? AND period = ? AND adjust_type = ?`),
			coverage.Dataset, coverage.Symbol.Market, coverage.Symbol.Code, coverage.TradeDate, coverage.Period, coverage.AdjustType); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, bind(s.dialect, `
INSERT INTO history_coverage (
	dataset, market, code, trade_date, period, adjust_type, status, row_count, checksum, source_address, last_fetch_time, last_error, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			coverage.Dataset, coverage.Symbol.Market, coverage.Symbol.Code, coverage.TradeDate, coverage.Period, coverage.AdjustType, coverage.Status, coverage.RowCount, coverage.Checksum, coverage.SourceAddress, nullableTime(coverage.LastFetchTime), coverage.LastError, now)
		return err
	})
}

func (s *SQLStore) PutSecurities(ctx context.Context, items []domain.SecurityInfo) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	now := time.Now()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, item := range items {
			if err := item.Symbol.Validate(); err != nil {
				return err
			}
			item.Cached = false
			fields, err := json.Marshal(item.Fields)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, bind(s.dialect, `DELETE FROM securities WHERE market = ? AND code = ?`), item.Symbol.Market, item.Symbol.Code); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, bind(s.dialect, `
INSERT INTO securities (market, code, name, type, status, fields, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`),
				item.Symbol.Market, item.Symbol.Code, item.Symbol.Name, item.Symbol.Type, item.Symbol.Status, string(fields), now)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLStore) QuerySecurities(ctx context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
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

	query := `
SELECT market, code, name, type, status, fields
FROM securities`
	var clauses []string
	var args []any
	if len(markets) > 0 {
		placeholders := make([]string, 0, len(markets))
		for market := range markets {
			placeholders = append(placeholders, "?")
			args = append(args, market)
		}
		clauses = append(clauses, "market IN ("+strings.Join(placeholders, ",")+")")
	}
	if len(symbols) > 0 {
		pairs := make([]string, 0, len(symbols))
		for _, symbol := range req.Symbols {
			pairs = append(pairs, "(market = ? AND code = ?)")
			args = append(args, symbol.Market, symbol.Code)
		}
		clauses = append(clauses, "("+strings.Join(pairs, " OR ")+")")
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY market ASC, code ASC"
	if req.Count > 0 {
		query += " LIMIT ? OFFSET ?"
		args = append(args, req.Count, req.Start)
	} else if req.Start > 0 {
		query += " LIMIT ? OFFSET ?"
		args = append(args, int64(^uint(0)>>1), req.Start)
	}

	rows, err := s.db.QueryContext(ctx, bind(s.dialect, query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.SecurityInfo, 0)
	for rows.Next() {
		var item domain.SecurityInfo
		var market, code, name, securityType, status string
		var rawFields string
		if err := rows.Scan(&market, &code, &name, &securityType, &status, &rawFields); err != nil {
			return nil, err
		}
		item.Symbol = domain.Symbol{Market: domain.Market(market), Code: code, Name: name, Type: domain.SecurityType(securityType), Status: domain.SecurityStatus(status)}
		if strings.TrimSpace(rawFields) != "" {
			if err := json.Unmarshal([]byte(rawFields), &item.Fields); err != nil {
				return nil, err
			}
		}
		item.Cached = true
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, domain.ErrNoData
	}
	return items, nil
}

// PutTicks stores historical ticks, replacing any rows with the same primary key.
func (s *SQLStore) PutTicks(ctx context.Context, ticks []domain.Tick) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if len(ticks) == 0 {
		return nil
	}
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, tick := range ticks {
			if err := tick.Symbol.Validate(); err != nil {
				return err
			}
			tradeDate := tick.TradeDate
			if tradeDate.IsZero() {
				tradeDate = tick.TradeTime
			}
			tradeDate = domain.NormalizeDate(tradeDate)
			if tradeDate.IsZero() {
				return domain.ErrInvalidRequest
			}
			if _, err := tx.ExecContext(ctx, bind(s.dialect, `DELETE FROM history_ticks WHERE market = ? AND code = ? AND trade_date = ? AND trade_time = ? AND sequence = ?`),
				tick.Symbol.Market, tick.Symbol.Code, tradeDate, tick.TradeTime, tick.Sequence); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, bind(s.dialect, `
INSERT INTO history_ticks (
	market, code, trade_date, trade_time, price, volume, amount, direction, sequence, source, source_address, fetch_time, checksum
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
				tick.Symbol.Market, tick.Symbol.Code, tradeDate, tick.TradeTime, tick.Price, tick.Volume, tick.Amount, tick.Direction, tick.Sequence, tick.Source, "", nil, ""); err != nil {
				return err
			}
		}
		return nil
	})
}

// QueryTicks loads a historical tick page from the SQL store.
func (s *SQLStore) QueryTicks(ctx context.Context, req domain.HistoryTickQuery) ([]domain.Tick, error) {
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
	query := `
SELECT market, code, trade_date, trade_time, price, volume, amount, direction, sequence, source
FROM history_ticks
WHERE market = ? AND code = ? AND trade_date = ?
ORDER BY trade_time ASC, sequence ASC`
	args := []any{req.Symbol.Market, req.Symbol.Code, date}
	if req.Limit > 0 {
		query += ` LIMIT ? OFFSET ?`
		args = append(args, req.Limit, req.Start)
	}
	rows, err := s.db.QueryContext(ctx, bind(s.dialect, query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanTicks(rows)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		if req.Start > 0 && req.Limit > 0 {
			return []domain.Tick{}, nil
		}
		return nil, domain.ErrNoData
	}
	if req.Limit == 0 && req.Start > 0 {
		items = pageTicks(items, req.Start, 0)
	}
	return items, nil
}

// PutBars stores bars keyed by symbol, period and adjustment type.
func (s *SQLStore) PutBars(ctx context.Context, bars []domain.Bar) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if len(bars) == 0 {
		return nil
	}
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, bar := range bars {
			if err := bar.Symbol.Validate(); err != nil {
				return err
			}
			if bar.Period == domain.PeriodUnknown || bar.Time.IsZero() {
				return domain.ErrInvalidRequest
			}
			adjust := bar.AdjustType
			if adjust == "" {
				adjust = domain.AdjustNone
			}
			if _, err := tx.ExecContext(ctx, bind(s.dialect, `DELETE FROM kline_bars WHERE market = ? AND code = ? AND period = ? AND adjust_type = ? AND bar_time = ?`),
				bar.Symbol.Market, bar.Symbol.Code, bar.Period, adjust, bar.Time); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, bind(s.dialect, `
INSERT INTO kline_bars (
	market, code, period, adjust_type, bar_time, open, high, low, close, volume, amount, source, source_address, fetch_time, checksum
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
				bar.Symbol.Market, bar.Symbol.Code, bar.Period, adjust, bar.Time, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, bar.Amount, bar.Source, "", nil, ""); err != nil {
				return err
			}
		}
		return nil
	})
}

// QueryBars loads bars from the SQL store and filters them by the requested range.
func (s *SQLStore) QueryBars(ctx context.Context, req domain.BarQuery) ([]domain.Bar, error) {
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
	query := `
SELECT market, code, period, adjust_type, bar_time, open, high, low, close, volume, amount, source
FROM kline_bars
WHERE market = ? AND code = ? AND period = ? AND adjust_type = ?`
	args := []any{req.Symbol.Market, req.Symbol.Code, req.Period, adjust}
	if !req.Start.IsZero() {
		query += ` AND bar_time >= ?`
		args = append(args, req.Start)
	}
	if !req.End.IsZero() {
		query += ` AND bar_time <= ?`
		args = append(args, req.End)
	}
	query += ` ORDER BY bar_time ASC`
	rows, err := s.db.QueryContext(ctx, bind(s.dialect, query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanBars(rows)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, domain.ErrNoData
	}
	return items, nil
}

// Enqueue inserts a backfill task or returns the existing one for the same work key.
func (s *SQLStore) Enqueue(ctx context.Context, task domain.BackfillTask) (domain.BackfillTask, bool, error) {
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
	keyArgs := []any{task.Dataset, task.Symbol.Market, task.Symbol.Code, task.StartDate, task.EndDate, task.Period, task.AdjustType}
	row := s.db.QueryRowContext(ctx, bind(s.dialect, `
SELECT task_id, dataset, market, code, start_date, end_date, period, adjust_type, priority, status, retry_count, next_retry_time, error_message, created_at, updated_at
FROM backfill_tasks
WHERE dataset = ? AND market = ? AND code = ? AND start_date = ? AND end_date = ? AND period = ? AND adjust_type = ?`), keyArgs...)
	var existing domain.BackfillTask
	if err := scanTask(row, &existing); err == nil {
		return existing, false, nil
	} else if !errorsIsNoRows(err) {
		return domain.BackfillTask{}, false, err
	}
	now := time.Now()
	if task.TaskID == "" {
		task.TaskID = stableTaskID(taskKey(task))
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	task.UpdatedAt = now
	if err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, bind(s.dialect, `
INSERT INTO backfill_tasks (
	task_id, dataset, market, code, start_date, end_date, period, adjust_type, priority, status, retry_count, next_retry_time, error_message, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			task.TaskID, task.Dataset, task.Symbol.Market, task.Symbol.Code, task.StartDate, task.EndDate, task.Period, task.AdjustType, task.Priority, task.Status, task.RetryCount, nullableTime(task.NextRetryTime), task.ErrorMessage, task.CreatedAt, task.UpdatedAt)
		return err
	}); err != nil {
		row := s.db.QueryRowContext(ctx, bind(s.dialect, `
SELECT task_id, dataset, market, code, start_date, end_date, period, adjust_type, priority, status, retry_count, next_retry_time, error_message, created_at, updated_at
FROM backfill_tasks
WHERE dataset = ? AND market = ? AND code = ? AND start_date = ? AND end_date = ? AND period = ? AND adjust_type = ?`), keyArgs...)
		if scanErr := scanTask(row, &existing); scanErr == nil {
			return existing, false, nil
		}
		return domain.BackfillTask{}, false, err
	}
	return task, true, nil
}

// Next claims the highest-priority pending or retrying task.
func (s *SQLStore) Next(ctx context.Context) (domain.BackfillTask, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return domain.BackfillTask{}, false, err
	}
	row := s.db.QueryRowContext(ctx, bind(s.dialect, `
SELECT task_id, dataset, market, code, start_date, end_date, period, adjust_type, priority, status, retry_count, next_retry_time, error_message, created_at, updated_at
FROM backfill_tasks
WHERE status = ? OR status = ?
ORDER BY priority DESC, created_at ASC, task_id ASC`), domain.BackfillPending, domain.BackfillRetrying)
	var task domain.BackfillTask
	if err := scanTask(row, &task); err != nil {
		if errorsIsNoRows(err) {
			return domain.BackfillTask{}, false, nil
		}
		return domain.BackfillTask{}, false, err
	}
	task.Status = domain.BackfillRunning
	task.UpdatedAt = time.Now()
	if err := s.Update(ctx, task); err != nil {
		return domain.BackfillTask{}, false, err
	}
	return task, true, nil
}

// Update stores the provided task by task ID.
func (s *SQLStore) Update(ctx context.Context, task domain.BackfillTask) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if task.TaskID == "" {
		return domain.ErrInvalidRequest
	}
	if task.AdjustType == "" {
		task.AdjustType = domain.AdjustNone
	}
	task.UpdatedAt = time.Now()
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, bind(s.dialect, `
UPDATE backfill_tasks
SET dataset = ?, market = ?, code = ?, start_date = ?, end_date = ?, period = ?, adjust_type = ?, priority = ?, status = ?, retry_count = ?, next_retry_time = ?, error_message = ?, created_at = ?, updated_at = ?
WHERE task_id = ?`),
			task.Dataset, task.Symbol.Market, task.Symbol.Code, task.StartDate, task.EndDate, task.Period, task.AdjustType, task.Priority, task.Status, task.RetryCount, nullableTime(task.NextRetryTime), task.ErrorMessage, task.CreatedAt, task.UpdatedAt, task.TaskID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err == nil && n == 0 {
			return domain.ErrNoData
		}
		return nil
	})
}

// List returns tasks filtered by dataset, status or symbol.
func (s *SQLStore) List(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillTask, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	var (
		clauses []string
		args    []any
	)
	if filter.Dataset != "" {
		clauses = append(clauses, "dataset = ?")
		args = append(args, filter.Dataset)
	}
	if filter.Status != "" {
		clauses = append(clauses, "status = ?")
		args = append(args, filter.Status)
	}
	if filter.Symbol.Code != "" {
		clauses = append(clauses, "market = ? AND code = ?")
		args = append(args, filter.Symbol.Market, filter.Symbol.Code)
	}
	query := `
SELECT task_id, dataset, market, code, start_date, end_date, period, adjust_type, priority, status, retry_count, next_retry_time, error_message, created_at, updated_at
FROM backfill_tasks`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at ASC, task_id ASC"
	rows, err := s.db.QueryContext(ctx, bind(s.dialect, query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.BackfillTask, 0)
	for rows.Next() {
		var task domain.BackfillTask
		if err := scanTask(rows, &task); err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func bind(dialect Dialect, query string) string {
	if dialect != DialectPostgres {
		return query
	}
	var builder strings.Builder
	builder.Grow(len(query) + 8)
	arg := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			builder.WriteByte('$')
			builder.WriteString(strconv.Itoa(arg))
			arg++
			continue
		}
		builder.WriteByte(query[i])
	}
	return builder.String()
}

func withTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

type timeValue struct {
	time.Time
	Valid bool
}

func (t *timeValue) Scan(src any) error {
	switch value := src.(type) {
	case nil:
		t.Time = time.Time{}
		t.Valid = false
		return nil
	case time.Time:
		t.Time = value
		t.Valid = true
		return nil
	case string:
		return t.parse(value)
	case []byte:
		return t.parse(string(value))
	default:
		return fmt.Errorf("unsupported time value %T", src)
	}
}

func (t *timeValue) parse(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		t.Time = time.Time{}
		t.Valid = false
		return nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if parsed, err := time.ParseInLocation(layout, raw, time.Local); err == nil {
			t.Time = parsed
			t.Valid = true
			return nil
		}
	}
	return fmt.Errorf("parse time %q", raw)
}

func scanTicks(rows *sql.Rows) ([]domain.Tick, error) {
	items := make([]domain.Tick, 0)
	for rows.Next() {
		var (
			market    string
			code      string
			tradeDate timeValue
			tradeTime timeValue
			tick      domain.Tick
		)
		if err := rows.Scan(&market, &code, &tradeDate, &tradeTime, &tick.Price, &tick.Volume, &tick.Amount, &tick.Direction, &tick.Sequence, &tick.Source); err != nil {
			return nil, err
		}
		tick.Symbol = domain.Symbol{Market: domain.Market(market), Code: code}
		if tradeDate.Valid {
			tick.TradeDate = tradeDate.Time
		}
		if tradeTime.Valid {
			tick.TradeTime = tradeTime.Time
		}
		items = append(items, tick)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func scanBars(rows *sql.Rows) ([]domain.Bar, error) {
	items := make([]domain.Bar, 0)
	for rows.Next() {
		var (
			market  string
			code    string
			period  string
			adjust  string
			barTime timeValue
			bar     domain.Bar
		)
		if err := rows.Scan(&market, &code, &period, &adjust, &barTime, &bar.Open, &bar.High, &bar.Low, &bar.Close, &bar.Volume, &bar.Amount, &bar.Source); err != nil {
			return nil, err
		}
		bar.Symbol = domain.Symbol{Market: domain.Market(market), Code: code}
		bar.Period = domain.Period(period)
		bar.AdjustType = domain.AdjustType(adjust)
		if barTime.Valid {
			bar.Time = barTime.Time
		}
		items = append(items, bar)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func scanTask(scanner interface{ Scan(...any) error }, task *domain.BackfillTask) error {
	var (
		market  string
		code    string
		start   timeValue
		end     timeValue
		next    timeValue
		created timeValue
		updated timeValue
	)
	if err := scanner.Scan(&task.TaskID, &task.Dataset, &market, &code, &start, &end, &task.Period, &task.AdjustType, &task.Priority, &task.Status, &task.RetryCount, &next, &task.ErrorMessage, &created, &updated); err != nil {
		if errorsIsNoRows(err) {
			return sql.ErrNoRows
		}
		return err
	}
	task.Symbol = domain.Symbol{Market: domain.Market(market), Code: code}
	if start.Valid {
		task.StartDate = start.Time
	}
	if end.Valid {
		task.EndDate = end.Time
	}
	if next.Valid {
		task.NextRetryTime = next.Time
	}
	if created.Valid {
		task.CreatedAt = created.Time
	}
	if updated.Valid {
		task.UpdatedAt = updated.Time
	}
	return nil
}

func pageTicks(items []domain.Tick, start int, limit int) []domain.Tick {
	if start < 0 {
		start = 0
	}
	if start > len(items) {
		return []domain.Tick{}
	}
	end := len(items)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	out := append([]domain.Tick(nil), items[start:end]...)
	return out
}

func stableTaskID(key string) string {
	sum := sha1.Sum([]byte(key))
	return hex.EncodeToString(sum[:8])
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

func errorsIsNoRows(err error) bool {
	return err == sql.ErrNoRows
}
