package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestSQLStoreTicksBarsAndCoverage(t *testing.T) {
	t.Parallel()

	store := openSQLiteStore(t)
	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)

	coverage, err := store.Coverage(context.Background(), domain.CoverageRequest{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date})
	if err != nil {
		t.Fatalf("Coverage missing: %v", err)
	}
	if coverage.Status != domain.CoverageMissing {
		t.Fatalf("missing coverage = %+v", coverage)
	}

	ticks := []domain.Tick{
		{Symbol: symbol, TradeDate: date, TradeTime: date.Add(10 * time.Hour), Price: 10, Volume: 100, Sequence: 2, Source: "test"},
		{Symbol: symbol, TradeDate: date, TradeTime: date.Add(9*time.Hour + 30*time.Minute), Price: 9.9, Volume: 200, Sequence: 1, Source: "test"},
	}
	if err := store.PutTicks(context.Background(), ticks); err != nil {
		t.Fatalf("PutTicks: %v", err)
	}
	gotTicks, err := store.QueryTicks(context.Background(), domain.HistoryTickQuery{Symbol: symbol, TradeDate: date, Limit: 1})
	if err != nil {
		t.Fatalf("QueryTicks: %v", err)
	}
	if len(gotTicks) != 1 || gotTicks[0].Price != 9.9 || gotTicks[0].Sequence != 1 {
		t.Fatalf("QueryTicks = %+v", gotTicks)
	}

	coverage = domain.HistoryCoverage{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date, Status: domain.CoverageCovered, RowCount: 2, Checksum: "sum"}
	if err := store.PutCoverage(context.Background(), coverage); err != nil {
		t.Fatalf("PutCoverage: %v", err)
	}
	coverage, err = store.Coverage(context.Background(), domain.CoverageRequest{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date})
	if err != nil {
		t.Fatalf("Coverage after put: %v", err)
	}
	if coverage.Status != domain.CoverageCovered || coverage.RowCount != 2 || coverage.Checksum != "sum" {
		t.Fatalf("coverage = %+v", coverage)
	}

	bars := []domain.Bar{
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: date.AddDate(0, 0, -2), Close: 1, Source: "test"},
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: date.AddDate(0, 0, -1), Close: 2, Source: "test"},
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: date, Close: 3, Source: "test"},
	}
	if err := store.PutBars(context.Background(), bars); err != nil {
		t.Fatalf("PutBars: %v", err)
	}
	gotBars, err := store.QueryBars(context.Background(), domain.BarQuery{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Start: date.AddDate(0, 0, -1), End: date})
	if err != nil {
		t.Fatalf("QueryBars: %v", err)
	}
	if len(gotBars) != 2 || gotBars[0].Close != 2 || gotBars[1].Close != 3 {
		t.Fatalf("QueryBars = %+v", gotBars)
	}
	_, err = store.QueryBars(context.Background(), domain.BarQuery{Symbol: symbol, Period: domain.PeriodWeek})
	if !errors.Is(err, domain.ErrNoData) {
		t.Fatalf("missing QueryBars error = %v", err)
	}
}

func TestSQLStorePutOperationsAreIdempotent(t *testing.T) {
	t.Parallel()

	store := openSQLiteStore(t)
	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	tickTime := date.Add(10 * time.Hour)
	if err := store.PutTicks(context.Background(), []domain.Tick{{Symbol: symbol, TradeDate: date, TradeTime: tickTime, Sequence: 1, Price: 10}}); err != nil {
		t.Fatalf("PutTicks first: %v", err)
	}
	if err := store.PutTicks(context.Background(), []domain.Tick{{Symbol: symbol, TradeDate: date, TradeTime: tickTime, Sequence: 1, Price: 11}}); err != nil {
		t.Fatalf("PutTicks second: %v", err)
	}
	ticks, err := store.QueryTicks(context.Background(), domain.HistoryTickQuery{Symbol: symbol, TradeDate: date})
	if err != nil {
		t.Fatalf("QueryTicks: %v", err)
	}
	if len(ticks) != 1 || ticks[0].Price != 11 {
		t.Fatalf("ticks = %+v", ticks)
	}

	barTime := date.AddDate(0, 0, -1)
	if err := store.PutBars(context.Background(), []domain.Bar{{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: barTime, Close: 1}}); err != nil {
		t.Fatalf("PutBars first: %v", err)
	}
	if err := store.PutBars(context.Background(), []domain.Bar{{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: barTime, Close: 2}}); err != nil {
		t.Fatalf("PutBars second: %v", err)
	}
	bars, err := store.QueryBars(context.Background(), domain.BarQuery{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone})
	if err != nil {
		t.Fatalf("QueryBars: %v", err)
	}
	if len(bars) != 1 || bars[0].Close != 2 {
		t.Fatalf("bars = %+v", bars)
	}
}

func TestSQLStoreCoverageDoesNotDowngradeCovered(t *testing.T) {
	t.Parallel()

	store := openSQLiteStore(t)
	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	covered := domain.HistoryCoverage{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date, Status: domain.CoverageCovered, RowCount: 100, Checksum: "full"}
	if err := store.PutCoverage(context.Background(), covered); err != nil {
		t.Fatalf("PutCoverage covered: %v", err)
	}
	missing := domain.HistoryCoverage{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date, Status: domain.CoverageMissing, RowCount: 0, LastError: "temporary"}
	if err := store.PutCoverage(context.Background(), missing); err != nil {
		t.Fatalf("PutCoverage missing: %v", err)
	}
	got, err := store.Coverage(context.Background(), domain.CoverageRequest{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date})
	if err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	if got.Status != domain.CoverageCovered || got.RowCount != 100 || got.Checksum != "full" {
		t.Fatalf("coverage downgraded = %+v", got)
	}
}

func TestSQLStoreBackfillQueueLifecycle(t *testing.T) {
	t.Parallel()

	store := openSQLiteStore(t)
	symbol := mustSymbol(t)
	task := domain.BackfillTask{
		Dataset:    domain.DatasetKLine,
		Symbol:     symbol,
		StartDate:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.Local),
		EndDate:    time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local),
		Period:     domain.PeriodDay,
		AdjustType: domain.AdjustNone,
		Priority:   10,
	}

	created, ok, err := store.Enqueue(context.Background(), task)
	if err != nil {
		t.Fatalf("Enqueue create: %v", err)
	}
	if !ok || created.TaskID == "" || created.Status != domain.BackfillPending {
		t.Fatalf("created task = %+v ok=%v", created, ok)
	}
	existing, ok, err := store.Enqueue(context.Background(), task)
	if err != nil {
		t.Fatalf("Enqueue existing: %v", err)
	}
	if ok || existing.TaskID != created.TaskID {
		t.Fatalf("existing task = %+v ok=%v", existing, ok)
	}

	next, ok, err := store.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if !ok || next.TaskID != created.TaskID || next.Status != domain.BackfillRunning {
		t.Fatalf("next = %+v ok=%v", next, ok)
	}

	next.Status = domain.BackfillSuccess
	if err := store.Update(context.Background(), next); err != nil {
		t.Fatalf("Update: %v", err)
	}
	items, err := store.List(context.Background(), domain.BackfillFilter{Dataset: domain.DatasetKLine, Status: domain.BackfillSuccess, Symbol: symbol})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 || items[0].TaskID != created.TaskID {
		t.Fatalf("items = %+v", items)
	}
	_, ok, err = store.Next(context.Background())
	if err != nil {
		t.Fatalf("Next empty: %v", err)
	}
	if ok {
		t.Fatal("expected no pending task")
	}
}

func openSQLiteStore(t *testing.T) *SQLStore {
	t.Helper()
	dsn := "file:" + filepath.ToSlash(filepath.Join(t.TempDir(), "marketdata.sqlite")) + "?_pragma=foreign_keys(1)&_time_format=sqlite"
	store, err := Open(context.Background(), Config{Dialect: DialectSQLite, DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	return store
}

func mustSymbol(t *testing.T) domain.Symbol {
	t.Helper()
	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	return symbol
}
