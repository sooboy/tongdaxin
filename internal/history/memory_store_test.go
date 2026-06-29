package history

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestMemoryStoreLocalTickHit(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	ticks := []domain.Tick{
		{Symbol: symbol, TradeDate: date, TradeTime: date.Add(10 * time.Hour), Price: 10, Volume: 100, Sequence: 2},
		{Symbol: symbol, TradeDate: date, TradeTime: date.Add(9*time.Hour + 30*time.Minute), Price: 9.9, Volume: 200, Sequence: 1},
	}
	if err := store.PutTicks(context.Background(), ticks); err != nil {
		t.Fatalf("PutTicks error: %v", err)
	}

	got, err := store.QueryTicks(context.Background(), domain.HistoryTickQuery{Symbol: symbol, TradeDate: date, Limit: 1})
	if err != nil {
		t.Fatalf("QueryTicks error: %v", err)
	}
	if len(got) != 1 || got[0].Price != 9.9 {
		t.Fatalf("QueryTicks = %+v", got)
	}
}

func TestMemoryStorePutTicksOverwritesDuplicateTimeSequence(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	tradeTime := date.Add(10 * time.Hour)
	if err := store.PutTicks(context.Background(), []domain.Tick{{Symbol: symbol, TradeDate: date, TradeTime: tradeTime, Sequence: 1, Price: 10}}); err != nil {
		t.Fatalf("PutTicks first: %v", err)
	}
	if err := store.PutTicks(context.Background(), []domain.Tick{{Symbol: symbol, TradeDate: date, TradeTime: tradeTime, Sequence: 1, Price: 11}}); err != nil {
		t.Fatalf("PutTicks overwrite: %v", err)
	}

	got, err := store.QueryTicks(context.Background(), domain.HistoryTickQuery{Symbol: symbol, TradeDate: date})
	if err != nil {
		t.Fatalf("QueryTicks: %v", err)
	}
	if len(got) != 1 || got[0].Price != 11 {
		t.Fatalf("ticks = %+v", got)
	}
}

func TestMemoryStoreCoverageMissingAndPut(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	symbol := mustSymbol(t)
	date := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	req := domain.CoverageRequest{Dataset: domain.DatasetHistoryTick, Symbol: symbol, TradeDate: date}

	coverage, err := store.Coverage(context.Background(), req)
	if err != nil {
		t.Fatalf("Coverage error: %v", err)
	}
	if coverage.Status != domain.CoverageMissing {
		t.Fatalf("Coverage status = %q", coverage.Status)
	}

	coverage.Status = domain.CoverageCovered
	coverage.RowCount = 2
	if err := store.PutCoverage(context.Background(), coverage); err != nil {
		t.Fatalf("PutCoverage error: %v", err)
	}
	coverage, err = store.Coverage(context.Background(), req)
	if err != nil {
		t.Fatalf("Coverage after put error: %v", err)
	}
	if coverage.Status != domain.CoverageCovered || coverage.RowCount != 2 {
		t.Fatalf("Coverage after put = %+v", coverage)
	}
}

func TestMemoryStoreBackfillDeduplicatesConcurrentEnqueue(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
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

	const workers = 16
	var wg sync.WaitGroup
	created := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := store.Enqueue(context.Background(), task)
			if err != nil {
				t.Errorf("Enqueue error: %v", err)
				return
			}
			created <- ok
		}()
	}
	wg.Wait()
	close(created)

	createdCount := 0
	for ok := range created {
		if ok {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}

	items, err := store.List(context.Background(), domain.BackfillFilter{Dataset: domain.DatasetKLine})
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("List len = %d, want 1", len(items))
	}
}

func TestMemoryStoreQueryBarsFiltersRange(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	symbol := mustSymbol(t)
	base := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	bars := []domain.Bar{
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: base.AddDate(0, 0, -2), Close: 1},
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: base.AddDate(0, 0, -1), Close: 2},
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: base, Close: 3},
	}
	if err := store.PutBars(context.Background(), bars); err != nil {
		t.Fatalf("PutBars error: %v", err)
	}
	got, err := store.QueryBars(context.Background(), domain.BarQuery{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Start: base.AddDate(0, 0, -1), End: base})
	if err != nil {
		t.Fatalf("QueryBars error: %v", err)
	}
	if len(got) != 2 || got[0].Close != 2 || got[1].Close != 3 {
		t.Fatalf("QueryBars = %+v", got)
	}

	_, err = store.QueryBars(context.Background(), domain.BarQuery{Symbol: symbol, Period: domain.PeriodWeek})
	if !errors.Is(err, domain.ErrNoData) {
		t.Fatalf("QueryBars missing error = %v", err)
	}
}

func TestMemoryStorePutBarsOverwritesDuplicateTime(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	symbol := mustSymbol(t)
	base := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	if err := store.PutBars(context.Background(), []domain.Bar{
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: base, Close: 1},
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: base.AddDate(0, 0, 1), Close: 2},
	}); err != nil {
		t.Fatalf("PutBars first error: %v", err)
	}
	if err := store.PutBars(context.Background(), []domain.Bar{
		{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone, Time: base, Close: 10},
	}); err != nil {
		t.Fatalf("PutBars second error: %v", err)
	}

	got, err := store.QueryBars(context.Background(), domain.BarQuery{Symbol: symbol, Period: domain.PeriodDay, AdjustType: domain.AdjustNone})
	if err != nil {
		t.Fatalf("QueryBars error: %v", err)
	}
	if len(got) != 2 || got[0].Close != 10 || got[1].Close != 2 {
		t.Fatalf("QueryBars = %+v", got)
	}
}

func mustSymbol(t *testing.T) domain.Symbol {
	t.Helper()
	symbol, err := domain.NewSymbol(domain.MarketSH, "600000")
	if err != nil {
		t.Fatalf("NewSymbol: %v", err)
	}
	return symbol
}
