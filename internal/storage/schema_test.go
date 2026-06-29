package storage

import (
	"strings"
	"testing"
)

func TestRequiredTablesMatchDevelopmentPlan(t *testing.T) {
	t.Parallel()

	got := RequiredTables()
	want := []string{
		"securities",
		"trading_days",
		"history_ticks",
		"kline_bars",
		"history_coverage",
		"xdxr_events",
		"finance_snapshots",
		"backfill_tasks",
	}
	if len(got) != len(want) {
		t.Fatalf("RequiredTables len = %d", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RequiredTables[%d] = %q want %q", i, got[i], want[i])
		}
	}
}

func TestMigrationsContainHistoryIndexesAndBackfillDedupe(t *testing.T) {
	t.Parallel()

	for _, dialect := range []Dialect{DialectPostgres, DialectMySQL, DialectSQLite} {
		dialect := dialect
		t.Run(string(dialect), func(t *testing.T) {
			t.Parallel()
			migrations, err := Migrations(dialect)
			if err != nil {
				t.Fatalf("Migrations: %v", err)
			}
			joined := joinStatements(migrations)
			for _, required := range []string{"history_ticks", "kline_bars", "history_coverage", "backfill_tasks", "source_address", "checksum"} {
				if !strings.Contains(joined, required) {
					t.Fatalf("%s migration missing %q", dialect, required)
				}
			}
			if !strings.Contains(joined, "history_ticks_symbol_time_idx") || !strings.Contains(joined, "kline_bars_symbol_range_idx") || !strings.Contains(joined, "backfill_tasks_pick_idx") {
				t.Fatalf("%s migration missing expected indexes", dialect)
			}
		})
	}
}

func TestMigrationsRejectUnknownDialect(t *testing.T) {
	t.Parallel()

	if _, err := Migrations(Dialect("unknown")); err == nil {
		t.Fatal("expected error for unknown dialect")
	}
}

func joinStatements(migrations []Migration) string {
	var builder strings.Builder
	for _, migration := range migrations {
		for _, statement := range migration.Statements {
			builder.WriteString(statement)
			builder.WriteByte('\n')
		}
	}
	return builder.String()
}
