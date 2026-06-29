package domain

import (
	"errors"
	"testing"
	"time"
)

func TestParsePeriod(t *testing.T) {
	t.Parallel()

	cases := map[string]Period{
		"1m":      Period1Min,
		"5min":    Period5Min,
		"15m":     Period15Min,
		"30min":   Period30Min,
		"60m":     Period1Hour,
		"daily":   PeriodDay,
		"weekly":  PeriodWeek,
		"monthly": PeriodMonth,
		"quarter": PeriodQuarter,
		"yearly":  PeriodYear,
	}
	for input, want := range cases {
		got, err := ParsePeriod(input)
		if err != nil {
			t.Fatalf("ParsePeriod(%q) error: %v", input, err)
		}
		if got != want {
			t.Fatalf("ParsePeriod(%q) = %q, want %q", input, got, want)
		}
	}

	if _, err := ParsePeriod("bad"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ParsePeriod(bad) error = %v, want ErrInvalidRequest", err)
	}
}

func TestParseAdjustType(t *testing.T) {
	t.Parallel()

	cases := map[string]AdjustType{
		"":     AdjustNone,
		"none": AdjustNone,
		"前复权":  AdjustQFQ,
		"qfq":  AdjustQFQ,
		"后复权":  AdjustHFQ,
		"back": AdjustHFQ,
	}
	for input, want := range cases {
		got, err := ParseAdjustType(input)
		if err != nil {
			t.Fatalf("ParseAdjustType(%q) error: %v", input, err)
		}
		if got != want {
			t.Fatalf("ParseAdjustType(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestSymbolKeyAndNormalizeDate(t *testing.T) {
	t.Parallel()

	symbol, err := NewSymbol(" sh ", " 600000 ")
	if err != nil {
		t.Fatalf("NewSymbol error: %v", err)
	}
	if symbol.Key() != "SH:600000" {
		t.Fatalf("Symbol key = %q", symbol.Key())
	}

	date := NormalizeDate(time.Date(2026, 6, 25, 14, 30, 1, 0, time.Local))
	if date.Hour() != 0 || date.Minute() != 0 || date.Format("20060102") != "20260625" {
		t.Fatalf("NormalizeDate returned %s", date)
	}
}
