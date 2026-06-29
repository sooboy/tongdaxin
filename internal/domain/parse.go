package domain

import (
	"strings"
	"time"
)

// ParseMarket normalizes user or provider market strings into service-owned values.
func ParseMarket(value string) (Market, error) {
	market := NormalizeMarket(Market(value))
	switch market {
	case MarketSH, MarketSZ, MarketBJ, MarketHK, MarketUS:
		return market, nil
	default:
		return MarketUnknown, ErrInvalidRequest
	}
}

// ParsePeriod normalizes API period strings.
func ParsePeriod(value string) (Period, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1m", "1min", "minute", "min1":
		return Period1Min, nil
	case "5m", "5min", "min5":
		return Period5Min, nil
	case "15m", "15min", "min15":
		return Period15Min, nil
	case "30m", "30min", "min30":
		return Period30Min, nil
	case "1h", "60m", "60min", "hour":
		return Period1Hour, nil
	case "day", "daily", "d", "1d":
		return PeriodDay, nil
	case "week", "weekly", "w":
		return PeriodWeek, nil
	case "month", "monthly", "m":
		return PeriodMonth, nil
	case "quarter", "quarterly", "q":
		return PeriodQuarter, nil
	case "year", "yearly", "y":
		return PeriodYear, nil
	default:
		return PeriodUnknown, ErrInvalidRequest
	}
}

// ParseAdjustType normalizes adjustment mode strings.
func ParseAdjustType(value string) (AdjustType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none", "raw", "不复权":
		return AdjustNone, nil
	case "qfq", "front", "前复权":
		return AdjustQFQ, nil
	case "hfq", "back", "后复权":
		return AdjustHFQ, nil
	default:
		return AdjustNone, ErrInvalidRequest
	}
}

// NormalizeDate returns local midnight for date-only keys.
func NormalizeDate(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}
