package gotdxadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/bensema/gotdx/proto"

	"github.com/sooboy/tongdaxin/internal/domain"
	"github.com/sooboy/tongdaxin/internal/source"
)

const macServerDateLayout = "2006-01-02"

func (p *Provider) GetTradingDay(ctx context.Context) (*domain.TradingDayInfo, error) {
	lease, err := p.acquireMAC(ctx)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	reply, err := lease.Client.MACServerInfo()
	if err != nil {
		_ = lease.ReportFailure(err)
		return nil, err
	}
	_ = lease.ReportSuccess()
	info, err := tradingDayToDomain(reply, p.now())
	if err != nil {
		return nil, err
	}
	p.enrichPreviousTradingDay(ctx, info)
	return info, nil
}

func (p *Provider) acquireMAC(ctx context.Context) (*source.Lease[macClient], error) {
	if p.macPool == nil {
		return nil, domain.ErrUpstreamUnavailable
	}
	return p.macPool.Acquire(ctx)
}

func tradingDayToDomain(reply *proto.MACServerInfoReply, sourceTime time.Time) (*domain.TradingDayInfo, error) {
	if reply == nil {
		return nil, fmt.Errorf("%w: empty MAC server info", domain.ErrUpstreamUnavailable)
	}
	today, err := parseMACServerDateString(reply.Today, "today")
	if err != nil {
		return nil, err
	}
	latest, err := parseMACServerDateString(reply.LastTradingDay, "last_trading_day")
	if err != nil {
		return nil, err
	}
	secondLatest, err := parseOptionalMACServerDateString(reply.LastTradingDay2, "last_trading_day_2")
	if err != nil {
		return nil, err
	}
	isTodayTradingDay := sameDate(today, latest)
	var previous time.Time
	var previousString string
	if isTodayTradingDay && !secondLatest.IsZero() && secondLatest.Before(today) {
		previous = secondLatest
		previousString = reply.LastTradingDay2
	} else if !isTodayTradingDay {
		previous = latest
		previousString = reply.LastTradingDay
	}
	return &domain.TradingDayInfo{
		Today:                    today,
		TodayString:              reply.Today,
		IsTodayTradingDay:        isTodayTradingDay,
		LatestTradingDay:         latest,
		LatestTradingDayString:   reply.LastTradingDay,
		PreviousTradingDay:       previous,
		PreviousTradingDayString: previousString,
		TradingSessions:          mapMACTradingSessions(reply.Sessions1),
		AlternateTradingSessions: mapMACTradingSessions(reply.Sessions2),
		SourceTime:               sourceTime,
	}, nil
}

func (p *Provider) enrichPreviousTradingDay(ctx context.Context, info *domain.TradingDayInfo) {
	if info == nil || !info.IsTodayTradingDay || previousTradingDayBefore(info.PreviousTradingDay, info.Today) {
		return
	}
	previous, ok := p.inferPreviousTradingDayFromKLine(ctx, info.Today)
	if !ok {
		return
	}
	info.PreviousTradingDay = previous
	info.PreviousTradingDayString = previous.Format(macServerDateLayout)
}

func (p *Provider) inferPreviousTradingDayFromKLine(ctx context.Context, latest time.Time) (time.Time, bool) {
	for _, symbol := range tradingDayCalendarSymbols() {
		bars, err := p.GetKLine(ctx, symbol, domain.KLineRequest{Period: domain.PeriodDay, Count: 30})
		if err != nil {
			continue
		}
		if previous, ok := previousTradingDayFromBars(latest, bars); ok {
			return previous, true
		}
	}
	return time.Time{}, false
}

func tradingDayCalendarSymbols() []domain.Symbol {
	return []domain.Symbol{
		{Market: domain.MarketSH, Code: "600000"},
		{Market: domain.MarketSZ, Code: "000001"},
		{Market: domain.MarketSH, Code: "600519"},
	}
}

func previousTradingDayFromBars(latest time.Time, bars []domain.Bar) (time.Time, bool) {
	var previous time.Time
	for _, bar := range bars {
		day := domain.NormalizeDate(bar.Time)
		if day.IsZero() || !day.Before(domain.NormalizeDate(latest)) {
			continue
		}
		if previous.IsZero() || day.After(previous) {
			previous = day
		}
	}
	if previous.IsZero() {
		return time.Time{}, false
	}
	return previous, true
}

func previousTradingDayBefore(previous time.Time, today time.Time) bool {
	return !previous.IsZero() && domain.NormalizeDate(previous).Before(domain.NormalizeDate(today))
}

func mapMACTradingSessions(items []proto.MACTradingSession) []domain.TradingSession {
	out := make([]domain.TradingSession, 0, len(items))
	seen := make(map[[2]uint16]struct{}, len(items))
	for _, item := range items {
		if item.CloseMinutes <= item.OpenMinutes {
			continue
		}
		key := [2]uint16{item.OpenMinutes, item.CloseMinutes}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, domain.TradingSession{OpenMinutes: item.OpenMinutes, CloseMinutes: item.CloseMinutes, Open: item.Open, Close: item.Close})
	}
	return out
}

func parseMACServerDateString(raw string, field string) (time.Time, error) {
	parsed, err := parseOptionalMACServerDateString(raw, field)
	if err != nil {
		return time.Time{}, err
	}
	if parsed.IsZero() {
		return time.Time{}, fmt.Errorf("%w: empty MAC server %s", domain.ErrUpstreamUnavailable, field)
	}
	return parsed, nil
}

func parseOptionalMACServerDateString(raw string, field string) (time.Time, error) {
	if raw == "" || raw == "0000-00-00" {
		return time.Time{}, nil
	}
	parsed, err := time.ParseInLocation(macServerDateLayout, raw, time.Local)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: parse MAC server %s %q: %v", domain.ErrUpstreamUnavailable, field, raw, err)
	}
	return parsed, nil
}

func sameDate(left time.Time, right time.Time) bool {
	ly, lm, ld := left.Date()
	ry, rm, rd := right.Date()
	return ly == ry && lm == rm && ld == rd
}
