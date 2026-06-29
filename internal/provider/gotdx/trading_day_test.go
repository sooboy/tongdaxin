package gotdxadapter

import (
	"errors"
	"testing"
	"time"

	"github.com/bensema/gotdx/proto"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestTradingDayToDomainTradingDay(t *testing.T) {
	sourceTime := time.Date(2026, 6, 29, 10, 0, 0, 0, time.Local)
	info, err := tradingDayToDomain(&proto.MACServerInfoReply{
		Today:           "2026-06-29",
		LastTradingDay:  "2026-06-29",
		LastTradingDay2: "2026-06-26",
		Sessions1: []proto.MACTradingSession{{
			OpenMinutes:  570,
			CloseMinutes: 690,
			Open:         "9:30",
			Close:        "11:30",
		}},
	}, sourceTime)
	if err != nil {
		t.Fatalf("tradingDayToDomain: %v", err)
	}
	if !info.IsTodayTradingDay {
		t.Fatal("IsTodayTradingDay = false, want true")
	}
	if info.TodayString != "2026-06-29" || info.LatestTradingDayString != "2026-06-29" || info.PreviousTradingDayString != "2026-06-26" {
		t.Fatalf("dates = today %q latest %q previous %q", info.TodayString, info.LatestTradingDayString, info.PreviousTradingDayString)
	}
	if len(info.TradingSessions) != 1 || info.TradingSessions[0].Open != "9:30" || info.TradingSessions[0].Close != "11:30" {
		t.Fatalf("sessions = %#v", info.TradingSessions)
	}
	if !info.SourceTime.Equal(sourceTime) {
		t.Fatalf("source time = %v, want %v", info.SourceTime, sourceTime)
	}
}

func TestTradingDayToDomainNonTradingDay(t *testing.T) {
	info, err := tradingDayToDomain(&proto.MACServerInfoReply{
		Today:           "2026-06-28",
		LastTradingDay:  "2026-06-26",
		LastTradingDay2: "2026-06-25",
	}, time.Time{})
	if err != nil {
		t.Fatalf("tradingDayToDomain: %v", err)
	}
	if info.IsTodayTradingDay {
		t.Fatal("IsTodayTradingDay = true, want false")
	}
	if info.LatestTradingDayString != "2026-06-26" || info.PreviousTradingDayString != "2026-06-26" {
		t.Fatalf("dates = latest %q previous %q", info.LatestTradingDayString, info.PreviousTradingDayString)
	}
}

func TestTradingDayToDomainDoesNotUseSameDayAsPrevious(t *testing.T) {
	info, err := tradingDayToDomain(&proto.MACServerInfoReply{
		Today:           "2026-06-29",
		LastTradingDay:  "2026-06-29",
		LastTradingDay2: "2026-06-29",
		Sessions1: []proto.MACTradingSession{
			{OpenMinutes: 570, CloseMinutes: 690, Open: "9:30", Close: "11:30"},
			{OpenMinutes: 780, CloseMinutes: 900, Open: "13:00", Close: "15:00"},
			{OpenMinutes: 900, CloseMinutes: 900, Open: "15:00", Close: "15:00"},
			{OpenMinutes: 900, CloseMinutes: 900, Open: "15:00", Close: "15:00"},
		},
	}, time.Time{})
	if err != nil {
		t.Fatalf("tradingDayToDomain: %v", err)
	}
	if !info.PreviousTradingDay.IsZero() || info.PreviousTradingDayString != "" {
		t.Fatalf("previous = %q/%v, want empty until K-line enrichment", info.PreviousTradingDayString, info.PreviousTradingDay)
	}
	if len(info.TradingSessions) != 2 {
		t.Fatalf("sessions = %#v, want only two real trading ranges", info.TradingSessions)
	}
}

func TestPreviousTradingDayFromBars(t *testing.T) {
	latest := time.Date(2026, 6, 29, 0, 0, 0, 0, time.Local)
	previous, ok := previousTradingDayFromBars(latest, []domain.Bar{
		{Time: time.Date(2026, 6, 24, 0, 0, 0, 0, time.Local)},
		{Time: time.Date(2026, 6, 26, 0, 0, 0, 0, time.Local)},
		{Time: latest},
	})
	if !ok {
		t.Fatal("previousTradingDayFromBars ok = false, want true")
	}
	if got := previous.Format(macServerDateLayout); got != "2026-06-26" {
		t.Fatalf("previous = %s, want 2026-06-26", got)
	}
}

func TestTradingDayToDomainRejectsInvalidDate(t *testing.T) {
	_, err := tradingDayToDomain(&proto.MACServerInfoReply{
		Today:          "2026/06/29",
		LastTradingDay: "2026-06-29",
	}, time.Time{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, domain.ErrUpstreamUnavailable) {
		t.Fatalf("error = %v, want ErrUpstreamUnavailable", err)
	}
}
