package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

type stubService struct {
	quotesReq     []domain.Symbol
	orderBookReq  []domain.Symbol
	tickSymbol    domain.Symbol
	tickReq       domain.TickRequest
	historySymbol domain.Symbol
	historyReq    domain.HistoryTickRequest
	klineSymbol   domain.Symbol
	klineReq      domain.KLineRequest
	adjustSymbol  domain.Symbol
	adjustReq     domain.AdjustedKLineRequest
	xdxrSymbol    domain.Symbol
	securityReq   domain.SecurityQuery
	financeSymbol domain.Symbol
	quotes        []domain.Quote
	orderBooks    []domain.OrderBook
	ticks         []domain.Tick
	historyTicks  []domain.Tick
	bars          []domain.Bar
	adjustedBars  []domain.Bar
	xdxr          []domain.XDXREvent
	securities    []domain.SecurityInfo
	finance       *domain.FinanceInfo
	tradingDay    *domain.TradingDayInfo
	err           error
}

func (s *stubService) GetQuotes(_ context.Context, symbols []domain.Symbol) ([]domain.Quote, error) {
	s.quotesReq = append([]domain.Symbol(nil), symbols...)
	return s.quotes, s.err
}
func (s *stubService) GetOrderBook(_ context.Context, symbols []domain.Symbol) ([]domain.OrderBook, error) {
	s.orderBookReq = append([]domain.Symbol(nil), symbols...)
	return s.orderBooks, s.err
}
func (s *stubService) GetTicks(_ context.Context, symbol domain.Symbol, req domain.TickRequest) ([]domain.Tick, error) {
	s.tickSymbol = symbol
	s.tickReq = req
	return s.ticks, s.err
}
func (s *stubService) GetHistoryTicks(_ context.Context, symbol domain.Symbol, req domain.HistoryTickRequest) ([]domain.Tick, error) {
	s.historySymbol = symbol
	s.historyReq = req
	return s.historyTicks, s.err
}
func (s *stubService) GetKLine(_ context.Context, symbol domain.Symbol, req domain.KLineRequest) ([]domain.Bar, error) {
	s.klineSymbol = symbol
	s.klineReq = req
	return s.bars, s.err
}
func (s *stubService) GetAdjustedKLine(_ context.Context, symbol domain.Symbol, req domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	s.adjustSymbol = symbol
	s.adjustReq = req
	return s.adjustedBars, s.err
}
func (s *stubService) GetXDXR(_ context.Context, symbol domain.Symbol) ([]domain.XDXREvent, error) {
	s.xdxrSymbol = symbol
	return s.xdxr, s.err
}
func (s *stubService) GetSecurityInfo(_ context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	s.securityReq = req
	return s.securities, s.err
}
func (s *stubService) GetFinance(_ context.Context, symbol domain.Symbol) (*domain.FinanceInfo, error) {
	s.financeSymbol = symbol
	return s.finance, s.err
}
func (s *stubService) GetTradingDay(_ context.Context) (*domain.TradingDayInfo, error) {
	return s.tradingDay, s.err
}

func TestTradingDayEndpoint(t *testing.T) {
	t.Parallel()

	service := &stubService{tradingDay: &domain.TradingDayInfo{
		TodayString:              "2026-06-29",
		IsTodayTradingDay:        true,
		LatestTradingDayString:   "2026-06-29",
		PreviousTradingDayString: "2026-06-26",
		TradingSessions:          []domain.TradingSession{{OpenMinutes: 570, CloseMinutes: 690, Open: "9:30", Close: "11:30"}},
		AlternateTradingSessions: []domain.TradingSession{{OpenMinutes: 780, CloseMinutes: 900, Open: "13:00", Close: "15:00"}},
	}}
	body, status := requestJSON(t, New(service), "/api/v1/trading-day")
	if status != http.StatusOK {
		t.Fatalf("status = %d body=%s", status, body)
	}
	data := body["data"].(map[string]any)
	if data["today"] != "2026-06-29" || data["is_today_trading_day"] != true || data["previous_trading_day"] != "2026-06-26" {
		t.Fatalf("trading day response = %+v", data)
	}
	if sessions, ok := data["trading_sessions"].([]any); !ok || len(sessions) != 1 {
		t.Fatalf("sessions = %+v", data["trading_sessions"])
	}
}

func TestSecuritiesParsesSymbolsQuery(t *testing.T) {
	t.Parallel()

	service := &stubService{securities: []domain.SecurityInfo{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}}}
	_, status := requestJSON(t, New(service), "/api/v1/securities?symbol=SH:600000&count=10")
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if len(service.securityReq.Symbols) != 1 || service.securityReq.Symbols[0].Key() != "SH:600000" || service.securityReq.Count != 10 {
		t.Fatalf("security req = %+v", service.securityReq)
	}
}

func TestQuotesParsesBatchSymbols(t *testing.T) {
	t.Parallel()

	service := &stubService{quotes: []domain.Quote{
		{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5},
		{Symbol: domain.Symbol{Market: domain.MarketSZ, Code: "000001"}, LastPrice: 11.5},
	}}
	body, status := requestJSON(t, New(service), "/api/v1/quotes?symbols=SH:600000,SZ:000001")
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if len(service.quotesReq) != 2 || service.quotesReq[0].Key() != "SH:600000" || service.quotesReq[1].Key() != "SZ:000001" {
		t.Fatalf("quotesReq = %+v", service.quotesReq)
	}
	data := body["data"].([]any)
	if len(data) != 2 || data[0].(map[string]any)["last_price"] != 10.5 || data[1].(map[string]any)["last_price"] != 11.5 {
		t.Fatalf("body = %#v", body)
	}
}

func TestQuotesAcceptsJSONPostBody(t *testing.T) {
	t.Parallel()

	service := &stubService{quotes: []domain.Quote{
		{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5},
		{Symbol: domain.Symbol{Market: domain.MarketSZ, Code: "000001"}, LastPrice: 11.5},
	}}
	body, status := requestJSONWithMethod(t, http.MethodPost, "/api/v1/quotes", `{"symbols":["SH:600000","SZ:000001"]}`, New(service))
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if len(service.quotesReq) != 2 || service.quotesReq[0].Key() != "SH:600000" || service.quotesReq[1].Key() != "SZ:000001" {
		t.Fatalf("quotesReq = %+v", service.quotesReq)
	}
	data := body["data"].([]any)
	if len(data) != 2 || data[0].(map[string]any)["last_price"] != 10.5 || data[1].(map[string]any)["last_price"] != 11.5 {
		t.Fatalf("body = %#v", body)
	}
}

func TestOrderBookAcceptsJSONPostBody(t *testing.T) {
	t.Parallel()

	service := &stubService{orderBooks: []domain.OrderBook{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}}}
	body, status := requestJSONWithMethod(t, http.MethodPost, "/api/v1/orderbook", `{"market":"SH","code":"600000"}`, New(service))
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if len(service.orderBookReq) != 1 || service.orderBookReq[0].Key() != "SH:600000" {
		t.Fatalf("orderBookReq = %+v", service.orderBookReq)
	}
	data := body["data"].([]any)
	symbol := data[0].(map[string]any)["symbol"].(map[string]any)
	if symbol["market"] != "SH" || symbol["code"] != "600000" {
		t.Fatalf("body = %#v", body)
	}
}

func TestRoutesAcceptSymbolQuery(t *testing.T) {
	t.Parallel()

	barTime := time.Date(2026, 6, 25, 10, 0, 0, 0, time.Local)
	cases := []struct {
		name   string
		target string
		setup  func(*stubService)
		assert func(*testing.T, *stubService)
	}{
		{
			name:   "quotes",
			target: "/api/v1/quotes?symbol=SH:600000",
			setup: func(service *stubService) {
				service.quotes = []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.quotesReq[0], "SH:600000") },
		},
		{
			name:   "orderbook",
			target: "/api/v1/orderbook?symbol=SH:600000",
			setup: func(service *stubService) {
				service.orderBooks = []domain.OrderBook{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.orderBookReq[0], "SH:600000") },
		},
		{
			name:   "ticks",
			target: "/api/v1/ticks?symbol=SH:600000&count=10",
			setup: func(service *stubService) {
				service.ticks = []domain.Tick{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, TradeDate: domain.NormalizeDate(barTime), TradeTime: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.tickSymbol, "SH:600000") },
		},
		{
			name:   "history ticks",
			target: "/api/v1/history-ticks?symbol=SH:600000&date=2026-06-25",
			setup: func(service *stubService) {
				service.historyTicks = []domain.Tick{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, TradeDate: domain.NormalizeDate(barTime), TradeTime: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.historySymbol, "SH:600000") },
		},
		{
			name:   "kline",
			target: "/api/v1/kline?symbol=SH:600000&period=day",
			setup: func(service *stubService) {
				service.bars = []domain.Bar{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, Period: domain.PeriodDay, Time: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.klineSymbol, "SH:600000") },
		},
		{
			name:   "adjusted kline",
			target: "/api/v1/adjusted-kline?symbol=SH:600000&period=day&adjust=qfq",
			setup: func(service *stubService) {
				service.adjustedBars = []domain.Bar{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, Period: domain.PeriodDay, AdjustType: domain.AdjustQFQ, Time: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.adjustSymbol, "SH:600000") },
		},
		{
			name:   "xdxr",
			target: "/api/v1/xdxr?symbol=SH:600000",
			setup: func(service *stubService) {
				service.xdxr = []domain.XDXREvent{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, EventDate: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.xdxrSymbol, "SH:600000") },
		},
		{
			name:   "finance",
			target: "/api/v1/finance?symbol=SH:600000",
			setup: func(service *stubService) {
				service.finance = &domain.FinanceInfo{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.financeSymbol, "SH:600000") },
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			service := &stubService{}
			tc.setup(service)
			body, status := requestJSON(t, New(service), tc.target)
			if status != http.StatusOK {
				t.Fatalf("status = %d body = %#v", status, body)
			}
			tc.assert(t, service)
		})
	}
}

func TestRoutesUseSmokeDefaults(t *testing.T) {
	t.Parallel()

	barTime := time.Date(2026, 6, 25, 10, 0, 0, 0, time.Local)
	cases := []struct {
		name   string
		target string
		setup  func(*stubService)
		assert func(*testing.T, *stubService)
	}{
		{
			name:   "quotes",
			target: "/api/v1/quotes",
			setup: func(service *stubService) {
				service.quotes = []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.quotesReq[0], "SH:600000") },
		},
		{
			name:   "orderbook",
			target: "/api/v1/orderbook",
			setup: func(service *stubService) {
				service.orderBooks = []domain.OrderBook{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.orderBookReq[0], "SH:600000") },
		},
		{
			name:   "ticks",
			target: "/api/v1/ticks",
			setup: func(service *stubService) {
				service.ticks = []domain.Tick{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, TradeDate: domain.NormalizeDate(barTime), TradeTime: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.tickSymbol, "SH:600000") },
		},
		{
			name:   "history ticks",
			target: "/api/v1/history-ticks",
			setup: func(service *stubService) {
				service.historyTicks = []domain.Tick{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, TradeDate: domain.NormalizeDate(barTime), TradeTime: barTime}}
			},
			assert: func(t *testing.T, service *stubService) {
				assertSymbolKey(t, service.historySymbol, "SH:600000")
				if service.historyReq.TradeDate.IsZero() {
					t.Fatal("default history date is zero")
				}
			},
		},
		{
			name:   "kline",
			target: "/api/v1/kline",
			setup: func(service *stubService) {
				service.bars = []domain.Bar{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, Period: domain.PeriodDay, Time: barTime}}
			},
			assert: func(t *testing.T, service *stubService) {
				assertSymbolKey(t, service.klineSymbol, "SH:600000")
				if service.klineReq.Period != domain.PeriodDay {
					t.Fatalf("period = %q", service.klineReq.Period)
				}
			},
		},
		{
			name:   "adjusted kline",
			target: "/api/v1/adjusted-kline",
			setup: func(service *stubService) {
				service.bars = []domain.Bar{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, Period: domain.PeriodDay, Time: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.klineSymbol, "SH:600000") },
		},
		{
			name:   "xdxr",
			target: "/api/v1/xdxr",
			setup: func(service *stubService) {
				service.xdxr = []domain.XDXREvent{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, EventDate: barTime}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.xdxrSymbol, "SH:600000") },
		},
		{
			name:   "securities",
			target: "/api/v1/securities",
			setup: func(service *stubService) {
				service.securities = []domain.SecurityInfo{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}}
			},
			assert: func(t *testing.T, service *stubService) {
				if len(service.securityReq.Markets) != 0 {
					t.Fatalf("markets = %+v", service.securityReq.Markets)
				}
			},
		},
		{
			name:   "finance",
			target: "/api/v1/finance",
			setup: func(service *stubService) {
				service.finance = &domain.FinanceInfo{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}}
			},
			assert: func(t *testing.T, service *stubService) { assertSymbolKey(t, service.financeSymbol, "SH:600000") },
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			service := &stubService{}
			tc.setup(service)
			body, status := requestJSON(t, New(service), tc.target)
			if status != http.StatusOK {
				t.Fatalf("status = %d body = %#v", status, body)
			}
			tc.assert(t, service)
		})
	}
}

func assertSymbolKey(t *testing.T, symbol domain.Symbol, want string) {
	t.Helper()
	if symbol.Key() != want {
		t.Fatalf("symbol = %+v, want %s", symbol, want)
	}
}
func TestHistoryTicksParsesDateAndFlags(t *testing.T) {
	t.Parallel()

	service := &stubService{historyTicks: []domain.Tick{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, TradeDate: time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local), TradeTime: time.Date(2026, 6, 25, 10, 0, 0, 0, time.Local), Price: 9.9}}}
	body, status := requestJSON(t, New(service), "/api/v1/history-ticks?market=SH&code=600000&date=2026-06-25&start=2&count=3&full=true&with_transaction_flag=true&force_refresh=true")
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if service.historySymbol.Key() != "SH:600000" {
		t.Fatalf("symbol = %+v", service.historySymbol)
	}
	if service.historyReq.TradeDate.Format(dateLayout) != "2026-06-25" || service.historyReq.Start != 2 || service.historyReq.Count != 3 || !service.historyReq.Full || !service.historyReq.WithTransactionFlag || !service.historyReq.ForceRefresh {
		t.Fatalf("historyReq = %+v", service.historyReq)
	}
	data := body["data"].([]any)
	if data[0].(map[string]any)["trade_date"] != "2026-06-25" {
		t.Fatalf("body = %#v", body)
	}
}

func TestKLineUsesAdjustedPath(t *testing.T) {
	t.Parallel()

	barTime := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	service := &stubService{adjustedBars: []domain.Bar{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, Period: domain.PeriodDay, AdjustType: domain.AdjustQFQ, Time: barTime, Close: 12.3}}}
	body, status := requestJSON(t, New(service), "/api/v1/kline?market=SH&code=600000&period=day&adjust=qfq&count=10&times=1&start_date=2026-06-01&end_date=2026-06-25")
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if service.adjustSymbol.Key() != "SH:600000" || service.adjustReq.AdjustType != domain.AdjustQFQ || service.adjustReq.Period != domain.PeriodDay || service.adjustReq.Count != 10 || service.adjustReq.Times != 1 {
		t.Fatalf("adjust call = symbol:%+v req:%+v", service.adjustSymbol, service.adjustReq)
	}
	data := body["data"].([]any)
	if data[0].(map[string]any)["adjust_type"] != "qfq" {
		t.Fatalf("body = %#v", body)
	}
}

func TestAdjustedKLineRoute(t *testing.T) {
	t.Parallel()

	barTime := time.Date(2026, 6, 25, 0, 0, 0, 0, time.Local)
	service := &stubService{adjustedBars: []domain.Bar{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, Period: domain.PeriodDay, AdjustType: domain.AdjustHFQ, Time: barTime, Close: 11.8}}}
	body, status := requestJSON(t, New(service), "/api/v1/adjusted-kline?market=SH&code=600000&period=day&adjust=hfq&count=8&times=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if service.adjustSymbol.Key() != "SH:600000" || service.adjustReq.AdjustType != domain.AdjustHFQ || service.adjustReq.Period != domain.PeriodDay || service.adjustReq.Count != 8 || service.adjustReq.Times != 2 {
		t.Fatalf("adjust call = symbol:%+v req:%+v", service.adjustSymbol, service.adjustReq)
	}
	data := body["data"].([]any)
	if data[0].(map[string]any)["adjust_type"] != "hfq" {
		t.Fatalf("body = %#v", body)
	}
}
func TestInvalidRequestReturns400(t *testing.T) {
	t.Parallel()

	body, status := requestJSON(t, New(&stubService{}), "/api/v1/history-ticks?symbol=bad&date=bad")
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d body = %#v", status, body)
	}
	if body["error"].(map[string]any)["code"] != "invalid_request" {
		t.Fatalf("body = %#v", body)
	}
}

func TestServiceErrorsMapToHTTPStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "no data", err: domain.ErrNoData, status: http.StatusNotFound, code: "no_data"},
		{name: "unsupported", err: domain.ErrUnsupportedCapability, status: http.StatusNotImplemented, code: "unsupported_capability"},
		{name: "upstream", err: domain.ErrUpstreamUnavailable, status: http.StatusServiceUnavailable, code: "upstream_unavailable"},
		{name: "rate limited", err: domain.ErrRateLimited, status: http.StatusTooManyRequests, code: "rate_limited"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body, status := requestJSON(t, New(&stubService{err: tc.err}), "/api/v1/quotes?market=SH&code=600000")
			if status != tc.status {
				t.Fatalf("status = %d body = %#v", status, body)
			}
			if body["error"].(map[string]any)["code"] != tc.code {
				t.Fatalf("body = %#v", body)
			}
		})
	}
}

func TestMetricsEndpointRecordsRouteStatus(t *testing.T) {
	t.Parallel()

	service := &stubService{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}}
	handler := New(service)

	if _, status := requestJSON(t, handler, "/api/v1/quotes?market=SH&code=600000"); status != http.StatusOK {
		t.Fatalf("quotes status = %d", status)
	}
	if _, status := requestJSON(t, handler, "/api/v1/history-ticks?symbol=bad&date=bad"); status != http.StatusBadRequest {
		t.Fatalf("history status = %d", status)
	}
	body, status := requestJSON(t, handler, "/api/v1/metrics")
	if status != http.StatusOK {
		t.Fatalf("metrics status = %d body = %#v", status, body)
	}

	data := body["data"].(map[string]any)
	if data["requests_total"] != float64(2) || data["errors_total"] != float64(1) {
		t.Fatalf("metrics = %#v", data)
	}
	routes := data["routes"].(map[string]any)
	quotes := routes["quotes"].(map[string]any)
	if quotes["requests"] != float64(1) || quotes["status_2xx"] != float64(1) {
		t.Fatalf("quote metrics = %#v", quotes)
	}
	history := routes["history_ticks"].(map[string]any)
	if history["errors"] != float64(1) || history["status_4xx"] != float64(1) {
		t.Fatalf("history metrics = %#v", history)
	}
}

func TestRateLimiterReturns429AndMetrics(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	handler := NewWithOptions(
		&stubService{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}},
		Options{Metrics: metrics, RateLimiter: NewRateLimiter(RateLimitConfig{RequestsPerSecond: 1, Burst: 1})},
	)

	if _, status := requestJSON(t, handler, "/api/v1/quotes?market=SH&code=600000"); status != http.StatusOK {
		t.Fatalf("first status = %d", status)
	}
	body, status := requestJSON(t, handler, "/api/v1/quotes?market=SH&code=600000")
	if status != http.StatusTooManyRequests {
		t.Fatalf("second status = %d body = %#v", status, body)
	}
	if body["error"].(map[string]any)["code"] != "rate_limited" {
		t.Fatalf("body = %#v", body)
	}

	snapshot := metrics.Snapshot()
	quotes := snapshot.Routes["quotes"]
	if snapshot.RequestsTotal != 2 || snapshot.ErrorsTotal != 1 || quotes.Status2xx != 1 || quotes.Status4xx != 1 || quotes.Errors != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestRequestLoggerRecordsRouteStatusAndBytes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	handler := NewWithOptions(
		&stubService{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}},
		Options{Logger: logger},
	)
	if _, status := requestJSON(t, handler, "/api/v1/quotes?market=SH&code=600000"); status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	line := buf.String()
	for _, want := range []string{"route=quotes", "method=GET", "path=/api/v1/quotes", "status=200", "bytes="} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line %q missing %q", line, want)
		}
	}
}
func requestJSON(t *testing.T, handler http.Handler, target string) (map[string]any, int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v body=%q", err, rec.Body.String())
	}
	return body, rec.Code
}

func requestJSONWithMethod(t *testing.T, method, target, body string, handler http.Handler) (map[string]any, int) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var decoded map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode response: %v body=%q", err, rec.Body.String())
	}
	return decoded, rec.Code
}
