package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

// MarketDataService is the service surface required by the HTTP API layer.
type MarketDataService interface {
	GetQuotes(ctx context.Context, symbols []domain.Symbol) ([]domain.Quote, error)
	GetOrderBook(ctx context.Context, symbols []domain.Symbol) ([]domain.OrderBook, error)
	GetTicks(ctx context.Context, symbol domain.Symbol, req domain.TickRequest) ([]domain.Tick, error)
	GetHistoryTicks(ctx context.Context, symbol domain.Symbol, req domain.HistoryTickRequest) ([]domain.Tick, error)
	GetKLine(ctx context.Context, symbol domain.Symbol, req domain.KLineRequest) ([]domain.Bar, error)
	GetAdjustedKLine(ctx context.Context, symbol domain.Symbol, req domain.AdjustedKLineRequest) ([]domain.Bar, error)
	GetXDXR(ctx context.Context, symbol domain.Symbol) ([]domain.XDXREvent, error)
	GetSecurityInfo(ctx context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error)
	GetFinance(ctx context.Context, symbol domain.Symbol) (*domain.FinanceInfo, error)
	GetTradingDay(ctx context.Context) (*domain.TradingDayInfo, error)
}

// Handler exposes the first-phase market-data HTTP API.
type Handler struct {
	service MarketDataService
	mux     *http.ServeMux
	metrics *Metrics
	limiter *RateLimiter
	logger  *log.Logger
}

// Options customizes HTTP handler middleware.
type Options struct {
	Metrics     *Metrics
	RateLimiter *RateLimiter
	Logger      *log.Logger
}

func New(service MarketDataService) *Handler {
	return NewWithOptions(service, Options{Metrics: NewMetrics()})
}

func NewWithMetrics(service MarketDataService, metrics *Metrics) *Handler {
	return NewWithOptions(service, Options{Metrics: metrics})
}

func NewWithOptions(service MarketDataService, opts Options) *Handler {
	if opts.Metrics == nil {
		opts.Metrics = NewMetrics()
	}
	h := &Handler{service: service, mux: http.NewServeMux(), metrics: opts.Metrics, limiter: opts.RateLimiter, logger: opts.Logger}
	h.routes()
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) routes() {
	h.route("GET /api/v1/health", "health", h.handleHealth)
	h.route("GET /api/v1/quotes", "quotes", h.handleQuotes)
	h.route("POST /api/v1/quotes", "quotes", h.handleQuotes)
	h.route("GET /api/v1/orderbook", "orderbook", h.handleOrderBook)
	h.route("POST /api/v1/orderbook", "orderbook", h.handleOrderBook)
	h.route("GET /api/v1/ticks", "ticks", h.handleTicks)
	h.route("GET /api/v1/history-ticks", "history_ticks", h.handleHistoryTicks)
	h.route("GET /api/v1/kline", "kline", h.handleKLine)
	h.route("GET /api/v1/adjusted-kline", "adjusted_kline", h.handleKLine)
	h.route("GET /api/v1/xdxr", "xdxr", h.handleXDXR)
	h.route("GET /api/v1/securities", "securities", h.handleSecurities)
	h.route("GET /api/v1/finance", "finance", h.handleFinance)
	h.route("GET /api/v1/trading-day", "trading_day", h.handleTradingDay)
	h.route("GET /api/v1/metrics", "metrics", h.handleMetrics)
}

func (h *Handler) route(pattern string, name string, handler http.HandlerFunc) {
	h.mux.HandleFunc(pattern, h.instrument(name, handler))
}

func (h *Handler) instrument(name string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		done := h.metrics.start(name)
		defer func() {
			status := recorder.statusCode()
			done(status)
			if h.logger != nil {
				h.logger.Printf("http request route=%s method=%s path=%s query=%q status=%d bytes=%d duration=%s remote=%s", name, r.Method, r.URL.Path, r.URL.RawQuery, status, recorder.bytes, time.Since(started), r.RemoteAddr)
			}
		}()
		if h.limiter != nil && !h.limiter.Allow() {
			writeError(recorder, domain.ErrRateLimited)
			return
		}
		handler(recorder, r)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(payload []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	written, err := w.ResponseWriter.Write(payload)
	w.bytes += written
	return written, err
}

func (w *statusRecorder) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.metrics.Snapshot())
}

func (h *Handler) handleQuotes(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	symbols, err := parseSymbols(r)
	if err != nil {
		writeError(w, err)
		return
	}
	quotes, err := h.service.GetQuotes(r.Context(), symbols)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapQuotes(quotes))
}

func (h *Handler) handleOrderBook(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	symbols, err := parseSymbols(r)
	if err != nil {
		writeError(w, err)
		return
	}
	books, err := h.service.GetOrderBook(r.Context(), symbols)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapOrderBooks(books))
}

func (h *Handler) handleTicks(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	symbol, err := parseSymbol(r)
	if err != nil {
		writeError(w, err)
		return
	}
	start, err := parseUint16Query(r, "start", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	count, err := parseUint16Query(r, "count", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	req := domain.TickRequest{
		Start:        start,
		Count:        count,
		Full:         parseBoolQuery(r, "full"),
		ForceRefresh: parseBoolQuery(r, "force_refresh"),
	}
	ticks, err := h.service.GetTicks(r.Context(), symbol, req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapTicks(ticks))
}

func (h *Handler) handleHistoryTicks(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	symbol, err := parseSymbol(r)
	if err != nil {
		writeError(w, err)
		return
	}
	tradeDate, err := parseDateQuery(r, "date", false)
	if err != nil {
		writeError(w, err)
		return
	}
	if tradeDate.IsZero() {
		tradeDate = domain.NormalizeDate(time.Now())
	}
	start, err := parseUint16Query(r, "start", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	count, err := parseUint16Query(r, "count", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	req := domain.HistoryTickRequest{
		TradeDate:           tradeDate,
		Start:               start,
		Count:               count,
		Full:                parseBoolQuery(r, "full"),
		WithTransactionFlag: parseBoolQuery(r, "with_transaction_flag"),
		ForceRefresh:        parseBoolQuery(r, "force_refresh"),
	}
	ticks, err := h.service.GetHistoryTicks(r.Context(), symbol, req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapTicks(ticks))
}

func (h *Handler) handleKLine(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	symbol, err := parseSymbol(r)
	if err != nil {
		writeError(w, err)
		return
	}
	period, err := parsePeriodQuery(r, "period", domain.PeriodDay)
	if err != nil {
		writeError(w, err)
		return
	}
	adjust, err := domain.ParseAdjustType(r.URL.Query().Get("adjust"))
	if err != nil {
		writeError(w, err)
		return
	}
	start, err := parseUint16Query(r, "start", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	count, err := parseUint16Query(r, "count", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	times, err := parseUint16Query(r, "times", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	startDate, err := parseDateQuery(r, "start_date", false)
	if err != nil {
		writeError(w, err)
		return
	}
	endDate, err := parseDateQuery(r, "end_date", false)
	if err != nil {
		writeError(w, err)
		return
	}
	req := domain.KLineRequest{
		Period:       period,
		Start:        start,
		Count:        count,
		Times:        times,
		StartDate:    startDate,
		EndDate:      endDate,
		ForceRefresh: parseBoolQuery(r, "force_refresh"),
	}
	var bars []domain.Bar
	if adjust == domain.AdjustNone {
		bars, err = h.service.GetKLine(r.Context(), symbol, req)
	} else {
		bars, err = h.service.GetAdjustedKLine(r.Context(), symbol, domain.AdjustedKLineRequest{KLineRequest: req, AdjustType: adjust})
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapBars(bars))
}

func (h *Handler) handleXDXR(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	symbol, err := parseSymbol(r)
	if err != nil {
		writeError(w, err)
		return
	}
	events, err := h.service.GetXDXR(r.Context(), symbol)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapXDXR(events))
}

func (h *Handler) handleSecurities(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	markets, err := parseMarkets(r)
	if err != nil {
		writeError(w, err)
		return
	}
	start, err := parseUint32Query(r, "start", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	count, err := parseUint32Query(r, "count", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	symbols, err := parseOptionalSymbols(r)
	if err != nil {
		writeError(w, err)
		return
	}
	items, err := h.service.GetSecurityInfo(r.Context(), domain.SecurityQuery{Markets: markets, Symbols: symbols, Start: start, Count: count, Refresh: parseBoolQuery(r, "refresh")})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapSecurities(items))
}

func (h *Handler) handleTradingDay(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	info, err := h.service.GetTradingDay(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapTradingDay(info))
}

func (h *Handler) handleFinance(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		writeError(w, domain.ErrUpstreamUnavailable)
		return
	}
	symbol, err := parseSymbol(r)
	if err != nil {
		writeError(w, err)
		return
	}
	info, err := h.service.GetFinance(r.Context(), symbol)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mapFinance(info))
}

type apiResponse struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *apiError `json:"error,omitempty"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiResponse{OK: true, Data: data})
}

func writeError(w http.ResponseWriter, err error) {
	status, code := errorStatus(err)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiResponse{OK: false, Error: &apiError{Code: code, Message: err.Error()}})
}

func errorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, domain.ErrInvalidRequest):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, domain.ErrRateLimited):
		return http.StatusTooManyRequests, "rate_limited"
	case errors.Is(err, domain.ErrNoData):
		return http.StatusNotFound, "no_data"
	case errors.Is(err, domain.ErrUnsupportedCapability):
		return http.StatusNotImplemented, "unsupported_capability"
	case errors.Is(err, domain.ErrUpstreamUnavailable):
		return http.StatusServiceUnavailable, "upstream_unavailable"
	default:
		return http.StatusInternalServerError, "internal_error"
	}
}
