package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	httpapi "github.com/sooboy/tongdaxin/internal/api/http"
	"github.com/sooboy/tongdaxin/internal/domain"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

const serviceName = "tongdaxin.marketdata.v1.MarketData"

const dateLayout = "2006-01-02"

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Symbol is the transport-level symbol used by the JSON-coded gRPC API.
type Symbol struct {
	Market string `json:"market,omitempty"`
	Code   string `json:"code,omitempty"`
}

type Empty struct{}

type HealthResponse struct {
	Status string `json:"status"`
}

type SymbolsRequest struct {
	Symbols []Symbol `json:"symbols,omitempty"`
	Market  string   `json:"market,omitempty"`
	Code    string   `json:"code,omitempty"`
}

type SymbolRequest struct {
	Symbol Symbol `json:"symbol,omitempty"`
	Market string `json:"market,omitempty"`
	Code   string `json:"code,omitempty"`
}

type QuotesResponse struct {
	Quotes []domain.Quote `json:"quotes"`
}

type OrderBooksResponse struct {
	OrderBooks []domain.OrderBook `json:"order_books"`
}

type TicksRequest struct {
	Symbol       Symbol `json:"symbol,omitempty"`
	Market       string `json:"market,omitempty"`
	Code         string `json:"code,omitempty"`
	Start        uint32 `json:"start,omitempty"`
	Count        uint32 `json:"count,omitempty"`
	Full         bool   `json:"full,omitempty"`
	ForceRefresh bool   `json:"force_refresh,omitempty"`
}

type HistoryTicksRequest struct {
	Symbol              Symbol `json:"symbol,omitempty"`
	Market              string `json:"market,omitempty"`
	Code                string `json:"code,omitempty"`
	Date                string `json:"date,omitempty"`
	Start               uint32 `json:"start,omitempty"`
	Count               uint32 `json:"count,omitempty"`
	Full                bool   `json:"full,omitempty"`
	WithTransactionFlag bool   `json:"with_transaction_flag,omitempty"`
	ForceRefresh        bool   `json:"force_refresh,omitempty"`
}

type TicksResponse struct {
	Ticks []domain.Tick `json:"ticks"`
}

type KLineRequest struct {
	Symbol       Symbol `json:"symbol,omitempty"`
	Market       string `json:"market,omitempty"`
	Code         string `json:"code,omitempty"`
	Period       string `json:"period,omitempty"`
	Adjust       string `json:"adjust,omitempty"`
	Start        uint32 `json:"start,omitempty"`
	Count        uint32 `json:"count,omitempty"`
	Times        uint32 `json:"times,omitempty"`
	StartDate    string `json:"start_date,omitempty"`
	EndDate      string `json:"end_date,omitempty"`
	ForceRefresh bool   `json:"force_refresh,omitempty"`
}

type KLineResponse struct {
	Bars []domain.Bar `json:"bars"`
}

type XDXRResponse struct {
	Events []domain.XDXREvent `json:"events"`
}

type SecuritiesRequest struct {
	Markets []string `json:"markets,omitempty"`
	Symbols []Symbol `json:"symbols,omitempty"`
	Start   uint32   `json:"start,omitempty"`
	Count   uint32   `json:"count,omitempty"`
	Refresh bool     `json:"refresh,omitempty"`
}

type SecuritiesResponse struct {
	Items []domain.SecurityInfo `json:"items"`
}

type FinanceResponse struct {
	Info *domain.FinanceInfo `json:"info,omitempty"`
}

type TradingDayResponse struct {
	Info *domain.TradingDayInfo `json:"info,omitempty"`
}

// MarketDataServer is the gRPC service surface.
type MarketDataServer interface {
	Health(context.Context, *Empty) (*HealthResponse, error)
	GetQuotes(context.Context, *SymbolsRequest) (*QuotesResponse, error)
	GetOrderBook(context.Context, *SymbolsRequest) (*OrderBooksResponse, error)
	GetTicks(context.Context, *TicksRequest) (*TicksResponse, error)
	GetHistoryTicks(context.Context, *HistoryTicksRequest) (*TicksResponse, error)
	GetKLine(context.Context, *KLineRequest) (*KLineResponse, error)
	GetXDXR(context.Context, *SymbolRequest) (*XDXRResponse, error)
	GetSecurities(context.Context, *SecuritiesRequest) (*SecuritiesResponse, error)
	GetFinance(context.Context, *SymbolRequest) (*FinanceResponse, error)
	GetTradingDay(context.Context, *Empty) (*TradingDayResponse, error)
}

// Server adapts the internal market-data service to the gRPC API.
type Server struct {
	service httpapi.MarketDataService
	now     func() time.Time
}

func NewServer(service httpapi.MarketDataService) *Server {
	return &Server{service: service, now: time.Now}
}

func NewGRPCServer(service httpapi.MarketDataService, opts ...grpcgo.ServerOption) *grpcgo.Server {
	server := grpcgo.NewServer(opts...)
	RegisterMarketDataServer(server, NewServer(service))
	return server
}

func (s *Server) Health(context.Context, *Empty) (*HealthResponse, error) {
	return &HealthResponse{Status: "ok"}, nil
}

func (s *Server) GetQuotes(ctx context.Context, req *SymbolsRequest) (*QuotesResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	symbols, err := parseSymbols(req)
	if err != nil {
		return nil, grpcError(err)
	}
	quotes, err := s.service.GetQuotes(ctx, symbols)
	if err != nil {
		return nil, grpcError(err)
	}
	return &QuotesResponse{Quotes: quotes}, nil
}

func (s *Server) GetOrderBook(ctx context.Context, req *SymbolsRequest) (*OrderBooksResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	symbols, err := parseSymbols(req)
	if err != nil {
		return nil, grpcError(err)
	}
	books, err := s.service.GetOrderBook(ctx, symbols)
	if err != nil {
		return nil, grpcError(err)
	}
	return &OrderBooksResponse{OrderBooks: books}, nil
}

func (s *Server) GetTicks(ctx context.Context, req *TicksRequest) (*TicksResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, grpcError(domain.ErrInvalidRequest)
	}
	symbol, err := parseSingleSymbol(req.Symbol, req.Market, req.Code)
	if err != nil {
		return nil, grpcError(err)
	}
	start, err := toUint16(req.Start)
	if err != nil {
		return nil, grpcError(err)
	}
	count, err := toUint16(req.Count)
	if err != nil {
		return nil, grpcError(err)
	}
	ticks, err := s.service.GetTicks(ctx, symbol, domain.TickRequest{Start: start, Count: count, Full: req.Full, ForceRefresh: req.ForceRefresh})
	if err != nil {
		return nil, grpcError(err)
	}
	return &TicksResponse{Ticks: ticks}, nil
}

func (s *Server) GetHistoryTicks(ctx context.Context, req *HistoryTicksRequest) (*TicksResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, grpcError(domain.ErrInvalidRequest)
	}
	symbol, err := parseSingleSymbol(req.Symbol, req.Market, req.Code)
	if err != nil {
		return nil, grpcError(err)
	}
	date, err := parseDate(req.Date)
	if err != nil {
		return nil, grpcError(err)
	}
	if date.IsZero() {
		date = domain.NormalizeDate(s.now())
	}
	start, err := toUint16(req.Start)
	if err != nil {
		return nil, grpcError(err)
	}
	count, err := toUint16(req.Count)
	if err != nil {
		return nil, grpcError(err)
	}
	ticks, err := s.service.GetHistoryTicks(ctx, symbol, domain.HistoryTickRequest{TradeDate: date, Start: start, Count: count, Full: req.Full, WithTransactionFlag: req.WithTransactionFlag, ForceRefresh: req.ForceRefresh})
	if err != nil {
		return nil, grpcError(err)
	}
	return &TicksResponse{Ticks: ticks}, nil
}

func (s *Server) GetKLine(ctx context.Context, req *KLineRequest) (*KLineResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, grpcError(domain.ErrInvalidRequest)
	}
	symbol, err := parseSingleSymbol(req.Symbol, req.Market, req.Code)
	if err != nil {
		return nil, grpcError(err)
	}
	period, err := domain.ParsePeriod(req.Period)
	if err != nil {
		return nil, grpcError(err)
	}
	if period == domain.PeriodUnknown {
		period = domain.PeriodDay
	}
	adjust, err := domain.ParseAdjustType(req.Adjust)
	if err != nil {
		return nil, grpcError(err)
	}
	start, err := toUint16(req.Start)
	if err != nil {
		return nil, grpcError(err)
	}
	count, err := toUint16(req.Count)
	if err != nil {
		return nil, grpcError(err)
	}
	times, err := toUint16(req.Times)
	if err != nil {
		return nil, grpcError(err)
	}
	startDate, err := parseDate(req.StartDate)
	if err != nil {
		return nil, grpcError(err)
	}
	endDate, err := parseDate(req.EndDate)
	if err != nil {
		return nil, grpcError(err)
	}
	kreq := domain.KLineRequest{Period: period, Start: start, Count: count, Times: times, StartDate: startDate, EndDate: endDate, ForceRefresh: req.ForceRefresh}
	var bars []domain.Bar
	if adjust == domain.AdjustNone {
		bars, err = s.service.GetKLine(ctx, symbol, kreq)
	} else {
		bars, err = s.service.GetAdjustedKLine(ctx, symbol, domain.AdjustedKLineRequest{KLineRequest: kreq, AdjustType: adjust})
	}
	if err != nil {
		return nil, grpcError(err)
	}
	return &KLineResponse{Bars: bars}, nil
}

func (s *Server) GetXDXR(ctx context.Context, req *SymbolRequest) (*XDXRResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	symbol, err := parseSymbolRequest(req)
	if err != nil {
		return nil, grpcError(err)
	}
	events, err := s.service.GetXDXR(ctx, symbol)
	if err != nil {
		return nil, grpcError(err)
	}
	return &XDXRResponse{Events: events}, nil
}

func (s *Server) GetSecurities(ctx context.Context, req *SecuritiesRequest) (*SecuritiesResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, grpcError(domain.ErrInvalidRequest)
	}
	markets := make([]domain.Market, 0, len(req.Markets))
	for _, raw := range req.Markets {
		market, err := domain.ParseMarket(raw)
		if err != nil {
			return nil, grpcError(err)
		}
		markets = append(markets, market)
	}
	symbols := make([]domain.Symbol, 0, len(req.Symbols))
	for _, raw := range req.Symbols {
		symbol, err := parseSingleSymbol(raw, "", "")
		if err != nil {
			return nil, grpcError(err)
		}
		symbols = append(symbols, symbol)
	}
	items, err := s.service.GetSecurityInfo(ctx, domain.SecurityQuery{Markets: markets, Symbols: symbols, Start: req.Start, Count: req.Count, Refresh: req.Refresh})
	if err != nil {
		return nil, grpcError(err)
	}
	return &SecuritiesResponse{Items: items}, nil
}

func (s *Server) GetFinance(ctx context.Context, req *SymbolRequest) (*FinanceResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	symbol, err := parseSymbolRequest(req)
	if err != nil {
		return nil, grpcError(err)
	}
	info, err := s.service.GetFinance(ctx, symbol)
	if err != nil {
		return nil, grpcError(err)
	}
	return &FinanceResponse{Info: info}, nil
}

func (s *Server) GetTradingDay(ctx context.Context, _ *Empty) (*TradingDayResponse, error) {
	if err := s.requireService(); err != nil {
		return nil, err
	}
	info, err := s.service.GetTradingDay(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	return &TradingDayResponse{Info: info}, nil
}

func (s *Server) requireService() error {
	if s == nil || s.service == nil {
		return grpcError(domain.ErrUpstreamUnavailable)
	}
	return nil
}

func RegisterMarketDataServer(registrar grpcgo.ServiceRegistrar, server MarketDataServer) {
	registrar.RegisterService(&MarketData_ServiceDesc, server)
}

var MarketData_ServiceDesc = grpcgo.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*MarketDataServer)(nil),
	Methods: []grpcgo.MethodDesc{
		{MethodName: "Health", Handler: _MarketData_Health_Handler},
		{MethodName: "GetQuotes", Handler: _MarketData_GetQuotes_Handler},
		{MethodName: "GetOrderBook", Handler: _MarketData_GetOrderBook_Handler},
		{MethodName: "GetTicks", Handler: _MarketData_GetTicks_Handler},
		{MethodName: "GetHistoryTicks", Handler: _MarketData_GetHistoryTicks_Handler},
		{MethodName: "GetKLine", Handler: _MarketData_GetKLine_Handler},
		{MethodName: "GetXDXR", Handler: _MarketData_GetXDXR_Handler},
		{MethodName: "GetSecurities", Handler: _MarketData_GetSecurities_Handler},
		{MethodName: "GetFinance", Handler: _MarketData_GetFinance_Handler},
		{MethodName: "GetTradingDay", Handler: _MarketData_GetTradingDay_Handler},
	},
	Streams:  []grpcgo.StreamDesc{},
	Metadata: "marketdata.json.grpc",
}

func _MarketData_Health_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[Empty, HealthResponse](srv, ctx, dec, interceptor, "Health", func(s MarketDataServer, ctx context.Context, req *Empty) (*HealthResponse, error) {
		return s.Health(ctx, req)
	})
}

func _MarketData_GetQuotes_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[SymbolsRequest, QuotesResponse](srv, ctx, dec, interceptor, "GetQuotes", func(s MarketDataServer, ctx context.Context, req *SymbolsRequest) (*QuotesResponse, error) {
		return s.GetQuotes(ctx, req)
	})
}

func _MarketData_GetOrderBook_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[SymbolsRequest, OrderBooksResponse](srv, ctx, dec, interceptor, "GetOrderBook", func(s MarketDataServer, ctx context.Context, req *SymbolsRequest) (*OrderBooksResponse, error) {
		return s.GetOrderBook(ctx, req)
	})
}

func _MarketData_GetTicks_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[TicksRequest, TicksResponse](srv, ctx, dec, interceptor, "GetTicks", func(s MarketDataServer, ctx context.Context, req *TicksRequest) (*TicksResponse, error) {
		return s.GetTicks(ctx, req)
	})
}

func _MarketData_GetHistoryTicks_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[HistoryTicksRequest, TicksResponse](srv, ctx, dec, interceptor, "GetHistoryTicks", func(s MarketDataServer, ctx context.Context, req *HistoryTicksRequest) (*TicksResponse, error) {
		return s.GetHistoryTicks(ctx, req)
	})
}

func _MarketData_GetKLine_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[KLineRequest, KLineResponse](srv, ctx, dec, interceptor, "GetKLine", func(s MarketDataServer, ctx context.Context, req *KLineRequest) (*KLineResponse, error) {
		return s.GetKLine(ctx, req)
	})
}

func _MarketData_GetXDXR_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[SymbolRequest, XDXRResponse](srv, ctx, dec, interceptor, "GetXDXR", func(s MarketDataServer, ctx context.Context, req *SymbolRequest) (*XDXRResponse, error) {
		return s.GetXDXR(ctx, req)
	})
}

func _MarketData_GetSecurities_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[SecuritiesRequest, SecuritiesResponse](srv, ctx, dec, interceptor, "GetSecurities", func(s MarketDataServer, ctx context.Context, req *SecuritiesRequest) (*SecuritiesResponse, error) {
		return s.GetSecurities(ctx, req)
	})
}

func _MarketData_GetFinance_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[SymbolRequest, FinanceResponse](srv, ctx, dec, interceptor, "GetFinance", func(s MarketDataServer, ctx context.Context, req *SymbolRequest) (*FinanceResponse, error) {
		return s.GetFinance(ctx, req)
	})
}

func _MarketData_GetTradingDay_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor) (any, error) {
	return unaryHandler[Empty, TradingDayResponse](srv, ctx, dec, interceptor, "GetTradingDay", func(s MarketDataServer, ctx context.Context, req *Empty) (*TradingDayResponse, error) {
		return s.GetTradingDay(ctx, req)
	})
}

func unaryHandler[Req any, Resp any](srv any, ctx context.Context, dec func(any) error, interceptor grpcgo.UnaryServerInterceptor, method string, call func(MarketDataServer, context.Context, *Req) (*Resp, error)) (any, error) {
	in := new(Req)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return call(srv.(MarketDataServer), ctx, in)
	}
	info := &grpcgo.UnaryServerInfo{Server: srv, FullMethod: fullMethod(method)}
	handler := func(ctx context.Context, req any) (any, error) {
		return call(srv.(MarketDataServer), ctx, req.(*Req))
	}
	return interceptor(ctx, in, info, handler)
}

func fullMethod(method string) string {
	return "/" + serviceName + "/" + method
}

func parseSymbols(req *SymbolsRequest) ([]domain.Symbol, error) {
	if req == nil {
		return nil, domain.ErrInvalidRequest
	}
	raw := req.Symbols
	if len(raw) == 0 && (req.Market != "" || req.Code != "") {
		raw = []Symbol{{Market: req.Market, Code: req.Code}}
	}
	if len(raw) == 0 {
		return nil, domain.ErrInvalidRequest
	}
	symbols := make([]domain.Symbol, 0, len(raw))
	for _, item := range raw {
		symbol, err := parseSingleSymbol(item, "", "")
		if err != nil {
			return nil, err
		}
		symbols = append(symbols, symbol)
	}
	return symbols, nil
}

func parseSymbolRequest(req *SymbolRequest) (domain.Symbol, error) {
	if req == nil {
		return domain.Symbol{}, domain.ErrInvalidRequest
	}
	return parseSingleSymbol(req.Symbol, req.Market, req.Code)
}

func parseSingleSymbol(symbol Symbol, market string, code string) (domain.Symbol, error) {
	if symbol.Market != "" || symbol.Code != "" {
		market = symbol.Market
		code = symbol.Code
	}
	return domain.NewSymbol(domain.Market(market), code)
}

func parseDate(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{dateLayout, "20060102", time.RFC3339} {
		parsed, err := time.ParseInLocation(layout, raw, time.Local)
		if err == nil {
			return domain.NormalizeDate(parsed), nil
		}
	}
	return time.Time{}, domain.ErrInvalidRequest
}

func toUint16(value uint32) (uint16, error) {
	if value > 65535 {
		return 0, domain.ErrInvalidRequest
	}
	return uint16(value), nil
}

func grpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, domain.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrRateLimited):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, domain.ErrNoData):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrUnsupportedCapability):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, domain.ErrUpstreamUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
