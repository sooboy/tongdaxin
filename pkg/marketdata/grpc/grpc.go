// Package grpc exposes a public gRPC adapter and client for third-party Go
// applications. The implementation uses the project's internal JSON-codec gRPC
// service so consumers do not need protoc for Go-to-Go embedding.
package grpc

import (
	"context"

	internalgrpc "github.com/sooboy/tongdaxin/internal/api/grpc"
	"github.com/sooboy/tongdaxin/pkg/marketdata"
	grpcgo "google.golang.org/grpc"
)

type (
	Symbol              = internalgrpc.Symbol
	Empty               = internalgrpc.Empty
	HealthResponse      = internalgrpc.HealthResponse
	SymbolsRequest      = internalgrpc.SymbolsRequest
	SymbolRequest       = internalgrpc.SymbolRequest
	QuotesResponse      = internalgrpc.QuotesResponse
	OrderBooksResponse  = internalgrpc.OrderBooksResponse
	TicksRequest        = internalgrpc.TicksRequest
	HistoryTicksRequest = internalgrpc.HistoryTicksRequest
	TicksResponse       = internalgrpc.TicksResponse
	KLineRequest        = internalgrpc.KLineRequest
	KLineResponse       = internalgrpc.KLineResponse
	XDXRResponse        = internalgrpc.XDXRResponse
	SecuritiesRequest   = internalgrpc.SecuritiesRequest
	SecuritiesResponse  = internalgrpc.SecuritiesResponse
	FinanceResponse     = internalgrpc.FinanceResponse
	TradingDayResponse  = internalgrpc.TradingDayResponse
	Client              = internalgrpc.Client
)

func NewGRPCServer(service marketdata.Service, opts ...grpcgo.ServerOption) *grpcgo.Server {
	return internalgrpc.NewGRPCServer(service, opts...)
}

func NewClient(cc grpcgo.ClientConnInterface) *Client { return internalgrpc.NewClient(cc) }

func DialContext(ctx context.Context, target string, opts ...grpcgo.DialOption) (*grpcgo.ClientConn, error) {
	return internalgrpc.DialContext(ctx, target, opts...)
}
