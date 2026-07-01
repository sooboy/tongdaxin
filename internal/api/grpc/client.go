package grpcapi

import (
	"context"

	grpcgo "google.golang.org/grpc"
)

// Client is a JSON-coded gRPC client for MarketData.
type Client struct {
	cc grpcgo.ClientConnInterface
}

func NewClient(cc grpcgo.ClientConnInterface) *Client {
	return &Client{cc: cc}
}

// DialContext dials a MarketData gRPC server and configures the JSON codec used
// by this package. Add credentials options at the call site, for example
// grpc.WithTransportCredentials(insecure.NewCredentials()) for local testing.
func DialContext(ctx context.Context, target string, opts ...grpcgo.DialOption) (*grpcgo.ClientConn, error) {
	dialOpts := append([]grpcgo.DialOption{grpcgo.WithDefaultCallOptions(grpcgo.ForceCodec(jsonCodec{}))}, opts...)
	return grpcgo.DialContext(ctx, target, dialOpts...)
}

func (c *Client) Health(ctx context.Context, req *Empty, opts ...grpcgo.CallOption) (*HealthResponse, error) {
	out := new(HealthResponse)
	err := c.cc.Invoke(ctx, fullMethod("Health"), nonNil(req), out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetQuotes(ctx context.Context, req *SymbolsRequest, opts ...grpcgo.CallOption) (*QuotesResponse, error) {
	out := new(QuotesResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetQuotes"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetOrderBook(ctx context.Context, req *SymbolsRequest, opts ...grpcgo.CallOption) (*OrderBooksResponse, error) {
	out := new(OrderBooksResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetOrderBook"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetTicks(ctx context.Context, req *TicksRequest, opts ...grpcgo.CallOption) (*TicksResponse, error) {
	out := new(TicksResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetTicks"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetHistoryTicks(ctx context.Context, req *HistoryTicksRequest, opts ...grpcgo.CallOption) (*TicksResponse, error) {
	out := new(TicksResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetHistoryTicks"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetKLine(ctx context.Context, req *KLineRequest, opts ...grpcgo.CallOption) (*KLineResponse, error) {
	out := new(KLineResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetKLine"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetXDXR(ctx context.Context, req *SymbolRequest, opts ...grpcgo.CallOption) (*XDXRResponse, error) {
	out := new(XDXRResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetXDXR"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetSecurities(ctx context.Context, req *SecuritiesRequest, opts ...grpcgo.CallOption) (*SecuritiesResponse, error) {
	out := new(SecuritiesResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetSecurities"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetFinance(ctx context.Context, req *SymbolRequest, opts ...grpcgo.CallOption) (*FinanceResponse, error) {
	out := new(FinanceResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetFinance"), req, out, callOpts(opts)...)
	return out, err
}

func (c *Client) GetTradingDay(ctx context.Context, req *Empty, opts ...grpcgo.CallOption) (*TradingDayResponse, error) {
	out := new(TradingDayResponse)
	err := c.cc.Invoke(ctx, fullMethod("GetTradingDay"), nonNil(req), out, callOpts(opts)...)
	return out, err
}

func callOpts(opts []grpcgo.CallOption) []grpcgo.CallOption {
	return append([]grpcgo.CallOption{grpcgo.ForceCodec(jsonCodec{})}, opts...)
}

func nonNil(req *Empty) *Empty {
	if req == nil {
		return &Empty{}
	}
	return req
}
