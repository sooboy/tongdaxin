package grpcapi

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/sooboy/tongdaxin/internal/domain"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type grpcStubService struct {
	quotes []domain.Quote
}

func (s *grpcStubService) GetQuotes(context.Context, []domain.Symbol) ([]domain.Quote, error) {
	return s.quotes, nil
}
func (s *grpcStubService) GetOrderBook(context.Context, []domain.Symbol) ([]domain.OrderBook, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetTicks(context.Context, domain.Symbol, domain.TickRequest) ([]domain.Tick, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetHistoryTicks(context.Context, domain.Symbol, domain.HistoryTickRequest) ([]domain.Tick, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetKLine(context.Context, domain.Symbol, domain.KLineRequest) ([]domain.Bar, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetAdjustedKLine(context.Context, domain.Symbol, domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetXDXR(context.Context, domain.Symbol) ([]domain.XDXREvent, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetSecurityInfo(context.Context, domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetFinance(context.Context, domain.Symbol) (*domain.FinanceInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *grpcStubService) GetTradingDay(context.Context) (*domain.TradingDayInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}

func TestGRPCClientCallsQuotes(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestClient(t, &grpcStubService{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}})
	defer cleanup()

	resp, err := client.GetQuotes(context.Background(), &SymbolsRequest{Symbols: []Symbol{{Market: "SH", Code: "600000"}}})
	if err != nil {
		t.Fatalf("GetQuotes: %v", err)
	}
	if len(resp.Quotes) != 1 || resp.Quotes[0].LastPrice != 10.5 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestGRPCInvalidRequestUsesInvalidArgument(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestClient(t, &grpcStubService{})
	defer cleanup()

	_, err := client.GetQuotes(context.Background(), &SymbolsRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s err=%v", status.Code(err), err)
	}
}

func newTestClient(t *testing.T, service *grpcStubService) (*Client, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := NewGRPCServer(service)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	ctx := context.Background()
	conn, err := DialContext(ctx, "bufnet",
		grpcgo.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		if err := <-serveErr; err != nil && !errors.Is(err, grpcgo.ErrServerStopped) {
			t.Fatalf("Serve: %v", err)
		}
	}
	return NewClient(conn), cleanup
}
