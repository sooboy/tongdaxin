package ginapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sooboy/tongdaxin/internal/domain"
)

type stubService struct {
	quotes []domain.Quote
}

func (s *stubService) GetQuotes(context.Context, []domain.Symbol) ([]domain.Quote, error) {
	return s.quotes, nil
}
func (s *stubService) GetOrderBook(context.Context, []domain.Symbol) ([]domain.OrderBook, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetTicks(context.Context, domain.Symbol, domain.TickRequest) ([]domain.Tick, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetHistoryTicks(context.Context, domain.Symbol, domain.HistoryTickRequest) ([]domain.Tick, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetKLine(context.Context, domain.Symbol, domain.KLineRequest) ([]domain.Bar, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetAdjustedKLine(context.Context, domain.Symbol, domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetXDXR(context.Context, domain.Symbol) ([]domain.XDXREvent, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetSecurityInfo(context.Context, domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetFinance(context.Context, domain.Symbol) (*domain.FinanceInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}
func (s *stubService) GetTradingDay(context.Context) (*domain.TradingDayInfo, error) {
	return nil, domain.ErrUnsupportedCapability
}

func TestGinRouterServesExistingHTTPContract(t *testing.T) {
	t.Parallel()

	router := New(&stubService{quotes: []domain.Quote{{Symbol: domain.Symbol{Market: domain.MarketSH, Code: "600000"}, LastPrice: 10.5}}})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quotes?market=SH&code=600000", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}
	if body["ok"] != true {
		t.Fatalf("body = %#v", body)
	}
	data := body["data"].([]any)
	quote := data[0].(map[string]any)
	if quote["last_price"] != 10.5 {
		t.Fatalf("quote = %#v", quote)
	}
}
