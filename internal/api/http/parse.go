package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

const dateLayout = "2006-01-02"

func defaultSymbol() domain.Symbol {
	return domain.Symbol{Market: domain.MarketSH, Code: "600000"}
}

func parseSymbol(r *http.Request) (domain.Symbol, error) {
	if raw := strings.TrimSpace(r.URL.Query().Get("symbol")); raw != "" {
		return parseSymbolString(raw)
	}
	market := strings.TrimSpace(r.URL.Query().Get("market"))
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if market == "" && code == "" {
		return defaultSymbol(), nil
	}
	if market == "" {
		return parseSymbolString(code)
	}
	parsedMarket, err := domain.ParseMarket(market)
	if err != nil {
		return domain.Symbol{}, err
	}
	return domain.NewSymbol(parsedMarket, code)
}

func parseOptionalSymbols(r *http.Request) ([]domain.Symbol, error) {
	if raw := strings.TrimSpace(r.URL.Query().Get("symbols")); raw != "" {
		return parseSymbolList(raw)
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("symbol")); raw != "" {
		return parseSymbolList(raw)
	}
	return nil, nil
}

func parseSymbols(r *http.Request) ([]domain.Symbol, error) {
	if raw := strings.TrimSpace(r.URL.Query().Get("symbols")); raw != "" {
		return parseSymbolList(raw)
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("symbol")); raw != "" {
		return parseSymbolList(raw)
	}
	if market := strings.TrimSpace(r.URL.Query().Get("market")); market != "" || strings.TrimSpace(r.URL.Query().Get("code")) != "" {
		symbol, err := parseSymbol(r)
		if err != nil {
			return nil, err
		}
		return []domain.Symbol{symbol}, nil
	}
	if r.Method == http.MethodPost {
		symbols, err := parseSymbolsBody(r)
		if err != nil {
			return nil, err
		}
		if len(symbols) > 0 {
			return symbols, nil
		}
	}
	symbol, err := parseSymbol(r)
	if err != nil {
		return nil, err
	}
	return []domain.Symbol{symbol}, nil
}

type symbolsRequest struct {
	Market  string   `json:"market"`
	Code    string   `json:"code"`
	Symbol  string   `json:"symbol"`
	Symbols []string `json:"symbols"`
}

func parseSymbolsBody(r *http.Request) ([]domain.Symbol, error) {
	defer r.Body.Close()
	var req symbolsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return []domain.Symbol{defaultSymbol()}, nil
		}
		return nil, domain.ErrInvalidRequest
	}
	if len(req.Symbols) > 0 {
		return parseSymbolList(strings.Join(req.Symbols, ","))
	}
	if symbol := strings.TrimSpace(req.Symbol); symbol != "" {
		return parseSymbolList(symbol)
	}
	market := strings.TrimSpace(req.Market)
	code := strings.TrimSpace(req.Code)
	if market == "" && code == "" {
		return []domain.Symbol{defaultSymbol()}, nil
	}
	if market == "" {
		symbol, err := parseSymbolString(code)
		if err != nil {
			return nil, err
		}
		return []domain.Symbol{symbol}, nil
	}
	parsedMarket, err := domain.ParseMarket(market)
	if err != nil {
		return nil, err
	}
	symbol, err := domain.NewSymbol(parsedMarket, code)
	if err != nil {
		return nil, err
	}
	return []domain.Symbol{symbol}, nil
}

func parseSymbolList(raw string) ([]domain.Symbol, error) {
	parts := strings.Split(raw, ",")
	symbols := make([]domain.Symbol, 0, len(parts))
	for _, part := range parts {
		symbol, err := parseSymbolString(part)
		if err != nil {
			return nil, err
		}
		symbols = append(symbols, symbol)
	}
	if len(symbols) == 0 {
		return nil, domain.ErrInvalidRequest
	}
	return symbols, nil
}

func parseSymbolString(raw string) (domain.Symbol, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return domain.Symbol{}, domain.ErrInvalidRequest
	}
	for _, sep := range []string{":", "."} {
		left, right, ok := strings.Cut(value, sep)
		if ok {
			return parseDelimitedSymbol(left, right)
		}
	}
	return parseCompactSymbol(value)
}

func parseDelimitedSymbol(left string, right string) (domain.Symbol, error) {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if market, err := domain.ParseMarket(left); err == nil {
		return domain.NewSymbol(market, right)
	}
	if market, err := domain.ParseMarket(right); err == nil {
		return domain.NewSymbol(market, left)
	}
	return domain.Symbol{}, domain.ErrInvalidRequest
}

func parseCompactSymbol(raw string) (domain.Symbol, error) {
	upper := strings.ToUpper(raw)
	for _, market := range []domain.Market{domain.MarketSH, domain.MarketSZ, domain.MarketBJ, domain.MarketHK, domain.MarketUS} {
		prefix := string(market)
		if strings.HasPrefix(upper, prefix) && len(raw) > len(prefix) {
			return domain.NewSymbol(market, raw[len(prefix):])
		}
		if strings.HasSuffix(upper, prefix) && len(raw) > len(prefix) {
			return domain.NewSymbol(market, raw[:len(raw)-len(prefix)])
		}
	}
	if market, ok := inferMarketFromCode(raw); ok {
		return domain.NewSymbol(market, raw)
	}
	return domain.Symbol{}, domain.ErrInvalidRequest
}

func inferMarketFromCode(code string) (domain.Market, bool) {
	if code == "" {
		return domain.MarketUnknown, false
	}
	switch code[0] {
	case '6', '5', '9':
		return domain.MarketSH, true
	case '0', '2', '3':
		return domain.MarketSZ, true
	case '4', '8':
		return domain.MarketBJ, true
	default:
		return domain.MarketUnknown, false
	}
}

func parseMarkets(r *http.Request) ([]domain.Market, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("markets"))
	if raw == "" {
		marketRaw := strings.TrimSpace(r.URL.Query().Get("market"))
		if marketRaw == "" {
			return nil, nil
		}
		market, err := domain.ParseMarket(marketRaw)
		if err != nil {
			return nil, err
		}
		return []domain.Market{market}, nil
	}
	parts := strings.Split(raw, ",")
	markets := make([]domain.Market, 0, len(parts))
	for _, part := range parts {
		market, err := domain.ParseMarket(part)
		if err != nil {
			return nil, err
		}
		markets = append(markets, market)
	}
	return markets, nil
}

func parsePeriodQuery(r *http.Request, name string, fallback domain.Period) (domain.Period, error) {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return fallback, nil
	}
	return domain.ParsePeriod(value)
}

func parseBoolQuery(r *http.Request, name string) bool {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return false
	}
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
}

func parseUint16Query(r *http.Request, name string, fallback uint16) (uint16, error) {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0, domain.ErrInvalidRequest
	}
	return uint16(parsed), nil
}

func parseUint32Query(r *http.Request, name string, fallback uint32) (uint32, error) {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, domain.ErrInvalidRequest
	}
	return uint32(parsed), nil
}

func parseDateQuery(r *http.Request, name string, required bool) (time.Time, error) {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		if required {
			return time.Time{}, domain.ErrInvalidRequest
		}
		return time.Time{}, nil
	}
	parsed, err := time.ParseInLocation(dateLayout, value, time.Local)
	if err != nil {
		return time.Time{}, domain.ErrInvalidRequest
	}
	return domain.NormalizeDate(parsed), nil
}
