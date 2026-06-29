package httpapi

import (
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

type symbolDTO struct {
	Market string `json:"market"`
	Code   string `json:"code"`
	Name   string `json:"name,omitempty"`
	Type   string `json:"type,omitempty"`
	Status string `json:"status,omitempty"`
}

type levelDTO struct {
	Price  float64 `json:"price"`
	Volume int64   `json:"volume"`
}

type quoteDTO struct {
	Symbol     symbolDTO  `json:"symbol"`
	LastPrice  float64    `json:"last_price"`
	Open       float64    `json:"open"`
	High       float64    `json:"high"`
	Low        float64    `json:"low"`
	PreClose   float64    `json:"pre_close"`
	Volume     int64      `json:"volume"`
	Amount     float64    `json:"amount"`
	BidLevels  []levelDTO `json:"bid_levels,omitempty"`
	AskLevels  []levelDTO `json:"ask_levels,omitempty"`
	QuoteTime  string     `json:"quote_time,omitempty"`
	SourceTime string     `json:"source_time,omitempty"`
	Cached     bool       `json:"cached"`
}

type orderBookDTO struct {
	Symbol     symbolDTO  `json:"symbol"`
	BidLevels  []levelDTO `json:"bid_levels"`
	AskLevels  []levelDTO `json:"ask_levels"`
	SourceTime string     `json:"source_time,omitempty"`
	Cached     bool       `json:"cached"`
}

type tickDTO struct {
	Symbol    symbolDTO `json:"symbol"`
	TradeDate string    `json:"trade_date"`
	TradeTime string    `json:"trade_time"`
	Price     float64   `json:"price"`
	Volume    int64     `json:"volume"`
	Amount    float64   `json:"amount"`
	Direction string    `json:"direction"`
	Sequence  int64     `json:"sequence"`
	Source    string    `json:"source,omitempty"`
	Cached    bool      `json:"cached"`
}

type barDTO struct {
	Symbol     symbolDTO `json:"symbol"`
	Period     string    `json:"period"`
	AdjustType string    `json:"adjust_type"`
	Time       string    `json:"time"`
	Open       float64   `json:"open"`
	High       float64   `json:"high"`
	Low        float64   `json:"low"`
	Close      float64   `json:"close"`
	Volume     float64   `json:"volume"`
	Amount     float64   `json:"amount"`
	Source     string    `json:"source,omitempty"`
}

type xdxrDTO struct {
	Symbol         symbolDTO      `json:"symbol"`
	EventDate      string         `json:"event_date"`
	EventType      string         `json:"event_type"`
	CashDividend   float64        `json:"cash_dividend"`
	BonusShare     float64        `json:"bonus_share"`
	AllotmentPrice float64        `json:"allotment_price"`
	AllotmentRatio float64        `json:"allotment_ratio"`
	RawFields      map[string]any `json:"raw_fields,omitempty"`
}

type securityDTO struct {
	Symbol symbolDTO      `json:"symbol"`
	Fields map[string]any `json:"fields,omitempty"`
	Cached bool           `json:"cached"`
}

type tradingSessionDTO struct {
	OpenMinutes  uint16 `json:"open_minutes"`
	CloseMinutes uint16 `json:"close_minutes"`
	Open         string `json:"open"`
	Close        string `json:"close"`
}

type tradingDayDTO struct {
	Today                    string              `json:"today"`
	IsTodayTradingDay        bool                `json:"is_today_trading_day"`
	LatestTradingDay         string              `json:"latest_trading_day"`
	PreviousTradingDay       string              `json:"previous_trading_day"`
	TradingSessions          []tradingSessionDTO `json:"trading_sessions,omitempty"`
	AlternateTradingSessions []tradingSessionDTO `json:"alternate_trading_sessions,omitempty"`
	SourceTime               string              `json:"source_time,omitempty"`
	Cached                   bool                `json:"cached"`
}

type financeDTO struct {
	Symbol     symbolDTO      `json:"symbol"`
	Fields     map[string]any `json:"fields,omitempty"`
	SourceTime string         `json:"source_time,omitempty"`
	Cached     bool           `json:"cached"`
}

func mapQuotes(items []domain.Quote) []quoteDTO {
	out := make([]quoteDTO, 0, len(items))
	for _, item := range items {
		out = append(out, quoteDTO{
			Symbol:     mapSymbol(item.Symbol),
			LastPrice:  item.LastPrice,
			Open:       item.Open,
			High:       item.High,
			Low:        item.Low,
			PreClose:   item.PreClose,
			Volume:     item.Volume,
			Amount:     item.Amount,
			BidLevels:  mapLevels(item.BidLevels),
			AskLevels:  mapLevels(item.AskLevels),
			QuoteTime:  formatTime(item.QuoteTime),
			SourceTime: formatTime(item.SourceTime),
			Cached:     item.Cached,
		})
	}
	return out
}

func mapOrderBooks(items []domain.OrderBook) []orderBookDTO {
	out := make([]orderBookDTO, 0, len(items))
	for _, item := range items {
		out = append(out, orderBookDTO{
			Symbol:     mapSymbol(item.Symbol),
			BidLevels:  mapLevels(item.BidLevels),
			AskLevels:  mapLevels(item.AskLevels),
			SourceTime: formatTime(item.SourceTime),
			Cached:     item.Cached,
		})
	}
	return out
}

func mapTicks(items []domain.Tick) []tickDTO {
	out := make([]tickDTO, 0, len(items))
	for _, item := range items {
		out = append(out, tickDTO{
			Symbol:    mapSymbol(item.Symbol),
			TradeDate: formatDate(item.TradeDate),
			TradeTime: formatTime(item.TradeTime),
			Price:     item.Price,
			Volume:    item.Volume,
			Amount:    item.Amount,
			Direction: string(item.Direction),
			Sequence:  item.Sequence,
			Source:    item.Source,
			Cached:    item.Cached,
		})
	}
	return out
}

func mapBars(items []domain.Bar) []barDTO {
	out := make([]barDTO, 0, len(items))
	for _, item := range items {
		out = append(out, barDTO{
			Symbol:     mapSymbol(item.Symbol),
			Period:     string(item.Period),
			AdjustType: string(item.AdjustType),
			Time:       formatTime(item.Time),
			Open:       item.Open,
			High:       item.High,
			Low:        item.Low,
			Close:      item.Close,
			Volume:     item.Volume,
			Amount:     item.Amount,
			Source:     item.Source,
		})
	}
	return out
}

func mapXDXR(items []domain.XDXREvent) []xdxrDTO {
	out := make([]xdxrDTO, 0, len(items))
	for _, item := range items {
		out = append(out, xdxrDTO{
			Symbol:         mapSymbol(item.Symbol),
			EventDate:      formatDate(item.EventDate),
			EventType:      item.EventType,
			CashDividend:   item.CashDividend,
			BonusShare:     item.BonusShare,
			AllotmentPrice: item.AllotmentPrice,
			AllotmentRatio: item.AllotmentRatio,
			RawFields:      item.RawFields,
		})
	}
	return out
}

func mapSecurities(items []domain.SecurityInfo) []securityDTO {
	out := make([]securityDTO, 0, len(items))
	for _, item := range items {
		out = append(out, securityDTO{Symbol: mapSymbol(item.Symbol), Fields: item.Fields, Cached: item.Cached})
	}
	return out
}

func mapTradingDay(info *domain.TradingDayInfo) *tradingDayDTO {
	if info == nil {
		return nil
	}
	return &tradingDayDTO{
		Today:                    firstNonEmpty(info.TodayString, formatDate(info.Today)),
		IsTodayTradingDay:        info.IsTodayTradingDay,
		LatestTradingDay:         firstNonEmpty(info.LatestTradingDayString, formatDate(info.LatestTradingDay)),
		PreviousTradingDay:       firstNonEmpty(info.PreviousTradingDayString, formatDate(info.PreviousTradingDay)),
		TradingSessions:          mapTradingSessions(info.TradingSessions),
		AlternateTradingSessions: mapTradingSessions(info.AlternateTradingSessions),
		SourceTime:               formatTime(info.SourceTime),
		Cached:                   info.Cached,
	}
}

func mapTradingSessions(items []domain.TradingSession) []tradingSessionDTO {
	out := make([]tradingSessionDTO, 0, len(items))
	for _, item := range items {
		out = append(out, tradingSessionDTO{OpenMinutes: item.OpenMinutes, CloseMinutes: item.CloseMinutes, Open: item.Open, Close: item.Close})
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func mapFinance(item *domain.FinanceInfo) *financeDTO {
	if item == nil {
		return nil
	}
	return &financeDTO{Symbol: mapSymbol(item.Symbol), Fields: item.Fields, SourceTime: formatTime(item.SourceTime), Cached: item.Cached}
}

func mapSymbol(symbol domain.Symbol) symbolDTO {
	return symbolDTO{
		Market: string(domain.NormalizeMarket(symbol.Market)),
		Code:   domain.NormalizeCode(symbol.Code),
		Name:   symbol.Name,
		Type:   string(symbol.Type),
		Status: string(symbol.Status),
	}
}

func mapLevels(items []domain.Level) []levelDTO {
	out := make([]levelDTO, 0, len(items))
	for _, item := range items {
		out = append(out, levelDTO{Price: item.Price, Volume: item.Volume})
	}
	return out
}

func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return domain.NormalizeDate(t).Format(dateLayout)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
