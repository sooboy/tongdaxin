package gotdxadapter

import (
	"fmt"
	"strings"
	"time"

	"github.com/bensema/gotdx/proto"
	gotdxtypes "github.com/bensema/gotdx/types"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func marketToGotdx(market domain.Market) (uint8, error) {
	switch domain.NormalizeMarket(market) {
	case domain.MarketSZ:
		return gotdxtypes.MarketSZ.Uint8(), nil
	case domain.MarketSH:
		return gotdxtypes.MarketSH.Uint8(), nil
	case domain.MarketBJ:
		return gotdxtypes.MarketBJ.Uint8(), nil
	case domain.MarketHK:
		return gotdxtypes.MarketHK.Uint8(), nil
	case domain.MarketUS:
		return gotdxtypes.MarketUSA.Uint8(), nil
	default:
		return 0, domain.ErrInvalidRequest
	}
}

func marketFromGotdx(market uint8) domain.Market {
	switch gotdxtypes.Market(market) {
	case gotdxtypes.MarketSZ:
		return domain.MarketSZ
	case gotdxtypes.MarketSH:
		return domain.MarketSH
	case gotdxtypes.MarketBJ:
		return domain.MarketBJ
	case gotdxtypes.MarketHK:
		return domain.MarketHK
	case gotdxtypes.MarketUSA:
		return domain.MarketUS
	default:
		return domain.MarketUnknown
	}
}

func periodToGotdx(period domain.Period) (uint16, error) {
	switch period {
	case domain.Period1Min:
		return gotdxtypes.KLINE_TYPE_1MIN, nil
	case domain.Period5Min:
		return gotdxtypes.KLINE_TYPE_5MIN, nil
	case domain.Period15Min:
		return gotdxtypes.KLINE_TYPE_15MIN, nil
	case domain.Period30Min:
		return gotdxtypes.KLINE_TYPE_30MIN, nil
	case domain.Period1Hour:
		return gotdxtypes.KLINE_TYPE_1HOUR, nil
	case domain.PeriodDay:
		return gotdxtypes.KLINE_TYPE_DAILY, nil
	case domain.PeriodWeek:
		return gotdxtypes.KLINE_TYPE_WEEKLY, nil
	case domain.PeriodMonth:
		return gotdxtypes.KLINE_TYPE_MONTHLY, nil
	case domain.PeriodQuarter:
		return gotdxtypes.KLINE_TYPE_3MONTH, nil
	case domain.PeriodYear:
		return gotdxtypes.KLINE_TYPE_YEARLY, nil
	default:
		return 0, domain.ErrInvalidRequest
	}
}

func adjustToGotdx(adjust domain.AdjustType) (uint16, error) {
	switch adjust {
	case "", domain.AdjustNone:
		return gotdxtypes.AdjustNone, nil
	case domain.AdjustQFQ:
		return gotdxtypes.AdjustQFQ, nil
	case domain.AdjustHFQ:
		return gotdxtypes.AdjustHFQ, nil
	default:
		return 0, domain.ErrInvalidRequest
	}
}

func quoteToDomain(item proto.SecurityQuote, sourceTime time.Time) domain.Quote {
	symbol := domain.Symbol{Market: marketFromGotdx(item.Market), Code: strings.TrimSpace(item.Code)}
	return domain.Quote{
		Symbol:     symbol,
		LastPrice:  firstNonZero(item.Price, item.Close),
		Open:       item.Open,
		High:       item.High,
		Low:        item.Low,
		PreClose:   firstNonZero(item.PreClose, item.LastClose),
		Volume:     int64(item.Vol),
		Amount:     item.Amount,
		BidLevels:  levelsToDomain(item.BidLevels),
		AskLevels:  levelsToDomain(item.AskLevels),
		SourceTime: sourceTime,
	}
}

func quoteToOrderBook(item proto.SecurityQuote, sourceTime time.Time) domain.OrderBook {
	return domain.OrderBook{
		Symbol:     domain.Symbol{Market: marketFromGotdx(item.Market), Code: strings.TrimSpace(item.Code)},
		BidLevels:  levelsToDomain(item.BidLevels),
		AskLevels:  levelsToDomain(item.AskLevels),
		SourceTime: sourceTime,
	}
}

func levelsToDomain(levels []proto.Level) []domain.Level {
	out := make([]domain.Level, 0, len(levels))
	for _, level := range levels {
		out = append(out, domain.Level{Price: level.Price, Volume: int64(level.Vol)})
	}
	return out
}

func transactionToDomain(symbol domain.Symbol, item proto.TransactionData, seq int64, now time.Time) domain.Tick {
	tradeTime := parseIntradayTime(now, item.Time)
	return domain.Tick{
		Symbol:    symbol,
		TradeDate: domain.NormalizeDate(tradeTime),
		TradeTime: tradeTime,
		Price:     item.Price,
		Volume:    int64(item.Vol),
		Amount:    item.Price * float64(item.Vol),
		Direction: actionToDirection(item.Action),
		Sequence:  seq,
		Source:    "gotdx",
	}
}

func historyTransactionToDomain(symbol domain.Symbol, item proto.HistoryTransactionData, seq int64) domain.Tick {
	return domain.Tick{
		Symbol:    symbol,
		TradeDate: domain.NormalizeDate(item.Time),
		TradeTime: item.Time,
		Price:     item.Price,
		Volume:    int64(item.Vol),
		Amount:    item.Price * float64(item.Vol),
		Direction: actionToDirection(item.Action),
		Sequence:  seq,
		Source:    "gotdx",
	}
}

func historyTransactionWithTransToDomain(symbol domain.Symbol, item proto.HistoryTransactionDataWithTrans, seq int64) domain.Tick {
	return domain.Tick{
		Symbol:    symbol,
		TradeDate: domain.NormalizeDate(item.Time),
		TradeTime: item.Time,
		Price:     item.Price,
		Volume:    int64(item.Vol),
		Amount:    item.Price * float64(item.Vol),
		Direction: actionToDirection(item.Action),
		Sequence:  seq,
		Source:    "gotdx",
	}
}

func barToDomain(symbol domain.Symbol, period domain.Period, adjust domain.AdjustType, item proto.SecurityBar) domain.Bar {
	return domain.Bar{
		Symbol:     symbol,
		Period:     period,
		AdjustType: adjust,
		Time:       item.DateTime,
		Open:       item.Open,
		High:       item.High,
		Low:        item.Low,
		Close:      item.Close,
		Volume:     item.Vol,
		Amount:     item.Amount,
		Source:     "gotdx",
	}
}

func xdxrToDomain(symbol domain.Symbol, item proto.XDXRItem) domain.XDXREvent {
	raw := map[string]any{
		"category": item.Category,
		"name":     item.Name,
	}
	putFloat32(raw, "fenhong", item.Fenhong)
	putFloat32(raw, "peigujia", item.Peigujia)
	putFloat32(raw, "songzhuangu", item.Songzhuangu)
	putFloat32(raw, "peigu", item.Peigu)
	putFloat32(raw, "suogu", item.Suogu)
	putFloat32(raw, "xingquanjia", item.Xingquanjia)
	putFloat32(raw, "fenshu", item.Fenshu)
	putFloat32(raw, "pre_float_shares", item.PreFloatShares)
	putFloat32(raw, "pre_total_shares", item.PreTotalShares)
	putFloat32(raw, "post_float_shares", item.PostFloatShares)
	putFloat32(raw, "post_total_shares", item.PostTotalShares)

	event := domain.XDXREvent{
		Symbol:    symbol,
		EventDate: domain.NormalizeDate(item.Date),
		EventType: item.Name,
		RawFields: raw,
	}
	if item.Fenhong != nil {
		event.CashDividend = float64(*item.Fenhong)
	}
	if item.Songzhuangu != nil {
		event.BonusShare = float64(*item.Songzhuangu)
	}
	if item.Peigujia != nil {
		event.AllotmentPrice = float64(*item.Peigujia)
	}
	if item.Peigu != nil {
		event.AllotmentRatio = float64(*item.Peigu)
	}
	return event
}

func securityToDomain(market domain.Market, item proto.Security) domain.SecurityInfo {
	return domain.SecurityInfo{
		Symbol: domain.Symbol{
			Market: market,
			Code:   strings.TrimSpace(item.Code),
			Name:   strings.TrimSpace(item.Name),
			Status: domain.SecurityStatusActive,
		},
		Fields: map[string]any{
			"vol_unit":      item.VolUnit,
			"decimal_point": item.DecimalPoint,
			"pre_close":     item.PreClose,
		},
	}
}

func financeToDomain(symbol domain.Symbol, item *proto.GetFinanceInfoReply) *domain.FinanceInfo {
	if item == nil {
		return nil
	}
	return &domain.FinanceInfo{
		Symbol: symbol,
		Fields: map[string]any{
			"float_shares":         item.FloatShares,
			"total_shares":         item.TotalShares,
			"eps":                  item.EPS,
			"total_assets":         item.TotalAssets,
			"operating_revenue":    item.OperatingRevenue,
			"operating_profit":     item.OperatingProfit,
			"net_profit":           item.NetProfit,
			"net_assets_per_share": item.NetAssetsPerShare,
			"updated_date":         item.UpdatedDate,
			"ipo_date":             item.IPODate,
			"shareholder_count":    item.ShareholderCount,
			"undistributed_profit": item.UndistributedProfit,
		},
	}
}

func actionToDirection(action string) domain.TickDirection {
	switch strings.ToUpper(strings.TrimSpace(action)) {
	case "BUY":
		return domain.TickDirectionBuy
	case "SELL":
		return domain.TickDirectionSell
	case "NEUTRAL":
		return domain.TickDirectionNeutral
	default:
		return domain.TickDirectionUnknown
	}
}

func parseIntradayTime(base time.Time, hhmm string) time.Time {
	date := domain.NormalizeDate(base)
	if date.IsZero() {
		date = domain.NormalizeDate(time.Now())
	}
	var hour, minute int
	_, _ = fmt.Sscanf(hhmm, "%d:%d", &hour, &minute)
	y, m, d := date.Date()
	return time.Date(y, m, d, hour, minute, 0, 0, date.Location())
}

func putFloat32(fields map[string]any, key string, value *float32) {
	if value != nil {
		fields[key] = float64(*value)
	}
}

func firstNonZero(values ...float64) float64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
