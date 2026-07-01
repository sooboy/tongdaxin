package gotdxadapter

import (
	"errors"
	"testing"

	"github.com/bensema/gotdx/proto"

	"github.com/sooboy/tongdaxin/internal/domain"
)

func TestLiveClientRecoversPanicAndRetriesAfterReconnect(t *testing.T) {
	t.Parallel()

	disconnects := 0
	client := &liveClient{
		address:    "tdx.test:7709",
		timeoutSec: 1,
		client: &fakeGotdxClient{
			stockKLine: func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
				panic("short kline packet")
			},
			disconnect: func() error {
				disconnects++
				return nil
			},
		},
		connector: func(address string, timeoutSec int) (Client, error) {
			return &fakeGotdxClient{
				stockKLine: func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
					return []proto.SecurityBar{{}}, nil
				},
			}, nil
		},
	}

	bars, err := client.StockKLine(4, 1, "600000", 0, 800, 0, 0)
	if err != nil {
		t.Fatalf("StockKLine: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("bars len = %d, want 1", len(bars))
	}
	if disconnects != 1 {
		t.Fatalf("disconnects = %d, want 1", disconnects)
	}
}

func TestLiveClientConvertsRepeatedPanicToUpstreamUnavailable(t *testing.T) {
	t.Parallel()

	client := &liveClient{
		address:    "tdx.test:7709",
		timeoutSec: 1,
		client: &fakeGotdxClient{
			stockKLine: func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
				panic("first short packet")
			},
		},
		connector: func(address string, timeoutSec int) (Client, error) {
			return &fakeGotdxClient{
				stockKLine: func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
					panic("second short packet")
				},
			}, nil
		},
	}

	_, err := client.StockKLine(4, 1, "600000", 0, 800, 0, 0)
	if !errors.Is(err, domain.ErrUpstreamUnavailable) {
		t.Fatalf("StockKLine error = %v, want ErrUpstreamUnavailable", err)
	}
}

func TestLiveMACClientRetriesAfterReconnect(t *testing.T) {
	t.Parallel()

	disconnects := 0
	client := &liveMACClient{
		address:    "mac.test:7709",
		timeoutSec: 1,
		client: &fakeMACClient{
			macServerInfo: func() (*proto.MACServerInfoReply, error) {
				return nil, errors.New("write: broken pipe")
			},
			disconnect: func() error {
				disconnects++
				return nil
			},
		},
		connector: func(address string, timeoutSec int) (macClient, error) {
			return &fakeMACClient{
				macServerInfo: func() (*proto.MACServerInfoReply, error) {
					return &proto.MACServerInfoReply{Today: "2026-06-29"}, nil
				},
			}, nil
		},
	}

	reply, err := client.MACServerInfo()
	if err != nil {
		t.Fatalf("MACServerInfo: %v", err)
	}
	if reply.Today != "2026-06-29" {
		t.Fatalf("reply = %+v", reply)
	}
	if disconnects != 1 {
		t.Fatalf("disconnects = %d, want 1", disconnects)
	}
}

func TestLiveMACClientConvertsRepeatedFailureToUpstreamUnavailable(t *testing.T) {
	t.Parallel()

	client := &liveMACClient{
		address:    "mac.test:7709",
		timeoutSec: 1,
		client: &fakeMACClient{
			macServerInfo: func() (*proto.MACServerInfoReply, error) {
				return nil, errors.New("first broken pipe")
			},
		},
		connector: func(address string, timeoutSec int) (macClient, error) {
			return &fakeMACClient{
				macServerInfo: func() (*proto.MACServerInfoReply, error) {
					return nil, errors.New("second broken pipe")
				},
			}, nil
		},
	}

	_, err := client.MACServerInfo()
	if !errors.Is(err, domain.ErrUpstreamUnavailable) {
		t.Fatalf("MACServerInfo error = %v, want ErrUpstreamUnavailable", err)
	}
}

type fakeGotdxClient struct {
	stockKLine     func(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error)
	stockFullKLine func(category uint16, market uint8, code string, times uint16, adjust uint16, fn func(kline proto.SecurityBar) bool) ([]proto.SecurityBar, error)
	stockList      func(market uint8, start uint32, count uint32) ([]proto.Security, error)
	stockAll       func(market uint8) ([]proto.Security, error)
	disconnect     func() error
}

func (f *fakeGotdxClient) StockQuotesDetail(markets []uint8, codes []string) ([]proto.SecurityQuote, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockTransaction(market uint8, code string, start uint16, count uint16) ([]proto.TransactionData, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockFullTransaction(market uint8, code string) ([]proto.TransactionData, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockHistoryTransaction(date uint32, market uint8, code string, start uint16, count uint16) ([]proto.HistoryTransactionData, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockHistoryFullTransaction(date uint32, market uint8, code string) ([]proto.HistoryTransactionData, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockHistoryTransactionWithTrans(date uint32, market uint8, code string, start uint16, count uint16) ([]proto.HistoryTransactionDataWithTrans, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockHistoryFullTransactionWithTrans(date uint32, market uint8, code string) ([]proto.HistoryTransactionDataWithTrans, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockKLine(category uint16, market uint8, code string, start uint16, count uint16, times uint16, adjust uint16) ([]proto.SecurityBar, error) {
	if f.stockKLine != nil {
		return f.stockKLine(category, market, code, start, count, times, adjust)
	}
	return nil, nil
}

func (f *fakeGotdxClient) StockFullKLine(category uint16, market uint8, code string, times uint16, adjust uint16, fn func(kline proto.SecurityBar) bool) ([]proto.SecurityBar, error) {
	if f.stockFullKLine != nil {
		return f.stockFullKLine(category, market, code, times, adjust, fn)
	}
	return nil, nil
}

func (f *fakeGotdxClient) GetXDXRInfo(market uint8, code string) (*proto.GetXDXRInfoReply, error) {
	return nil, nil
}

func (f *fakeGotdxClient) StockList(market uint8, start uint32, count uint32) ([]proto.Security, error) {
	if f.stockList != nil {
		return f.stockList(market, start, count)
	}
	return nil, nil
}

func (f *fakeGotdxClient) StockAll(market uint8) ([]proto.Security, error) {
	if f.stockAll != nil {
		return f.stockAll(market)
	}
	return nil, nil
}

func (f *fakeGotdxClient) GetFinanceInfo(market uint8, code string) (*proto.GetFinanceInfoReply, error) {
	return nil, nil
}

func (f *fakeGotdxClient) Disconnect() error {
	if f.disconnect != nil {
		return f.disconnect()
	}
	return nil
}

var _ Client = (*fakeGotdxClient)(nil)

type fakeMACClient struct {
	macServerInfo func() (*proto.MACServerInfoReply, error)
	disconnect    func() error
}

func (f *fakeMACClient) MACServerInfo() (*proto.MACServerInfoReply, error) {
	if f.macServerInfo != nil {
		return f.macServerInfo()
	}
	return nil, nil
}

func (f *fakeMACClient) Disconnect() error {
	if f.disconnect != nil {
		return f.disconnect()
	}
	return nil
}

var _ macClient = (*fakeMACClient)(nil)
