package bootstrap

import (
	"context"
	"sync"
	"time"

	"github.com/sooboy/tongdaxin/internal/domain"
)

type providerRuntime struct {
	mu             sync.RWMutex
	provider       domain.MarketDataProvider
	closeFn        func(context.Context) error
	closeRequested bool
	done           chan struct{}
	doneOnce       sync.Once
	closeOnce      sync.Once
	closeErr       error
	closeTimeout   time.Duration
}

func newProviderRuntime(closeTimeout time.Duration) *providerRuntime {
	return &providerRuntime{done: make(chan struct{}), closeTimeout: closeTimeout}
}

func (r *providerRuntime) finish(provider domain.MarketDataProvider, closeFn func(context.Context) error) {
	if r == nil {
		return
	}
	closeAfterFinish := false
	r.mu.Lock()
	if r.closeRequested {
		closeAfterFinish = closeFn != nil
	}
	r.provider = provider
	r.closeFn = closeFn
	r.mu.Unlock()
	r.doneOnce.Do(func() {
		close(r.done)
	})
	if closeAfterFinish {
		closeCtx := context.Background()
		if r.closeTimeout > 0 {
			var cancel context.CancelFunc
			closeCtx, cancel = context.WithTimeout(context.Background(), r.closeTimeout)
			defer cancel()
		}
		_ = r.Close(closeCtx)
	}
}

func (r *providerRuntime) current() domain.MarketDataProvider {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closeRequested {
		return nil
	}
	return r.provider
}

func providerRuntimeCall[T any](r *providerRuntime, fn func(domain.MarketDataProvider) (T, error)) (T, error) {
	provider := r.current()
	if provider == nil {
		var zero T
		return zero, domain.ErrUpstreamUnavailable
	}
	return fn(provider)
}

func (r *providerRuntime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	r.closeRequested = true
	r.mu.Unlock()
	select {
	case <-r.done:
	default:
		select {
		case <-r.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.closeOnce.Do(func() {
		r.mu.Lock()
		closeFn := r.closeFn
		r.provider = nil
		r.closeFn = nil
		r.mu.Unlock()
		var closeErr error
		if closeFn != nil {
			closeErr = closeFn(ctx)
		}
		r.mu.Lock()
		r.closeErr = closeErr
		r.mu.Unlock()
	})
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closeErr
}

func (r *providerRuntime) GetQuotes(ctx context.Context, symbols []domain.Symbol) ([]domain.Quote, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.Quote, error) {
		return provider.GetQuotes(ctx, symbols)
	})
}

func (r *providerRuntime) GetOrderBook(ctx context.Context, symbols []domain.Symbol) ([]domain.OrderBook, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.OrderBook, error) {
		return provider.GetOrderBook(ctx, symbols)
	})
}

func (r *providerRuntime) GetTicks(ctx context.Context, symbol domain.Symbol, req domain.TickRequest) ([]domain.Tick, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.Tick, error) {
		return provider.GetTicks(ctx, symbol, req)
	})
}

func (r *providerRuntime) GetHistoryTicks(ctx context.Context, symbol domain.Symbol, req domain.HistoryTickRequest) ([]domain.Tick, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.Tick, error) {
		return provider.GetHistoryTicks(ctx, symbol, req)
	})
}

func (r *providerRuntime) GetKLine(ctx context.Context, symbol domain.Symbol, req domain.KLineRequest) ([]domain.Bar, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.Bar, error) {
		return provider.GetKLine(ctx, symbol, req)
	})
}

func (r *providerRuntime) GetAdjustedKLine(ctx context.Context, symbol domain.Symbol, req domain.AdjustedKLineRequest) ([]domain.Bar, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.Bar, error) {
		return provider.GetAdjustedKLine(ctx, symbol, req)
	})
}

func (r *providerRuntime) GetXDXR(ctx context.Context, symbol domain.Symbol) ([]domain.XDXREvent, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.XDXREvent, error) {
		return provider.GetXDXR(ctx, symbol)
	})
}

func (r *providerRuntime) GetSecurityInfo(ctx context.Context, req domain.SecurityQuery) ([]domain.SecurityInfo, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) ([]domain.SecurityInfo, error) {
		return provider.GetSecurityInfo(ctx, req)
	})
}

func (r *providerRuntime) GetFinance(ctx context.Context, symbol domain.Symbol) (*domain.FinanceInfo, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) (*domain.FinanceInfo, error) {
		return provider.GetFinance(ctx, symbol)
	})
}

func (r *providerRuntime) GetTradingDay(ctx context.Context) (*domain.TradingDayInfo, error) {
	return providerRuntimeCall(r, func(provider domain.MarketDataProvider) (*domain.TradingDayInfo, error) {
		return provider.GetTradingDay(ctx)
	})
}

var _ domain.MarketDataProvider = (*providerRuntime)(nil)
