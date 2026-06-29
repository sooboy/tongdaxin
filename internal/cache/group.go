package cache

import (
	"context"
	"sync"
)

// Group merges concurrent cache-miss loads for the same key.
type Group[T any] struct {
	mu    sync.Mutex
	calls map[string]*call[T]
}

type call[T any] struct {
	wg    sync.WaitGroup
	value T
	err   error
}

func NewGroup[T any]() *Group[T] {
	return &Group[T]{calls: make(map[string]*call[T])}
}

func (g *Group[T]) Do(ctx context.Context, key string, fn func(context.Context) (T, error)) (T, bool, error) {
	if g == nil {
		value, err := fn(ctx)
		return value, false, err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, false, err
		}
	}
	if key == "" {
		value, err := fn(ctx)
		return value, false, err
	}

	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*call[T])
	}
	if existing := g.calls[key]; existing != nil {
		g.mu.Unlock()
		existing.wg.Wait()
		return existing.value, true, existing.err
	}
	current := &call[T]{}
	current.wg.Add(1)
	g.calls[key] = current
	g.mu.Unlock()

	current.value, current.err = fn(ctx)
	current.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	return current.value, false, current.err
}
