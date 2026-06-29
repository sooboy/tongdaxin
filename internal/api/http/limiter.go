package httpapi

import (
	"sync"
	"time"
)

// RateLimitConfig controls the API-level token bucket.
type RateLimitConfig struct {
	RequestsPerSecond int
	Burst             int

	now func() time.Time
}

// RateLimiter is a small in-process token bucket for API ingress limiting.
type RateLimiter struct {
	mu             sync.Mutex
	requestsPerSec float64
	capacity       float64
	tokens         float64
	lastRefill     time.Time
	now            func() time.Time
}

// NewRateLimiter creates a limiter, or returns nil when the configured rate is disabled.
func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	if cfg.RequestsPerSecond <= 0 {
		return nil
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.RequestsPerSecond
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	return &RateLimiter{
		requestsPerSec: float64(cfg.RequestsPerSecond),
		capacity:       float64(cfg.Burst),
		tokens:         float64(cfg.Burst),
		lastRefill:     cfg.now(),
		now:            cfg.now,
	}
}

// Allow consumes one token when capacity is available.
func (l *RateLimiter) Allow() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	if elapsed := now.Sub(l.lastRefill); elapsed > 0 {
		l.tokens += elapsed.Seconds() * l.requestsPerSec
		if l.tokens > l.capacity {
			l.tokens = l.capacity
		}
		l.lastRefill = now
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
