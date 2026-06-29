package httpapi

import (
	"testing"
	"time"
)

func TestRateLimiterRefills(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 9, 30, 0, 0, time.UTC)
	limiter := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 2,
		Burst:             2,
		now: func() time.Time {
			return now
		},
	})
	if limiter == nil {
		t.Fatal("limiter is nil")
	}
	if !limiter.Allow() || !limiter.Allow() {
		t.Fatal("initial burst was not available")
	}
	if limiter.Allow() {
		t.Fatal("expected burst exhaustion")
	}
	now = now.Add(500 * time.Millisecond)
	if !limiter.Allow() {
		t.Fatal("expected one token after half-second refill")
	}
	if limiter.Allow() {
		t.Fatal("expected no second token after half-second refill")
	}
}

func TestNewRateLimiterDisabled(t *testing.T) {
	t.Parallel()

	if limiter := NewRateLimiter(RateLimitConfig{}); limiter != nil {
		t.Fatalf("limiter = %#v", limiter)
	}
}
