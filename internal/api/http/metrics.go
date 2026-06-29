package httpapi

import (
	"sync"
	"time"
)

// Metrics records lightweight HTTP API counters without adding a runtime dependency.
type Metrics struct {
	mu        sync.Mutex
	startedAt time.Time
	now       func() time.Time
	inFlight  int
	routes    map[string]*routeMetrics
}

type routeMetrics struct {
	requests            uint64
	errors              uint64
	status1xx           uint64
	status2xx           uint64
	status3xx           uint64
	status4xx           uint64
	status5xx           uint64
	statusOther         uint64
	lastStatus          int
	totalDurationMillis float64
	lastDurationMillis  float64
	maxDurationMillis   float64
}

// MetricsSnapshot is the JSON shape returned by /api/v1/metrics.
type MetricsSnapshot struct {
	StartedAt     time.Time                       `json:"started_at"`
	UptimeSeconds int64                           `json:"uptime_seconds"`
	InFlight      int                             `json:"in_flight"`
	RequestsTotal uint64                          `json:"requests_total"`
	ErrorsTotal   uint64                          `json:"errors_total"`
	Routes        map[string]RouteMetricsSnapshot `json:"routes"`
}

// RouteMetricsSnapshot is a point-in-time copy of one API route's counters.
type RouteMetricsSnapshot struct {
	Requests            uint64  `json:"requests"`
	Errors              uint64  `json:"errors"`
	Status1xx           uint64  `json:"status_1xx"`
	Status2xx           uint64  `json:"status_2xx"`
	Status3xx           uint64  `json:"status_3xx"`
	Status4xx           uint64  `json:"status_4xx"`
	Status5xx           uint64  `json:"status_5xx"`
	StatusOther         uint64  `json:"status_other"`
	LastStatus          int     `json:"last_status"`
	TotalDurationMillis float64 `json:"total_duration_ms"`
	LastDurationMillis  float64 `json:"last_duration_ms"`
	MaxDurationMillis   float64 `json:"max_duration_ms"`
}

// NewMetrics creates an API metrics collector.
func NewMetrics() *Metrics {
	return newMetrics(time.Now)
}

func newMetrics(now func() time.Time) *Metrics {
	if now == nil {
		now = time.Now
	}
	return &Metrics{
		startedAt: now(),
		now:       now,
		routes:    make(map[string]*routeMetrics),
	}
}

func (m *Metrics) start(route string) func(int) {
	if m == nil {
		return func(int) {}
	}
	started := m.now()
	m.mu.Lock()
	m.inFlight++
	m.mu.Unlock()

	return func(status int) {
		m.observe(route, status, m.now().Sub(started))
	}
}

func (m *Metrics) observe(route string, status int, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inFlight--
	if m.inFlight < 0 {
		m.inFlight = 0
	}
	stats := m.routes[route]
	if stats == nil {
		stats = &routeMetrics{}
		m.routes[route] = stats
	}
	stats.requests++
	if status >= 400 {
		stats.errors++
	}
	switch status / 100 {
	case 1:
		stats.status1xx++
	case 2:
		stats.status2xx++
	case 3:
		stats.status3xx++
	case 4:
		stats.status4xx++
	case 5:
		stats.status5xx++
	default:
		stats.statusOther++
	}
	millis := float64(duration) / float64(time.Millisecond)
	stats.lastStatus = status
	stats.lastDurationMillis = millis
	stats.totalDurationMillis += millis
	if millis > stats.maxDurationMillis {
		stats.maxDurationMillis = millis
	}
}

// Snapshot returns a concurrency-safe metrics copy for API output.
func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{Routes: map[string]RouteMetricsSnapshot{}}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	snapshot := MetricsSnapshot{
		StartedAt:     m.startedAt,
		UptimeSeconds: int64(now.Sub(m.startedAt).Seconds()),
		InFlight:      m.inFlight,
		Routes:        make(map[string]RouteMetricsSnapshot, len(m.routes)),
	}
	for route, stats := range m.routes {
		routeSnapshot := RouteMetricsSnapshot{
			Requests:            stats.requests,
			Errors:              stats.errors,
			Status1xx:           stats.status1xx,
			Status2xx:           stats.status2xx,
			Status3xx:           stats.status3xx,
			Status4xx:           stats.status4xx,
			Status5xx:           stats.status5xx,
			StatusOther:         stats.statusOther,
			LastStatus:          stats.lastStatus,
			TotalDurationMillis: stats.totalDurationMillis,
			LastDurationMillis:  stats.lastDurationMillis,
			MaxDurationMillis:   stats.maxDurationMillis,
		}
		snapshot.Routes[route] = routeSnapshot
		snapshot.RequestsTotal += routeSnapshot.Requests
		snapshot.ErrorsTotal += routeSnapshot.Errors
	}
	return snapshot
}
