package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

const (
	defaultClientsPerHost           = 1
	defaultFailureThreshold         = 3
	defaultHalfOpenSuccessThreshold = 1
	defaultOpenTimeout              = 30 * time.Second
)

var (
	// ErrPoolClosed is returned when acquiring from a pool that is closing or closed.
	ErrPoolClosed = errors.New("source pool closed")
	// ErrInvalidLease is returned when a lease does not belong to the pool.
	ErrInvalidLease = errors.New("source pool invalid lease")
	// ErrLeaseReleased is returned when releasing a lease that is no longer active.
	ErrLeaseReleased = errors.New("source pool lease already released")
	// ErrUnknownHost is returned when reporting health for a host outside the pool.
	ErrUnknownHost = errors.New("source pool unknown host")
)

// CircuitState describes whether a source host can receive traffic.
type CircuitState uint8

const (
	// CircuitClosed is the normal healthy state.
	CircuitClosed CircuitState = iota
	// CircuitOpen prevents new acquires until the open timeout elapses.
	CircuitOpen
	// CircuitHalfOpen allows one probe acquire before closing or reopening.
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// HostConfig configures one upstream market-data host without binding to a vendor SDK.
type HostConfig struct {
	Address string
	Clients int
}

// ClientFactory creates one independent client for a host and per-host client index.
type ClientFactory[T any] func(context.Context, HostConfig, int) (T, error)

// CloseFunc closes a client when the pool is drained.
type CloseFunc[T any] func(T) error

// Config configures a source pool for one market-data interface class.
type Config[T any] struct {
	Name           string
	Hosts          []HostConfig
	ClientsPerHost int
	Factory        ClientFactory[T]
	Close          CloseFunc[T]
	// MinReadyClients allows a live pool to skip failed client creations as long
	// as at least this many clients are ready. The zero value preserves fail-fast
	// construction for deterministic tests and in-process pools.
	MinReadyClients          int
	FailureThreshold         int
	HalfOpenSuccessThreshold int
	OpenTimeout              time.Duration
}

// Pool owns independent clients across multiple hosts and leases them one at a time.
type Pool[T any] struct {
	name                     string
	closeClient              CloseFunc[T]
	failureThreshold         int
	halfOpenSuccessThreshold int
	openTimeout              time.Duration

	mu           sync.Mutex
	cond         *sync.Cond
	hosts        []*hostPool[T]
	hostIndex    map[string]*hostPool[T]
	nextHost     int
	closing      bool
	closeRunning bool
	closed       bool
	closeErr     error
}

type hostPool[T any] struct {
	config HostConfig

	clients    []*pooledClient[T]
	nextClient int
	inFlight   int

	circuit              CircuitState
	halfOpenInFlight     bool
	successCount         uint64
	failureCount         uint64
	consecutiveFailures  int
	consecutiveSuccesses int
	lastError            string
	lastFailureAt        time.Time
	lastSuccessAt        time.Time
	openedAt             time.Time
	openUntil            time.Time
}

type pooledClient[T any] struct {
	host       *hostPool[T]
	value      T
	index      int
	inUse      bool
	generation uint64
}

// Lease is an acquired client. Release must be called exactly once when the caller is done.
type Lease[T any] struct {
	Client T
	Host   string

	pool       *Pool[T]
	client     *pooledClient[T]
	generation uint64
}

// HostState is a point-in-time health view for one source host.
type HostState struct {
	Address              string
	Circuit              CircuitState
	Healthy              bool
	TotalClients         int
	IdleClients          int
	InFlight             int
	SuccessCount         uint64
	FailureCount         uint64
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastError            string
	LastFailureAt        time.Time
	LastSuccessAt        time.Time
	OpenedAt             time.Time
	OpenUntil            time.Time
}

// HealthSnapshot is a point-in-time health view for a pool.
type HealthSnapshot struct {
	Name         string
	Closing      bool
	Closed       bool
	TotalClients int
	IdleClients  int
	InFlight     int
	Hosts        []HostState
}

// NewPool creates all configured clients eagerly so startup fails before serving traffic.
func NewPool[T any](ctx context.Context, cfg Config[T]) (*Pool[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Factory == nil {
		return nil, errors.New("source pool factory is required")
	}
	if cfg.MinReadyClients < 0 {
		return nil, fmt.Errorf("source pool min ready clients must be non-negative: %d", cfg.MinReadyClients)
	}
	if len(cfg.Hosts) == 0 {
		return nil, errors.New("source pool requires at least one host")
	}
	clientsPerHost := cfg.ClientsPerHost
	if clientsPerHost == 0 {
		clientsPerHost = defaultClientsPerHost
	}
	if clientsPerHost < 0 {
		return nil, fmt.Errorf("source pool clients per host must be positive: %d", clientsPerHost)
	}
	failureThreshold := cfg.FailureThreshold
	if failureThreshold == 0 {
		failureThreshold = defaultFailureThreshold
	}
	if failureThreshold < 0 {
		return nil, fmt.Errorf("source pool failure threshold must be positive: %d", failureThreshold)
	}
	halfOpenSuccessThreshold := cfg.HalfOpenSuccessThreshold
	if halfOpenSuccessThreshold == 0 {
		halfOpenSuccessThreshold = defaultHalfOpenSuccessThreshold
	}
	if halfOpenSuccessThreshold < 0 {
		return nil, fmt.Errorf("source pool half-open success threshold must be positive: %d", halfOpenSuccessThreshold)
	}
	openTimeout := cfg.OpenTimeout
	if openTimeout == 0 {
		openTimeout = defaultOpenTimeout
	}
	if openTimeout < 0 {
		return nil, fmt.Errorf("source pool open timeout must be positive: %s", openTimeout)
	}

	pool := &Pool[T]{
		name:                     cfg.Name,
		closeClient:              cfg.Close,
		failureThreshold:         failureThreshold,
		halfOpenSuccessThreshold: halfOpenSuccessThreshold,
		openTimeout:              openTimeout,
		hostIndex:                make(map[string]*hostPool[T], len(cfg.Hosts)),
	}
	pool.cond = sync.NewCond(&pool.mu)
	log.Printf("source pool build start name=%s hosts=%d default_clients_per_host=%d", cfg.Name, len(cfg.Hosts), clientsPerHost)

	for i, hostCfg := range cfg.Hosts {
		if err := ctx.Err(); err != nil {
			pool.closeInitializedClients()
			return nil, err
		}
		if hostCfg.Address == "" {
			pool.closeInitializedClients()
			return nil, fmt.Errorf("source pool host %d address is required", i)
		}
		if _, exists := pool.hostIndex[hostCfg.Address]; exists {
			pool.closeInitializedClients()
			return nil, fmt.Errorf("source pool duplicate host address %q", hostCfg.Address)
		}
		hostClients := hostCfg.Clients
		if hostClients == 0 {
			hostClients = clientsPerHost
		}
		if hostClients < 0 {
			pool.closeInitializedClients()
			return nil, fmt.Errorf("source pool host %q clients must be positive: %d", hostCfg.Address, hostClients)
		}

		host := &hostPool[T]{config: HostConfig{Address: hostCfg.Address, Clients: hostClients}}
		pool.hosts = append(pool.hosts, host)
		pool.hostIndex[host.config.Address] = host

		for clientIndex := 0; clientIndex < hostClients; clientIndex++ {
			if err := ctx.Err(); err != nil {
				pool.closeInitializedClients()
				return nil, err
			}
			client, err := cfg.Factory(ctx, host.config, clientIndex)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					pool.closeInitializedClients()
					return nil, ctxErr
				}
				if cfg.MinReadyClients > 0 {
					log.Printf("source pool client create skipped name=%s host=%s index=%d err=%v", cfg.Name, host.config.Address, clientIndex, err)
					continue
				}
				pool.closeInitializedClients()
				return nil, fmt.Errorf("create source client for host %q index %d: %w", host.config.Address, clientIndex, err)
			}
			if err := ctx.Err(); err != nil {
				pool.closeInitializedClients()
				_ = pool.closeOne(client)
				return nil, err
			}
			host.clients = append(host.clients, &pooledClient[T]{
				host:  host,
				value: client,
				index: clientIndex,
			})
		}
	}

	if err := ctx.Err(); err != nil {
		pool.closeInitializedClients()
		return nil, err
	}
	readyClients := pool.totalClientsLocked()
	if cfg.MinReadyClients > 0 && readyClients < cfg.MinReadyClients {
		pool.closeInitializedClients()
		return nil, fmt.Errorf("source pool ready clients %d below minimum %d", readyClients, cfg.MinReadyClients)
	}

	log.Printf("source pool build ready name=%s total_clients=%d", cfg.Name, readyClients)

	return pool, nil
}

// Acquire leases one healthy idle client, waiting until a client is released, a circuit half-opens, or ctx ends.
func (p *Pool[T]) Acquire(ctx context.Context) (*Lease[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if p.closed || p.closing {
			return nil, ErrPoolClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		now := time.Now()
		client := p.nextAvailableLocked(now)
		if client != nil {
			host := client.host
			client.inUse = true
			client.generation++
			host.inFlight++
			if host.circuit == CircuitHalfOpen {
				host.halfOpenInFlight = true
			}
			return &Lease[T]{
				Client:     client.value,
				Host:       host.config.Address,
				pool:       p,
				client:     client,
				generation: client.generation,
			}, nil
		}

		p.waitLocked(ctx, p.nextWakeLocked(now))
	}
}

// Release returns a leased client to the pool.
func (p *Pool[T]) Release(lease *Lease[T]) error {
	if lease == nil || lease.pool != p || lease.client == nil {
		return ErrInvalidLease
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	client := lease.client
	if !client.inUse || client.generation != lease.generation {
		return ErrLeaseReleased
	}
	client.inUse = false
	client.host.inFlight--
	if client.host.inFlight < 0 {
		client.host.inFlight = 0
	}
	if client.host.circuit == CircuitHalfOpen {
		client.host.halfOpenInFlight = false
	}
	p.cond.Broadcast()
	return nil
}

// Release returns the lease to its pool.
func (l *Lease[T]) Release() error {
	if l == nil || l.pool == nil {
		return ErrInvalidLease
	}
	return l.pool.Release(l)
}

// ReportSuccess records a successful operation against a leased client host.
func (l *Lease[T]) ReportSuccess() error {
	if l == nil || l.pool == nil {
		return ErrInvalidLease
	}
	return l.pool.ReportSuccess(l.Host)
}

// ReportFailure records a failed operation against a leased client host.
func (l *Lease[T]) ReportFailure(err error) error {
	if l == nil || l.pool == nil {
		return ErrInvalidLease
	}
	return l.pool.ReportFailure(l.Host, err)
}

// ReportSuccess records that a host completed a request successfully.
func (p *Pool[T]) ReportSuccess(address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	host, ok := p.hostIndex[address]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownHost, address)
	}

	host.refreshCircuitLocked(time.Now())
	host.successCount++
	host.consecutiveFailures = 0
	host.lastError = ""
	host.lastSuccessAt = time.Now()

	if host.circuit == CircuitHalfOpen {
		host.halfOpenInFlight = false
		host.consecutiveSuccesses++
		if host.consecutiveSuccesses >= p.halfOpenSuccessThreshold {
			host.closeCircuitLocked()
		}
	} else if host.circuit == CircuitClosed {
		host.consecutiveSuccesses = 0
	}

	p.cond.Broadcast()
	return nil
}

// ReportFailure records that a host failed a request and opens the circuit past the configured threshold.
func (p *Pool[T]) ReportFailure(address string, err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	host, ok := p.hostIndex[address]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownHost, address)
	}

	now := time.Now()
	host.refreshCircuitLocked(now)
	host.failureCount++
	host.consecutiveFailures++
	host.consecutiveSuccesses = 0
	host.halfOpenInFlight = false
	host.lastFailureAt = now
	if err != nil {
		host.lastError = err.Error()
	} else {
		host.lastError = ""
	}

	switch host.circuit {
	case CircuitHalfOpen, CircuitOpen:
		host.openCircuitLocked(now, p.openTimeout)
	case CircuitClosed:
		if host.consecutiveFailures >= p.failureThreshold {
			host.openCircuitLocked(now, p.openTimeout)
		}
	}

	p.cond.Broadcast()
	return nil
}

// HealthSnapshot returns a consistent health snapshot of the pool and its hosts.
func (p *Pool[T]) HealthSnapshot() HealthSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	snapshot := HealthSnapshot{
		Name:    p.name,
		Closing: p.closing,
		Closed:  p.closed,
		Hosts:   make([]HostState, 0, len(p.hosts)),
	}
	for _, host := range p.hosts {
		host.refreshCircuitLocked(now)
		idle := host.idleClientsLocked()
		state := HostState{
			Address:              host.config.Address,
			Circuit:              host.circuit,
			Healthy:              host.canAcquireLocked(),
			TotalClients:         len(host.clients),
			IdleClients:          idle,
			InFlight:             host.inFlight,
			SuccessCount:         host.successCount,
			FailureCount:         host.failureCount,
			ConsecutiveFailures:  host.consecutiveFailures,
			ConsecutiveSuccesses: host.consecutiveSuccesses,
			LastError:            host.lastError,
			LastFailureAt:        host.lastFailureAt,
			LastSuccessAt:        host.lastSuccessAt,
			OpenedAt:             host.openedAt,
			OpenUntil:            host.openUntil,
		}
		snapshot.TotalClients += state.TotalClients
		snapshot.IdleClients += state.IdleClients
		snapshot.InFlight += state.InFlight
		snapshot.Hosts = append(snapshot.Hosts, state)
	}
	return snapshot
}

// Close stops new acquires, waits for checked-out clients to be released, then closes every client.
func (p *Pool[T]) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	log.Printf("source pool close start name=%s", p.name)

	p.mu.Lock()
	if p.closed {
		err := p.closeErr
		p.mu.Unlock()
		return err
	}
	if p.closeRunning {
		for !p.closed {
			if err := ctx.Err(); err != nil {
				p.mu.Unlock()
				return err
			}
			p.waitLocked(ctx, time.Time{})
		}
		err := p.closeErr
		p.mu.Unlock()
		return err
	}

	p.closing = true
	p.closeRunning = true
	p.cond.Broadcast()
	p.mu.Unlock()

	return p.finishClose(ctx)
}

func (p *Pool[T]) finishClose(ctx context.Context) error {
	p.mu.Lock()
	for p.inFlightLocked() > 0 {
		if err := ctx.Err(); err != nil {
			p.scheduleBackgroundCloseLocked()
			p.mu.Unlock()
			return err
		}
		p.waitLocked(ctx, time.Time{})
	}
	if err := ctx.Err(); err != nil {
		p.scheduleBackgroundCloseLocked()
		p.mu.Unlock()
		return err
	}

	clients := make([]T, 0, p.totalClientsLocked())
	for _, host := range p.hosts {
		for _, client := range host.clients {
			clients = append(clients, client.value)
		}
	}
	p.mu.Unlock()

	var closeErr error
	for _, client := range clients {
		closeErr = errors.Join(closeErr, p.closeOne(client))
	}

	log.Printf("source pool close done name=%s err=%v", p.name, closeErr)

	p.mu.Lock()
	p.closeErr = closeErr
	p.closed = true
	p.closing = false
	p.closeRunning = false
	p.cond.Broadcast()
	p.mu.Unlock()
	return closeErr
}

func (p *Pool[T]) scheduleBackgroundCloseLocked() {
	if p.closed {
		return
	}
	p.closing = true
	p.closeRunning = true
	p.cond.Broadcast()
	log.Printf("source pool close background drain scheduled name=%s", p.name)
	go func() {
		_ = p.finishClose(context.Background())
	}()
}

func (p *Pool[T]) nextAvailableLocked(now time.Time) *pooledClient[T] {
	if len(p.hosts) == 0 {
		return nil
	}
	for i := 0; i < len(p.hosts); i++ {
		hostIndex := (p.nextHost + i) % len(p.hosts)
		host := p.hosts[hostIndex]
		host.refreshCircuitLocked(now)
		if !host.canAcquireLocked() {
			continue
		}
		client := host.nextIdleClientLocked()
		if client == nil {
			continue
		}
		p.nextHost = (hostIndex + 1) % len(p.hosts)
		return client
	}
	return nil
}

func (p *Pool[T]) nextWakeLocked(now time.Time) time.Time {
	var wake time.Time
	for _, host := range p.hosts {
		if host.circuit != CircuitOpen || host.openUntil.IsZero() || !host.hasIdleClientLocked() {
			continue
		}
		if !host.openUntil.After(now) {
			return now
		}
		if wake.IsZero() || host.openUntil.Before(wake) {
			wake = host.openUntil
		}
	}
	return wake
}

func (p *Pool[T]) waitLocked(ctx context.Context, wake time.Time) {
	if !wake.IsZero() {
		if delay := time.Until(wake); delay <= 0 {
			return
		}
	}

	var timer *time.Timer
	if !wake.IsZero() {
		timer = time.NewTimer(time.Until(wake))
	}
	done := make(chan struct{})
	ctxDone := ctx.Done()
	if ctxDone != nil || timer != nil {
		go func() {
			if timer == nil {
				select {
				case <-ctxDone:
					p.broadcast()
				case <-done:
				}
				return
			}

			select {
			case <-ctxDone:
				p.broadcast()
			case <-timer.C:
				p.broadcast()
			case <-done:
			}
		}()
	}
	p.cond.Wait()
	close(done)
	if timer != nil {
		timer.Stop()
	}
}

func (p *Pool[T]) broadcast() {
	p.mu.Lock()
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *Pool[T]) inFlightLocked() int {
	total := 0
	for _, host := range p.hosts {
		total += host.inFlight
	}
	return total
}

func (p *Pool[T]) totalClientsLocked() int {
	total := 0
	for _, host := range p.hosts {
		total += len(host.clients)
	}
	return total
}

func (p *Pool[T]) closeInitializedClients() {
	log.Printf("source pool cleanup initialized name=%s", p.name)
	for _, host := range p.hosts {
		for _, client := range host.clients {
			_ = p.closeOne(client.value)
		}
	}
}

func (p *Pool[T]) closeOne(client T) error {
	if p.closeClient != nil {
		return p.closeClient(client)
	}
	closer, ok := any(client).(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}

func (h *hostPool[T]) canAcquireLocked() bool {
	if len(h.clients) == 0 {
		return false
	}
	switch h.circuit {
	case CircuitClosed:
		return true
	case CircuitHalfOpen:
		return !h.halfOpenInFlight && h.inFlight == 0
	default:
		return false
	}
}

func (h *hostPool[T]) nextIdleClientLocked() *pooledClient[T] {
	if len(h.clients) == 0 {
		return nil
	}
	for i := 0; i < len(h.clients); i++ {
		index := (h.nextClient + i) % len(h.clients)
		client := h.clients[index]
		if client.inUse {
			continue
		}
		h.nextClient = (index + 1) % len(h.clients)
		return client
	}
	return nil
}

func (h *hostPool[T]) hasIdleClientLocked() bool {
	return h.idleClientsLocked() > 0
}

func (h *hostPool[T]) idleClientsLocked() int {
	idle := 0
	for _, client := range h.clients {
		if !client.inUse {
			idle++
		}
	}
	return idle
}

func (h *hostPool[T]) refreshCircuitLocked(now time.Time) {
	if h.circuit != CircuitOpen || h.openUntil.IsZero() || now.Before(h.openUntil) {
		return
	}
	h.circuit = CircuitHalfOpen
	h.halfOpenInFlight = false
	h.consecutiveSuccesses = 0
}

func (h *hostPool[T]) openCircuitLocked(now time.Time, timeout time.Duration) {
	h.circuit = CircuitOpen
	h.halfOpenInFlight = false
	h.consecutiveSuccesses = 0
	h.openedAt = now
	h.openUntil = now.Add(timeout)
}

func (h *hostPool[T]) closeCircuitLocked() {
	h.circuit = CircuitClosed
	h.halfOpenInFlight = false
	h.consecutiveFailures = 0
	h.consecutiveSuccesses = 0
	h.openedAt = time.Time{}
	h.openUntil = time.Time{}
}
