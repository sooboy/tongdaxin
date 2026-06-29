package source

import (
	"context"
	"errors"
	"testing"
	"time"
)

type testClient struct {
	host  string
	index int
}

func TestPoolAcquireReleaseRoundRobin(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, Config[testClient]{
		Name:           "quote-pool",
		Hosts:          []HostConfig{{Address: "a", Clients: 1}, {Address: "b", Clients: 1}},
		ClientsPerHost: 1,
	})
	defer pool.Close(context.Background())

	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire first: %v", err)
	}
	if first.Host != "a" {
		t.Fatalf("first host = %q, want a", first.Host)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release first: %v", err)
	}

	second, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire second: %v", err)
	}
	if second.Host != "b" {
		t.Fatalf("second host = %q, want b", second.Host)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("Release second: %v", err)
	}
}

func TestPoolSkipsOpenCircuitAndHalfOpenAfterTimeout(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, Config[testClient]{
		Hosts:            []HostConfig{{Address: "a", Clients: 1}, {Address: "b", Clients: 1}},
		FailureThreshold: 1,
		OpenTimeout:      20 * time.Millisecond,
	})
	defer pool.Close(context.Background())

	if err := pool.ReportFailure("a", errors.New("boom")); err != nil {
		t.Fatalf("ReportFailure: %v", err)
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire after failure: %v", err)
	}
	if lease.Host != "b" {
		t.Fatalf("host after a opened = %q, want b", lease.Host)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	lease, err = pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire half-open: %v", err)
	}
	if lease.Host != "a" {
		t.Fatalf("half-open host = %q, want a", lease.Host)
	}
	if err := lease.ReportSuccess(); err != nil {
		t.Fatalf("ReportSuccess: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release half-open: %v", err)
	}

	snapshot := pool.HealthSnapshot()
	if snapshot.Hosts[0].Circuit != CircuitClosed {
		t.Fatalf("host a circuit = %s, want closed", snapshot.Hosts[0].Circuit)
	}
}

func TestPoolCloseWaitsForReleaseAndRejectsAcquire(t *testing.T) {
	t.Parallel()

	closed := make(chan testClient, 1)
	pool := newTestPool(t, Config[testClient]{
		Hosts: []HostConfig{{Address: "a", Clients: 1}},
		Close: func(client testClient) error {
			closed <- client
			return nil
		},
	})

	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- pool.Close(context.Background()) }()

	select {
	case err := <-done:
		t.Fatalf("Close returned before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Acquire during close error = %v, want ErrPoolClosed", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case client := <-closed:
		if client.host != "a" {
			t.Fatalf("closed host = %q", client.host)
		}
	default:
		t.Fatalf("client was not closed")
	}
}

func TestPoolCloseTimeoutBackgroundDrainsAfterRelease(t *testing.T) {
	t.Parallel()

	closed := make(chan testClient, 1)
	pool := newTestPool(t, Config[testClient]{
		Hosts: []HostConfig{{Address: "a", Clients: 1}},
		Close: func(client testClient) error {
			closed <- client
			return nil
		},
	})

	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := pool.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close timeout error = %v, want context.DeadlineExceeded", err)
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Acquire after close timeout error = %v, want ErrPoolClosed", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	select {
	case client := <-closed:
		if client.host != "a" {
			t.Fatalf("closed host = %q", client.host)
		}
	case <-time.After(time.Second):
		t.Fatalf("client was not closed by background drain")
	}

	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close after background drain: %v", err)
	}
}

func TestNewPoolSkipsFactoryFailuresWhenMinReadySatisfied(t *testing.T) {
	t.Parallel()

	created := 0
	pool, err := NewPool[testClient](context.Background(), Config[testClient]{
		Hosts:           []HostConfig{{Address: "a", Clients: 1}, {Address: "b", Clients: 1}, {Address: "c", Clients: 1}},
		MinReadyClients: 2,
		Factory: func(ctx context.Context, host HostConfig, index int) (testClient, error) {
			if host.Address == "b" {
				return testClient{}, errors.New("dial failed")
			}
			created++
			return testClient{host: host.Address, index: index}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close(context.Background())
	if created != 2 {
		t.Fatalf("created clients = %d, want 2", created)
	}
	snapshot := pool.HealthSnapshot()
	if snapshot.TotalClients != 2 || len(snapshot.Hosts) != 3 || snapshot.Hosts[1].TotalClients != 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}

	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire first: %v", err)
	}
	if first.Host != "a" {
		t.Fatalf("first host = %q, want a", first.Host)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release first: %v", err)
	}
	second, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire second: %v", err)
	}
	if second.Host != "c" {
		t.Fatalf("second host = %q, want c", second.Host)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("Release second: %v", err)
	}
}

func TestNewPoolFailsWhenMinReadyNotMet(t *testing.T) {
	t.Parallel()

	pool, err := NewPool[testClient](context.Background(), Config[testClient]{
		Hosts:           []HostConfig{{Address: "a", Clients: 1}, {Address: "b", Clients: 1}},
		MinReadyClients: 2,
		Factory: func(ctx context.Context, host HostConfig, index int) (testClient, error) {
			if host.Address == "b" {
				return testClient{}, errors.New("dial failed")
			}
			return testClient{host: host.Address, index: index}, nil
		},
	})
	if err == nil {
		_ = pool.Close(context.Background())
		t.Fatal("NewPool succeeded with too few ready clients")
	}
	if got := err.Error(); got != "source pool ready clients 1 below minimum 2" {
		t.Fatalf("error = %q", got)
	}
}

func TestNewPoolStopsWhenContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan testClient, 1)
	created := 0
	pool, err := NewPool[testClient](ctx, Config[testClient]{
		Hosts: []HostConfig{{Address: "a", Clients: 1}},
		Factory: func(ctx context.Context, host HostConfig, index int) (testClient, error) {
			created++
			if index == 0 {
				cancel()
			}
			return testClient{host: host.Address, index: index}, nil
		},
		Close: func(client testClient) error {
			closed <- client
			return nil
		},
	})
	if err == nil {
		_ = pool.Close(context.Background())
		t.Fatal("NewPool succeeded after context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NewPool error = %v, want context.Canceled", err)
	}
	if created != 1 {
		t.Fatalf("created clients = %d, want 1", created)
	}
	select {
	case client := <-closed:
		if client.index != 0 {
			t.Fatalf("closed client index = %d, want 0", client.index)
		}
	default:
		t.Fatal("initialized client was not closed")
	}
}

func TestPoolReleaseTwiceFails(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t, Config[testClient]{Hosts: []HostConfig{{Address: "a", Clients: 1}}})
	defer pool.Close(context.Background())
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if err := lease.Release(); !errors.Is(err, ErrLeaseReleased) {
		t.Fatalf("second Release error = %v, want ErrLeaseReleased", err)
	}
}

func newTestPool(t *testing.T, cfg Config[testClient]) *Pool[testClient] {
	t.Helper()
	if cfg.Factory == nil {
		cfg.Factory = func(ctx context.Context, host HostConfig, index int) (testClient, error) {
			return testClient{host: host.Address, index: index}, nil
		}
	}
	pool, err := NewPool[testClient](context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return pool
}
