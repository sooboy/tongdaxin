package domain

import "errors"

var (
	// ErrUnsupportedCapability is returned when a provider cannot serve a requested capability.
	ErrUnsupportedCapability = errors.New("marketdata: unsupported capability")

	// ErrInvalidRequest is returned when request parameters fail domain validation.
	ErrInvalidRequest = errors.New("marketdata: invalid request")

	// ErrRateLimited is returned when API ingress exceeds the configured request budget.
	ErrRateLimited = errors.New("marketdata: rate limited")

	// ErrNoData is returned when a request is valid but no market data exists for it.
	ErrNoData = errors.New("marketdata: no data")

	// ErrUpstreamUnavailable is returned when the backing market data source cannot be reached.
	ErrUpstreamUnavailable = errors.New("marketdata: upstream unavailable")
)
