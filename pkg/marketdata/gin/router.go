// Package gin exposes a public Gin router adapter for the marketdata Service
// interface.
package gin

import (
	"net/http"

	gingo "github.com/gin-gonic/gin"
	internalgin "github.com/sooboy/tongdaxin/internal/api/gin"
	httpapi "github.com/sooboy/tongdaxin/internal/api/http"
	"github.com/sooboy/tongdaxin/pkg/marketdata"
)

// New creates a standalone Gin engine exposing the market-data HTTP v1 API.
// Third-party applications that already own a Gin server should prefer
// RegisterRoutes or RegisterRoutesWithOptions.
func New(service marketdata.Service) *gingo.Engine {
	return internalgin.New(service)
}

// NewWithOptions creates a standalone Gin engine with the same middleware
// options as the built-in HTTP handler.
func NewWithOptions(service marketdata.Service, opts httpapi.Options) *gingo.Engine {
	return internalgin.NewWithOptions(service, opts)
}

// RegisterRoutes mounts the market-data routes on an existing Gin engine or
// route group owned by the caller.
func RegisterRoutes(router gingo.IRoutes, service marketdata.Service) {
	RegisterRoutesWithOptions(router, service, httpapi.Options{Metrics: httpapi.NewMetrics()})
}

// RegisterRoutesWithOptions mounts the market-data routes on an existing Gin
// engine/group with explicit HTTP middleware options.
func RegisterRoutesWithOptions(router gingo.IRoutes, service marketdata.Service, opts httpapi.Options) {
	internalgin.RegisterRoutes(router, httpapi.NewWithOptions(service, opts))
}

// RegisterHTTPHandler mounts an already-built net/http market-data handler on
// an existing Gin engine/group. This is useful for advanced composition; normal
// third-party callers should use RegisterRoutes.
func RegisterHTTPHandler(router gingo.IRoutes, handler http.Handler) {
	internalgin.RegisterRoutes(router, handler)
}
