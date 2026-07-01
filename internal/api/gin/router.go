package ginapi

import (
	"net/http"

	"github.com/gin-gonic/gin"
	httpapi "github.com/sooboy/tongdaxin/internal/api/http"
)

// New creates a Gin-backed router that exposes the same v1 HTTP API as the
// standard-library handler. The actual endpoint parsing, DTO mapping and error
// response semantics are delegated to internal/api/http to keep one HTTP API
// contract.
func New(service httpapi.MarketDataService) *gin.Engine {
	return NewWithOptions(service, httpapi.Options{Metrics: httpapi.NewMetrics()})
}

// NewWithOptions creates a Gin router using the same middleware options as the
// net/http handler.
func NewWithOptions(service httpapi.MarketDataService, opts httpapi.Options) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	RegisterRoutes(engine, httpapi.NewWithOptions(service, opts))
	return engine
}

// RegisterRoutes registers the market-data v1 endpoints on an existing Gin
// engine or route group. Passing the shared http.Handler keeps Gin and net/http
// behavior identical while still allowing callers to compose Gin middleware.
func RegisterRoutes(router gin.IRoutes, handler http.Handler) {
	wrap := gin.WrapH(handler)
	router.GET("/api/v1/health", wrap)
	router.GET("/api/v1/quotes", wrap)
	router.POST("/api/v1/quotes", wrap)
	router.GET("/api/v1/orderbook", wrap)
	router.POST("/api/v1/orderbook", wrap)
	router.GET("/api/v1/ticks", wrap)
	router.GET("/api/v1/history-ticks", wrap)
	router.GET("/api/v1/kline", wrap)
	router.GET("/api/v1/adjusted-kline", wrap)
	router.GET("/api/v1/xdxr", wrap)
	router.GET("/api/v1/securities", wrap)
	router.GET("/api/v1/finance", wrap)
	router.GET("/api/v1/trading-day", wrap)
	router.GET("/api/v1/metrics", wrap)
}
