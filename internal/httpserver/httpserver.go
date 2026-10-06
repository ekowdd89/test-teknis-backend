package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ekowdd89/test-teknis-backend/internal/httpserver/openapi"
	"github.com/ekowdd89/test-teknis-backend/internal/postgres/sqlc"
	"github.com/gin-gonic/gin"
)

type OptFunc func(*HTTPServer) (err error)

// HealthCheck mengembalikan error bila dependensi tidak sehat.
type HealthCheck func(ctx context.Context) (err error)

type HTTPServer struct {
	addr               string
	queries            sqlc.Querier
	checks             map[string]HealthCheck
	healthCheckTimeout time.Duration
	shutdownTimeout    time.Duration

	listener   net.Listener
	handler    *gin.Engine
	server     *http.Server
	tracerName string
	logger     *slog.Logger
}

func WithAddr(addr string) OptFunc {
	return func(h *HTTPServer) error {
		h.addr = addr
		return nil
	}
}

// WithListener memakai listener yang sudah ada (mis. port acak untuk test).
func WithListener(l net.Listener) OptFunc {
	return func(h *HTTPServer) error {
		h.listener = l
		return nil
	}
}

func WithQuerier(q sqlc.Querier) OptFunc {
	return func(h *HTTPServer) error {
		h.queries = q
		return nil
	}
}

// WithHealthCheck menambahkan dependensi yang dicek oleh GET /healthz,
// mis. WithHealthCheck("postgres", pg.Ping).
func WithHealthCheck(name string, check HealthCheck) OptFunc {
	return func(h *HTTPServer) error {
		if check == nil {
			return fmt.Errorf("httpserver: health check %q is nil", name)
		}
		h.checks[name] = check
		return nil
	}
}

func WithShutdownTimeout(d time.Duration) OptFunc {
	return func(h *HTTPServer) error {
		h.shutdownTimeout = d
		return nil
	}
}

func WithLogger(l *slog.Logger) OptFunc {
	return func(h *HTTPServer) error {
		h.logger = l
		return nil
	}
}

func New(opts ...OptFunc) (hs *HTTPServer, err error) {
	hs = &HTTPServer{
		addr:               os.Getenv("HTTP_ADDR"),
		checks:             make(map[string]HealthCheck),
		healthCheckTimeout: 2 * time.Second,
		shutdownTimeout:    10 * time.Second,
		logger:             slog.Default(),
	}
	if hs.addr == "" {
		hs.addr = ":8080"
	}
	for _, opt := range opts {
		if err = opt(hs); err != nil {
			return nil, err
		}
	}
	if hs.queries == nil {
		return nil, errors.New("httpserver: querier is required (WithQuerier)")
	}

	if os.Getenv(gin.EnvGinMode) == "" {
		gin.SetMode(gin.ReleaseMode)
	}
	hs.handler = gin.New()
	hs.handler.Use(hs.logMiddleware, hs.recoveryMiddleware)
	hs.handler.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, openapi.Error{Code: codeNotFound, Message: "route not found"})
	})
	hs.handler.NoMethod(func(c *gin.Context) {
		c.JSON(http.StatusMethodNotAllowed, openapi.Error{Code: "METHOD_NOT_ALLOWED", Message: "method not allowed"})
	})
	hs.handler.HandleMethodNotAllowed = true

	openapi.RegisterHandlersWithOptions(hs.handler,
		openapi.NewStrictHandler(&openapiServerImplementation{h: hs}, nil),
		openapi.GinServerOptions{ErrorHandler: paramErrorHandler},
	)

	hs.server = &http.Server{
		Addr:              hs.addr,
		Handler:           hs.handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return hs, nil
}

// Handler mengembalikan router Gin (dipakai untuk test dengan httptest).
func (h *HTTPServer) Handler() http.Handler {
	return h.handler
}

// Serve berjalan sampai ctx selesai, lalu melakukan graceful shutdown.
func (h *HTTPServer) Serve(ctx context.Context) (err error) {
	if h.listener == nil {
		if h.listener, err = net.Listen("tcp", h.addr); err != nil {
			return fmt.Errorf("httpserver: listen %s: %w", h.addr, err)
		}
	}

	errCh := make(chan error, 1)
	go func() { errCh <- h.server.Serve(h.listener) }()
	h.logger.Info("http server started", "addr", h.listener.Addr().String())

	select {
	case err = <-errCh:
		return fmt.Errorf("httpserver: serve: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), h.shutdownTimeout)
	defer cancel()
	if err = h.server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("httpserver: shutdown: %w", err)
	}
	h.logger.Info("http server stopped")
	return nil
}

func (h *HTTPServer) logMiddleware(c *gin.Context) {
	start := time.Now()
	c.Next()
	h.logger.LogAttrs(c, slog.LevelDebug, "http request",
		slog.String("method", c.Request.Method),
		slog.String("path", c.Request.URL.Path),
		slog.Int("status", c.Writer.Status()),
		slog.Duration("duration", time.Since(start)),
	)
}

func (h *HTTPServer) recoveryMiddleware(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("http panic recovered", "path", c.Request.URL.Path, "panic", r)
			c.AbortWithStatusJSON(http.StatusInternalServerError, openapi.Error{Code: codeInternal, Message: msgInternal})
		}
	}()
	c.Next()
}

// paramErrorHandler mengubah error binding parameter dari kode generate
// (mis. start bukan angka) menjadi format Error sesuai spesifikasi.
func paramErrorHandler(c *gin.Context, err error, statusCode int) {
	c.JSON(statusCode, openapi.Error{Code: codeInvalidArgument, Message: err.Error()})
}
