// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension // import "go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension"

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/controller"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"

	// Existing middleware package for HTTP/gRPC ingress
	"go.opentelemetry.io/collector/extension/extensionmiddleware"
	"go.opentelemetry.io/collector/pipeline"
)

var (
	_ extensionmiddleware.GRPCServer    = (*adaptiveConcurrency)(nil)
	_ extensionmiddleware.HTTPServer    = (*adaptiveConcurrency)(nil)
	_ xexporterhelper.RequestMiddleware = (*adaptiveConcurrency)(nil)
)

type adaptiveConcurrency struct {
	cfg *Config
	tel component.TelemetrySettings
	id  component.ID

	// mainCtrl is used if this extension is used as Server Middleware (legacy/ingress usage)
	mainCtrl *controller.Controller
}

func newAdaptiveConcurrency(cfg *Config, set extension.Settings) (extension.Extension, error) {
	return &adaptiveConcurrency{
		cfg: cfg,
		tel: set.TelemetrySettings,
		id:  set.ID,
	}, nil
}

func (c *adaptiveConcurrency) Start(_ context.Context, _ component.Host) error {
	// Only initialize mainCtrl and its telemetry if explicitly requested for middleware usage.
	// This prevents unused metric series when the extension is only used by exporters.
	if c.cfg.Middleware.Enabled {
		mb, err := metadata.NewTelemetryBuilder(c.tel)
		if err != nil {
			return fmt.Errorf("failed to create telemetry builder: %w", err)
		}

		ctrl, err := controller.NewController(c.cfg.toControllerConfig(), mb, c.id, c.id, pipeline.Signal{})
		if err != nil {
			// Ensure cleanup if controller creation fails
			mb.Shutdown()
			return err
		}
		c.mainCtrl = ctrl
	}
	return nil
}

// WrapSender creates a middleware sender that enforces adaptive concurrency limits.
// This allows the ExporterHelper to manage its own queue concurrency.
func (c *adaptiveConcurrency) WrapSender(set xexporterhelper.RequestMiddlewareSettings, next xexporterhelper.Sender[xexporterhelper.Request]) (xexporterhelper.Sender[xexporterhelper.Request], error) {
	mb, err := metadata.NewTelemetryBuilder(set.Telemetry)
	if err != nil {
		return nil, err
	}

	ctrlCfg := c.cfg.toControllerConfig()

	// Returns the controller instance specifically for this exporter.
	ctrl, err := controller.NewController(ctrlCfg, mb, c.id, set.ID, set.Signal)
	if err != nil {
		mb.Shutdown() // Fix: Shutdown telemetry builder on error to avoid leak
		return nil, err
	}

	return &adaptiveSender{ctrl: ctrl, next: next}, nil
}

func (c *adaptiveConcurrency) Shutdown(ctx context.Context) error {
	if c.mainCtrl != nil {
		return c.mainCtrl.Shutdown(ctx)
	}
	return nil
}

// adaptiveSender adapts the internal controller to the Sender interface.
type adaptiveSender struct {
	ctrl *controller.Controller
	next xexporterhelper.Sender[xexporterhelper.Request]
}

func (s *adaptiveSender) Send(ctx context.Context, req xexporterhelper.Request) error {
	// 1. Acquire
	if err := s.ctrl.Acquire(ctx); err != nil {
		return err
	}

	// 2. Execute
	start := time.Now()
	var errExec error
	defer func() {
		// 3. Record
		s.ctrl.Record(ctx, time.Since(start), errExec)
	}()

	errExec = s.next.Send(ctx, req)
	return errExec
}

func (s *adaptiveSender) Start(context.Context, component.Host) error {
	return nil
}

func (s *adaptiveSender) Shutdown(ctx context.Context) error {
	return s.ctrl.Shutdown(ctx)
}

// --- HTTP Middleware (Ingress usage) ---

// GetHTTPHandler implements extensionmiddleware.HTTPServer.
// It returns a WrapHTTPHandlerFunc that wraps an HTTP handler with adaptive concurrency control.
func (c *adaptiveConcurrency) GetHTTPHandler(_ context.Context) (extensionmiddleware.WrapHTTPHandlerFunc, error) {
	// Guard: Ensure middleware is enabled and controller is initialized.
	if !c.cfg.Middleware.Enabled || c.mainCtrl == nil {
		return nil, errors.New("adaptiveconcurrencyextension middleware is disabled; set middleware.enabled: true")
	}

	return func(_ context.Context, next http.Handler) (http.Handler, error) {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 1. Acquire a permit.
			if err := c.mainCtrl.Acquire(r.Context()); err != nil {
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				// Do NOT call Record here; permit was not acquired.
				return
			}

			// 2. Serve.
			start := time.Now()
			sw := newStatusResponseWriter(w)
			next.ServeHTTP(sw, r)
			dur := time.Since(start)

			// 3. Record the outcome.
			var reportErr error
			if sw.statusCode == http.StatusTooManyRequests || sw.statusCode == http.StatusServiceUnavailable {
				reportErr = fmt.Errorf("http_backpressure_%d", sw.statusCode)
			}
			c.mainCtrl.Record(r.Context(), dur, reportErr)
		}), nil
	}, nil
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newStatusResponseWriter(w http.ResponseWriter) *statusResponseWriter {
	return &statusResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

func (w *statusResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

// --- gRPC Middleware (Ingress usage) ---

// GetGRPCServerOptions implements extensionmiddleware.GRPCServer.
func (c *adaptiveConcurrency) GetGRPCServerOptions(_ context.Context) ([]grpc.ServerOption, error) {
	// Guard: Ensure middleware is enabled and controller is initialized.
	if !c.cfg.Middleware.Enabled || c.mainCtrl == nil {
		return nil, errors.New("adaptiveconcurrencyextension middleware is disabled; set middleware.enabled: true")
	}

	return []grpc.ServerOption{
		grpc.UnaryInterceptor(c.unaryInterceptor),
		grpc.StreamInterceptor(c.streamInterceptor),
	}, nil
}

func (c *adaptiveConcurrency) unaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	// 1. Acquire
	if err := c.mainCtrl.Acquire(ctx); err != nil {
		return nil, status.Error(codes.ResourceExhausted, "Concurrency limit reached")
	}

	// 2. Serve
	start := time.Now()
	resp, err := handler(ctx, req)
	dur := time.Since(start)

	// 3. Record
	c.mainCtrl.Record(ctx, dur, err)

	return resp, err
}

func (c *adaptiveConcurrency) streamInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()

	// 1. Acquire
	if err := c.mainCtrl.Acquire(ctx); err != nil {
		return status.Error(codes.ResourceExhausted, "Concurrency limit reached")
	}

	// 2. Serve
	start := time.Now()
	err := handler(srv, ss)
	dur := time.Since(start)

	// 3. Record
	c.mainCtrl.Record(ctx, dur, err)

	return err
}
