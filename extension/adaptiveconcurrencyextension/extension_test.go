// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pipeline"

	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"
)

func TestExtensionLifecycle(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	set := extensiontest.NewNopSettings(metadata.Type)

	ext, err := newAdaptiveConcurrency(cfg, set)
	require.NoError(t, err)

	// Start
	err = ext.Start(context.Background(), nil)
	require.NoError(t, err)

	// Shutdown
	err = ext.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestWrapSender(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	set := extensiontest.NewNopSettings(metadata.Type)
	extObj, err := newAdaptiveConcurrency(cfg, set)
	require.NoError(t, err)

	ext := extObj.(*adaptiveConcurrency)
	// Must start to initialize the extension (telemetry builder)
	require.NoError(t, ext.Start(context.Background(), nil))
	defer func() { _ = ext.Shutdown(context.Background()) }()

	id := component.NewIDWithName(component.MustNewType("otlp"), "1")
	mwSettings := xexporterhelper.RequestMiddlewareSettings{
		ID:        id,
		Signal:    pipeline.SignalTraces,
		Telemetry: componenttest.NewNopTelemetrySettings(),
	}

	next := &mockSender{}
	// Verify we can wrap a sender
	wrapped, err := ext.WrapSender(mwSettings, next)
	require.NoError(t, err)
	assert.NotNil(t, wrapped)

	// Verify the wrapped sender calls the next sender
	req := &mockRequest{}
	err = wrapped.Send(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, next.called)
}

func TestHTTPMiddleware(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	// Ensure high limit to prevent blocking during test
	cfg.Middleware.Enabled = true
	cfg.MinConcurrency = 100

	set := extensiontest.NewNopSettings(metadata.Type)
	extObj, err := newAdaptiveConcurrency(cfg, set)
	require.NoError(t, err)

	ext := extObj.(*adaptiveConcurrency)
	require.NoError(t, ext.Start(context.Background(), nil))
	defer func() { _ = ext.Shutdown(context.Background()) }()

	// Create a dummy next handler
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { // FIX: Rename unused 'r' to '_'
		w.WriteHeader(http.StatusOK)
	})

	// GetHTTPHandler now returns a WrapHTTPHandlerFunc factory, not the handler directly.
	wrapFn, err := ext.GetHTTPHandler(context.Background())
	require.NoError(t, err)

	handler, err := wrapFn(context.Background(), nextHandler)
	require.NoError(t, err)

	// Test Request
	req := httptest.NewRequest("POST", "http://example.com", http.NoBody)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGRPCMiddleware(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Middleware.Enabled = true
	set := extensiontest.NewNopSettings(metadata.Type)
	extObj, err := newAdaptiveConcurrency(cfg, set)
	require.NoError(t, err)

	ext := extObj.(*adaptiveConcurrency)
	require.NoError(t, ext.Start(context.Background(), nil))
	defer func() { _ = ext.Shutdown(context.Background()) }()

	// Simply verify options are returned; full gRPC integration test is usually done in integration tests.
	opts, err := ext.GetGRPCServerOptions(context.Background())
	require.NoError(t, err)
	assert.Len(t, opts, 2) // Expecting Unary and Stream interceptors
}

// Mocks used for testing WrapSender

type mockSender struct {
	component.StartFunc    // Implements Start
	component.ShutdownFunc // Implements Shutdown
	called                 bool
}

func (m *mockSender) Send(_ context.Context, _ xexporterhelper.Request) error {
	m.called = true
	return nil
}

type mockRequest struct {
	xexporterhelper.Request
}

func (m *mockRequest) ItemsCount() int {
	return 1
}
