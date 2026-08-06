// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension

// End-to-end tests that wire a mock HTTP or gRPC backend through WrapSender
// and verify the ARC controller's full lifecycle: warmup → backpressure → (recovery).
//
// WHY the two-phase design
// ────────────────────────
// The controller starts at MinConcurrency (the floor) and cannot decrease
// further.  Therefore every backpressure test must first warm the limit
// above the floor, then introduce errors, then assert the limit dropped
// from the elevated level back toward the floor.
//
// The warm-up phase uses a backend that adds a small latency (10 ms) so the
// controller's robust EWMA accumulates a non-zero RTT baseline.  Without a
// non-zero prevRTTMean the additive-increase logic never fires.
//
// Full path exercised by every test
// ──────────────────────────────────
//   mock error
//   → adaptiveSender.Send
//   → ctrl.Record
//   → controller.IsRetryableError  (ClassifyErrorReason under the hood)
//   → pressure = true
//   → controlStep (after period expires)
//   → limit decreases
//
// Scenarios covered
// ─────────────────
//   HTTP:  429 Too Many Requests, 503 Service Unavailable, Retry-After header
//   gRPC:  RESOURCE_EXHAUSTED, UNAVAILABLE, DEADLINE_EXCEEDED
//   Mix:   concurrent senders, alternating 429/200, alternating gRPC codes
//   E2E:   full degrade → recover cycle (HTTP 429 then 200)

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/controller"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pipeline"
)

// controlPeriod is how long we sleep so the controller's measurement period
// expires and the next Record() call fires controlStep().
// Initial period = 300 ms; 380 ms gives a safe margin.
const controlPeriod = 380 * time.Millisecond

// warmupLatency is the per-request latency used during the warm-up phase.
// It must be ≥1 ms so that dur.Milliseconds() > 0 and the EWMA initialises.
const warmupLatency = 10 * time.Millisecond

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

// httpRespCarrier wraps an HTTP response so ClassifyErrorReason detects the
// status code via the respCarrier interface used in retryable_error.go.
type httpRespCarrier struct {
	resp *http.Response
	msg  string
}

func (e *httpRespCarrier) Error() string            { return e.msg }
func (e *httpRespCarrier) Response() *http.Response { return e.resp }

// httpBackendSender makes a real HTTP POST to the test server and converts
// 429 / 503 / 504 / Retry-After responses into httpRespCarrier errors.
type httpBackendSender struct {
	component.StartFunc
	component.ShutdownFunc
	client *http.Client
	url    string
}

func (s *httpBackendSender) Send(ctx context.Context, _ xexporterhelper.Request) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return &httpRespCarrier{
			resp: resp,
			msg:  fmt.Sprintf("backend returned HTTP %d", resp.StatusCode),
		}
	}
	if resp.Header.Get("Retry-After") != "" {
		return &httpRespCarrier{resp: resp, msg: "backend sent Retry-After"}
	}
	return nil
}

// ─── gRPC / generic helpers ───────────────────────────────────────────────────

// funcSender is a Sender backed by an arbitrary function — allows inline
// behaviour without a full struct for every scenario.
type funcSender struct {
	component.StartFunc
	component.ShutdownFunc
	sendFn func(context.Context, xexporterhelper.Request) error
}

func (s *funcSender) Send(ctx context.Context, req xexporterhelper.Request) error {
	return s.sendFn(ctx, req)
}

// ─── Test fixture helpers ─────────────────────────────────────────────────────

// newExtForBackpressureTest creates an extension with aggressive AIMD settings
// so that limit changes are clearly visible within a few hundred milliseconds.
func newExtForBackpressureTest(t *testing.T) *adaptiveConcurrency {
	t.Helper()
	cfg := &Config{
		Enabled:        true,
		MinConcurrency: 2,
		MaxConcurrency: 50,
		DecreaseRatio:  0.5, // halve on each backpressure event
		EwmaAlpha:      0.5, // fast EWMA convergence
		DeviationScale: 2.0,
	}
	set := extensiontest.NewNopSettings(metadata.Type)
	ext, err := newAdaptiveConcurrency(cfg, set)
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), nil))
	t.Cleanup(func() { _ = ext.Shutdown(context.Background()) })
	return ext.(*adaptiveConcurrency)
}

// wrapAndGetCtrl calls WrapSender and returns both the wrapped sender and a
// direct pointer to its ARC controller for limit inspection.
func wrapAndGetCtrl(
	t *testing.T,
	ext *adaptiveConcurrency,
	next xexporterhelper.Sender[xexporterhelper.Request],
) (xexporterhelper.Sender[xexporterhelper.Request], *controller.Controller) {
	t.Helper()
	settings := xexporterhelper.NewRequestMiddlewareSettings(
		component.MustNewID("otlp"),
		pipeline.SignalTraces,
		componenttest.NewNopTelemetrySettings(),
	)
	wrapped, err := ext.WrapSender(settings, next)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wrapped.Shutdown(context.Background()) })
	return wrapped, wrapped.(*adaptiveSender).ctrl
}

// periodConcurrency is the number of goroutines used inside runPeriod.
// It must exceed MinConcurrency (2) so that inFlight reaches the limit,
// which is required for the controller's credit counter to increment and
// the additive-increase path to fire.
const periodConcurrency = 10

// runPeriod sends n requests through wrapped using periodConcurrency goroutines,
// waits for the measurement period to expire, then sends one more request to
// trigger the control step inside Record(). Returns after the control step fires.
func runPeriod(wrapped xexporterhelper.Sender[xexporterhelper.Request], n int) {
	perG := n / periodConcurrency
	if perG < 1 {
		perG = 1
	}
	var wg sync.WaitGroup
	for range periodConcurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perG {
				_ = wrapped.Send(context.Background(), &mockRequest{})
			}
		}()
	}
	wg.Wait()
	time.Sleep(controlPeriod)
	_ = wrapped.Send(context.Background(), &mockRequest{}) // triggers controlStep
}

// warmupToAboveFloor drives successful requests until ctrl.CurrentLimit() > minFloor.
// The backend sender (next) MUST return nil (success) with ≥1 ms latency during
// this phase so the EWMA baseline is non-zero and additive increase can fire.
func warmupToAboveFloor(
	t *testing.T,
	wrapped xexporterhelper.Sender[xexporterhelper.Request],
	ctrl *controller.Controller,
	minFloor int,
) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		runPeriod(wrapped, 15)
		if ctrl.CurrentLimit() > minFloor {
			return
		}
	}
	require.Greater(t, ctrl.CurrentLimit(), minFloor,
		"warm-up timed out; controller did not increase limit above floor=%d", minFloor)
}

// ─── HTTP backpressure tests ──────────────────────────────────────────────────

// TestWrapSender_HTTP429_TriggersBackoff verifies that HTTP 429 from a real
// backend causes the controller to reduce its concurrency limit.
func TestWrapSender_HTTP429_TriggersBackoff(t *testing.T) {
	var erroring atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(warmupLatency)
		if erroring.Load() {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, &httpBackendSender{client: srv.Client(), url: srv.URL})

	// Phase 1: warm up so limit > MinConcurrency.
	warmupToAboveFloor(t, wrapped, ctrl, ext.cfg.MinConcurrency)
	limitAfterWarmup := ctrl.CurrentLimit()

	// Phase 2: switch to 429 and run one period.
	erroring.Store(true)
	runPeriod(wrapped, 15)

	assert.Less(t, ctrl.CurrentLimit(), limitAfterWarmup,
		"HTTP 429 should trigger AIMD decrease; warmup=%d current=%d",
		limitAfterWarmup, ctrl.CurrentLimit())
}

// TestWrapSender_HTTP503_TriggersBackoff verifies 503 Service Unavailable causes backoff.
func TestWrapSender_HTTP503_TriggersBackoff(t *testing.T) {
	var erroring atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(warmupLatency)
		if erroring.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, &httpBackendSender{client: srv.Client(), url: srv.URL})

	warmupToAboveFloor(t, wrapped, ctrl, ext.cfg.MinConcurrency)
	limitAfterWarmup := ctrl.CurrentLimit()

	erroring.Store(true)
	runPeriod(wrapped, 15)

	assert.Less(t, ctrl.CurrentLimit(), limitAfterWarmup,
		"HTTP 503 should trigger AIMD decrease; warmup=%d current=%d",
		limitAfterWarmup, ctrl.CurrentLimit())
}

// TestWrapSender_HTTP429_RetryAfterHeader verifies that the Retry-After header
// is classified as backpressure.
func TestWrapSender_HTTP429_RetryAfterHeader(t *testing.T) {
	var erroring atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(warmupLatency)
		if erroring.Load() {
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, &httpBackendSender{client: srv.Client(), url: srv.URL})

	warmupToAboveFloor(t, wrapped, ctrl, ext.cfg.MinConcurrency)
	limitAfterWarmup := ctrl.CurrentLimit()

	erroring.Store(true)
	runPeriod(wrapped, 15)

	assert.Less(t, ctrl.CurrentLimit(), limitAfterWarmup,
		"Retry-After header should trigger AIMD decrease; warmup=%d current=%d",
		limitAfterWarmup, ctrl.CurrentLimit())
}

// TestWrapSender_HTTP_HealthyBackend_LimitRampsUp verifies the additive-increase
// path: a healthy backend causes the limit to grow over multiple periods.
func TestWrapSender_HTTP_HealthyBackend_LimitRampsUp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(warmupLatency)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, &httpBackendSender{client: srv.Client(), url: srv.URL})

	initialLimit := ctrl.CurrentLimit()
	for range 5 {
		runPeriod(wrapped, 15)
	}

	assert.Greater(t, ctrl.CurrentLimit(), initialLimit,
		"healthy backend should increase limit; initial=%d current=%d",
		initialLimit, ctrl.CurrentLimit())
}

// ─── gRPC backpressure tests ──────────────────────────────────────────────────

// grpcBackpressureTest is the shared body for all gRPC error code tests.
func grpcBackpressureTest(t *testing.T, errorCode codes.Code) {
	t.Helper()

	var erroring atomic.Bool

	next := &funcSender{
		sendFn: func(_ context.Context, _ xexporterhelper.Request) error {
			time.Sleep(warmupLatency)
			if erroring.Load() {
				return status.Error(errorCode, "simulated backend overload")
			}
			return nil
		},
	}

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, next)

	warmupToAboveFloor(t, wrapped, ctrl, ext.cfg.MinConcurrency)
	limitAfterWarmup := ctrl.CurrentLimit()

	erroring.Store(true)
	runPeriod(wrapped, 15)

	assert.Less(t, ctrl.CurrentLimit(), limitAfterWarmup,
		"gRPC %v should trigger AIMD decrease; warmup=%d current=%d",
		errorCode, limitAfterWarmup, ctrl.CurrentLimit())
}

// TestWrapSender_gRPC_ResourceExhausted_TriggersBackoff verifies gRPC
// RESOURCE_EXHAUSTED (canonical OTLP overload signal) causes backoff.
func TestWrapSender_gRPC_ResourceExhausted_TriggersBackoff(t *testing.T) {
	grpcBackpressureTest(t, codes.ResourceExhausted)
}

// TestWrapSender_gRPC_Unavailable_TriggersBackoff verifies gRPC
// UNAVAILABLE (rolling restart / shard move) causes backoff.
func TestWrapSender_gRPC_Unavailable_TriggersBackoff(t *testing.T) {
	grpcBackpressureTest(t, codes.Unavailable)
}

// TestWrapSender_gRPC_DeadlineExceeded_TriggersBackoff verifies gRPC
// DEADLINE_EXCEEDED (slow backend) causes backoff.
func TestWrapSender_gRPC_DeadlineExceeded_TriggersBackoff(t *testing.T) {
	grpcBackpressureTest(t, codes.DeadlineExceeded)
}

// ─── Recovery test ────────────────────────────────────────────────────────────

// TestWrapSender_RecoveryAfterHTTP429Backoff verifies the full ARC lifecycle:
//  1. Warm up → limit rises above the floor.
//  2. HTTP 429 burst → limit drops below warm-up level.
//  3. Backend recovers → limit climbs back up.
func TestWrapSender_RecoveryAfterHTTP429Backoff(t *testing.T) {
	var mu sync.Mutex
	phase := "warmup" // "warmup" | "degrade" | "recover"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		p := phase
		mu.Unlock()
		switch p {
		case "degrade":
			time.Sleep(2 * time.Millisecond)
			w.WriteHeader(http.StatusTooManyRequests)
		default: // warmup & recover
			time.Sleep(warmupLatency)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, &httpBackendSender{client: srv.Client(), url: srv.URL})

	// Phase 1: warm up.
	warmupToAboveFloor(t, wrapped, ctrl, ext.cfg.MinConcurrency)
	limitAfterWarmup := ctrl.CurrentLimit()

	// Phase 2: degrade — trigger backpressure and verify backoff.
	mu.Lock()
	phase = "degrade"
	mu.Unlock()
	runPeriod(wrapped, 15)
	limitAfterDegradation := ctrl.CurrentLimit()

	require.Less(t, limitAfterDegradation, limitAfterWarmup,
		"limit must drop during degradation (warmup=%d degraded=%d)",
		limitAfterWarmup, limitAfterDegradation)

	// Phase 3: recover — backend returns 200; limit should climb.
	mu.Lock()
	phase = "recover"
	mu.Unlock()
	for range 6 {
		runPeriod(wrapped, 15)
	}

	assert.Greater(t, ctrl.CurrentLimit(), limitAfterDegradation,
		"limit should increase after recovery (degraded=%d recovered=%d)",
		limitAfterDegradation, ctrl.CurrentLimit())
}

// ─── Mixed error / concurrent tests ──────────────────────────────────────────

// TestWrapSender_ConcurrentHTTP_MixedErrors verifies backpressure detection
// under concurrent load with intermittent 429s (every 3rd request).
func TestWrapSender_ConcurrentHTTP_MixedErrors(t *testing.T) {
	var reqCount atomic.Int64
	var erroring atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(warmupLatency)
		n := reqCount.Add(1)
		if erroring.Load() && n%3 == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, &httpBackendSender{client: srv.Client(), url: srv.URL})

	// Warm up first.
	warmupToAboveFloor(t, wrapped, ctrl, ext.cfg.MinConcurrency)
	limitAfterWarmup := ctrl.CurrentLimit()

	// Switch to intermittent 429s and send concurrently.
	erroring.Store(true)
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				_ = wrapped.Send(context.Background(), &mockRequest{})
			}
		}()
	}
	wg.Wait()
	time.Sleep(controlPeriod)
	_ = wrapped.Send(context.Background(), &mockRequest{})

	assert.LessOrEqual(t, ctrl.CurrentLimit(), limitAfterWarmup,
		"intermittent 429s under concurrent load should not increase limit; warmup=%d current=%d",
		limitAfterWarmup, ctrl.CurrentLimit())
}

// TestWrapSender_gRPC_MixedBackpressure verifies that alternating
// RESOURCE_EXHAUSTED and UNAVAILABLE errors both contribute to backpressure.
func TestWrapSender_gRPC_MixedBackpressure(t *testing.T) {
	var callN atomic.Int64
	var erroring atomic.Bool

	next := &funcSender{
		sendFn: func(_ context.Context, _ xexporterhelper.Request) error {
			time.Sleep(warmupLatency)
			if erroring.Load() {
				n := callN.Add(1)
				if n%2 == 0 {
					return status.Error(codes.ResourceExhausted, "too many requests")
				}
				return status.Error(codes.Unavailable, "backend restarting")
			}
			return nil
		},
	}

	ext := newExtForBackpressureTest(t)
	wrapped, ctrl := wrapAndGetCtrl(t, ext, next)

	warmupToAboveFloor(t, wrapped, ctrl, ext.cfg.MinConcurrency)
	limitAfterWarmup := ctrl.CurrentLimit()

	erroring.Store(true)
	runPeriod(wrapped, 15)

	assert.Less(t, ctrl.CurrentLimit(), limitAfterWarmup,
		"mixed gRPC backpressure codes should trigger AIMD decrease; warmup=%d current=%d",
		limitAfterWarmup, ctrl.CurrentLimit())
}
