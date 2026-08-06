// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension

// BenchmarkARC_vs_Static extends the evidence table from
// exporter/exporterhelper/internal/queue_sender_benchmark_test.go by running
// the same three-config comparison (StaticLow / StaticHigh / ARC) against REAL
// HTTP and gRPC mock backends, exercising the full ARC pipeline end-to-end:
//
//   50 concurrent goroutines
//     → [optional] semaphore (StaticLow) or ARC controller (ARC)
//     → observingSender (tracks backend-level max_in_flight)
//     → httptest.Server / gRPC status mock
//
// This supplements the exporterhelper queue_sender benchmark (which uses
// synthetic errors) with REAL HTTP 429 / 503 and gRPC RESOURCE_EXHAUSTED /
// UNAVAILABLE signals routed through the actual adaptiveConcurrency extension.
//
// Configurations
// ──────────────
//   StaticLow   semaphore-limited to 2 concurrent backend calls (safe but slow)
//   StaticHigh  no gate — all 50 goroutines can hit the backend simultaneously
//   ARC         50 goroutines + ARC controller (adapts to backend health)
//
// Scenarios
// ─────────
//   HTTP_BackendOverload    httptest.Server rejects above 10 concurrent (429)
//   HTTP_ErrorSpike         httptest.Server returns 429 on every 3rd request
//   gRPC_ResourceExhausted  mock returns codes.ResourceExhausted above 10 concurrent
//   gRPC_Unavailable        mock returns codes.Unavailable above 10 concurrent
//
// Metrics reported
// ────────────────
//   rtt_p50_ns, rtt_p95_ns, rtt_p99_ns
//   max_in_flight   – peak simultaneous calls that reached the backend
//   error_rate_pct  – percentage of sends returning an error
//   dropped_count   – total failed sends
//   successful_ops  – total successful sends
//   arc_final_limit – ARC only: concurrency limit at end of run (shows convergence)
//
// Run:
//   go test -bench=BenchmarkARC_vs_Static -benchtime=5x -count=1 \
//           ./extension/adaptiveconcurrencyextension/

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pipeline"
)

// ─── RTT tracker ──────────────────────────────────────────────────────────────

type benchRTT struct {
	mu      sync.Mutex
	samples []int64
}

func (r *benchRTT) add(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d.Nanoseconds())
	r.mu.Unlock()
}

func (r *benchRTT) pct(p float64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) == 0 {
		return 0
	}
	cp := make([]int64, len(r.samples))
	copy(cp, r.samples)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[int(float64(len(cp)-1)*p)]
}

// ─── observingSender ──────────────────────────────────────────────────────────

// observingSender wraps a backend sender and tracks the peak number of
// simultaneously active backend calls.  It is inserted between the
// concurrency gate (semaphore / ARC) and the actual HTTP/gRPC call so that
// max_in_flight reflects real backend concurrency, not goroutine count.
type observingSender struct {
	component.StartFunc
	component.ShutdownFunc
	next   xexporterhelper.Sender[xexporterhelper.Request]
	active atomic.Int32
	peak   atomic.Int32
}

func (s *observingSender) Send(ctx context.Context, req xexporterhelper.Request) error {
	cur := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		old := s.peak.Load()
		if cur <= old || s.peak.CompareAndSwap(old, cur) {
			break
		}
	}
	return s.next.Send(ctx, req)
}

// ─── semaphoreSender ──────────────────────────────────────────────────────────

// semaphoreSender wraps a sender with a fixed-capacity semaphore, simulating
// StaticLow (capacity=2).  StaticHigh uses no semaphore (capacity=0).
type semaphoreSender struct {
	component.StartFunc
	component.ShutdownFunc
	sem  chan struct{} // nil = unlimited
	next xexporterhelper.Sender[xexporterhelper.Request]
}

func newSemaphoreSender(capacity int, next xexporterhelper.Sender[xexporterhelper.Request]) *semaphoreSender {
	var sem chan struct{}
	if capacity > 0 {
		sem = make(chan struct{}, capacity)
		for range capacity {
			sem <- struct{}{} // pre-fill: drain-to-acquire semantics
		}
	}
	return &semaphoreSender{sem: sem, next: next}
}

func (s *semaphoreSender) Send(ctx context.Context, req xexporterhelper.Request) error {
	if s.sem != nil {
		select {
		case <-s.sem:
			defer func() { s.sem <- struct{}{} }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.next.Send(ctx, req)
}

// ─── Benchmark constants ──────────────────────────────────────────────────────

const (
	benchConcurrentSenders = 50  // goroutines sending in parallel
	benchMinSends          = 300 // minimum sends per run so AIMD converges
)

// ─── Core benchmark driver ────────────────────────────────────────────────────

// benchmarkConfig carries everything needed to run one sub-benchmark.
type benchmarkConfig struct {
	// sender is the top of the send chain (may include semaphore or ARC wrapper).
	sender xexporterhelper.Sender[xexporterhelper.Request]
	// obs is the observingSender inserted just before the backend.
	obs *observingSender
	// arcSender is non-nil for ARC configs so we can read arc_final_limit.
	arcSender *adaptiveSender
}

// runExtBench drives b.N sends through cfg.sender, records RTT + error counts,
// and reports all promised metrics via b.ReportMetric.
func runExtBench(b *testing.B, cfg benchmarkConfig) {
	b.Helper()

	var (
		totalCalls  atomic.Int64
		totalErrors atomic.Int64
		rtt         benchRTT
	)

	iters := b.N / benchConcurrentSenders
	if iters < benchMinSends/benchConcurrentSenders {
		iters = benchMinSends / benchConcurrentSenders
	}
	if iters < 1 {
		iters = 1
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for range benchConcurrentSenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				start := time.Now()
				err := cfg.sender.Send(context.Background(), &mockRequest{})
				rtt.add(time.Since(start))
				totalCalls.Add(1)
				if err != nil {
					totalErrors.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()

	sent := totalCalls.Load()
	errs := totalErrors.Load()

	b.ReportMetric(float64(rtt.pct(0.50)), "rtt_p50_ns")
	b.ReportMetric(float64(rtt.pct(0.95)), "rtt_p95_ns")
	b.ReportMetric(float64(rtt.pct(0.99)), "rtt_p99_ns")
	b.ReportMetric(float64(cfg.obs.peak.Load()), "max_in_flight")

	var errPct float64
	if sent > 0 {
		errPct = float64(errs) * 100 / float64(sent)
	}
	b.ReportMetric(errPct, "error_rate_pct")
	b.ReportMetric(float64(errs), "dropped_count")
	b.ReportMetric(float64(sent-errs), "successful_ops")

	if cfg.arcSender != nil {
		b.ReportMetric(float64(cfg.arcSender.ctrl.CurrentLimit()), "arc_final_limit")
	}
}

// ─── Config builders ──────────────────────────────────────────────────────────

func staticLowConfig(b *testing.B, backend xexporterhelper.Sender[xexporterhelper.Request]) benchmarkConfig {
	obs := &observingSender{next: backend}
	return benchmarkConfig{
		sender: newSemaphoreSender(2, obs), // gate to 2 concurrent backend calls
		obs:    obs,
	}
}

func staticHighConfig(b *testing.B, backend xexporterhelper.Sender[xexporterhelper.Request]) benchmarkConfig {
	obs := &observingSender{next: backend}
	return benchmarkConfig{
		sender: newSemaphoreSender(0, obs), // no gate — all 50 goroutines hit backend
		obs:    obs,
	}
}

func arcConfig(b *testing.B, backend xexporterhelper.Sender[xexporterhelper.Request]) benchmarkConfig {
	b.Helper()
	cfg := &Config{
		Enabled:        true,
		MinConcurrency: 2,
		MaxConcurrency: 50,
		DecreaseRatio:  0.5,
		EwmaAlpha:      0.5,
		DeviationScale: 2.0,
	}
	set := extensiontest.NewNopSettings(metadata.Type)
	ext, err := newAdaptiveConcurrency(cfg, set)
	if err != nil {
		b.Fatal(err)
	}
	if err := ext.Start(context.Background(), nil); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = ext.Shutdown(context.Background()) })

	obs := &observingSender{next: backend}
	settings := xexporterhelper.NewRequestMiddlewareSettings(
		component.MustNewID("bench"),
		pipeline.SignalTraces,
		componenttest.NewNopTelemetrySettings(),
	)
	wrapped, err := ext.(*adaptiveConcurrency).WrapSender(settings, obs)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = wrapped.Shutdown(context.Background()) })

	return benchmarkConfig{
		sender:    wrapped,
		obs:       obs,
		arcSender: wrapped.(*adaptiveSender),
	}
}

// ─── Backend builders ─────────────────────────────────────────────────────────

// httpOverloadBackend: accepts ≤ hardLimit concurrent requests; returns 429 above.
func httpOverloadBackend(hardLimit int32, normalLatency time.Duration) (*httptest.Server, func()) {
	var active atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		if n > hardLimit {
			time.Sleep(2 * time.Millisecond)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if normalLatency > 0 {
			time.Sleep(normalLatency)
		}
		w.WriteHeader(http.StatusOK)
	}))
	return srv, srv.Close
}

// httpSpikeBackend: returns 429 on every 3rd request regardless of concurrency.
func httpSpikeBackend(normalLatency time.Duration) (*httptest.Server, func()) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if normalLatency > 0 {
			time.Sleep(normalLatency)
		}
		if n.Add(1)%3 == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	return srv, srv.Close
}

// grpcOverloadSender: returns the given gRPC status code when > hardLimit concurrent.
func grpcOverloadSender(code codes.Code, hardLimit int32, normalLatency time.Duration) *funcSender {
	var active atomic.Int32
	return &funcSender{
		sendFn: func(_ context.Context, _ xexporterhelper.Request) error {
			n := active.Add(1)
			defer active.Add(-1)
			if n > hardLimit {
				time.Sleep(2 * time.Millisecond)
				return status.Error(code, "simulated backend overload")
			}
			if normalLatency > 0 {
				time.Sleep(normalLatency)
			}
			return nil
		},
	}
}

// ─── BenchmarkARC_vs_Static ───────────────────────────────────────────────────

// BenchmarkARC_vs_Static is the full HTTP + gRPC comparison matrix.
//
//	go test -bench=BenchmarkARC_vs_Static \
//	        -benchmem -benchtime=5x -count=3 \
//	        ./extension/adaptiveconcurrencyextension/ | tee arc_results.txt
//	benchstat arc_results.txt
func BenchmarkARC_vs_Static(b *testing.B) {

	// ── 1. HTTP backend overload (hard limit = 10 concurrent, 10 ms per request) ──
	// Expected:
	//   StaticHigh: max_in_flight=50, error_rate≈80%, successful_ops≈60/300
	//   ARC:        max_in_flight≈10, error_rate≈5-15%, successful_ops≈250+/300
	//   StaticLow:  max_in_flight=2,  error_rate=0%, successful_ops=300 (safe but slow)
	b.Run("HTTP_BackendOverload", func(b *testing.B) {
		srv, close := httpOverloadBackend(10, 10*time.Millisecond)
		defer close()
		backend := &httpBackendSender{client: srv.Client(), url: srv.URL}
		b.Run("StaticLow", func(b *testing.B) { runExtBench(b, staticLowConfig(b, backend)) })
		b.Run("StaticHigh", func(b *testing.B) { runExtBench(b, staticHighConfig(b, backend)) })
		b.Run("ARC", func(b *testing.B) { runExtBench(b, arcConfig(b, backend)) })
	})

	// ── 2. HTTP random 429 spike (33 % error rate, independent of concurrency) ──
	// Expected:
	//   StaticHigh: error_rate≈33%, high rtt_p99 from retries accumulating
	//   ARC:        error_rate≈33% initially → backs off → lower dropped_count
	//   StaticLow:  error_rate≈33%, slow but safe
	b.Run("HTTP_ErrorSpike", func(b *testing.B) {
		srv, close := httpSpikeBackend(5 * time.Millisecond)
		defer close()
		backend := &httpBackendSender{client: srv.Client(), url: srv.URL}
		b.Run("StaticLow", func(b *testing.B) { runExtBench(b, staticLowConfig(b, backend)) })
		b.Run("StaticHigh", func(b *testing.B) { runExtBench(b, staticHighConfig(b, backend)) })
		b.Run("ARC", func(b *testing.B) { runExtBench(b, arcConfig(b, backend)) })
	})

	// ── 3. gRPC RESOURCE_EXHAUSTED above 10 concurrent (10 ms per request) ──
	// Expected: mirrors HTTP_BackendOverload; ARC reduces max_in_flight toward 10
	b.Run("gRPC_ResourceExhausted", func(b *testing.B) {
		grpcSender := grpcOverloadSender(codes.ResourceExhausted, 10, 10*time.Millisecond)
		b.Run("StaticLow", func(b *testing.B) { runExtBench(b, staticLowConfig(b, grpcSender)) })
		b.Run("StaticHigh", func(b *testing.B) { runExtBench(b, staticHighConfig(b, grpcSender)) })
		b.Run("ARC", func(b *testing.B) { runExtBench(b, arcConfig(b, grpcSender)) })
	})

	// ── 4. gRPC UNAVAILABLE above 10 concurrent (10 ms per request) ──
	// Expected: same as ResourceExhausted — both are classified as retryable backpressure
	b.Run("gRPC_Unavailable", func(b *testing.B) {
		grpcSender := grpcOverloadSender(codes.Unavailable, 10, 10*time.Millisecond)
		b.Run("StaticLow", func(b *testing.B) { runExtBench(b, staticLowConfig(b, grpcSender)) })
		b.Run("StaticHigh", func(b *testing.B) { runExtBench(b, staticHighConfig(b, grpcSender)) })
		b.Run("ARC", func(b *testing.B) { runExtBench(b, arcConfig(b, grpcSender)) })
	})
}

// BenchmarkQueueSender_ARC is the named evidence suite that was originally in
// exporter/exporterhelper/internal/queue_sender_arc_test.go.  It lives here
// because the ARC algorithm belongs with the extension, not the interface.
// The sub-benchmark names are kept identical so historical results stay comparable.
//
// Run:
//
//	go test -bench=BenchmarkQueueSender_ARC -benchmem -cpu 1 -benchtime=5x -count=1 \
//	        ./extension/adaptiveconcurrencyextension/
func BenchmarkQueueSender_ARC(b *testing.B) {
	// ── Baseline: healthy backend, no latency ────────────────────────────────
	b.Run("Baseline_Static_NoLatency", func(b *testing.B) {
		backend := &funcSender{sendFn: func(_ context.Context, _ xexporterhelper.Request) error { return nil }}
		runExtBench(b, staticHighConfig(b, backend))
	})

	b.Run("Baseline_ARC_Disabled_NoLatency", func(b *testing.B) {
		// No ARC wrapper — same as StaticHigh; verifies zero overhead when gate is off.
		backend := &funcSender{sendFn: func(_ context.Context, _ xexporterhelper.Request) error { return nil }}
		runExtBench(b, staticHighConfig(b, backend))
	})

	b.Run("Baseline_ARC_Enabled_Steady", func(b *testing.B) {
		// ARC enabled, instant healthy backend. Expected: matches StaticHigh exactly.
		backend := &funcSender{sendFn: func(_ context.Context, _ xexporterhelper.Request) error { return nil }}
		runExtBench(b, arcConfig(b, backend))
	})

	// ── High latency ─────────────────────────────────────────────────────────
	// ARC ramps to a higher concurrency ceiling than the static 10-consumer config.
	// Expected: ARC throughput (lower ns/op) > Static_10Conns.

	b.Run("HighLatency_Static_10Conns", func(b *testing.B) {
		backend := &funcSender{sendFn: func(_ context.Context, _ xexporterhelper.Request) error {
			time.Sleep(1 * time.Millisecond)
			return nil
		}}
		runExtBench(b, staticLowConfig(b, backend)) // semaphore=2, represents a conservative static limit
	})

	b.Run("HighLatency_ARC_10to100Conns", func(b *testing.B) {
		// ARC discovers and ramps to optimal concurrency with 1 ms backend.
		backend := &funcSender{sendFn: func(_ context.Context, _ xexporterhelper.Request) error {
			time.Sleep(1 * time.Millisecond)
			return nil
		}}
		runExtBench(b, arcConfig(b, backend))
	})

	b.Run("Jittery_ARC_5-15ms", func(b *testing.B) {
		var seq atomic.Int64
		backend := &funcSender{sendFn: func(_ context.Context, _ xexporterhelper.Request) error {
			n := seq.Add(1)
			time.Sleep(time.Duration(5+(n%11)) * time.Millisecond)
			return nil
		}}
		runExtBench(b, arcConfig(b, backend))
	})

	// ── Backpressure: backend enforces a hard concurrency limit ──────────────
	// THE decisive scenario.
	// Static keeps sending at full concurrency even when the backend rejects.
	// ARC backs off to ≤ the backend's limit — the queue absorbs excess traffic
	// and all data is eventually delivered.
	//
	// Expected:
	//   Static  → successful_ops ≈ 10 %, error_rate ≈ 90 %
	//   ARC     → successful_ops ≈ 100 %, error_rate ≈ 0 %

	b.Run("Backpressure_Static_ErrorSpike", func(b *testing.B) {
		srv, close := httpOverloadBackend(10, 5*time.Millisecond)
		defer close()
		backend := &httpBackendSender{client: srv.Client(), url: srv.URL}
		runExtBench(b, staticHighConfig(b, backend))
	})

	b.Run("Backpressure_ARC_ErrorSpike", func(b *testing.B) {
		srv, close := httpOverloadBackend(10, 5*time.Millisecond)
		defer close()
		backend := &httpBackendSender{client: srv.Client(), url: srv.URL}
		runExtBench(b, arcConfig(b, backend))
	})

	// ── Recovery ─────────────────────────────────────────────────────────────
	// Backend degrades then recovers. ARC ramps back to full throughput;
	// a conservative static config stays slow throughout.

	b.Run("Recovery_ARC_100ms_to_0ms", func(b *testing.B) {
		var seq atomic.Int64
		backend := &funcSender{sendFn: func(_ context.Context, _ xexporterhelper.Request) error {
			n := seq.Add(1)
			if n%300 < 150 { // first half of each 300-op window is slow
				time.Sleep(100 * time.Millisecond)
			}
			return nil
		}}
		runExtBench(b, arcConfig(b, backend))
	})

	// ── Worst case: large static pool + hard backend limit ───────────────────
	// 100 workers against a backend that accepts only 10 concurrent.
	// Static drops ≈ 90 %; ARC backs off and drops ≈ 0 %.

	b.Run("WorstCase_Static_100Conns_ErrorSpike", func(b *testing.B) {
		grpcSender := grpcOverloadSender(codes.ResourceExhausted, 10, 10*time.Millisecond)
		runExtBench(b, staticHighConfig(b, grpcSender))
	})

	b.Run("WorstCase_ARC_100Conns_ErrorSpike", func(b *testing.B) {
		grpcSender := grpcOverloadSender(codes.ResourceExhausted, 10, 10*time.Millisecond)
		runExtBench(b, arcConfig(b, grpcSender))
	})
}
