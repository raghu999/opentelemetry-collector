// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package controller // import "go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/controller"

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"
	"go.opentelemetry.io/collector/pipeline"
)

// Controller coordinates Adaptive Request Concurrency (ARC).
// Internally it uses an AIMD control law and a TokenPool gate.
type Controller struct {
	cfg  Config
	pool *TokenPool
	tel  *metadata.TelemetryBuilder

	rttInst          metric.Int64Histogram
	failuresInst     metric.Int64Counter
	limitChangesInst metric.Int64Counter
	limitUpAttrs     metric.MeasurementOption
	limitDownAttrs   metric.MeasurementOption
	backoffInst      metric.Int64Counter
	syncAttrs        metric.MeasurementOption

	mu sync.Mutex
	st struct {
		limit       int
		inFlight    int
		credits     int
		pressure    bool
		periodStart time.Time
		periodDur   time.Duration
		reMean      robustEWMA
		prevRTTMean float64
		prevRTTDev  float64
		lastRTTMean float64
		lastRTTDev  float64
	}
}

// NewController creates a new ARC Controller.
// It requires a controllerID (the extension) and a controlledID (the component being throttled).
func NewController(cfg Config, tel *metadata.TelemetryBuilder, controllerID, controlledID component.ID, signal pipeline.Signal) (*Controller, error) {
	if tel == nil {
		return nil, errors.New("telemetry builder is nil")
	}

	def := DefaultConfig()
	if cfg.MinConcurrency <= 0 {
		cfg.MinConcurrency = def.MinConcurrency
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = def.MaxConcurrency
	}
	if cfg.MinConcurrency > cfg.MaxConcurrency {
		cfg.MinConcurrency = cfg.MaxConcurrency
	}
	if cfg.DecreaseRatio <= 0 || cfg.DecreaseRatio >= 1 {
		cfg.DecreaseRatio = def.DecreaseRatio
	}
	if cfg.EwmaAlpha <= 0 || cfg.EwmaAlpha >= 1 {
		cfg.EwmaAlpha = def.EwmaAlpha
	}
	if cfg.DeviationScale < 0 {
		cfg.DeviationScale = def.DeviationScale
	}

	// Create unique attributes for metrics to avoid collisions when multiple
	// components use the same concurrency controller instance.
	attrs := []attribute.KeyValue{
		attribute.String("concurrency_controller", controllerID.String()),
		attribute.String("controlled_component", controlledID.String()),
	}
	if signal != (pipeline.Signal{}) {
		attrs = append(attrs, attribute.String("signal", signal.String()))
	}

	attrSet := attribute.NewSet(attrs...)
	syncAttrs := metric.WithAttributeSet(attrSet)
	limitUpAttrs := metric.WithAttributeSet(attribute.NewSet(append(attrs, attribute.String("direction", "up"))...))
	limitDownAttrs := metric.WithAttributeSet(attribute.NewSet(append(attrs, attribute.String("direction", "down"))...))

	c := &Controller{
		cfg:              cfg,
		pool:             newTokenPool(cfg.MinConcurrency),
		tel:              tel,
		rttInst:          tel.AdaptiveConcurrencyRtt,
		failuresInst:     tel.AdaptiveConcurrencyFailures,
		limitChangesInst: tel.AdaptiveConcurrencyLimitChanges,
		limitUpAttrs:     limitUpAttrs,
		limitDownAttrs:   limitDownAttrs,
		backoffInst:      tel.AdaptiveConcurrencyBackoffEvents,
		syncAttrs:        syncAttrs,
	}

	c.st.limit = cfg.MinConcurrency
	c.st.reMean = newRobustEWMA(cfg.EwmaAlpha)
	c.st.periodStart = time.Now()
	c.st.periodDur = clampDur(minPeriod, maxPeriod, 300*time.Millisecond)

	// Register metric callbacks with proper cleanup on failure.
	if err := tel.RegisterAdaptiveConcurrencyLimitCallback(func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(int64(c.CurrentLimit()), syncAttrs)
		return nil
	}); err != nil {
		c.pool.Close()
		tel.Shutdown()
		return nil, fmt.Errorf("failed to register limit callback: %w", err)
	}

	if err := tel.RegisterAdaptiveConcurrencyPermitsInUseCallback(func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(int64(c.PermitsInUse()), syncAttrs)
		return nil
	}); err != nil {
		c.pool.Close()
		tel.Shutdown()
		return nil, fmt.Errorf("failed to register permits callback: %w", err)
	}

	return c, nil
}

// CurrentLimit returns the current dynamic concurrency limit.
func (c *Controller) CurrentLimit() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.st.limit
}

// PermitsInUse returns the number of requests currently processed.
func (c *Controller) PermitsInUse() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.st.inFlight
}

// controlStep applies the ARC control law (AIMD) at the end of a measurement period.
func (c *Controller) controlStep(ctx context.Context) {
	limitBefore := c.st.limit

	hasBaseline := c.st.prevRTTMean > 0
	threshold := c.st.prevRTTMean + c.cfg.DeviationScale*c.st.prevRTTDev
	isSpike := hasBaseline && c.st.reMean.initialized() && c.st.lastRTTMean > threshold

	// Decrease on explicit backpressure or latency spikes.
	if c.st.limit > c.cfg.MinConcurrency && (c.st.pressure || isSpike) {
		newLimit := int(math.Floor(float64(c.st.limit) * c.cfg.DecreaseRatio))
		newLimit = max(newLimit, c.cfg.MinConcurrency)

		if dec := c.st.limit - newLimit; dec > 0 {
			c.pool.Shrink(dec)
			c.st.limit = newLimit
			if c.tel != nil {
				if c.backoffInst != nil {
					c.backoffInst.Add(contextOrBG(ctx), 1, c.syncAttrs)
				}
				if c.limitChangesInst != nil && c.st.limit != limitBefore {
					c.limitChangesInst.Add(contextOrBG(ctx), 1, c.limitDownAttrs)
				}
			}
			c.st.credits = 0
			c.st.pressure = false
			return
		}
	}

	// Additive increase if health signals are good.
	canIncreaseRTT := !c.st.reMean.initialized() || c.st.lastRTTMean <= threshold
	if hasBaseline &&
		c.st.limit < c.cfg.MaxConcurrency &&
		!c.st.pressure &&
		c.st.credits >= requiredCredits(c.st.limit) &&
		canIncreaseRTT {
		c.pool.Grow(1)
		c.st.limit++
		if c.tel != nil && c.limitChangesInst != nil && c.st.limit != limitBefore {
			c.limitChangesInst.Add(contextOrBG(ctx), 1, c.limitUpAttrs)
		}
		c.st.credits = 0
		return
	}

	c.st.credits = int(float64(c.st.credits) * 0.5)
	c.st.pressure = false
}

// Shutdown stops the controller and releases resources.
func (c *Controller) Shutdown(_ context.Context) error {
	if c == nil {
		return nil
	}
	if c.tel != nil {
		c.tel.Shutdown()
	}
	if c.pool != nil {
		c.pool.Close()
	}
	return nil
}

// Acquire blocks until a concurrency permit is available or the context is cancelled.
func (c *Controller) Acquire(ctx context.Context) error {
	if !c.cfg.Enabled {
		return nil
	}

	// Wait for a token from the pool before modifying internal counters.
	// This prevents counter drift if the context is cancelled during wait.
	if err := c.pool.Acquire(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	c.st.inFlight++
	if c.st.inFlight >= c.st.limit {
		c.st.credits++
	}
	c.mu.Unlock()

	return nil
}

// Record updates the controller with the result of a request and releases the permit.
func (c *Controller) Record(ctx context.Context, dur time.Duration, err error) {
	if !c.cfg.Enabled {
		return
	}

	c.pool.Release()

	isBackpressure := false
	success := err == nil

	if err != nil {
		isBackpressure = IsRetryableError(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.st.inFlight > 0 {
		c.st.inFlight--
	}

	if success {
		ms := float64(dur.Milliseconds())
		c.st.lastRTTMean, c.st.lastRTTDev = c.st.reMean.update(ms)
		if c.rttInst != nil {
			c.rttInst.Record(contextOrBG(ctx), dur.Milliseconds(), c.syncAttrs)
		}
	}

	if isBackpressure {
		c.st.pressure = true
		if c.failuresInst != nil {
			c.failuresInst.Add(contextOrBG(ctx), 1, c.syncAttrs)
		}
	} else if !success {
		if c.failuresInst != nil {
			c.failuresInst.Add(contextOrBG(ctx), 1, c.syncAttrs)
		}
	}

	now := time.Now()
	if now.Sub(c.st.periodStart) > c.st.periodDur {
		c.controlStep(ctx)
		c.st.prevRTTMean = c.st.lastRTTMean
		c.st.prevRTTDev = c.st.lastRTTDev
		c.st.periodStart = now

		if c.st.reMean.initialized() {
			newDur := time.Duration(c.st.lastRTTMean+c.st.lastRTTDev) * time.Millisecond
			c.st.periodDur = clampDur(minPeriod, maxPeriod, newDur)
		}
	}
}

func requiredCredits(limit int) int {
	if limit <= 1 {
		return 1
	}
	if limit < 8 {
		return 2
	}
	if limit < 32 {
		return 3
	}
	return 4
}

func clampDur(minDur, maxDur, v time.Duration) time.Duration {
	if v < minDur {
		return minDur
	}
	if v > maxDur {
		return maxDur
	}
	return v
}

func contextOrBG(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}
