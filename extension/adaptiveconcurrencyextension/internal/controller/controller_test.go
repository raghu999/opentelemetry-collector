// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"
	"go.opentelemetry.io/collector/pipeline"
)

// newTestController creates a controller with NOP telemetry.
func newTestController(t *testing.T, cfg Config) *Controller {
	set := componenttest.NewNopTelemetrySettings()
	tel, err := metadata.NewTelemetryBuilder(set)
	require.NoError(t, err)
	c, err := NewController(cfg, tel, component.MustNewID("test"), component.MustNewID("test"), pipeline.Signal{})
	require.NoError(t, err)
	return c
}

// forceControlStep triggers a feedback loop that is guaranteed to be after the period duration.
func forceControlStep(c *Controller) {
	// Wait past the period
	time.Sleep(c.st.periodDur + 1*time.Millisecond)

	// We need to trigger the control loop inside Record.
	// We simulate a quick successful request.
	// Note: We don't call Acquire here to avoid blocking if limit is full,
	// just directly Record to trigger the periodic check.
	// In the real flow, Record must follow Acquire, but for white-box testing
	// of the control loop, calling Record directly is safe.
	c.Record(context.Background(), 1*time.Millisecond, nil)
}

func TestController_NewShutdown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	c := newTestController(t, cfg)
	require.NotNil(t, c)
	assert.Equal(t, cfg.MinConcurrency, c.CurrentLimit())
	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_ConfigClamping(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	// Intentionally bad values that should be clamped to defaults
	cfg.MinConcurrency = -5
	cfg.MaxConcurrency = 0
	cfg.DecreaseRatio = 1.5
	cfg.EwmaAlpha = -1.0
	cfg.DeviationScale = -2.0

	c := newTestController(t, cfg)
	require.NotNil(t, c)

	// NewController should have clamped these per DefaultConfig()+rules
	def := DefaultConfig()
	assert.GreaterOrEqual(t, c.cfg.MinConcurrency, 1)
	assert.Equal(t, def.MaxConcurrency, c.cfg.MaxConcurrency)
	assert.InDelta(t, def.DecreaseRatio, c.cfg.DecreaseRatio, 1e-9)
	assert.InDelta(t, def.EwmaAlpha, c.cfg.EwmaAlpha, 1e-9)
	assert.InDelta(t, def.DeviationScale, c.cfg.DeviationScale, 1e-9)

	// Also ensure MinConcurrency <= MaxConcurrency
	assert.LessOrEqual(t, c.cfg.MinConcurrency, c.cfg.MaxConcurrency)
	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_AcquireRelease(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 2
	c := newTestController(t, cfg)
	require.NotNil(t, c)

	// Acquire two permits
	require.NoError(t, c.Acquire(context.Background()))
	assert.Equal(t, 1, c.PermitsInUse())

	require.NoError(t, c.Acquire(context.Background()))
	assert.Equal(t, 2, c.PermitsInUse())

	// Third acquire should block and fail with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := c.Acquire(ctx)
	require.Error(t, err) // Expect timeout/context error
	assert.Equal(t, 2, c.PermitsInUse())

	// Release one via Record and acquire again
	c.Record(context.Background(), 10*time.Millisecond, nil)
	assert.Equal(t, 1, c.PermitsInUse())

	require.NoError(t, c.Acquire(context.Background()))
	assert.Equal(t, 2, c.PermitsInUse())

	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_AcquireRespectsContextCancel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 1

	c := newTestController(t, cfg)
	require.NotNil(t, c)

	// Take the only permit
	require.NoError(t, c.Acquire(context.Background()))

	// Second acquire should obey context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := c.Acquire(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Cleanup
	c.Record(context.Background(), 1*time.Millisecond, nil)
	c.Shutdown(context.Background())
}

func TestController_CreditsOnSaturation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 2

	c := newTestController(t, cfg)

	// Force a short period
	c.mu.Lock()
	c.st.periodDur = 10 * time.Millisecond
	c.mu.Unlock()

	// At start, no in-flight and no credits.
	assert.Equal(t, 0, c.PermitsInUse())
	assert.Equal(t, 0, c.st.credits)

	// First request (inFlight=1 < limit=2) -> no credit expected.
	require.NoError(t, c.Acquire(context.Background()))
	assert.Equal(t, 1, c.PermitsInUse())
	assert.Equal(t, 0, c.st.credits)

	// Second request (inFlight=2 == limit=2) -> credit expected.
	// The internal logic increments inFlight *before* acquiring the pool,
	// checking `if inFlight >= limit`.
	require.NoError(t, c.Acquire(context.Background()))
	assert.Equal(t, 2, c.PermitsInUse())
	assert.Equal(t, 1, c.st.credits)

	// We cannot acquire a 3rd time without blocking, but we've verified
	// that reaching saturation generates a credit.

	// Release both
	c.Record(context.Background(), 10*time.Millisecond, nil)
	c.Record(context.Background(), 10*time.Millisecond, nil)
	assert.Equal(t, 0, c.PermitsInUse())

	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_Feedback_UpdatesEWMAOnlyOnSuccess(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 1
	cfg.EwmaAlpha = 0.5

	c := newTestController(t, cfg)

	// 1. Successful sample initializes the robust EWMA
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 100*time.Millisecond, nil)

	require.True(t, c.st.reMean.initialized())
	mean1 := c.st.lastRTTMean
	dev1 := c.st.lastRTTDev

	// 2. Failed (non-backpressure) sample should NOT update EWMA
	// We use a generic error which Record treats as failure but not backpressure
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 400*time.Millisecond, errors.New("generic error"))

	assert.InDelta(t, mean1, c.st.lastRTTMean, 1e-9)
	assert.InDelta(t, dev1, c.st.lastRTTDev, 1e-9)

	// 3. Successful sample should update EWMA
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 200*time.Millisecond, nil)

	assert.Greater(t, math.Abs(mean1-c.st.lastRTTMean), 1e-9)

	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_AdditiveIncreaseAndCreditReset(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 1
	cfg.MaxConcurrency = 5
	cfg.EwmaAlpha = 0.5
	cfg.DeviationScale = 2.0

	c := newTestController(t, cfg)
	// Short test period
	c.mu.Lock()
	c.st.periodDur = 10 * time.Millisecond
	c.mu.Unlock()

	// Prime EWMA with a stable RTT
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 100*time.Millisecond, nil)

	// Force a control step to set prevRTTMean
	forceControlStep(c)
	assert.Equal(t, 1, c.CurrentLimit())
	assert.Positive(t, c.st.prevRTTMean)

	// Build up enough credits to trigger an increase.
	// For limit 1, requiredCredits is 1.
	reqCredits := requiredCredits(c.CurrentLimit())
	for range reqCredits {
		require.NoError(t, c.Acquire(context.Background()))
		// Release immediately to allow loop to continue if reqCredits > limit (though here limit=1)
		// We need to release so the next Acquire doesn't block if limit is small.
		// However, credits are accrued on Acquire.
		c.Record(context.Background(), 100*time.Millisecond, nil)
	}

	// Wait past the period and poke again to trigger controlStep
	forceControlStep(c)

	assert.Equal(t, 2, c.CurrentLimit(), "limit should increase additively by 1")
	assert.Equal(t, 0, c.st.credits, "credits reset after increase")

	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_MultiplicativeDecreaseOnBackpressure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 1 // Set floor to 1 so we can start higher and drop
	cfg.MaxConcurrency = 10
	cfg.DecreaseRatio = 0.5

	c := newTestController(t, cfg)

	// Manually force limit to 4 to test decrease logic
	c.mu.Lock()
	c.st.limit = 4
	c.pool.Grow(3) // Grow pool from 1 to 4
	c.st.periodDur = 10 * time.Millisecond
	c.mu.Unlock()

	require.Equal(t, 4, c.CurrentLimit())

	// Cause an explicit backpressure signal.
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 50*time.Millisecond, context.DeadlineExceeded)

	// Wait and trigger the control step
	forceControlStep(c)

	// newLimit = floor(4 * 0.5) = 2
	assert.Equal(t, 2, c.CurrentLimit())
	assert.Equal(t, 0, c.st.credits)

	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_DecreaseOnRTTSpike(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 1 // Set floor to 1
	cfg.DeviationScale = 1.0
	cfg.EwmaAlpha = 0.5

	c := newTestController(t, cfg)

	// Manually set limit to 4
	c.mu.Lock()
	c.st.limit = 4
	c.pool.Grow(3) // from 1 to 4
	c.st.periodDur = 10 * time.Millisecond
	c.mu.Unlock()

	// Establish baseline RTT ~100ms
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 100*time.Millisecond, nil)

	// Force a control step to set prevRTTMean
	forceControlStep(c)
	assert.Equal(t, 4, c.CurrentLimit())
	assert.Positive(t, c.st.prevRTTMean)

	// Trigger a spike sample (600ms > 100ms + dev)
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 600*time.Millisecond, nil)

	// After control step, limit should drop by DecreaseRatio (default 0.9)
	// floor(4 * 0.9) = 3.6 -> 3
	forceControlStep(c)

	assert.Equal(t, 3, c.CurrentLimit())
	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_MinFloorAndMaxCap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 1
	cfg.MaxConcurrency = 2
	cfg.DecreaseRatio = 0.1

	c := newTestController(t, cfg)

	// Set short period
	c.mu.Lock()
	c.st.periodDur = 10 * time.Millisecond
	c.mu.Unlock()

	// Establish baseline first! Increase logic requires initialized RTT.
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 10*time.Millisecond, nil)
	forceControlStep(c)

	// Try to decrease at the floor; should stay at 1
	require.NoError(t, c.Acquire(context.Background()))
	c.Record(context.Background(), 50*time.Millisecond, context.DeadlineExceeded) // pressure
	forceControlStep(c)
	assert.Equal(t, 1, c.CurrentLimit(), "should not go below 1")

	// Build credits to increase to the cap = 2
	reqCredits := requiredCredits(c.CurrentLimit())
	for range reqCredits {
		require.NoError(t, c.Acquire(context.Background()))
		c.Record(context.Background(), 50*time.Millisecond, nil)
	}
	forceControlStep(c)
	assert.Equal(t, 2, c.CurrentLimit(), "should increase to cap")

	// Further credit should not exceed MaxConcurrency
	reqCredits = requiredCredits(c.CurrentLimit())
	for range reqCredits {
		require.NoError(t, c.Acquire(context.Background()))
		c.Record(context.Background(), 50*time.Millisecond, nil)
	}
	forceControlStep(c)
	assert.Equal(t, 2, c.CurrentLimit(), "should not exceed cap")

	require.NoError(t, c.Shutdown(context.Background()))
}

func TestController_RecordReleases(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MinConcurrency = 1

	c := newTestController(t, cfg)

	// Acquire via the pool to bump inUse
	require.NoError(t, c.Acquire(context.Background()))
	assert.Equal(t, 1, c.PermitsInUse())

	// Record (replacing ReleaseWithSample) must release the pool permit
	c.Record(context.Background(), 10*time.Millisecond, nil)

	assert.Equal(t, 0, c.PermitsInUse())
	require.NoError(t, c.Shutdown(context.Background()))
}

func Test_requiredCredits(t *testing.T) {
	assert.Equal(t, 1, requiredCredits(0))
	assert.Equal(t, 1, requiredCredits(1))
	assert.Equal(t, 2, requiredCredits(2))
	assert.Equal(t, 2, requiredCredits(7))
	assert.Equal(t, 3, requiredCredits(8))
	assert.Equal(t, 3, requiredCredits(31))
	assert.Equal(t, 4, requiredCredits(32))
	assert.Equal(t, 4, requiredCredits(100))
}

func Test_contextOrBG(t *testing.T) {
	var nilCtx context.Context // avoid SA1012 by not passing a literal nil
	bg := contextOrBG(nilCtx)
	require.NotNil(t, bg)

	ctx := context.Background()
	require.Equal(t, ctx, contextOrBG(ctx))
}
