// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package controller // import "go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/controller"

import (
	"context"
	"errors"
	"sync"
)

// TokenPool is a fair gate for request concurrency.
// It manages the active number of permits allowed in-flight.
type TokenPool struct {
	mu    sync.Mutex
	cond  *sync.Cond
	cap   int
	inUse int
	dead  bool
}

func newTokenPool(initial int) *TokenPool {
	if initial < 1 {
		initial = 1
	}
	p := &TokenPool{cap: initial}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Close wakes up all waiting goroutines and prevents further acquisitions.
func (p *TokenPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dead = true
	p.cond.Broadcast()
}

// Acquire obtains a token, blocking if the limit is reached.
func (p *TokenPool) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dead {
		return errors.New("token pool closed")
	}

	// Performance Optimization: Fast Path.
	// If a permit is available immediately, increment and return to skip allocations.
	if p.inUse < p.cap {
		p.inUse++
		return nil
	}

	// Slow path: register context cancellation and wait.
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	})
	defer stop()

	for !p.dead && p.inUse >= p.cap {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.cond.Wait()
	}

	if p.dead {
		return errors.New("token pool closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	p.inUse++
	return nil
}

// Release returns a token to the pool, signaling one waiter to proceed.
func (p *TokenPool) Release() {
	p.mu.Lock()
	if p.inUse > 0 {
		p.inUse--
		p.cond.Signal()
	}
	p.mu.Unlock()
}

// Grow increases the total permit capacity of the pool.
func (p *TokenPool) Grow(n int) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	p.cap += n
	p.mu.Unlock()
	p.cond.Broadcast()
}

// Shrink decreases the total permit capacity of the pool (minimum 1).
func (p *TokenPool) Shrink(n int) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	p.cap -= n
	if p.cap < 1 {
		p.cap = 1
	}
	p.mu.Unlock()
}

// Resize updates the capacity to a specific value.
func (p *TokenPool) Resize(newCap int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newCap < 1 {
		newCap = 1
	}

	oldCap := p.cap
	p.cap = newCap

	// Only broadcast if the capacity grew, allowing waiters to proceed.
	if newCap > oldCap {
		p.cond.Broadcast()
	}
}
