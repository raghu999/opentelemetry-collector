// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package controller // import "go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/controller"

import "time"

// Config holds the operational parameters for the logic.
type Config struct {
	Enabled        bool
	MinConcurrency int
	MaxConcurrency int
	DecreaseRatio  float64
	EwmaAlpha      float64
	DeviationScale float64
}

func DefaultConfig() Config {
	return Config{
		Enabled:        false,
		MinConcurrency: 2,
		MaxConcurrency: 200,
		DecreaseRatio:  0.9,
		EwmaAlpha:      0.4,
		DeviationScale: 2.5,
	}
}

const (
	minPeriod = 50 * time.Millisecond
	maxPeriod = 2 * time.Second
)
