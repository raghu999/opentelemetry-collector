// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension // import "go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension"

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/controller"
)

// Config exposes the configuration parameters for the concurrency limiter extension.
type Config struct {
	Enabled        bool             `mapstructure:"enabled"`
	MinConcurrency int              `mapstructure:"min_concurrency"`
	MaxConcurrency int              `mapstructure:"max_concurrency"`
	DecreaseRatio  float64          `mapstructure:"decrease_ratio"`
	EwmaAlpha      float64          `mapstructure:"ewma_alpha"`
	DeviationScale float64          `mapstructure:"deviation_scale"`
	Middleware     MiddlewareConfig `mapstructure:"middleware"`
}

type MiddlewareConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	return &Config{
		Enabled:        true,
		MinConcurrency: 2,
		MaxConcurrency: 200,
		DecreaseRatio:  0.7,
		EwmaAlpha:      0.05,
		DeviationScale: 2.0,
		Middleware: MiddlewareConfig{
			Enabled: false,
		},
	}
}

func (c *Config) Validate() error {
	if c.MinConcurrency <= 0 {
		return fmt.Errorf("min_concurrency must be greater than 0, got %d", c.MinConcurrency)
	}
	if c.MaxConcurrency <= 0 {
		return fmt.Errorf("max_concurrency must be greater than 0, got %d", c.MaxConcurrency)
	}
	if c.MinConcurrency > c.MaxConcurrency {
		return fmt.Errorf("min_concurrency (%d) cannot be greater than max_concurrency (%d)", c.MinConcurrency, c.MaxConcurrency)
	}
	if c.DecreaseRatio <= 0 || c.DecreaseRatio >= 1 {
		return fmt.Errorf("decrease_ratio must be in the range (0, 1), got %f", c.DecreaseRatio)
	}
	if c.EwmaAlpha <= 0 || c.EwmaAlpha >= 1 {
		return fmt.Errorf("ewma_alpha must be in the range (0, 1), got %f", c.EwmaAlpha)
	}
	if c.DeviationScale < 0 {
		return fmt.Errorf("deviation_scale must be greater than or equal to 0, got %f", c.DeviationScale)
	}
	return nil
}

func (c *Config) toControllerConfig() controller.Config {
	return controller.Config{
		Enabled:        c.Enabled,
		MinConcurrency: c.MinConcurrency,
		MaxConcurrency: c.MaxConcurrency,
		DecreaseRatio:  c.DecreaseRatio,
		EwmaAlpha:      c.EwmaAlpha,
		DeviationScale: c.DeviationScale,
	}
}
