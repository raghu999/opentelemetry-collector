// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/confmap/xconfmap"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name: "default_valid",
			cfg:  createDefaultConfig().(*Config),
		},
		{
			name: "valid_custom",
			cfg: &Config{
				Enabled:        true,
				MinConcurrency: 5,
				MaxConcurrency: 100,
				DecreaseRatio:  0.5,
				EwmaAlpha:      0.5,
				DeviationScale: 1.0,
			},
		},
		{
			name: "min_concurrency_zero",
			cfg: &Config{
				MinConcurrency: 0,
				MaxConcurrency: 10,
				DecreaseRatio:  0.9, EwmaAlpha: 0.5, DeviationScale: 1,
			},
			wantErr: "min_concurrency must be greater than 0",
		},
		{
			name: "max_concurrency_zero",
			cfg: &Config{
				MinConcurrency: 1,
				MaxConcurrency: 0,
				DecreaseRatio:  0.9, EwmaAlpha: 0.5, DeviationScale: 1,
			},
			wantErr: "max_concurrency must be greater than 0",
		},
		{
			name: "min_greater_than_max",
			cfg: &Config{
				MinConcurrency: 20,
				MaxConcurrency: 10,
				DecreaseRatio:  0.9, EwmaAlpha: 0.5, DeviationScale: 1,
			},
			wantErr: "min_concurrency (20) cannot be greater than max_concurrency (10)",
		},
		{
			name: "decrease_ratio_too_low",
			cfg: &Config{
				MinConcurrency: 1, MaxConcurrency: 10, EwmaAlpha: 0.5, DeviationScale: 1,
				DecreaseRatio: 0.0,
			},
			wantErr: "decrease_ratio must be in the range (0, 1)",
		},
		{
			name: "decrease_ratio_too_high",
			cfg: &Config{
				MinConcurrency: 1, MaxConcurrency: 10, EwmaAlpha: 0.5, DeviationScale: 1,
				DecreaseRatio: 1.0,
			},
			wantErr: "decrease_ratio must be in the range (0, 1)",
		},
		{
			name: "ewma_alpha_too_low",
			cfg: &Config{
				MinConcurrency: 1, MaxConcurrency: 10, DecreaseRatio: 0.9, DeviationScale: 1,
				EwmaAlpha: 0.0,
			},
			wantErr: "ewma_alpha must be in the range (0, 1)",
		},
		{
			name: "ewma_alpha_too_high",
			cfg: &Config{
				MinConcurrency: 1, MaxConcurrency: 10, DecreaseRatio: 0.9, DeviationScale: 1,
				EwmaAlpha: 1.0,
			},
			wantErr: "ewma_alpha must be in the range (0, 1)",
		},
		{
			name: "deviation_scale_negative",
			cfg: &Config{
				MinConcurrency: 1, MaxConcurrency: 10, DecreaseRatio: 0.9, EwmaAlpha: 0.5,
				DeviationScale: -0.1,
			},
			wantErr: "deviation_scale must be greater than or equal to 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := xconfmap.Validate(tt.cfg)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
