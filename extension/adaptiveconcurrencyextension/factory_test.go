// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	assert.NotNil(t, f)
	assert.Equal(t, metadata.Type, f.Type())
	assert.Equal(t, metadata.ExtensionStability, f.Stability())
}

func TestCreateDefaultConfig(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	require.NoError(t, componenttest.CheckConfigStruct(cfg))

	// Check specific default values
	c, ok := cfg.(*Config)
	require.True(t, ok)
	assert.Equal(t, 2, c.MinConcurrency)
	assert.Equal(t, 200, c.MaxConcurrency)
}

func TestCreateExtension(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()

	// Use test settings
	ctx := context.Background()
	set := extensiontest.NewNopSettings(metadata.Type)
	ext, err := f.Create(ctx, set, cfg)
	require.NoError(t, err)
	assert.NotNil(t, ext)
	assert.Implements(t, (*extension.Extension)(nil), ext)
}
