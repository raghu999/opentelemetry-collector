// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptiveconcurrencyextension // import "go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/adaptiveconcurrencyextension/internal/metadata"
)

// NewFactory creates a factory for the concurrency limiter extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

// createExtension creates the extension based on the given config.
func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	// Cast the component.Config to our specific Config struct.
	// The OpenTelemetry collector framework guarantees that cfg is the correct type
	// created by createDefaultConfig.
	extensionConfig := cfg.(*Config)

	return newAdaptiveConcurrency(extensionConfig, set)
}
