//go:build linux

package files

import "example.test/app/internal/plugins/source/postgres"

// platformProbe proves a build-tagged production import reaches the graph.
func platformProbe() string { return postgres.Probe }
