// Package defaults holds the service's own defaults document: the values a
// deployment does not have to state, merged under its configuration file by the
// engine before any byte of it reaches a plugin. It is a leaf — one embedded
// file and nothing else.
package defaults

import _ "embed"

// Defaults is defaults.yaml as it is embedded: YAML, comments included — what
// `config defaults` prints and `internal/cli` converts for the engine.
//
//go:embed defaults.yaml
var Defaults []byte
