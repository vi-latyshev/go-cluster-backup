package configuration

import "example.test/app/internal/loader"

// Spec is the compiled plugin identity of the configuration library.
var Spec = loader.Spec{ID: "configuration", Summary: "the unified configuration document"}

// Apply registers the distribution's bundle table, so a build always resolves
// its bundles — the library is not only metadata.
func Apply(lr *loader.Registry) error { return nil }
