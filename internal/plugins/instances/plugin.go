package instances

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every plugin that registers one imports it directly.
var Spec = loader.Spec{
	ID:      "instances",
	Summary: "the typed factory registry: one registration per plugin type, its descriptor and its pure schema hooks",
}
